"""Helpers for black-box validation of an installed hiveq-sdk wheel."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

CHECKPOINT_TYPE = "SDK_RELEASE_CHECKPOINT"
SENSITIVE_COLUMNS = {
    "user_id", "user_name", "org_id", "trader_id", "account_id",
}

# Local (in-process) runs keep no order history in Python memory. With
# ``BacktestConfig.export_orders_csv=True`` the engine streams every order event
# to ``~/.tmp/[<run_id>_]<Strategy>_order_events.csv`` and the analyzer reads it
# back into ``run.orders()``/``run.fills()``.  Deployed runs publish orders from
# C++ straight to the platform and this file never exists.  Reading the raw
# event stream is the only way to see *intermediate* states — a partial fill, a
# cancel-reject that raced a fill — because the analyzer collapses each order to
# one terminal row.
LOCAL_ORDER_EVENT_DIR = Path.home() / ".tmp"


def export_run_artifacts(
    run: Any,
    *,
    root: str | Path | None = None,
    validation: dict[str, Any] | None = None,
) -> Path:
    """Persist the full review surface for a completed designated run.

    Each run gets its own directory keyed by run id. Identity columns are
    removed, while order/position identifiers needed for reconciliation remain.
    Empty or unavailable tables are still written as empty CSVs so absence is
    explicit during post-processing.
    """
    run_id = str(getattr(run, "run_id", None) or "unknown-run")
    base = Path(root) if root is not None else Path(__file__).parent / "run_artifacts"
    out = base / run_id
    out.mkdir(parents=True, exist_ok=True)

    readers = {
        # orders_frame, not run.orders: an in-memory run leaves its order history
        # only in the streamed capture file, and an empty orders.csv would then
        # misreport a run that really did trade.
        "orders": lambda: orders_frame(run),
        "trades": run.trades,
        "positions": run.positions,
        "event_logs": lambda: _event_logs(run),
    }
    table_rows: dict[str, int] = {}
    for name, reader in readers.items():
        value = _read_with_retry(reader, f"run.{name}()")
        if value is None:
            (out / f"{name}.csv").write_text("", encoding="utf-8")
            table_rows[name] = 0
            continue
        drop = [column for column in value.columns if str(column) in SENSITIVE_COLUMNS]
        cleaned = value.drop(columns=drop) if drop else value
        cleaned.to_csv(out / f"{name}.csv", index=False)
        table_rows[name] = len(cleaned)

    status = _read_with_retry(run.status, "run.status()")
    metadata = {
        "run_id": run_id,
        "task_id": getattr(run, "task_id", None),
        "status": status,
        "table_rows": table_rows,
        "validation": validation or {},
    }
    (out / "validation.json").write_text(
        json.dumps(metadata, indent=2, default=str), encoding="utf-8"
    )
    return out


def order_events(run: Any) -> Any:
    """Raw order-event stream for a local run, newest file per strategy.

    ``run.orders()`` collapses each order to one terminal row, so an
    intermediate state — ``ORDER_PARTIALLY_FILLED``, a cancel-reject that raced
    a fill, a stop that triggered before it filled — is invisible there. This
    reads the streamed capture file instead, which holds every event.

    Returns an empty frame when the run is remote or capture was off, so a
    caller can treat "no intermediate evidence available" uniformly.
    """
    import pandas as pd

    run_id = str(getattr(run, "run_id", "") or "")
    if not run_id or not LOCAL_ORDER_EVENT_DIR.is_dir():
        return pd.DataFrame()
    matches = sorted(LOCAL_ORDER_EVENT_DIR.glob(f"{run_id}_*_order_events.csv"))
    frames = []
    for path in matches:
        try:
            frame = pd.read_csv(path)
        except (OSError, ValueError):
            continue
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def orders_frame(run: Any) -> Any:
    """Orders for a run, preferring the public reader and falling back locally.

    An in-memory run only populates ``run.orders()`` when the backtest enabled
    ``export_orders_csv``; when it did not, the streamed event capture is the
    only order evidence that exists. Returning it here keeps a test's order
    assertions honest instead of silently passing on an empty table.
    """
    import pandas as pd

    public = _read_with_retry(run.orders, "run.orders()")
    if public is not None and not getattr(public, "empty", True):
        return public
    events = order_events(run)
    if events.empty:
        return public if public is not None else pd.DataFrame()
    # Collapse to one row per order id, keeping the last event seen — the same
    # shape the analyzer produces, so callers can count orders either way.
    return events.drop_duplicates(subset=["order_id"], keep="last")


def emit_checkpoint(ctx: Any, name: str, state: dict[str, Any]) -> None:
    ctx.add_event_log(name, sub_event_type=CHECKPOINT_TYPE, state_variable=state)


def _read_with_retry(read: Any, what: str, attempts: int = 6, delay: float = 3.0) -> Any:
    """Call a read-only platform endpoint, retrying transient server faults.

    A 5xx on ``/status`` or ``/event-logs`` is a platform read fault, not a
    strategy result — retrying it keeps an unrelated gateway hiccup from being
    reported as a validation failure.  Every retry is printed so a flaky
    environment stays visible rather than silently absorbed.  Client errors
    (4xx) and non-HTTP exceptions propagate unchanged.
    """
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return read()
        except Exception as exc:  # noqa: BLE001 - re-raised below unless transient
            status = getattr(getattr(exc, "response", None), "status_code", None)
            transient = (status is not None and 500 <= int(status) < 600) or (
                status is None
                and exc.__class__.__name__ in {
                    "ConnectionError", "Timeout", "ReadTimeout", "ChunkedEncodingError",
                }
            )
            if not transient:
                raise
            last_exc = exc
            print(
                f"[RETRY] {what}: transient platform fault "
                f"({status or exc.__class__.__name__}); attempt {attempt}/{attempts}",
                flush=True,
            )
            time.sleep(delay)
    raise last_exc


def wait_for_final(run: Any, timeout: float = 900.0, poll_interval: float = 2.0) -> dict[str, Any]:
    """Wait for the run resource itself, independent of submission-task state."""
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = _read_with_retry(run.status, "run.status()") or {}
        status = str(last.get("status", "")).lower()
        if status in {"failed", "terminated", "error"}:
            log_tail = None
            task_id = getattr(run, "task_id", None)
            if task_id:
                try:
                    from hiveq.flow.jobs import get_logs
                    log_tail = get_logs(task_id=task_id, limit=200)
                except Exception as exc:
                    log_tail = f"unavailable: {exc}"
            raise AssertionError(f"run failed: {last}; task_logs={log_tail}")
        # STOPPED is a strategy/executor lifecycle state, not necessarily a
        # terminal run state.  The platform may transition STOPPED -> PENDING
        # while it publishes reports (including TCA), so returning there races
        # run.report() and yields an empty/partial report.
        if last.get("is_final") or status in {"completed", "done"}:
            return last
        time.sleep(poll_interval)
    raise TimeoutError(f"run did not finish within {timeout}s; last={last}")


def _event_logs(run: Any) -> Any:
    """Event logs for a run, platform or local.

    ``Run.event_logs()`` reads the platform's persisted logs and returns an
    empty frame when ``is_local`` — an in-process engine run keeps them in the
    app singleton instead, reachable via ``hf.event_logs()``.
    """
    logs = _read_with_retry(run.event_logs, "run.event_logs()")
    if logs is not None and not getattr(logs, "empty", True):
        return logs
    if getattr(run, "is_local", False):
        import hiveq.flow as hf

        return hf.event_logs()
    return logs


def checkpoint(run: Any, name: str, timeout: float = 30.0) -> dict[str, Any]:
    """Read a checkpoint, allowing for asynchronous event-log persistence."""
    deadline = time.monotonic() + timeout
    logs = None
    while time.monotonic() < deadline:
        logs = _event_logs(run)
        if logs is not None and not getattr(logs, "empty", True):
            rows = logs[
                (logs["sub_event_type"] == CHECKPOINT_TYPE)
                & (logs["message"] == name)
            ]
            if not rows.empty:
                value = rows.iloc[-1]["state_variables"]
                if isinstance(value, (bytes, bytearray)):
                    # local runs serialize state_variables with orjson -> bytes
                    value = value.decode("utf-8")
                if isinstance(value, str):
                    value = json.loads(value or "{}")
                if not isinstance(value, dict):
                    raise AssertionError(f"{name}: checkpoint is not an object: {value!r}")
                return value
        time.sleep(1.0)
    available = []
    if logs is not None and not getattr(logs, "empty", True):
        available = logs[["sub_event_type", "message"]].tail(20).to_dict("records")
    raise AssertionError(f"{name}: checkpoint missing after persistence wait; tail={available}")


def completed_checkpoint(run: Any, name: str) -> dict[str, Any]:
    try:
        wait_for_final(run)
    except AssertionError as exc:
        try:
            state = checkpoint(run, name, timeout=5.0)
        except Exception:
            raise exc
        raise AssertionError(f"{exc}; last_checkpoint={state}") from exc
    state = checkpoint(run, name)
    # Every release-validation run must leave the complete public review
    # surface behind, not only its summarized checkpoint.  This makes even a
    # passing data/callback test independently reviewable for orders, trades,
    # positions, and the event values used by its assertions.
    export_run_artifacts(run, validation={"checkpoint": name, "state": state})
    return state


def evidence_checks(run: Any, *, orders: int = 1, trades: int = 0,
                    event_logs: int = 1) -> dict[str, bool]:
    """Public-surface evidence gate for a validation, as named checks.

    The suite's PASS contract is that a case exercised the real execution and
    result path, not only that a callback fired. Folding that into the same
    ``checks`` dict a test already reports keeps the requirement visible in the
    RESULT line rather than buried in a helper's side effects.

    ``trades=0`` is for the cases whose contract is a rejection, a cancellation,
    or a data gap and which therefore cannot close a round trip on the
    instrument under test — they must still show orders and event logs.
    """
    def rows(value) -> int:
        # `value or []` is not usable here: a DataFrame has no truth value.
        return 0 if value is None else len(value)

    order_rows = rows(orders_frame(run))
    trade_rows = rows(_read_with_retry(run.trades, "run.trades()"))
    log_rows = rows(_event_logs(run))
    result = {}
    if orders:
        result[f"evidence_orders_ge_{orders}"] = order_rows >= orders
    if trades:
        result[f"evidence_trades_ge_{trades}"] = trade_rows >= trades
    if event_logs:
        result[f"evidence_event_logs_ge_{event_logs}"] = log_rows >= event_logs
    return result


def finish(name: str, checks: dict[str, bool], extra: str = "", gap: bool = False) -> None:
    failed = [key for key, ok in checks.items() if not ok]
    status = ("GAP" if gap else "PASS") if not failed else "FAIL"
    detail = "; ".join(f"{key}={'ok' if ok else 'FAIL'}" for key, ok in checks.items())
    if extra:
        detail = f"{detail}; {extra}"
    print(f"RESULT: {status} {name} — {detail}")
    if failed:
        raise AssertionError(f"{name}: failed checks: {', '.join(failed)}")
