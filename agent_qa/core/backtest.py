"""One ``run()`` that works under both ``hiveq.flow`` providers.

``hiveq-flow.run_backtest`` executes in-process and takes ``config``/
``engine_config``; ``hiveq-sdk.run_backtest`` submits to the platform and takes
``**kwargs`` plus submit-side options (``silent``, ``task_name``, …). The first
six parameters are identical in both, so this module passes those straight
through and routes the rest per engine.

It also enforces the two run-discipline rules the suite must never break:

* always block with ``run.wait(progress=False)`` — never the live progress bar
  (R11); a scripted run that renders a progress bar corrupts the RESULT stream.
* never set ``hiveq_log_level`` in a committed test. Evidence goes through
  ``Probe``/``add_event_log``, which works at the default ``WARNING``.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import hiveq.flow as hf

from agent_qa.core.profiles import ENGINE_INPROC, apply_profile, detect_engine

#: Hard ceiling on a single test's run. The runner also imposes a subprocess
#: timeout; this one produces a clean FAIL instead of an opaque ERROR.
DEFAULT_TIMEOUT_S = float(os.environ.get("AGENT_QA_RUN_TIMEOUT", "900"))


class RunTimeout(RuntimeError):
    """The platform never reached a terminal state within the budget."""


def run(
    strategy_configs: List[Any],
    *,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    data_configs: Optional[List[dict]] = None,
    backtest_config: Optional[Any] = None,
    engine_config: Optional[Dict[str, Any]] = None,
    wait: bool = True,
    timeout: Optional[float] = None,
):
    """Submit/execute a backtest and (by default) block until it is terminal.

    ``engine_config`` is the engine tuning dict (the thing the SDK spreads as
    ``**kwargs`` and the fat package takes as ``config=``). Pass engine settings
    here and this function puts them where the installed package expects them.
    """
    apply_profile()
    if engine_config and "hiveq_log_level" in engine_config:
        raise ValueError(
            "hiveq_log_level must not appear in a committed test — record "
            "evidence with Probe/add_event_log instead (AGENTS.md non-negotiable)"
        )

    common: Dict[str, Any] = {
        "strategy_configs": strategy_configs,
        "symbols": symbols,
        "start_date": start_date,
        "end_date": end_date,
        "data_configs": data_configs,
        "backtest_config": backtest_config,
    }
    common = {k: v for k, v in common.items() if v is not None}

    if detect_engine() == ENGINE_INPROC:
        if engine_config:
            common["config"] = dict(engine_config)
        result = hf.run_backtest(**common)
    else:
        common["silent"] = True
        result = hf.run_backtest(**common, **(engine_config or {}))

    if wait:
        wait_for(result, timeout=timeout)
    return result


def wait_for(run_handle, timeout: Optional[float] = None):
    """Block until terminal, quietly. Raises :class:`RunTimeout` on overrun.

    In-process runs are already finished by the time ``run_backtest`` returns,
    so ``wait`` is a no-op there — but calling it unconditionally keeps test
    bodies engine-agnostic.

    ``wait()`` alone is NOT trusted. It is documented to block until terminal,
    but it can return with the run still ``PENDING`` — its poll loop gives up
    after a bounded period of unreachable/failing polls
    (``_WAIT_ERROR_GIVEUP``), and a throttled status endpoint looks exactly like
    that. Observed on vm.hiveq.ai 2026-07-28: two submits returned from
    ``wait()`` while still queued, and every downstream assertion then measured a
    run that had never started — reporting "0 bars" as though it were a data
    problem. So terminality is re-verified here, explicitly.
    """
    budget = DEFAULT_TIMEOUT_S if timeout is None else timeout
    started = time.time()
    try:
        run_handle.wait(progress=False, timeout=budget)
    except TypeError:
        # Older/local Run.wait may not accept timeout.
        run_handle.wait(progress=False)
    except Exception as exc:  # noqa: BLE001 - surface as a test-visible failure
        raise RunTimeout(f"run did not complete: {exc}") from exc

    # Re-verify, and keep polling with the remaining budget if wait() bailed out.
    while True:
        raw = _raw_status(run_handle)
        if raw is None:
            return run_handle  # local run: no REST status to confirm
        if raw.get("is_final") or _is_terminal_status(raw):
            return run_handle
        if time.time() - started >= budget:
            raise RunTimeout(
                f"run {getattr(run_handle, 'run_id', '?')} still "
                f"{raw.get('status')!r} (is_final={raw.get('is_final')}) after "
                f"{budget:.0f}s — it never left the queue, so nothing about it "
                f"can be asserted"
            )
        time.sleep(min(5.0, max(1.0, budget - (time.time() - started))))


def _raw_status(run_handle) -> Optional[Dict[str, Any]]:
    """The status dict, or None when there is no REST status (local runs)."""
    if getattr(run_handle, "is_local", False):
        return None
    try:
        raw = run_handle.status()
    except Exception:  # noqa: BLE001
        return {}
    return raw if isinstance(raw, dict) else {}


def _is_terminal_status(raw: Dict[str, Any]) -> bool:
    return str(raw.get("status") or "").lower() in (
        "completed", "done", "failed", "terminated", "stopped", "error", "success",
        "finished",
    )


def status_of(run_handle) -> str:
    """Best-effort terminal status string, lowercased; '' when unavailable."""
    try:
        raw = run_handle.status()
    except Exception:  # noqa: BLE001
        return ""
    if isinstance(raw, dict):
        for key in ("status", "state", "run_status"):
            if raw.get(key):
                return str(raw[key]).lower()
        return ""
    return str(raw or "").lower()


def completed_ok(run_handle) -> bool:
    """True when the platform says the run finished without failing.

    A local run has no REST status; ``hiveq-flow`` returning at all means it
    completed, so treat an empty status as success in-process only.
    """
    st = status_of(run_handle)
    if not st:
        return detect_engine() == ENGINE_INPROC
    return st in ("completed", "done", "success", "finished")


def crash_lines(run_handle, needle: str = "STRATEGY_CALLBACK_ERROR") -> List[str]:
    """Executor stdout lines matching ``needle``.

    ``event_logs()`` never carries callback crashes — only ``logs()`` does — so
    every test that asserts "the strategy ran cleanly" should check this.
    Returns [] in-process, where there is no remote log to fetch.
    """
    try:
        lines = run_handle.logs() or []
    except Exception:  # noqa: BLE001
        return []
    return [ln for ln in lines if needle in ln]


def historical(dataset: str, schema, **extra) -> Dict[str, Any]:
    """Build a ``type='hiveq_historical'`` data_configs entry."""
    cfg: Dict[str, Any] = {"type": "hiveq_historical", "dataset": dataset}
    cfg["schema"] = [schema] if isinstance(schema, str) else list(schema)
    cfg.update(extra)
    return cfg


def csv_source(data_id: str, path: str, data_type: str = "custom", **extra) -> Dict[str, Any]:
    """Build a ``type='csv'`` data_configs entry."""
    cfg: Dict[str, Any] = {
        "type": "csv",
        "data_type": data_type,
        "id": data_id,
        "path": path,
    }
    cfg.update(extra)
    return cfg


def stage_csv(local_path: str, base: Optional[str] = None) -> str:
    """Upload a CSV to the persistent-data store; return the path to reference.

    Strategies run on the platform, so a CSV in ``data_configs`` must be uploaded
    to the caller's persistent-data store **before** submitting, and referenced by
    the **store-relative path it was uploaded as** (§9.2 / the ``hiveq-data``
    section). The spec is blunt about the failure mode this avoids: *"Do not use
    absolute or Path(__file__)-based paths in data_configs — they resolve on your
    local machine but not on the platform."* That is exactly why ``l5`` passed
    in-process with 5 rows and delivered 0 remotely.

    The source bundler will not rescue this either: ``.csv`` is deliberately
    outside its config extensions, so data files are never swept into the payload.

    ``base`` anchors the stored path, matching ``hiveq-data -u`` semantics —
    uploading ``<base>/agent_qa/fixtures/x.csv`` with ``base=<sdk root>`` stores
    and returns ``agent_qa/fixtures/x.csv``. Defaults to the repo root above
    ``agent_qa/`` so fixtures stay namespaced in the store rather than colliding
    at its root.

    Returns the local absolute path under ``inproc`` (nothing to upload), or the
    store-relative path under ``remote``. Uploads are incremental (server-side
    MD5 compare), so calling this every run is one listing request when unchanged.
    """
    local_path = os.path.abspath(local_path)
    if detect_engine() == ENGINE_INPROC:
        return local_path

    apply_profile()
    from hiveq.flow.uploads import upload_files  # noqa: PLC0415 - SDK-only

    if base is None:
        # .../hiveq-sdk/agent_qa/core/backtest.py -> .../hiveq-sdk
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    base = os.path.abspath(base)

    rel = os.path.relpath(local_path, base).replace(os.sep, "/")
    upload_files(local_path, base=base, progress=False)
    return rel
