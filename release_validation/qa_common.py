"""Helpers for black-box validation of an installed hiveq-sdk wheel."""

from __future__ import annotations

import json
import time
from typing import Any

CHECKPOINT_TYPE = "SDK_RELEASE_CHECKPOINT"


def emit_checkpoint(ctx: Any, name: str, state: dict[str, Any]) -> None:
    ctx.add_event_log(name, sub_event_type=CHECKPOINT_TYPE, state_variable=state)


def wait_for_final(run: Any, timeout: float = 900.0, poll_interval: float = 2.0) -> dict[str, Any]:
    """Wait for the run resource itself, independent of submission-task state."""
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        last = run.status() or {}
        status = str(last.get("status", "")).lower()
        if status in {"failed", "terminated", "error"}:
            raise AssertionError(f"run failed: {last}")
        # STOPPED is a strategy/executor lifecycle state, not necessarily a
        # terminal run state.  The platform may transition STOPPED -> PENDING
        # while it publishes reports (including TCA), so returning there races
        # run.report() and yields an empty/partial report.
        if last.get("is_final") or status in {"completed", "done"}:
            return last
        time.sleep(poll_interval)
    raise TimeoutError(f"run did not finish within {timeout}s; last={last}")


def checkpoint(run: Any, name: str, timeout: float = 30.0) -> dict[str, Any]:
    """Read a checkpoint, allowing for asynchronous event-log persistence."""
    deadline = time.monotonic() + timeout
    logs = None
    while time.monotonic() < deadline:
        logs = run.event_logs()
        if logs is not None and not getattr(logs, "empty", True):
            rows = logs[
                (logs["sub_event_type"] == CHECKPOINT_TYPE)
                & (logs["message"] == name)
            ]
            if not rows.empty:
                value = rows.iloc[-1]["state_variables"]
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
    wait_for_final(run)
    return checkpoint(run, name)


def finish(name: str, checks: dict[str, bool], extra: str = "", gap: bool = False) -> None:
    failed = [key for key, ok in checks.items() if not ok]
    status = ("GAP" if gap else "PASS") if not failed else "FAIL"
    detail = "; ".join(f"{key}={'ok' if ok else 'FAIL'}" for key, ok in checks.items())
    if extra:
        detail = f"{detail}; {extra}"
    print(f"RESULT: {status} {name} — {detail}")
    if failed:
        raise AssertionError(f"{name}: failed checks: {', '.join(failed)}")
