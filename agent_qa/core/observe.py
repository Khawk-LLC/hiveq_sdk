"""Polling and observation helpers for asynchronous (livesim) assertions.

A backtest is synchronous — submit, wait, read. A livesim is not: a deployment
becomes healthy some seconds after the REST call returns, a param change takes
effect on the next callback, and a container stops on its own schedule. Tests
must therefore poll with a deadline rather than sleep-and-hope, and they must
distinguish "not yet" from "never".

Everything here returns rather than raises, so a timeout becomes a failed check
with readable detail instead of a stack trace.
"""

from __future__ import annotations

import time
from datetime import datetime, time as dtime
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

DEFAULT_TIMEOUT_S = 180.0
DEFAULT_INTERVAL_S = 3.0


class Waited:
    """Outcome of a poll: did it happen, what was last seen, how long it took."""

    def __init__(self, ok: bool, last: Any = None, elapsed: float = 0.0, polls: int = 0) -> None:
        self.ok = ok
        self.last = last
        self.elapsed = elapsed
        self.polls = polls

    def __bool__(self) -> bool:
        return self.ok

    def __repr__(self) -> str:
        return f"Waited(ok={self.ok}, polls={self.polls}, {self.elapsed:.0f}s, last={self.last!r})"


def wait_until(
    probe_fn: Callable[[], Any],
    predicate: Callable[[Any], bool],
    timeout: float = DEFAULT_TIMEOUT_S,
    interval: float = DEFAULT_INTERVAL_S,
) -> Waited:
    """Poll ``probe_fn`` until ``predicate`` holds or the deadline passes.

    Exceptions inside ``probe_fn`` are treated as "not yet" — a service that is
    still coming up refuses connections, and that is a normal intermediate
    state, not a test failure.
    """
    deadline = time.time() + timeout
    started = time.time()
    polls = 0
    last: Any = None
    while True:
        polls += 1
        try:
            last = probe_fn()
            if predicate(last):
                return Waited(True, last, time.time() - started, polls)
        except Exception as exc:  # noqa: BLE001
            last = exc
        if time.time() >= deadline:
            return Waited(False, last, time.time() - started, polls)
        time.sleep(min(interval, max(0.0, deadline - time.time())))


def wait_for_deployment(client, deployment_id: str, statuses=("running", "active", "healthy"),
                        timeout: float = DEFAULT_TIMEOUT_S) -> Waited:
    """Poll ``GET /livesim/deployments`` until this deployment reaches a status."""
    wanted = {s.lower() for s in statuses}

    def probe():
        body, _ = client.list_deployments()
        return find_deployment(body, deployment_id)

    return wait_until(probe, lambda d: bool(d) and str(
        d.get("status") or d.get("state") or "").lower() in wanted, timeout)


def wait_for_deployment_gone(client, deployment_id: str,
                             timeout: float = DEFAULT_TIMEOUT_S) -> Waited:
    def probe():
        body, _ = client.list_deployments()
        return find_deployment(body, deployment_id)

    return wait_until(probe, lambda d: not d, timeout)


def wait_for_log(client, deployment_id: str, instance_name: str, needle: str,
                 timeout: float = DEFAULT_TIMEOUT_S) -> Waited:
    """Poll a strategy instance's logs until ``needle`` appears.

    The durable way to prove a callback fired inside a livesim: the strategy
    writes a marker, this waits for it.
    """
    def probe():
        body, _ = client.strategy_logs(deployment_id, instance_name)
        return log_text(body)

    return wait_until(probe, lambda text: needle in (text or ""), timeout)


def find_deployment(body: Any, deployment_id: str) -> Optional[Dict[str, Any]]:
    """Locate one deployment record in a list response of any envelope shape."""
    items = body
    if isinstance(body, dict):
        for key in ("deployments", "data", "items", "results"):
            if isinstance(body.get(key), list):
                items = body[key]
                break
    if not isinstance(items, list):
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in ("deployment_id", "deploymentId", "id"):
            if str(item.get(key) or "") == str(deployment_id):
                return item
    return None


def log_text(body: Any) -> str:
    """Flatten a logs response into one searchable string."""
    if isinstance(body, str):
        return body
    if isinstance(body, list):
        return "\n".join(str(x) for x in body)
    if isinstance(body, dict):
        for key in ("logs", "lines", "data", "content", "raw", "output"):
            got = body.get(key)
            if got is not None:
                return log_text(got)
    return ""


# ------------------------------------------------------------------ market hours


def market_is_open(now: Optional[datetime] = None) -> bool:
    """Rough RTH check in ET — weekday, 09:30-16:00.

    Deliberately approximate: it does not know holidays. Its only job is to let
    a live-data test report GAP ("market closed") instead of FAIL when it runs
    at 3am. A test that needs holiday precision should assert on the data it
    actually received, not on the clock.
    """
    now = now or datetime.now(ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    now = now.astimezone(ET)
    if now.weekday() >= 5:
        return False
    return dtime(9, 30) <= now.time() <= dtime(16, 0)


def futures_session_is_open(now: Optional[datetime] = None) -> bool:
    """CME-style near-24h session in ET: Sun 18:00 -> Fri 17:00, daily 17:00-18:00 break."""
    now = now or datetime.now(ET)
    if now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    now = now.astimezone(ET)
    wd, t = now.weekday(), now.time()
    if dtime(17, 0) <= t < dtime(18, 0):
        return False
    if wd == 5:  # Saturday
        return False
    if wd == 6:  # Sunday: opens 18:00
        return t >= dtime(18, 0)
    if wd == 4 and t >= dtime(17, 0):  # Friday after the close
        return False
    return True
