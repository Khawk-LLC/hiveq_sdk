"""The ``Run`` handle — a memorable, REST-mirroring view of a backtest run.

A ``Run`` is what ``run_backtest`` returns for platform (non-dev) runs and what
``hf.get_run(run_id)`` attaches to. Its methods map 1:1 to the platform's ``runs``
REST endpoints so the SDK "feels like" the API:

    run = hf.run_backtest(...)        # blocks with a live progress line, returns Run
    run.report()                      # GET /runs/{id}/report  -> PerformanceReport
    run.positions(); run.orders(); run.trades()
    run.daily_returns(); run.equity_curve(); run.metrics(); run.summary()
    run.stat
    us(); run.event_logs(); run.wait()

Tabular resources come back as pandas DataFrames; scalar ones as dicts.
"""
import os
import sys
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from hiveq.flow.data.reader import HiveQDataReader
from hiveq.flow.logger import logger

logger = logger()

_TERMINAL = {"completed", "failed", "terminated", "done", "stopped", "error"}

# How long the wait() loop tolerates the platform being continuously unreachable
# (every attempted poll failing) before giving up and pointing to the dashboard.
_WAIT_ERROR_GIVEUP = float(os.environ.get("HIVEQ_WAIT_ERROR_GIVEUP", "120"))


def _is_terminal(status: Optional[str]) -> bool:
    return bool(status) and status.lower() in _TERMINAL


def _http_status(exc: Exception) -> Optional[int]:
    """HTTP status code from a requests error, else None."""
    resp = getattr(exc, "response", None)
    return getattr(resp, "status_code", None)


class CredentialsError(RuntimeError):
    """Raised when the platform rejects the caller's credentials for a run."""


def _fmt_money(v: Optional[float]) -> str:
    if v is None:
        return "—"
    sign = "+" if v >= 0 else "-"
    return f"{sign}{abs(v):,.2f}"


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    pct = v * 100
    if pct == 0:
        return "+0.00%"
    # Significant-figure format so small but non-zero returns don't round to
    # 0.00% (e.g. a tiny position against large capital).
    return f"{pct:+.3g}%"


class _ProgressPrinter:
    """In-place, single-line progress for a running backtest.

    Writes to the terminal with a carriage return so the line refreshes *in place*
    (run_id, date window, day progress, pnl/return, current date, elapsed) — one
    live line, not a new log line per poll. Honored by real terminals and IDE
    consoles (PyCharm, etc.). The final state is left on screen with a newline.

    (A progress bar needs raw ``\\r`` to the terminal — a stdlib ``Logger`` emits a
    newline per record, so it can't update in place; hence direct stdout here.)
    """

    _BAR_WIDTH = 20

    def __init__(self, run_id=None, task_id=None, start_date=None, end_date=None, stream=None):
        self.run_id = run_id
        self.task_id = task_id
        self.start_date = start_date
        self.end_date = end_date
        self.stream = stream or sys.stdout
        self._t0 = time.monotonic()

    def _desc(self) -> str:
        # bold-cyan label, then dim run/task ids + the backtest window, each
        # separated by a dim "│" (same separator used between line groups).
        parts = [self._c("backtest", "1;36")]
        if self.run_id:
            parts.append(self._c(f"run {self.run_id}", "2"))
        if self.task_id:
            parts.append(self._c(f"task {self.task_id}", "2"))
        if self.start_date or self.end_date:
            parts.append(self._c(f"{self.start_date or '?'}→{self.end_date or '?'}", "2"))
        return self._c(" │ ", "2").join(parts)

    @staticmethod
    def _fmt_elapsed(seconds: float) -> str:
        s = int(seconds)
        h, rem = divmod(s, 3600)
        m, sec = divmod(rem, 60)
        return f"{h:d}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"

    # ANSI colors for the live line (opt out with NO_COLOR=1). Codes: 2=dim,
    # 31=red, 32=green, 36=cyan, 1=bold.
    _COLOR = os.environ.get("NO_COLOR") in (None, "")

    @classmethod
    def _c(cls, text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if cls._COLOR else text

    def _signed(self, value, text: str) -> str:
        # green for ≥0, red for <0, dim for unknown
        if value is None:
            return self._c(text, "2")
        return self._c(text, "32" if value >= 0 else "31")

    def _bar(self, cur: int, total) -> str:
        if not total:
            return ""
        frac = max(0.0, min(1.0, cur / total))
        filled = int(round(frac * self._BAR_WIDTH))
        bar = self._c("█" * filled, "32") + self._c("░" * (self._BAR_WIDTH - filled), "2")
        return " |" + bar + "|"

    def _line(self, status: Dict[str, Any]) -> str:
        cur = status.get("current_day") or 0
        total = status.get("total_days")
        days = f"{min(cur, total)}/{total}" if total else f"{cur}/?"
        pnl = status.get("net_pnl")
        ret = status.get("return")
        sep = self._c(" │ ", "2")  # dim separator between groups
        groups = [
            self._desc(),
            f"{self._bar(cur, total).lstrip()} {self._c(days, '1')} days".strip(),
            f"pnl {self._signed(pnl, _fmt_money(pnl))}  "
            f"ret {self._signed(ret, _fmt_pct(ret))}  "
            f"{self._c(status.get('current_date') or '—', '2')}",
            self._c("[" + self._fmt_elapsed(time.monotonic() - self._t0) + "]", "36"),
        ]
        return sep.join(g for g in groups if g)

    def update(self, status: Dict[str, Any]) -> None:
        # Refresh the single line in place (\r + clear-to-EOL).
        try:
            self.stream.write("\r\033[K" + self._line(status))
            self.stream.flush()
        except Exception:
            pass

    def finish(self, status: Dict[str, Any]) -> None:
        try:
            self.stream.write("\r\033[K" + self._line(status) + "\n")
            self.stream.flush()
        except Exception:
            pass


class Run:
    """A handle to a single backtest run, mirroring the ``runs`` REST family."""

    def __init__(
        self,
        run_id: str,
        task_id: Optional[str] = None,
        reader: Optional[HiveQDataReader] = None,
        report=None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        if not run_id:
            raise ValueError("run_id is required")
        self.run_id = run_id
        self.task_id = task_id
        # Backtest window, surfaced on the live progress line (best-effort: set
        # by run_backtest at submit time; None when attaching via get_run).
        self.start_date = start_date
        self.end_date = end_date
        # local=True / dev runs attach an in-process PerformanceReport: a "local"
        # Run serves everything from it and never touches the REST API.
        self._local_report = report
        self._reader = None if report is not None else (reader or HiveQDataReader())

    @property
    def is_local(self) -> bool:
        return self._local_report is not None

    def __repr__(self) -> str:
        kind = "local" if self.is_local else "remote"
        return f"Run(run_id={self.run_id!r}, task_id={self.task_id!r}, {kind})"

    @staticmethod
    def _as_df(value) -> pd.DataFrame:
        """Coerce a report attribute (DataFrame/Series/None) to a DataFrame."""
        if isinstance(value, pd.DataFrame):
            return value
        if isinstance(value, pd.Series):
            return value.rename("value").to_frame()
        return pd.DataFrame()

    # --- scalar resources ---------------------------------------------------
    def status(self) -> Dict[str, Any]:
        if self.is_local:
            return {
                "run_id": self.run_id,
                "status": "completed",
                "is_final": True,
                "net_pnl": getattr(self._local_report, "net_pnl", None),
            }
        return self._reader.status(self.run_id)

    def overview(self) -> Dict[str, Any]:
        if self.is_local:
            return {"run_id": self.run_id, "status": "completed", "is_final": True}
        return self._reader.overview(self.run_id)

    def summary(self) -> Dict[str, Any]:
        if self.is_local:
            try:
                return self._local_report.summary_stats()
            except Exception:
                return {}
        return self._reader.summary(self.run_id)

    def report(self, include: Optional[List[str]] = None):
        """Return the run's ``PerformanceReport``.

        Local runs return the in-process report. Remote runs prefer the runs
        REST API and fall back to the platform task-result snapshot when the
        REST API has no rows yet (a deployed run whose sandbox hiveq-flow
        predates the run_id->payload_id unification).
        """
        if self.is_local:
            return self._local_report

        from hiveq.flow.metrics.report import PerformanceReport

        payload = {}
        try:
            payload = self._reader.report(self.run_id, include=include) or {}
        except Exception as e:
            logger.debug(f"runs REST report failed, will try platform task result: {e}")

        has_rest_data = bool(
            payload.get("summary")
            or payload.get("daily_returns")
            or payload.get("strategy_metrics")
        )
        if not has_rest_data and self.task_id:
            try:
                from hiveq.flow import jobs

                return PerformanceReport.from_task_result(jobs.get_result(self.task_id))
            except Exception as e:
                logger.debug(f"platform task-result fallback failed: {e}")

        return PerformanceReport.from_rest(payload)

    # --- tabular resources (DataFrames) ------------------------------------
    def metrics(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return self._as_df(self._local_report.strategy_stats)
        return pd.DataFrame(self._reader.metrics(self.run_id, **kw) or [])

    def daily_returns(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return self._as_df(self._local_report.daily_returns)
        return pd.DataFrame(self._reader.daily_returns(self.run_id, **kw) or [])

    def equity_curve(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return self._as_df(self._local_report.returns_series)
        return pd.DataFrame(self._reader.equity_curve(self.run_id, **kw) or [])

    def positions(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return self._as_df(self._local_report.positions)
        return pd.DataFrame(self._reader.positions(self.run_id, **kw) or [])

    def orders(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return self._as_df(self._local_report.orders)
        return pd.DataFrame(self._reader.orders(self.run_id, **kw) or [])

    def trades(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return self._as_df(self._local_report.trades)
        return pd.DataFrame(self._reader.trades(self.run_id, **kw) or [])

    def event_logs(self, **kw) -> pd.DataFrame:
        if self.is_local:
            return pd.DataFrame()
        return pd.DataFrame(self._reader.event_logs(self.run_id, **kw) or [])

    def logs(self) -> List[str]:
        """The COMPLETE remote executor log for this run, as a list of lines.

        These are the executor's stdout / strategy-callback output — including
        crashes like ``STRATEGY_CALLBACK_ERROR`` — which are NOT in the runs REST
        API's ``event-logs``. They live only on the platform ``GET /logs`` endpoint
        (keyed by ``task_id``; run_id != task_id). Fetched as the full gzipped log
        (``format=gz``) over direct REST and decompressed — the whole log, not a
        tail, so errors anywhere in the run are captured. Use :meth:`download_logs`
        to stream a very large log straight to a file instead.
        """
        if not self.task_id:
            return []
        from hiveq.flow import jobs

        text = jobs.get_logs_gz(task_id=self.task_id)
        return text.splitlines() if text else []

    def download_logs(self, path: str) -> str:
        """Stream the full gzipped executor log to ``path`` (a ``.gz`` file).

        For huge logs — never decompresses in memory. Returns ``path``. Keyed by
        ``task_id`` via ``GET /logs?format=gz`` (direct REST).
        """
        if not self.task_id:
            raise ValueError("this run has no task_id; cannot fetch logs")
        from hiveq.flow import jobs

        return jobs.get_logs_gz(task_id=self.task_id, dest=path)

    # --- lifecycle ----------------------------------------------------------
    def check_credentials(self) -> "Run":
        """Fail fast if the platform rejects our credentials for this run.

        The runs ``/status`` endpoint swallows auth errors (returns PENDING), so
        a bad key would otherwise hang ``wait()`` until timeout. The platform
        task read returns a real 401/403, so we probe it once and raise immediately
        on an auth error. Transient/other errors are ignored (wait() handles
        them). No-op for local runs or when there's no task to read.
        """
        if self.is_local or not self.task_id:
            return self
        from hiveq.flow import jobs

        try:
            jobs.get_result(self.task_id)
        except Exception as e:
            code = _http_status(e)
            if code in (401, 403):
                raise CredentialsError(
                    f"Platform rejected credentials (HTTP {code}) for run "
                    f"{self.run_id}. The API key / org in use is not authorized on "
                    f"this platform "
                    f"(HIVEQ_BASE_URL={os.environ.get('HIVEQ_BASE_URL', '')!r}, "
                    f"HIVEQ_ORG_ID={os.environ.get('HIVEQ_ORG_ID', '')!r}). "
                    f"Use credentials valid for the target environment."
                ) from e
            logger.debug(f"credential probe inconclusive (continuing): {e}")
        return self

    def wait(
        self,
        timeout: Optional[float] = None,
        poll_interval: float = 1.0,
        progress: bool = True,
    ) -> "Run":
        """Block until the run reaches a terminal state.

        Renders a live progress line by default. Returns ``self`` so callers can
        chain ``hf.get_run(id).wait().report()``.
        """
        if self.is_local:
            return self  # already complete; nothing to poll
        printer = (
            _ProgressPrinter(
                run_id=self.run_id,
                task_id=self.task_id,
                start_date=self.start_date,
                end_date=self.end_date,
            )
            if progress
            else None
        )
        start = time.time()
        last: Dict[str, Any] = {}
        # Throttle the runs-gateway /status polling: the progress line only
        # refreshes every few seconds, and hammering it every poll_interval trips
        # the gateway's per-user rate limiter (429). Terminal detection stays
        # responsive via the cheap platform task check below.
        status_every = max(poll_interval, 5.0)
        last_status_poll = 0.0
        # Don't poll forever if the platform becomes unreachable: once every
        # attempted poll has failed continuously for this long, stop and point
        # the user to the dashboard rather than spinning indefinitely.
        last_reachable = start
        while True:
            # Rich progress (pnl/day/date) comes from the runs REST /status, but
            # that's throttled to avoid the gateway's per-user rate limiter (429).
            now = time.time()
            attempted = False
            succeeded = False
            if now - last_status_poll >= status_every:
                attempted = True
                try:
                    last = self.status() or {}
                    succeeded = True
                except Exception as e:  # transient REST hiccup — keep polling
                    logger.debug(f"status poll failed (continuing): {e}")
                    last = last or {"status": "pending"}
                last_status_poll = now
            # Refresh the in-place line every loop so elapsed ticks smoothly,
            # even between (throttled) status re-polls.
            if printer:
                printer.update(last)

            # Completion is authoritative from the platform task lifecycle,
            # which works regardless of the runs REST data being present yet.
            orch_status = None
            if self.task_id:
                attempted = True
                try:
                    from hiveq.flow import jobs

                    # get_result carries the authoritative task status and works
                    # reliably (get_status can 404 right after submit).
                    orch_status = (jobs.get_result(self.task_id) or {}).get("status")
                    succeeded = True
                except Exception as e:
                    logger.debug(f"platform task status poll failed: {e}")

            if (
                last.get("is_final")
                or _is_terminal(last.get("status"))
                or _is_terminal(orch_status)
            ):
                # Final authoritative refresh. The in-loop /status poll is throttled
                # (every status_every seconds) and may be stale by the time the run
                # turns terminal — so the last rendered line can show an intermediate
                # day/pnl rather than the final one. Re-poll once here so the finished
                # line reflects the completed run (final day count, final pnl/return).
                try:
                    final = self.status()
                    if final:
                        last = final
                except Exception as e:
                    logger.debug(f"final status poll failed (using last): {e}")
                if printer:
                    printer.finish(last)
                return self

            # Track reachability: only count iterations where we actually tried
            # to reach the platform and every attempt failed.
            if attempted and succeeded:
                last_reachable = now
            elif attempted and not succeeded and (now - last_reachable) >= _WAIT_ERROR_GIVEUP:
                if printer:
                    printer.finish(last)
                raise RuntimeError(
                    f"Lost contact with the HiveQ platform for "
                    f"{int(now - last_reachable)}s while waiting on run {self.run_id}. "
                    f"Stopping so this doesn't run indefinitely — the run may still "
                    f"be in progress; check the HiveQ dashboard later "
                    f"(run {self.run_id})."
                )

            if timeout is not None and (time.time() - start) >= timeout:
                if printer:
                    printer.finish(last)
                raise TimeoutError(
                    f"Run {self.run_id} not finished within {timeout}s "
                    f"(last status: {last.get('status') or orch_status})"
                )
            time.sleep(poll_interval)
