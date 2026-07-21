"""The ``Run`` handle — a memorable, REST-mirroring view of a backtest run.

A ``Run`` is what ``run_backtest`` returns for platform (non-dev) runs and what
``hf.get_run(run_id)`` attaches to. Its methods map 1:1 to the platform's ``runs``
REST endpoints so the SDK "feels like" the API:

    run = hf.run_backtest(...)        # blocks with a live progress line, returns Run
    run.report()                      # GET /runs/{id}/report  -> PerformanceReport
    run.positions(); run.orders(); run.trades(); run.fills()
    run.daily_returns(); run.equity_curve(); run.metrics(); run.summary()
    run.tearsheet()                   # quantstats HTML tearsheet
    run.status(); run.event_logs(); run.logs(); run.wait()

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
    """Live progress for a running backtest, rendered with ``tqdm``.

    Most users run in Jupyter, marimo, or an IDE console, each of which renders
    progress differently (Jupyter ignores ``\\r``; marimo isn't IPython). Rather
    than detect and hand-render each, we drive ``tqdm.auto``, which already picks
    the right backend per environment — the ipywidgets bar in Jupyter, marimo's
    native bar in marimo, and a carriage-return text bar in terminals / IDE
    consoles / pipes.

    The day count drives the bar; pnl / return / current date ride in the
    postfix; the backtest window is the description. tqdm shows elapsed/rate.
    """

    def __init__(self, run_id=None, task_id=None, start_date=None, end_date=None, stream=None):
        self.run_id = run_id
        self.task_id = task_id
        self.start_date = start_date
        self.end_date = end_date
        self.stream = stream  # optional file override (tests); None -> tqdm default
        self._bar = None
        self._closed = False
        self._header_printed = False
        # Whether it's safe to put ANSI color in the metrics text. Set once the
        # tqdm backend is known: yes for the text bar (terminal/IDE), no for the
        # Jupyter widget (which would render escape codes as literal garbage).
        self._ansi = False
        # Monotonic display state. The runs gateway occasionally returns a
        # partial/stale /status (current_day missing -> 0, or total_days/pnl
        # absent). Backtest progress only moves forward, so the day count and
        # total never regress; pnl/return/date carry their last known value.
        self._best_day = 0
        self._best_total = None
        self._shown: Dict[str, Any] = {}

    def _window(self) -> str:
        return f"{self.start_date or '?'}→{self.end_date or '?'}"

    def _g(self, text: str) -> str:
        # Green (only when ANSI is safe — see _ansi).
        return f"\033[32m{text}\033[0m" if self._ansi else text

    def _grey(self, text: str) -> str:
        # Grey/bright-black — used for the `|` delimiters.
        return f"\033[90m{text}\033[0m" if self._ansi else text

    def _absorb(self, status: Dict[str, Any]) -> None:
        cur = status.get("current_day") or 0
        if cur > self._best_day:
            self._best_day = cur
        if status.get("total_days"):
            self._best_total = status.get("total_days")
        for k in ("net_pnl", "return", "current_date"):
            v = status.get(k)
            if v is not None:
                self._shown[k] = v

    def _print_header(self) -> None:
        # Run/task ids on their own line above the bar (handy for re-attaching
        # via hf.get_run(run_id) or pulling logs by task_id). Printed once.
        # Labels + ids in green; the `|` delimiter in grey.
        if self._header_printed:
            return
        self._header_printed = True
        parts = []
        if self.run_id:
            parts.append(self._g(f"run {self.run_id}"))
        if self.task_id:
            parts.append(self._g(f"task {self.task_id}"))
        if not parts:
            return
        out = self.stream if self.stream is not None else sys.stdout
        try:
            out.write(self._grey(" | ").join(parts) + "\n")
            out.flush()
        except Exception:
            pass

    def _ensure_bar(self) -> None:
        if self._bar is not None:
            return
        try:
            from tqdm.auto import tqdm
        except Exception:
            self._bar = None
            return

        # Decide color BEFORE building the header/format: ANSI is safe for the
        # text bar but NOT the Jupyter widget (it renders escape codes literally).
        # tqdm.auto resolves to tqdm.notebook.tqdm in Jupyter — detect via module.
        is_widget = "notebook" in tqdm.__module__
        self._ansi = (not is_widget) and (os.environ.get("NO_COLOR") in (None, ""))

        self._print_header()
        try:
            # `|`-delimited fields, all green except the grey `|` separators (and
            # the pnl/return values, which are sign-colored in {desc}). Metrics go
            # in {desc} — not tqdm's {postfix}, which would force a ", " in front.
            sep = self._grey("|")
            bar_format = (
                self._g("backtest " + self._window()) + " "
                + sep + "{bar}" + sep + " "
                + self._g("{n_fmt}/{total_fmt} days") + " " + sep + " "
                + self._g("{elapsed}") + " " + sep + " {desc}"
            )
            kwargs = dict(
                total=self._best_total,
                dynamic_ncols=True,
                leave=True,
                bar_format=bar_format,
                colour="green",  # the bar fill itself
                # stdout, not tqdm's default stderr: PyCharm tints stderr red,
                # which masks our green/red ANSI. stdout renders color cleanly.
                file=self.stream if self.stream is not None else sys.stdout,
            )
            self._bar = tqdm(**kwargs)
        except Exception:
            self._bar = None

    def _signed(self, value, text: str) -> str:
        # Green for >= 0, red for < 0 — only when ANSI is safe (see _ansi).
        if not self._ansi or value is None:
            return text
        return f"\033[{'32' if value >= 0 else '31'}m{text}\033[0m"

    def _metrics(self) -> str:
        pnl = self._shown.get("net_pnl")
        ret = self._shown.get("return")
        sep = self._grey(" | ")
        date = self._shown.get("current_date") or "—"
        return (
            self._g("pnl") + " " + self._signed(pnl, _fmt_money(pnl)) + sep
            + self._g("ret") + " " + self._signed(ret, _fmt_pct(ret)) + sep
            + self._g(date)
        )

    def _refresh(self) -> None:
        self._ensure_bar()
        if self._bar is None:
            return
        try:
            if self._best_total and self._bar.total != self._best_total:
                self._bar.total = self._best_total
            target = self._best_day
            if self._best_total:
                target = min(target, self._best_total)
            self._bar.n = target  # advance the bar to the current day
            self._bar.set_description_str(self._metrics(), refresh=False)
            self._bar.refresh()
        except Exception:
            pass

    def update(self, status: Dict[str, Any]) -> None:
        self._absorb(status)
        self._refresh()

    def finish(self, status: Dict[str, Any]) -> None:
        self._absorb(status)
        self._refresh()
        if self._bar is not None and not self._closed:
            try:
                self._bar.close()
            except Exception:
                pass
            self._closed = True


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
        task_name: Optional[str] = None,
    ):
        if not run_id:
            raise ValueError("run_id is required")
        self.run_id = run_id
        self.task_id = task_id
        # Optional human-friendly backtest name; used to name tearsheet files.
        self.task_name = task_name
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

        # run_backtest deploys silently by default, so guard the classic
        # footgun: reading the report before the run has finished.
        try:
            st = self.status() or {}
            if not (st.get("is_final") or _is_terminal(st.get("status"))):
                logger.warning(
                    f"run {self.run_id} has not finished (status="
                    f"{st.get('status')!r}) — results may be empty or partial; "
                    f"call run.wait() first."
                )
        except Exception as e:
            logger.debug(f"pre-report status check inconclusive: {e}")

        from hiveq.flow.metrics.report import PerformanceReport

        payload = {}
        try:
            payload = self._reader.report(self.run_id, include=include) or {}
        except Exception as e:
            logger.debug(f"runs REST report failed, will try platform: {e}")

        has_rest_data = bool(
            payload.get("summary")
            or payload.get("daily_returns")
            or payload.get("strategy_metrics")
        )
        if not has_rest_data and self.task_id:
            try:
                from hiveq.flow import jobs

                report = PerformanceReport.from_task_result(jobs.get_result(self.task_id))
                return self._backfill_orders_and_fills(report)
            except Exception as e:
                logger.debug(f"platform result fallback failed: {e}")

        return self._backfill_orders_and_fills(PerformanceReport.from_rest(payload))

    def _backfill_orders_and_fills(self, report):
        """Populate ``report.orders`` / ``report.fills`` for a remote report.

        The ``GET /runs/{id}/report`` payload carries the summary, returns and
        PnL scalars but NOT the tabular order resource — orders live at their
        own ``GET /runs/{id}/orders`` endpoint. Without this, ``report.orders``
        is empty on remote runs and any fills derived from it come back empty
        (the bug behind ``run.fills()`` returning nothing). So backfill orders
        from that endpoint and derive fills from them, making ``report.fills``
        and :meth:`fills` return identical rows. Best-effort: any REST failure
        leaves the report as-is.
        """
        from hiveq.flow.metrics.report import _fills_from_orders

        try:
            if report.orders is None or self._as_df(report.orders).empty:
                orders_df = pd.DataFrame(self._reader.orders(self.run_id) or [])
                if not orders_df.empty:
                    report.orders = orders_df
        except Exception as e:
            logger.debug(f"orders backfill failed: {e}")

        try:
            if report.fills is None or self._as_df(report.fills).empty:
                derived = _fills_from_orders(report.orders)
                if derived is not None:
                    report.fills = derived
        except Exception as e:
            logger.debug(f"fills derivation failed: {e}")

        return report

    def tearsheet(self, output: Optional[str] = None) -> str:
        """Render the performance tearsheet for this run to a file.

        The single entry point for producing a tearsheet — equity curve,
        drawdowns, monthly-returns heatmap, rolling risk, return distribution,
        and a full metrics table. The format is chosen from ``output``'s
        extension: ``.html`` writes a standalone HTML file to open in a browser;
        anything else (including no extension) writes a PDF. When ``output`` is
        omitted the file is a PDF named after the run (the backtest task name
        when known, otherwise the run id), written to the current directory.
        Returns the path written.

            run.tearsheet()                      # -> '<task_name|run_id>.pdf'
            run.tearsheet(output='my_report.pdf')
            run.tearsheet(output='my_report.html')

        For the HTML *string* to render inline in a Jupyter/marimo notebook, use
        ``run.report().create_tearsheet()`` instead.
        """
        if output is None:
            base = self.task_name or self.run_id
            output = f"{base}.pdf"
        ext = os.path.splitext(output)[1].lower()
        if ext not in (".pdf", ".html", ".htm"):
            output = f"{output}.pdf"
            ext = ".pdf"
        # Stamp identifiers into the file so it can be traced back to the DB record.
        period = None
        if self.start_date or self.end_date:
            period = f"{self.start_date or '?'} → {self.end_date or '?'}"
        meta = {
            "Run ID": self.run_id,
            "Task ID": self.task_id,
            "Task": self.task_name,
            "Period": period,
        }
        report = self.report()
        if ext in (".html", ".htm"):
            path = report.save_tearsheet_html(output, meta=meta)
        else:
            path = report.save_tearsheet_pdf(output, meta=meta)
        logger.info(f"Tearsheet written to {path}")
        return path

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

    def fills(self) -> pd.DataFrame:
        """Order fills for this run as a DataFrame (empty if none).

        Returns exactly the same rows as ``run.report().fills``. There is no
        dedicated ``/fills`` REST resource — each order already carries its
        fill state — so fills are the executed subset of the orders, derived by
        filtering (see ``metrics.report._fills_from_orders``). For remote runs
        :meth:`report` backfills orders from the ``/orders`` endpoint and derives
        fills there; for local runs the in-process report supplies them (with the
        same derivation as a fallback).
        """
        from hiveq.flow.metrics.report import _fills_from_orders

        report = self._local_report if self.is_local else self.report()
        fills = self._as_df(getattr(report, "fills", None))
        if fills.empty:
            derived = _fills_from_orders(self._as_df(getattr(report, "orders", None)))
            if derived is not None and not derived.empty:
                return derived
        return fills

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
        API's ``event-logs``. Fetched as the full gzipped log (``format=gz``) over
        direct REST and decompressed — the whole log, not a tail, so errors
        anywhere in the run are captured. Use :meth:`download_logs` to stream a
        very large log straight to a file instead.

        Works from a bare ``run_id`` too (e.g. ``hf.get_run(run_id).logs()`` after
        the run finished): the platform ``GET /logs`` endpoint accepts ``run_id``
        directly for backtests (it resolves logs by ``run_id`` or ``task_id``), so
        we don't need the task_id in hand. If neither id is set, or the platform
        has no log under that id, returns ``[]``.
        """
        text = self._fetch_log_text()
        return text.splitlines() if text else []

    def download_logs(self, path: str) -> str:
        """Stream the full gzipped executor log to ``path`` (a ``.gz`` file).

        For huge logs — never decompresses in memory. Returns ``path``. Resolved
        by ``task_id`` when known, else by ``run_id`` (the ``GET /logs`` endpoint
        accepts either for backtests).
        """
        if not (self.task_id or self.run_id):
            raise ValueError("this run has neither task_id nor run_id; cannot fetch logs")
        from hiveq.flow import jobs

        # Prefer task_id (the known-good key); fall back to run_id, which the
        # platform accepts for backtests.
        if self.task_id:
            return jobs.get_logs_gz(task_id=self.task_id, dest=path)
        return jobs.get_logs_gz(run_id=self.run_id, dest=path)

    def _fetch_log_text(self):
        """Best-effort fetch of the full executor log text (or ``None``).

        Prefer ``task_id`` (set during the launching session); when it's absent —
        e.g. attached via ``hf.get_run(run_id)`` — fall back to ``run_id``, which
        the ``/logs`` endpoint resolves for backtests. A missing log (404) is
        treated as "no logs yet", not an error.
        """
        from hiveq.flow import jobs

        if self.task_id:
            return jobs.get_logs_gz(task_id=self.task_id)
        if self.run_id:
            try:
                return jobs.get_logs_gz(run_id=self.run_id)
            except Exception as e:  # 404 / not-found -> no logs available by run_id
                logger.debug(f"logs by run_id={self.run_id} unavailable: {e}")
                return None
        return None

    # --- lifecycle ----------------------------------------------------------
    def check_credentials(self) -> "Run":
        """Fail fast if the platform rejects our credentials for this run.

        The runs ``/status`` endpoint swallows auth errors (returns PENDING), so
        a bad key would otherwise hang ``wait()`` until timeout. The platform
        read returns a real 401/403, so we probe it once and raise immediately
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
        # How often to re-poll the runs-gateway /status for day/pnl. Kept at the
        # caller's poll_interval (default 1s) so the line advances per day even on
        # fast backtests. For very long runs where per-second polling could trip
        # the gateway's per-user rate limiter (429), pass a larger poll_interval.
        status_every = max(poll_interval, 1.0)
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
                    logger.debug(f"platform status poll failed: {e}")

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
