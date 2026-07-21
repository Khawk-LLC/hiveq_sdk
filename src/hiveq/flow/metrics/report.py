import logging
import os
import uuid
from dataclasses import dataclass
import pandas as pd
import warnings
from typing import Optional, TYPE_CHECKING

from hiveq.flow import utils

if TYPE_CHECKING:
    from hiveq.flow.tca.types import TCAReport


def _find_browser() -> Optional[str]:
    """Locate a headless-capable Chromium/Chrome/Edge binary on PATH."""
    import shutil

    env = os.environ.get("HIVEQ_CHROME") or os.environ.get("CHROME_BIN")
    if env:
        return shutil.which(env) or (env if os.path.exists(env) else None)
    for name in (
        "google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
        "microsoft-edge", "chrome",
    ):
        path = shutil.which(name)
        if path:
            return path
    return None


def _html_to_pdf(html_path: str, output: str) -> None:
    """Convert an HTML file to PDF, preferring a headless browser, else WeasyPrint.

    A browser renders quantstats' report CSS with the highest fidelity; WeasyPrint
    is the portable pure-Python fallback. Force one with ``HIVEQ_HTML2PDF``
    (``browser`` | ``weasyprint``). Raises ``RuntimeError`` if neither works.
    """
    backend = os.environ.get("HIVEQ_HTML2PDF", "").strip().lower()
    out_abs = os.path.abspath(output)

    def via_browser() -> bool:
        browser = _find_browser()
        if not browser:
            return False
        import subprocess

        url = "file://" + os.path.abspath(html_path)
        # `--headless=new` on modern Chrome, plain `--headless` on older builds.
        for headless in ("--headless=new", "--headless"):
            cmd = [
                browser, headless, "--disable-gpu", "--no-sandbox",
                "--no-pdf-header-footer",
                "--run-all-compositor-stages-before-draw",
                "--virtual-time-budget=10000",
                f"--print-to-pdf={out_abs}", url,
            ]
            try:
                r = subprocess.run(cmd, capture_output=True, timeout=180)
            except (OSError, subprocess.TimeoutExpired):
                continue
            if r.returncode == 0 and os.path.exists(out_abs) and os.path.getsize(out_abs) > 0:
                return True
        return False

    def via_weasyprint() -> bool:
        try:
            from weasyprint import HTML
        except Exception:
            return False
        HTML(filename=html_path).write_pdf(out_abs)
        return os.path.exists(out_abs) and os.path.getsize(out_abs) > 0

    if backend == "weasyprint":
        order = [via_weasyprint]
    elif backend == "browser":
        order = [via_browser]
    else:
        order = [via_browser, via_weasyprint]  # browser first (best fidelity)

    for fn in order:
        try:
            if fn():
                return
        except Exception as e:  # noqa: BLE001 — try the next backend
            logging.warning(f"tearsheet PDF backend {fn.__name__} failed: {e}")

    raise RuntimeError(
        "Could not convert the HTML tearsheet to PDF — no working backend found. "
        "Install a Chromium/Chrome browser on PATH, or WeasyPrint "
        "(`pip install weasyprint`). Force a backend with "
        "HIVEQ_HTML2PDF=browser|weasyprint."
    )


# Styling for the run-metadata banner only. We intentionally do NOT inject any
# page/pagination CSS: quantstats' compact two-column layout is what we want, and
# fighting its page breaks (to stop a chart straddling an edge) forced the report
# onto far more pages, which read worse than the occasional break. So we leave
# pagination to the browser's default and only style the banner.
_PRINT_CSS = """
<style id="hiveq-print">
.hiveq-meta { margin: 6px 0 16px 0; font: 12px/1.55 Arial, sans-serif; color: #333; }
.hiveq-meta td { padding: 1px 16px 1px 0; font: 12px/1.55 Arial, sans-serif; }
.hiveq-meta .k { color: #888; white-space: nowrap; }
.hiveq-meta .v { font-weight: 600; }
</style>
"""


def _meta_header_html(meta: Optional[dict]) -> str:
    """Render a small key/value banner (run id, task, period, …) for traceability.

    Always stamps a UTC generation time so a PDF can be tied back to its DB record.
    """
    import html as _html
    from datetime import datetime, timezone

    fields = dict(meta or {})
    fields.setdefault("Generated", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    rows = "".join(
        f'<tr><td class="k">{_html.escape(str(k))}</td>'
        f'<td class="v">{_html.escape(str(v))}</td></tr>'
        for k, v in fields.items() if v not in (None, "", "None")
    )
    if not rows:
        return ""
    return f'<div class="hiveq-meta"><table>{rows}</table></div>'


def _decorate_report_html(html: str, meta: Optional[dict]) -> str:
    """Inject the print CSS (into <head>) and the run-metadata banner (under the
    report title) into quantstats' HTML before it is converted to PDF."""
    if "</head>" in html:
        html = html.replace("</head>", _PRINT_CSS + "</head>", 1)
    else:
        html = _PRINT_CSS + html

    banner = _meta_header_html(meta)
    if banner:
        # Prefer to land it right under the title/byline; fall back progressively.
        for anchor in ("</h4>", "<hr>", '<div class="container">', "<body>", "<body"):
            idx = html.find(anchor)
            if idx != -1:
                # for the bare "<body" case, jump to the end of that tag
                pos = idx + len(anchor)
                if anchor == "<body":
                    end = html.find(">", idx)
                    pos = end + 1 if end != -1 else pos
                return html[:pos] + banner + html[pos:]
        html = banner + html
    return html


def _create_no_data_html(title: str = "Strategy Performance Report", message: str = "No data available") -> str:
    """Generate a simple HTML page indicating no data is available.

    Parameters
    ----------
    title : str
        The title to display in the HTML page
    message : str
        The message to display explaining why there's no data

    Returns
    -------
    str
        HTML string with styled no-data message
    """
    return f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 200px;
            margin: 0;
            background-color: #f5f5f5;
        }}
        .container {{
            text-align: center;
            padding: 40px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            margin-bottom: 16px;
            font-size: 24px;
        }}
        p {{
            color: #666;
            font-size: 16px;
            margin: 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <p>{message}</p>
    </div>
</body>
</html>"""


def _first_col(df: pd.DataFrame, candidates) -> Optional[str]:
    """Return the first of ``candidates`` present in ``df`` (case-insensitive)."""
    lower = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name in lower:
            return lower[name]
    return None


def _fills_from_orders(orders) -> Optional[pd.DataFrame]:
    """Derive an executed-fills view by filtering the orders frame.

    The platform exposes no dedicated fills resource — each order row already
    carries its cumulative fill state (``SigmaOrder``: ``filled_qty``,
    ``avg_px``, ``status`` …). "Fills" is therefore the subset of orders that
    actually executed (``status`` FILLED / PARTIALLY_FILLED, else
    ``is_filled``, else ``filled_qty > 0``). Column names are matched
    case-insensitively against known REST variants. Returns ``None`` when
    there are no orders or no recognizable fill signal; an empty frame when
    orders exist but nothing executed.
    """
    if not isinstance(orders, pd.DataFrame) or orders.empty:
        return None

    status_col = _first_col(orders, ("status", "order_status", "ord_status"))
    flag_col = _first_col(orders, ("is_filled",))
    qty_col = _first_col(
        orders,
        ("filled_qty", "filled_quantity", "cum_qty", "cumulative_qty", "last_qty"),
    )

    if status_col is not None:
        # FILLED and PARTIALLY_FILLED both executed; REJECTED/CANCELED (no
        # partial) did not. Substring match tolerates enum prefixes/casing.
        mask = orders[status_col].astype(str).str.upper().str.contains("FILL")
    elif flag_col is not None:
        mask = orders[flag_col].astype(bool)
    elif qty_col is not None:
        mask = pd.to_numeric(orders[qty_col], errors="coerce").fillna(0) > 0
    else:
        logging.debug("orders frame has no fill-signal column; cannot derive fills")
        return None

    return orders[mask].reset_index(drop=True)


@dataclass
class PerformanceReport:
    """Backtest performance report containing metrics, returns, and trade history.

    This dataclass is returned by run_backtest() and contains comprehensive
    performance analysis including summary statistics, returns data, equity curves,
    positions, transactions, and run metadata. Use create_tearsheet() to generate
    a visual HTML report with charts and metrics.

    Attributes
    ----------
    return_stats : pd.DataFrame, optional
        Returns statistics for the run.
    returns_series : pd.Series, optional
        Time series of strategy returns used to generate equity curves and
        perform risk analysis. Index is datetime, values are returns.
    positions : pd.DataFrame, optional
        DataFrame of all positions taken during the backtest, including
        entry/exit times, quantities, prices, and P&L.
    fills : pd.DataFrame, optional
        DataFrame of all fills (order fills) with timestamps, symbols,
        quantities, prices, and fees.
    run_info : pd.DataFrame, optional
        Metadata about the backtest run including start/end dates, initial
        capital, number of trades, and strategy parameters.
    trades : pd.DataFrame
        Dataframe of consolidated trades for the portfolio
    pnl_stats : pd.DataFrame, optional
        PnL statistics including realized/unrealized PnL metrics.
    daily_returns : pd.DataFrame, optional
        Daily returns data sorted by date in ascending order.
    strategy_stats: pd.DataFrame, optional
        Strategy PNL and General stats
    orders: pd.DataFrame, optional
        Orders data
    tca_report : TCAReport, optional
        Transaction cost analysis report with per-trade slippage,
        VWAP deviation, and market impact metrics.
    Notes
    -----
    All DataFrame attributes are optional and may be None if the corresponding
    data is not available. The returns_series is required for create_tearsheet()
    to generate a quantstats report.

    Examples
    --------
    Access backtest results after running a backtest:

    >>> import hiveq.flow as hf
    >>> results = hf.run_backtest(strategy_configs, symbols=['AAPL'], start_date='2023-01-01')
    >>> print(results)  # Displays run stats, returns, and general stats
    >>> print(results.positions)  # View all positions
    >>> print(results.fills)  # View all fills

    Generate a visual tearsheet report:

    >>> html_report = results.create_tearsheet()
    >>> # In Jupyter/marimo notebook:
    >>> import marimo as mo
    >>> mo.md(html_report)

    See Also
    --------
    hiveq.flow.run_backtest : Run a backtest and get PerformanceReport
    create_tearsheet : Generate a quantstats HTML report
    """
    return_stats: pd.DataFrame = None  # Returns statistics, has sharpe, returns, Average return etc
    returns_series: Optional[pd.Series] = None  # Equity curve data
    positions: Optional[pd.DataFrame] = None  # Positions
    fills: Optional[pd.DataFrame] = None  # Fills (order fills)
    run_info: Optional[pd.DataFrame] = None  # Information about the backtest run
    trades: Optional[pd.DataFrame] = None  # User Trades and Equity curve on PNL
    pnl_stats: Optional[pd.DataFrame] = None  # PnL statistics
    daily_returns: Optional[pd.DataFrame] = None  # Daily returns sorted by date
    strategy_stats: Optional[pd.DataFrame] = None  # Per-strategy performance statistics
    orders: Optional[pd.DataFrame] = None  # Orders
    tca_report: Optional["TCAReport"] = None  # Transaction cost analysis

    # Scalar PnL fields (single source of truth for all publish paths)
    total_realized_pnl: float = 0.0
    total_unrealized_pnl: float = 0.0
    total_fees: float = 0.0
    net_pnl: float = 0.0

    # The run identifier (payload_id for deployed runs). Lets callers query the
    # runs REST API or re-attach via hf.get_run(report.run_id).
    run_id: Optional[str] = None

    @classmethod
    def from_rest(cls, payload: dict) -> "PerformanceReport":
        """Build a PerformanceReport from a ``GET /runs/{id}/report`` payload.

        Composes the same shape ``run_backtest`` returns locally, so existing
        consumers (``summary_stats``, ``create_tearsheet``, ``.positions`` …)
        keep working when results come from the platform over REST.
        """
        payload = payload or {}

        def _df(rows):
            return pd.DataFrame(rows) if rows else None

        summary = payload.get("summary") or {}
        # Flatten {metric: value} summary into a metric/value frame, mirroring the
        # local return_stats layout used by __repr__.
        return_stats = (
            pd.DataFrame(
                [{"metric": k, "value": v} for k, v in summary.items()]
            )
            if isinstance(summary, dict) and summary
            else None
        )

        daily_returns = payload.get("daily_returns") or []
        daily_df = _df(daily_returns)

        # Reconstruct a datetime-indexed returns series for tearsheets.
        returns_series = None
        if daily_returns:
            idx_key = "date" if "date" in daily_returns[0] else (
                "timestamp" if "timestamp" in daily_returns[0] else None
            )
            val_key = next(
                (k for k in ("daily_return", "returns", "return")
                 if k in daily_returns[0]),
                None,
            )
            if idx_key and val_key:
                try:
                    s = pd.Series(
                        [r.get(val_key) for r in daily_returns],
                        index=pd.to_datetime([r.get(idx_key) for r in daily_returns]),
                        name="returns",
                    )
                    returns_series = s.sort_index()
                except Exception:
                    returns_series = None

        def _num(*keys):
            for k in keys:
                v = summary.get(k)
                if isinstance(v, (int, float)):
                    return float(v)
            return 0.0

        # strategy_metrics carries the authoritative PnL scalars from the
        # engine (realized_pnl, unrealized_pnl, total_pnl, fees).  The server
        # may store multiple rows per strategy_id (stitcher + C++ publisher);
        # keep only the latest per strategy_id (by db_event_ts) to avoid
        # double-counting, then sum across strategies.
        strat_rows = payload.get("strategy_metrics") or []
        strat_df = _df(strat_rows)

        def _dedup_latest(rows):
            """Keep the latest row per strategy_id (by db_event_ts)."""
            if not rows:
                return []
            by_sid = {}
            for row in rows:
                sid = row.get("strategy_id", "")
                prev = by_sid.get(sid)
                if prev is None:
                    by_sid[sid] = row
                else:
                    cur_ts = str(row.get("db_event_ts", ""))
                    prev_ts = str(prev.get("db_event_ts", ""))
                    if cur_ts > prev_ts:
                        by_sid[sid] = row
            return list(by_sid.values())

        latest_rows = _dedup_latest(strat_rows)

        def _strat_sum(key):
            """Sum a column across deduped strategy_metrics rows."""
            if not latest_rows:
                return None
            total = 0.0
            found = False
            for row in latest_rows:
                v = row.get(key)
                if isinstance(v, (int, float)):
                    total += float(v)
                    found = True
            return total if found else None

        total_realized = _strat_sum("realized_pnl")
        total_unrealized = _strat_sum("unrealized_pnl")
        total_fees_val = _strat_sum("total_commission") or _strat_sum("fees")
        net_pnl_val = _strat_sum("total_pnl")

        # Fills have no dedicated REST resource: prefer an explicit ``fills``
        # payload if the platform ever sends one, else derive the executed
        # subset from the orders frame (see _fills_from_orders).
        orders_df = _df(payload.get("orders"))
        fills_df = _df(payload.get("fills"))
        if fills_df is None or fills_df.empty:
            fills_df = _fills_from_orders(orders_df)

        return cls(
            return_stats=return_stats,
            returns_series=returns_series,
            positions=_df(payload.get("positions")),
            fills=fills_df,
            trades=_df(payload.get("trades")),
            orders=orders_df,
            daily_returns=daily_df,
            strategy_stats=strat_df,
            run_info=_df([payload.get("config")] if payload.get("config") else None),
            total_realized_pnl=total_realized if total_realized is not None else _num("Total Realized PnL", "realized_pnl"),
            total_unrealized_pnl=total_unrealized if total_unrealized is not None else _num("Total Unrealized PnL", "unrealized_pnl"),
            total_fees=total_fees_val if total_fees_val is not None else _num("Total Commission", "total_fees", "fees"),
            net_pnl=net_pnl_val if net_pnl_val is not None else _num("Net PnL", "Total PnL", "net_pnl"),
            run_id=payload.get("run_id"),
        )

    @classmethod
    def from_task_result(cls, payload: dict) -> "PerformanceReport":
        """Build a report from the platform task result snapshot.

        Fallback for deployed runs when the runs REST API has no per-resource
        rows yet (e.g. the executor sandbox runs a hiveq-flow without the
        run_id->payload_id unification). The platform's task result still
        carries the computed summary/return/pnl stats and scalar PnLs.
        """
        payload = payload or {}
        inner = payload.get('result') or {}
        stats = inner.get('result') if isinstance(inner.get('result'), dict) else inner
        stats = stats or {}

        def _frame(d):
            try:
                return pd.DataFrame(d) if d else None
            except Exception:
                return None

        def _num(*keys):
            for k in keys:
                v = stats.get(k)
                if isinstance(v, (int, float)):
                    return float(v)
            return 0.0

        return cls(
            return_stats=_frame(stats.get('return_stats')),
            pnl_stats=_frame(stats.get('pnl_stats')),
            strategy_stats=_frame(stats.get('strategy_stats')),
            total_realized_pnl=_num('total_realized_pnl'),
            total_unrealized_pnl=_num('total_unrealized_pnl'),
            total_fees=_num('total_fees'),
            net_pnl=_num('net_pnl'),
        )

    def __repr__(self):
        sections = []

        if self.run_info is not None:
            sections.append("=== Run Stats ===\n" + self.run_info.to_string(index=False))

        if self.pnl_stats is not None:
            sections.append("=== PnL Stats ===\n" + self.pnl_stats.to_string(index=False))

        if self.strategy_stats is not None:
            sections.append("=== Strategy Stats ===\n" + self.strategy_stats.to_string(index=False))

        if self.return_stats is not None:
            sections.append("=== Returns Stats ===\n" + self.return_stats.to_string(index=False))

        if self.tca_report is not None and self.tca_report.summary is not None:
            sections.append("=== TCA Summary ===\n" + self.tca_report.summary.to_string(index=False))

        return "\n\n".join(sections) if sections else "PerformanceReport: No data available"

    def _prepared_returns(self) -> Optional[pd.Series]:
        """Return a clean, UTC-datetime-indexed returns Series, or ``None``.

        Shared normalization for the tearsheet / metrics paths: pull the
        returns out of ``returns_series`` (Series or DataFrame), drop NaN/NaT,
        coerce the index to a tz-aware (UTC) DatetimeIndex. Returns ``None``
        when there is no usable returns data.
        """
        if self.returns_series is None:
            return None
        if isinstance(self.returns_series, pd.DataFrame):
            returns = self.returns_series.get('returns')
            if returns is None:
                return None
        else:
            returns = self.returns_series.copy()

        returns = returns.dropna()
        if returns.empty:
            return None

        if not isinstance(returns.index, pd.DatetimeIndex):
            try:
                returns.index = pd.to_datetime(returns.index)
            except Exception as e:
                logging.warning(f"Could not convert returns index to datetime: {e}")
                return None
        if returns.index.isna().any():
            returns = returns[~returns.index.isna()]
        if returns.empty:
            return None

        if returns.index.tz is None:
            returns.index = returns.index.tz_localize('UTC')
        else:
            returns.index = returns.index.tz_convert('UTC')
        return returns

    def _build_tearsheet_html(self, meta: Optional[dict] = None) -> str:
        """Build quantstats' full HTML report (with the run-metadata banner).

        Shared source for both the PDF and the standalone-HTML tearsheet writers.
        Generates a self-contained report — metrics formatted as percentages with
        decimals, charts embedded (non-interactive, so no JS) — then injects the
        print CSS and the ``meta`` banner (e.g. ``{"Run ID": ..., "Task": ...}``)
        under the title so the file can be traced back to its DB record.

        Raises ``ValueError`` if the run has no usable returns data,
        ``RuntimeError`` if quantstats produces nothing.
        """
        returns = self._prepared_returns()
        if returns is None:
            raise ValueError(
                "No returns data available for this run — cannot build a tearsheet."
            )

        import tempfile

        import quantstats as qs
        qs.extend_pandas()
        logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)
        warnings.filterwarnings('ignore')

        with tempfile.TemporaryDirectory() as tmp:
            html_path = os.path.join(tmp, "tearsheet.html")
            # Non-interactive => charts are embedded, so the HTML carries no JS and
            # converts cleanly under any backend (WeasyPrint can't run JS).
            qs.reports.html(
                returns,
                title="Strategy Performance Report",
                output=html_path,
                download_filename=None,
                display=False,
            )
            if not os.path.exists(html_path) or os.path.getsize(html_path) == 0:
                raise RuntimeError(
                    "quantstats did not produce an HTML report."
                )
            with open(html_path, "r", encoding="utf-8") as fh:
                html = fh.read()

        return _decorate_report_html(html, meta)

    def save_tearsheet_pdf(self, output: str, meta: Optional[dict] = None) -> str:
        """Render the quantstats HTML tearsheet to a PDF at ``output``.

        Generates quantstats' full HTML report (see :meth:`_build_tearsheet_html`)
        and converts it to PDF (headless browser if available, else WeasyPrint).
        This replaces the old matplotlib path, whose raw 2-decimal table
        ("Cumulative Return 0.01") and whole-percent chart axes were unreadable
        for low-volatility runs.

        Returns the path written. Raises ``ValueError`` if the run has no usable
        returns data, ``RuntimeError`` if no HTML->PDF backend is available.
        """
        import tempfile

        html = self._build_tearsheet_html(meta)
        os.makedirs(os.path.dirname(os.path.abspath(output)) or '.', exist_ok=True)

        with tempfile.TemporaryDirectory() as tmp:
            html_path = os.path.join(tmp, "tearsheet.html")
            with open(html_path, "w", encoding="utf-8") as fh:
                fh.write(html)
            _html_to_pdf(html_path, output)

        return output

    def save_tearsheet_html(self, output: str, meta: Optional[dict] = None) -> str:
        """Write the quantstats HTML tearsheet to a standalone file at ``output``.

        Same self-contained report as :meth:`save_tearsheet_pdf` (charts embedded,
        run-metadata banner included) — just left as ``.html`` to open in a
        browser instead of converting to PDF. For inline rendering inside a
        notebook use :meth:`create_tearsheet`, which returns the HTML *string*.

        Returns the path written. Raises ``ValueError`` if the run has no usable
        returns data.
        """
        html = self._build_tearsheet_html(meta)
        os.makedirs(os.path.dirname(os.path.abspath(output)) or '.', exist_ok=True)
        with open(output, "w", encoding="utf-8") as fh:
            fh.write(html)
        return output

    def create_tearsheet(self) -> str:
        """Create a quantstats HTML report with comprehensive performance analysis.

        Generates a detailed visual tearsheet using the quantstats library,
        including equity curves, drawdown charts, monthly returns heatmap,
        risk metrics, and trade statistics. The report is optimized for
        display in Jupyter notebooks or marimo.

        Returns
        -------
        str
            HTML string containing the complete tearsheet report for display
            in notebooks. Returns basic text report if not in a notebook
            environment. If returns data is not available, quantstats is not
            installed, or an error occurs, returns a simple HTML page with
            the title and an appropriate "No data" message.

        Notes
        -----
        Requires the quantstats package to be installed:

            pip install quantstats

        The method automatically detects if running in a Jupyter/marimo
        notebook and adjusts output format accordingly. HTML reports are
        saved to ~/.tmp/ directory with unique filenames.

        The returns_series attribute should be populated for a full tearsheet.
        If returns data is missing, a simple HTML page with "No data" message
        is returned instead.

        Examples
        --------
        Display tearsheet in a marimo notebook:

        >>> import hiveq.flow as hf
        >>> import marimo as mo
        >>> results = hf.run_backtest(strategy_configs, symbols=['AAPL'])
        >>> html = results.create_tearsheet()
        >>> mo.md(html)

        Display tearsheet in Jupyter notebook:

        >>> from IPython.display import display, HTML
        >>> html = results.create_tearsheet()
        >>> display(HTML(html))

        Get basic text report in Python script:

        >>> results = hf.run_backtest(strategy_configs, symbols=['AAPL'])
        >>> text_report = results.create_tearsheet()
        >>> print(text_report)

        See Also
        --------
        hiveq.flow.run_backtest : Run backtest and get PerformanceReport
        PerformanceReport : Container for backtest results
        """
        try:

            # Lazy import quantstats only when needed
            import quantstats as qs

            # Configure quantstats for better display in Jupyter
            qs.extend_pandas()
            # Suppress font warnings
            logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)
            # Suppress warnings from quantstats
            warnings.filterwarnings('ignore')

            # Extract cumpnl from the DataFrame if it's a DataFrame with 'cumpnl' column
            # and rename it to 'returns' for quantstats
            returns = None
            if self.returns_series is not None:
                if isinstance(self.returns_series, pd.DataFrame):
                    returns = self.returns_series['returns']
                else:
                    # It's already a Series
                    returns = self.returns_series.copy()

            # Ensure returns has datetime index and handle timezone
            if returns is not None:
                # Drop any NaN or NaT values
                returns = returns.dropna()

                if returns.empty:
                    logging.info("No valid returns data available after dropping NaN values")
                    return _create_no_data_html(message="No valid returns data available")

                if not isinstance(returns.index, pd.DatetimeIndex):
                    try:
                        returns.index = pd.to_datetime(returns.index)
                    except Exception as e:
                        logging.warning(f"Warning: Could not convert returns index to datetime: {e}")
                        return _create_no_data_html(message="Could not process returns data")

                # Drop any NaT values from the index
                if returns.index.isna().any():
                    logging.warning("Dropping rows with NaT timestamps")
                    returns = returns[~returns.index.isna()]

                if returns.empty:
                    logging.info("No valid returns data available after dropping NaT timestamps")
                    return _create_no_data_html(message="No valid returns data available")

                # Make sure returns index is timezone-aware (UTC)
                if returns.index.tz is None:
                    returns.index = returns.index.tz_localize('UTC')
                else:
                    returns.index = returns.index.tz_convert('UTC')
            else:
                logging.info("No returns data available")
                return _create_no_data_html(message="No returns data available")

            # quantstats works with returns directly, no need for complex position formatting
            if utils.is_notebook():
                # Enable Jupyter notebook display mode
                from IPython.display import display, HTML

                # temporary file for the html report
                import tempfile, os
                # Ensure ~/.tmp directory exists
                tmpdir = os.path.expanduser("~/.tmp")
                os.makedirs(tmpdir, exist_ok=True)

                # Generate unique temporary file path inside ~/.tmp
                tmp_file = os.path.join(tmpdir, f"qs_report_{uuid.uuid4().hex}.html")
                # Force matplotlib to display plots in Jupyter
                qs.reports.html(
                    returns,
                    interactive=True,
                    title="Strategy Performance Report",
                    output=tmp_file,  # avoid writing to file
                    download_filename=None,
                    display=False  # <--- forces inline display
                )

                if os.path.exists(tmp_file):
                    # read the file and show inside the cell
                    with open(tmp_file, "r", encoding="utf-8") as f:
                        html = f.read()
                    # Delete the temporary file after reading
                    os.remove(tmp_file)
                    html = html.replace('<h4> Generated by <a href="http://quantstats.io" target="quantstats">QuantStats</a> (v. 0.0.77)</h4>', '')
                    return html
            else:
                return qs.reports.basic(returns)

        except ImportError:
            logging.warning("Error: quantstats is not installed. Please install it with: pip install quantstats")
            return _create_no_data_html(message="quantstats is not installed. Please install it with: pip install quantstats")
        except Exception as e:
            logging.exception(f"Error generating tear sheet: {e}")
            return _create_no_data_html(message=f"Error generating tearsheet: {str(e)}")

    def summary_stats(self):
        # Lazy import quantstats only when needed. quantstats is a declared
        # dependency, but guard anyway so a broken/absent install degrades to
        # None + a clear message instead of an unhandled ImportError.
        try:
            import quantstats as qs
        except ImportError:
            logging.warning(
                "quantstats is not installed. Please install it with: pip install quantstats"
            )
            return None

        # Configure quantstats for better display in Jupyter
        qs.extend_pandas()
        # Suppress font warnings
        logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)
        # Suppress warnings from quantstats
        warnings.filterwarnings('ignore')

        # Extract cumpnl from the DataFrame if it's a DataFrame with 'cumpnl' column
        # and rename it to 'returns' for quantstats
        returns = None
        if self.returns_series is not None:
            if isinstance(self.returns_series, pd.DataFrame):
                returns = self.returns_series['returns']
            else:
                # It's already a Series
                returns = self.returns_series.copy()

        # Ensure returns has datetime index and handle timezone
        if returns is not None:
            # Drop any NaN or NaT values
            returns = returns.dropna()

            if returns.empty:
                logging.info("No valid returns data available after dropping NaN values")
                return

            if not isinstance(returns.index, pd.DatetimeIndex):
                try:
                    returns.index = pd.to_datetime(returns.index)
                except Exception as e:
                    logging.warning(f"Warning: Could not convert returns index to datetime: {e}")
                    return

            # Drop any NaT values from the index
            if returns.index.isna().any():
                logging.warning("Dropping rows with NaT timestamps")
                returns = returns[~returns.index.isna()]

            if returns.empty:
                logging.info("No valid returns data available after dropping NaT timestamps")
                return

            # Make sure returns index is timezone-aware (UTC)
            if returns.index.tz is None:
                returns.index = returns.index.tz_localize('UTC')
            else:
                returns.index = returns.index.tz_convert('UTC')
        else:
            logging.info("No returns data available")
            return
        metrics_df = qs.reports.metrics(returns, display=False)
        if metrics_df is not None and len(metrics_df) > 0:
            metrics_dict = metrics_df.to_dict()
            if 'Strategy' in metrics_dict:
                return metrics_dict['Strategy']
        return None