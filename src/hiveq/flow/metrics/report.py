import logging
import uuid
from dataclasses import dataclass
import pandas as pd
import warnings
from typing import Optional, TYPE_CHECKING

from hiveq.flow import utils

if TYPE_CHECKING:
    from hiveq.flow.tca.types import TCAReport


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

        return cls(
            return_stats=return_stats,
            returns_series=returns_series,
            positions=_df(payload.get("positions")),
            trades=_df(payload.get("trades")),
            orders=_df(payload.get("orders")),
            daily_returns=daily_df,
            strategy_stats=_df(payload.get("strategy_metrics")),
            run_info=_df([payload.get("config")] if payload.get("config") else None),
            total_realized_pnl=_num("Total Realized PnL", "realized_pnl"),
            total_fees=_num("Total Commission", "total_fees", "fees"),
            net_pnl=_num("Net PnL", "Total PnL", "net_pnl"),
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