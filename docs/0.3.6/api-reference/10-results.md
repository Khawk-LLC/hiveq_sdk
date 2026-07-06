## 10. Results

### 10.0 `Run` handle (returned by `run_backtest` / `get_run`)
`run_backtest(...)` and `get_run(...)` return a `Run` (module `hiveq.flow.runs`). It is the single accessor for a run's status and results, local or remote.

```python
run.run_id -> str
run.task_id -> Optional[str]
run.is_local -> bool
run.wait(timeout=None, poll_interval=1.0, progress=True) -> Run   # block until terminal; returns self (chainable)
run.status() -> dict                       # {'status': 'PENDING'|'RUNNING'|'DONE'|'FAILED'|..., ...}
run.report(include: Optional[list[str]] = None) -> PerformanceReport   # §10.1
run.positions() / run.orders() / run.trades() / run.daily_returns() / run.equity_curve() / run.metrics() / run.event_logs() -> pandas.DataFrame
run.summary() -> dict
run.overview() -> dict
run.tearsheet(output: Optional[str] = None) -> str   # writes a quantstats tearsheet file; returns the path. Format from extension: .html -> HTML, anything else -> PDF. Default name: <task_name|run_id>.pdf
run.logs() -> list[str]                    # the COMPLETE remote executor log (stdout/strategy errors), by task_id
run.download_logs(path: str) -> str        # stream the full gzipped executor log to `path` (.gz); returns path
```
- Default `run_backtest()` already blocked until done, so `run.report()` is ready immediately.
- **`hf.get_run(run_id, task_id=None)`** re-attaches to a run that was already submitted and returns its `Run` handle. `run_backtest` runs on the platform, so its results outlive your Python process — `get_run` is how you reconnect later (a new session, a different machine, or after a `silent=True` deploy) to check status or pull results without re-running anything. Pass the `run_id` from an earlier `run.run_id`. Typical use: `hf.get_run(run_id).wait().report()`.
- **`event_logs()` vs `logs()`**: `event_logs()` is the strategy's structured event-log table from the runs REST API (`ctx.add_event_log(...)` rows, keyed by run_id). `logs()` is the raw executor **stdout** — `print(...)` output and strategy-callback crashes (e.g. `STRATEGY_CALLBACK_ERROR`) that never reach `event_logs()` — fetched as the whole gzip log by **task_id** (`run_id != task_id`). Use `logs()` to debug a run that produced nothing; `download_logs(path)` for very large logs.

### 10.1 `PerformanceReport` (obtained via `run.report()`)
| attribute | type |
|---|---|
| `return_stats` | DataFrame (Sharpe, Sortino, vol, drawdown, win rate, …) |
| `returns_series` | Series (datetime-indexed equity returns; used for tearsheet) |
| `positions` | DataFrame |
| `fills` | DataFrame |
| `orders` | DataFrame |
| `trades` | DataFrame |
| `pnl_stats` | DataFrame |
| `daily_returns` | DataFrame (ascending by date) |
| `strategy_stats` | DataFrame (per-strategy) |
| `run_info` | DataFrame (dates, capital, counts, params) |
| `tca_report` | TCAReport (only if `BacktestConfig.enable_tca=True`) |
| `total_realized_pnl` / `total_unrealized_pnl` / `net_pnl` / `total_fees` | float |
| `create_tearsheet()` | `-> str` (quantstats HTML tearsheet; for Jupyter/Marimo) |
| `summary_stats()` | `-> dict \| None` (quantstats metric → value, e.g. Sharpe/CAGR/max drawdown) |

DataFrame attrs may be `None`/empty — always guard (`if report.fills is not None and not report.fills.empty`).

> ⚠️ **Phantom PnL from stuck-open positions.** At end-of-backtest the engine
> liquidates any still-open position and books the mark-to-market difference as
> **realized** PnL in `total_realized_pnl` / `net_pnl`. If your exit logic
> silently failed (e.g. the cancel + close race in §5.2), the report will show
> a plausible-looking positive/negative PnL that is *not from your strategy* —
> it's from the underlying's price move between your intended exit date and
> the last day of the backtest. **Canary checks to catch this:**
> - `return_stats["Total Trades"] == 0` alongside `net_pnl != 0` is the loudest
>   signal — "Total Trades" counts completed round-trips, not fills.
> - `report.positions` (or `run.positions()`) row with `avg_px_close == 0`
>   means that position never closed cleanly during the run.
> - `report.trades` row with `exit_ts == '1970-01-01'` (null sentinel) means
>   the round-trip was never completed.
> Always eyeball these three columns before trusting the top-line PnL.

**Tearsheet / metrics (quantstats).** `create_tearsheet()` and `summary_stats()` are powered by **quantstats**, which ships with the SDK (no extra install). Both read `report.returns_series`, so they need a run that produced returns; on missing/empty returns `create_tearsheet()` returns a small "no data" HTML page and `summary_stats()` returns `None`.

> **To write a tearsheet FILE, always call `run.tearsheet()` — never `report.create_tearsheet()`.** `run.tearsheet()` **defaults to PDF** (no `output=` needed). `create_tearsheet()` is a *different, notebook-only* method: it returns an HTML/text string for inline display and is not a way to produce a saved report — do not take its return value and write it to `my_report.html` yourself; that is not "the tearsheet defaulting to HTML", that is calling the wrong method. If you want a file on disk, `output=` is optional and PDF is the default; pass `output='x.html'` only if you deliberately want HTML instead.

```python
report = run.report()                 # or hf.get_run(run_id).wait().report()

# Tearsheet FILE on disk (equity curve, drawdowns, monthly returns, risk metrics).
# run.tearsheet() is the single entry point for this — the format is chosen from
# the output extension: `.html` writes standalone HTML, anything else (including
# no output= at all) writes a PDF. PDF is the default.
path = run.tearsheet()                       # -> '<task_name|run_id>.pdf' in the cwd (PDF, default)
path = run.tearsheet(output='my_report.pdf')
path = run.tearsheet(output='my_report.html')  # explicit opt-in to HTML

# report.create_tearsheet() is NOT a file-saving method — it returns an HTML/text
# STRING for inline rendering inside a notebook cell only. Never use it to produce
# your tearsheet artifact.
html = report.create_tearsheet()
# In a Marimo notebook:
import marimo as mo; mo.md(html)
# In a Jupyter notebook:
from IPython.display import display, HTML; display(HTML(html))
# In a plain script create_tearsheet() returns a basic text report instead.

# Metrics as a dict (Sharpe, CAGR, max drawdown, …)
stats = report.summary_stats()        # None if the run has no returns_series
if stats:
    print(stats["Sharpe"], stats["Max Drawdown"])
```

### 10.2 `event_logs()` DataFrame columns (remote runs)
`time(datetime, tz-aware)` · `ts_event(str, ISO-8601 UTC)` · `strategy_id` · `trader_id` · `nav(float)` · `realized_pnl(float)` · `total_pnl(float)` · `symbol` · `event_log_type(str)` · `sub_event_type(str)` · `message(str)` · `state_variables(str, JSON)` · `trade_id(str?)`
> `event_logs()` returns rows for **remote** runs (fetched over REST). A **local** run returns an empty DataFrame (`runs.py` short-circuits when `run.is_local`). You can also pull logs for any job via §11.3.

---

