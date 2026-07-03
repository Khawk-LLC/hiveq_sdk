## 11. Remote deploy + observability  (`hiveq.flow.jobs`)

One surface to deploy a job and pull its status/logs/results. A thin **direct-REST** client (built on `requests`) against the platform API — there is no `hiveq_orchestrator` package or any second install. Uses the same API key (§3).

```python
from hiveq.flow.jobs import (
    TaskType,
    submit,                                     # deploy
    poll_result, get_status, get_result, get_logs, get_logs_gz, get_client,  # observe
)
```

### 11.1 Deploy a backtest (high-level)
Prefer **`hf.run_backtest(..., silent=True)`** (§2) — it captures your strategy class automatically and returns a `Run` handle immediately (`run.run_id`, `run.task_id`). Observe via the Run (`run.wait()`, `run.status()`, `run.report()`, §10.0) — no need to touch the lower-level `jobs` API for the common case.

```python
run = hf.run_backtest(strategy_configs=[...], symbols=['AAPL'],
                      start_date='2025-08-01', end_date='2025-08-02',
                      data_configs=[{'type':'hiveq_historical','dataset':'HIVEQ_US_EQ','schema':['bars_1m']}],
                      silent=True)
report = run.wait().report()                    # block to completion, then read results
```
For the common backtest case use `run_backtest(..., silent=True)` above; the `jobs` API below remains valid for any job type and for low-level control.

### 11.2 Generic submit
```python
submit(task_type: TaskType|str, task_name: str, task,
       entry_method: str = 'run', job_type=None,
       metadata=None, requirements=None,
       allow_duplicate: bool = False, duplicate_action=None) -> dict   # -> {'task_id', 'payload_id', ...}
#   duplicate_action ∈ {'override', 'terminate', 'duplicate'} (only consulted when allow_duplicate=True)
```

### 11.3 Observe (works for any job type)
```python
get_status(task_id: str) -> dict                # {'status': 'PENDING'|'RUNNING'|'DONE'|'FAILED'|..., 'created_at', 'started_at', 'completed_at'}
get_logs(task_id: str = None, task_name: str = None, limit: int = 1000) -> dict   # remote executor logs, JSON tail/paginate
get_logs_gz(task_id=None, run_id=None, task_name=None, dest: str = None) -> str    # FULL log via GET /logs?format=gz (streamed)
get_result(task_id: str) -> dict                # result/output (non-blocking; for completed task)
poll_result(task_id: str, timeout: Optional[int] = None, poll_interval: float = 1.0) -> dict  # BLOCK until terminal state
get_client() -> _Client                         # low-level transport handle (advanced)
```
- The platform `GET /logs` accepts `task_id` | `run_id` | `task_name` (one required), plus `limit` / `offset` / `tail` / `format` (`json` default, or `gz` for the whole gzipped log). `get_logs(...)` returns the JSON tail; `get_logs_gz(...)` returns the **complete** log text (or, with `dest`, streams the `.gz` to that path). Prefer the `Run` handle — `run.logs()` / `run.download_logs(path)` (§10.0) — which key off the run's `task_id` for you.

### 11.4 Recommended observe loop (poll status + pull logs)
```python
from hiveq.flow.jobs import get_logs

# Preferred: deploy and observe via the Run handle (§10.0).
run = hf.run_backtest(strategy_configs=[...], symbols=['ES.c.0'],
                      start_date='2024-01-01', end_date='2024-03-01',
                      data_configs=[{'type':'hiveq_historical','dataset':'HIVEQ_US_FUT',
                                     'schema':['bars_1m']}],
                      silent=True)
run.wait()                                       # blocks w/ live progress until terminal
print('status:', run.status()['status'])
logs = get_logs(task_id=run.task_id, limit=500)  # for debugging / course-correction
report = run.report()                            # PerformanceReport on success (§10.1)
```
> Incremental mid-run P&L is not streamed today; status transitions are PENDING→RUNNING→DONE, with the report available on completion. Use `get_logs` for in-flight diagnostics.

### 11.5 Debugging strategies

When a strategy produces no trades or misbehaves, follow this workflow:

**Step 1 — Instrument the strategy with logs before you run it.** Every callback and decision branch should have a `logger.debug(...)` call (§5.9.1). Milestone events (pattern detected, signal set, position opened) also warrant a `ctx.add_event_log(...)` row. If logging is sparse, add it now — re-running is cheaper than guessing.

**Step 2 — Re-run with DEBUG level and OMS echoing on:**
```python
run = hf.run_backtest(
    ...,
    config={'hiveq_log_level': 'DEBUG', 'oms_console_log': True},
)
```

**Step 3 — Guard `run_backtest` so the executor doesn't recurse.** The executor re-imports your script to restore the strategy class. Any unguarded `run_backtest(...)` at module level fires again inside the executor with no data, producing 0 bars. Wrap it:
```python
import os
if __name__ == "__main__" and os.environ.get("HIVEQ_SDK_CLIENT_RUN") == "1":
    run = hf.run_backtest(...)
```

**Step 4 — Read the raw executor log** (includes all `logger.*` output and any tracebacks):
```python
logs = run.logs()                       # list[str] — the complete executor stdout
# OR via jobs API (larger limit):
from hiveq.flow.jobs import get_logs
logs = get_logs(task_id=run.task_id, limit=5000)
for entry in logs.get('logs', []):
    print(entry)
```

**Step 5 — Read structured event logs** (milestone events written with `ctx.add_event_log`):
```python
df = run.event_logs()    # pandas DataFrame; columns: time, symbol, message, …
print(df[['time','symbol','message']])
```

**Common root causes checklist:**
- `on_bar` fires but no orders: the strategy has logging but no actual `ctx.buy_order(...)` call on the signal path — trace with `logger.debug` to confirm the entry condition is reached.
- 0 bars received: unguarded `run_backtest` at module level (Step 3 above).
- `logger.debug` silent: using `logging.getLogger(__name__)` instead of the HiveQ logger (§5.9.1).
- **`logger.info` silent too**: the executor's *actual* default log level has been observed at `WARNING` even though the doc default is `INFO`. Pass `config={'hiveq_log_level': 'INFO'}` (or `'DEBUG'`) explicitly — see §2.1.
- Pattern detected but no trade: detection fires on the last bar of the backtest — extend the date range so there are subsequent bars on which to act.
- **Duplicate `on_bar` deliveries (framework bug, fix pending)**: over multi-week runs the engine has been observed firing `on_bar` 2×–3× for the same daily bar (identical `ts_event`, identical OHLCV, back-to-back within one tick). This is **NOT** caused by per-session re-init (`__init__` runs once — see §16.2) and **NOT** caused by subscription accumulation (`on_start` re-subscribes are deduped internally). It is a framework bug currently pending a fix. Symptoms: rolling deques and SMAs silently drift; strategies that produced trades in a short backtest produce different (or no) trades over longer windows. **Do not** add a permanent `ts_event` dedup to production strategy code; if you must work around it while the fix is in flight, mark it clearly as a temporary local workaround.
- **Silent stuck position → phantom PnL**: `return_stats["Total Trades"] == 0` alongside `net_pnl != 0`, or a `positions` row with `avg_px_close == 0`, or a `trades` row with `exit_ts == '1970-01-01'`. Almost always the cancel + close race in §5.2 — your exit path returned `None` and end-of-backtest liquidation booked the position's mark-to-market. Use the two-phase exit pattern.

### 11.6 Deploy an arbitrary script/job (`QUANT_SCRIPTS`) — `deploy_job`

For a plain fetch/compute/publish script (not a `hiveq.flow` strategy) — e.g. pull data with `hiveq.dd.load(...)` and push a signal with `hiveq.dd.save(...)`. This is the `QUANT_SCRIPTS` counterpart to `run_backtest`: same cloudpickle-and-REST capture, no platform-internal package. Unlike `run_function` (§2.2, which blocks and returns the function's value), `deploy_job` is a **stub like `deploy_backtest`** — it submits and returns a handle immediately (`wait=False` is the default).

> **When this is the right tool.** `deploy_job` is for *after* the script's logic is written and working, not for developing it. Write and iterate on the function itself first — call it directly, inspect its output, fix bugs — the ordinary way you develop any Python function, using `hiveq_data` (§14.1) for any data access you need to exercise while iterating (`hiveq.dd`, by contrast, is a stub client-side; its `load`/`save` only do real work once the function is actually running on the platform executor, §14.1). Reach for `deploy_job` once the function is correct and you're ready to run it on the platform itself — either as a one-off to validate it under real platform conditions (sandboxed execution, real data access, real credentials) before committing to a schedule, or to put it into production as a one-off or recurring (`schedule`d) job. It is not a development loop — each call is a full platform round-trip (submit → sandbox → logs), much slower than local iteration.

> **Config: `HIVEQ_API_KEY` only — nothing else.** Exactly like `run_backtest`, identity and data access resolve **server-side** from the API key alone (§3). Do **not** generate code that sets `HIVEQ_DATA_URL`, `HIVEQ_USER_ID`, `HIVEQ_ORG_ID`, or `HIVEQ_USER_NAME` for a `deploy_job` call — none of them are read anywhere on this path. `HIVEQ_DATA_URL` in particular is a `run_backtest`-only concern (it gets baked into the `EngineConfig` the backtest payload carries); `deploy_job` never builds an `EngineConfig`, and the executor gets its own `HIVEQ_DATA_URL` injected server-side regardless of what the client sends. `HIVEQ_BASE_URL` only needs to be set to target a non-default platform host, exactly as with `run_backtest` — not something specific to `deploy_job`.

```python
from hiveq.flow.jobs import deploy_job, Job, Schedule, ScheduleFrequency

deploy_job(
    func: Callable, *,
    task_name: str = None,              # defaults to 'job-<func name>-<short id>'
    args: tuple = None, kwargs: dict = None,
    requirements: list[str] = None,     # pip specs installed in the sandbox — best-effort (see caveat below)
    schedule: Schedule | dict = None,   # recurring execution instead of a one-off run
    job_type: str = None,
    metadata: dict = None,
    wait: bool = False,                 # True -> block for a terminal result before returning
    allow_duplicate: bool = True, duplicate_action: str = 'override',
) -> Job

class Job:                              # lighter than Run (§10.0) — no metrics/positions/trades
    task_id: str; task_name: str
    status() -> dict                    # full status; falls back to get_result() if the
                                         # ClickHouse-backed /status route has no row yet
                                         # (e.g. a schedule that hasn't fired its first run)
    result() -> dict                    # GET /result/{task_id}
    logs(limit: int = 1000) -> dict     # tail
    download_logs(dest: str = None) -> str   # full gzipped log
    wait(timeout=None, poll_interval=2.0) -> dict   # block for terminal state (not meaningful
                                         # for a recurring `schedule` job — it never reaches one)
    terminate() -> dict                 # cancels the schedule too, if any
```

```python
import hiveq.flow as hf   # credentials: fully automatic, see §3 — do not add any setup for this

def fetch_aapl():
    from hiveq import dd
    from hiveq.dd import DateRange

    df = dd.load(dataset="HIVEQ_US_EQ", schema="bars_1d", symbols=["AAPL"],
                 date=DateRange("2025-08-01", "2025-08-08"))
    print(f"Fetched {len(df)} rows for AAPL")
    print(df.to_string())     # ends up in job.logs() / job.download_logs()
    return {"rows": len(df)}  # ends up in job.result()['result']

# One-off, blocks until it finishes (wait=True), then inspect result + logs:
job = hf.deploy_job(fetch_aapl, task_name="fetch-aapl-once", wait=True)
job.result()   # -> {'status': 'completed', 'result': {'rows': 6}, ...}
job.logs()     # -> the printed DataFrame, exactly as it ran on the executor

# Recurring — fires every day at 16:05 US/Eastern, non-blocking (the default):
job = hf.deploy_job(
    fetch_aapl,
    task_name="fetch-aapl-scheduled",
    schedule=hf.Schedule(frequency=hf.ScheduleFrequency.DAILY, start_time="16:05",
                         timezone="US/Eastern"),
)
job.status()   # -> {'status': 'scheduled', ...} — check back later; won't reach a terminal state itself

# A fetch + publish version follows the same shape:
def fetch_and_publish(multiplier=2):
    from hiveq import dd
    df = dd.load(dataset="HIVEQ_US_EQ", schema="bars_1d", symbols=["AAPL"])
    dd.save(df, schema="quant_features", key="my_signal")
    return {"rows": len(df)}

job = hf.deploy_job(fetch_and_publish, task_name="daily-signal", requirements=["pandas"])
```

`Schedule` fields: `frequency` (`ScheduleFrequency`, §12), `start_time` (`"HH:MM"`/`"HH:MM:SS"`), `timezone` (default `"UTC"`), `days_of_week` (list of `0`=Mon..`6`=Sun, for `WEEKLY`/`INTERVAL`), `day_of_month` (for `MONTHLY`), `end_time`/`interval_minutes` (for `INTERVAL`), `end_date` (`"YYYY-MM-DD"`, stops the schedule entirely), `enabled` (default `True`). A plain dict with the same keys works too — `Schedule` is a convenience dataclass with `.to_dict()`.

> **`requirements` caveat.** The client wires `requirements` into the sandbox in the shape the executor expects, and the executor runs `pip install` with them. Whether the install **succeeds** depends on the sandbox's package index being reachable for what you ask for; treat actual installation as environment-dependent rather than guaranteed by the SDK itself.

> **`status()` fallback.** `GET /status/{task_id}` is backed by a task-history pipeline that may not have a row yet for every deployment target — `Job.status()` falls back to `get_result()` automatically in that case, so this is transparent in normal use; it's only worth knowing if you drop to the low-level `get_status()` directly (§11.3).

---

