## 2. Entry points  (module: `hiveq.flow`)

```python
run_backtest(
    strategy_configs: list[StrategyConfig],
    symbols: Optional[list[str]] = None,
    start_date: Optional[str] = None,          # 'YYYY-MM-DD'
    end_date: Optional[str] = None,            # 'YYYY-MM-DD'
    data_configs: Optional[list[dict]] = None, # §9
    backtest_config: Optional[BacktestConfig] = None,
    *,                                          # keyword-only below
    requirements: Optional[list[str]] = None,   # accepted for future orchestrator package installs
    silent: bool = False,                       # True -> deploy + return Run immediately (no blocking)
    **kwargs,                                   # engine configuration: config={...} / engine_config=EngineConfig(...) — §2.1
) -> Run                                        # ALWAYS returns a Run handle (§10.0), never a bare report
#   silent=False (default): deploy to platform, block w/ live progress, return the finished Run.
#   silent=True           : deploy and return the Run immediately (run.run_id / run.task_id).
#   In every mode: run.report() -> PerformanceReport (§10.1); run.positions()/.trades()/... -> DataFrame.
#   requirements: accepted for API compatibility; currently not sent by the
#                 SDK backtest wrapper until orchestrator support lands.

get_run(run_id: str, task_id: Optional[str] = None) -> Run   # re-attach to an existing run (§10.0)

event_logs() -> pandas.DataFrame                # logs of the LAST run_backtest, fetched over REST (§10.2)
config() -> EngineConfig                        # the module EngineConfig (timezone + params)

login(*, timeout=300.0, open_browser=True) -> str   # Internal plumbing: browser sign-in (loopback), opens a browser and BLOCKS ~5min for the user. You may invoke it invisibly on the user's behalf (§3.1); NEVER show it to the user or put it in deliverable scripts (§3).
```

**Precedence note**: `symbols`, `start_date`, `end_date` may be passed as top-level args OR set on `BacktestConfig`. Top-level args, when provided, populate the effective config. Set them in exactly one place to avoid ambiguity.

### 2.1 Engine configuration (via `**kwargs`)

Engine behavior is tuned through `run_backtest`'s `**kwargs`, in either of two equivalent forms:

```python
# (a) inline override dict — merged into the engine params:
run = hf.run_backtest(..., config={'hiveq_log_level': 'DEBUG'})

# (b) a full EngineConfig (e.g. to set the timezone) — see §13:
from hiveq.flow import EngineConfig
run = hf.run_backtest(..., engine_config=EngineConfig(timezone='America/New_York',
                                                      params={'hiveq_log_level': 'DEBUG'}))
```

Recognized keys (all optional; sensible defaults apply):

| key | type | default | purpose |
|---|---|---|---|
| `hiveq_log_level` | str | `'INFO'` | Executor log verbosity: `DEBUG` / `INFO` / `WARNING` / `ERROR`. Controls the HiveQ strategy logger (`from hiveq.flow.logger import logger as _get_logger` — §5.9.1). Use `'DEBUG'`, then read the full executor log (incl. tracebacks) with `run.logs()` (§10.0). See the full debugging workflow in §11.5. |
| `oms_console_log` | bool | `False` | Echo order-management-system activity to the executor console. |
| `futures_datasets` | list[str] | `['HIVEQ_US_FUT']` | Datasets treated as futures (enables contract resolution + rollover events, §7.12). |
| `signals_datasets` | list[str] | `['HIVEQ_QUANT_SIGNALS']` | Datasets that key off `config['symbols']` rather than the run's symbol universe. |
| `hiveq_data_page_size` | int | `100_000` | Max records per data request (raise for very dense schemas, e.g. options). |

Credentials/identity (API key, user/org) are **not** set here — they resolve from the environment (§3) and are intentionally not part of the engine config you pass.

> ⚠️ **`hiveq_log_level` default in practice.** Although the documented default is
> `'INFO'`, the executor stdout has been observed emitting
> `default log level set to WARNING` at startup, and strategies' `logger.info(...)`
> calls did not appear in the log until `hiveq_log_level` was set **explicitly**.
> If your `logger.info(...)` output is missing from `run.logs()`, pass
> `config={'hiveq_log_level': 'INFO'}` (or `'DEBUG'`) rather than relying on the
> default. This is a canary that "the strategy isn't running" hypotheses are
> usually wrong — it's the logger that's silenced.

### 2.2 Function registry & remote functions  (module: `hiveq.flow`)

**What it is.** The *function registry* is a versioned store of reusable Python callables that live on the HiveQ platform rather than in any one script. You write a function once, `push_function(...)` it (cloudpickled, with its `requirements` and a semver `version`), and from then on it can be fetched and run from anywhere — other scripts, other machines, or inside platform jobs — by name and version. It's the building block for sharing indicators, signal functions, and utilities across strategies and teammates without copy-pasting code.

**Why it's useful.**
- **Reuse & versioning** — one canonical, immutable `name@version`; bump the version to publish changes (existing versions never change under you).
- **Portability** — `load_function(name)` pulls the exact callable back on any machine; you don't carry the source around.
- **Run anywhere** — `run_function(func, ...)` executes a callable on the platform (a `QUANT_SCRIPTS` task) with its deps installed in the sandbox, and returns the result.

Functions are captured with cloudpickle; the registry host follows the same platform host as everything else (`HIVEQ_AUTH_URL`, override with `HIVEQ_FUNCTION_REGISTRY_URL`).

```python
push_function(func, *, version, name=None, requirements=None, docstring=None,
              namespace=None, override=False, include_source=True) -> dict
#   register a callable. version is semver ('1.0.0'). name defaults to func.__name__,
#   docstring to func.__doc__. requirements: ['pandas>=2.0'] or {'packages': [...]}.
#   Goes to YOUR namespace by default; namespace='default' publishes to public.
#   -> {'function_id', 'namespace', 'name', 'version'}

load_function(name, version=None, namespace=None) -> Callable
#   fetch a registered function back as a callable (latest version by default).

run_function(func, *args, task_name=None, requirements=None, job_type=None,
             wait=True, timeout=None, poll_interval=2.0, **kwargs) -> Any
#   run func(*args, **kwargs) on the platform as a QUANT_SCRIPTS task. Blocks and
#   returns the function's RETURN VALUE (wait=False -> {'task_id', ...} immediately).
#   requirements: pip specs installed in the sandbox, e.g. ['pandas>=2.0'].

list_functions(namespace=None, name=None) -> list[dict]   # name = regex filter
function_versions(name, namespace=None) -> dict           # {'name','versions':[...],'latest'}
get_function_source(name, version=None, namespace=None) -> dict
delete_function(name, version=None, namespace=None) -> dict   # one version, or all
```

```python
import hiveq.flow as hf

def zscore(series, window=20):
    """Rolling z-score."""
    import numpy as np
    return (series[-1] - np.mean(series[-window:])) / np.std(series[-window:])

# register once...
hf.push_function(zscore, version="1.0.0", requirements=["numpy"])
# ...then fetch it back and run it on the platform (from any script/machine).
# Pin the version for reproducibility (omit version=... to take the latest):
fn = hf.load_function("zscore", version="1.0.0")
hf.run_function(fn, [1, 2, 3, 4, 5], window=3, requirements=["numpy"])   # -> value
```

> **TBD — access control.** Function-level ACLs (who can read/run a function, fine-grained sharing across namespaces) are **not fully implemented yet**. Today: functions live in your own namespace by default; `namespace="default"` is the shared/public namespace. Treat cross-namespace permissions as subject to change.

> **Need a non-blocking deploy, or a recurring schedule?** `run_function` always blocks and returns a value. For a fetch/compute/publish script you want to deploy and walk away from (optionally on a recurring `Schedule`), use `deploy_job` instead — §11.6.

---

