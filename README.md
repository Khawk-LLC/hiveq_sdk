# hiveq-flow-sdk

The **thin client SDK** for HiveQ Flow. You author a strategy against the type
surface here and deploy it; the HiveQ platform runs the engine and returns
results. **No engine code ships to the client** — the proprietary `oms/sigma`
core, PySigma, and the backtest run loop live only in the full `hiveq-flow`
package installed on the platform executor.

It provides the **same `hiveq.flow` import namespace** as the full package, so
strategy code is identical whether it runs on your machine (deploy) or on the
executor (run).

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig, BacktestConfig

run = hf.run_backtest(
    [StrategyConfig(name="S", type="MyStrategy")],
    symbols=["AAPL"], start_date="2025-01-01", end_date="2025-01-31",
)
run.report(); run.positions(); run.daily_returns()
```

Credentials come from the environment: `HIVEQ_API_KEY`, `HIVEQ_USER_NAME`,
`HIVEQ_USER_ID`, `HIVEQ_ORG_ID`.

## What's in here

**Real (runs on the client):**
- `__init__.py` — `run_backtest` / `deploy_backtest` / `get_run` / `config`
- `deploy_task.py` — capture your strategy (cloudpickle) → submit to the
  orchestrator REST API. `run()` is a client stub: the task is pickled *by
  reference*, so the executor runs its own full `run()`.
- `runs.py`, `data/reader.py`, `metrics/report.py` — observe results (REST)
- `jobs.py`, `logger/`, `utils/`
- `config.py` — `StrategyConfig` / `BacktestConfig` / `EngineConfig` + enums

**Stubs (authoring surface — used only inside strategy callbacks on the executor):**
- `oms/sigma/sigma_context.py` + `sigma_context.pyi` — `SigmaContext` is a type
  stub; the real PySigma-backed context exists only on the executor.
- `events/`, `oms/sigma/types/` (Order/Position/Fill/Portfolio/Bar/…),
  `context.py` — lightweight, engine-free authoring types.

## Important

This distribution **provides the `hiveq.flow` namespace** and must **not** be
co-installed with the full `hiveq-flow` package (they collide). Clients install
`hiveq-flow-sdk`; the platform executor installs the full `hiveq-flow`.

## Maintenance

The engine-free modules are synced from the full repo by
`hiveq-flow/scripts/build_sdk.py` (single source of truth). Hand-authored thin
files (`__init__.py`, `deploy_task.py`, the `oms/sigma` package init, the
`sigma_context` stub) are never overwritten by the sync.
