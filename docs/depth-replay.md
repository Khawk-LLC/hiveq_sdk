# Futures depth / L3 backtests

Run your strategy with recorded `HIVEQ_US_FUT / fut_trades` events and a
Databento MBO order book feeding `DepthPaperBroker`. Trades remain dataset
inputs; depth replay does not replace them or generate trade callbacks.

## Run one day

The complete, generic example is [run_l3_backtest.py](../examples/run_l3_backtest.py).
It subscribes to trades and quotes, submits one passive MES buy, and asks the
engine to flatten at session close. Replace that teaching strategy with your
own callbacks and parameters when evaluating trading behavior.

```bash
python examples/run_l3_backtest.py --date 2026-10-21
```

Use the already-configured HiveQ profile for the target platform. Selecting
localhost submits to localhost's executor; the Python launcher itself does not
perform the replay. Use the same date for `start_date` and `end_date`; for
comparisons, finish one day's run before submitting the next.

```python
engine_config=EngineConfig(timezone="America/New_York", params={
    "enable_mbo": True,
    "exchange_sim_adapter": "DepthPaperBroker",
    "databento.dataset": "GLBX.MDP3",
    "databento.publish_bbo": True,
    "latency_millis": 20,
})
data_configs=[{
    "type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
    "schema": ["fut_trades"],
}]
```

## What each input does

| Setting/input | Effect |
|---|---|
| `enable_mbo: true` | Enables market-by-order queue simulation and native depth replay. Requires `DepthPaperBroker`. |
| `exchange_sim_adapter: DepthPaperBroker` | Chooses the simulator that uses historical orders ahead of strategy orders. |
| `databento.dataset: GLBX.MDP3` | Selects the depth provider dataset, separate from HiveQ's dataset catalog. |
| `databento.publish_bbo: true` | Opts into reconstructed valid, uncrossed BBO callbacks (`on_quote`); default is false. Subscribe to quotes in `on_start`. |
| `fut_trades` | Provides recorded trade callbacks (`on_trade`) and trade-driven fills. Subscribe to trades in `on_start`. |
| `latency_millis: 20` | Paper-broker order/cancel execution latency, **not** market-data latency or a timestamp offset. |

Optional `bars_1m` can accompany `fut_trades` if the strategy subscribes to bars.
With a trade schema present, bars are analytical inputs rather than a second,
synthetic fill source. Do not remove `fut_trades` and expect L3 to supply trades.
No L3 trade-dissemination option is part of this configuration.

## Runtime and shared cache

The executor needs the depth-enabled Flow 0.3.21 implementation and the Sigma
adapter with exact fixed-point price conversion and optional BBO publication.
A version label alone does not prove an older image contains these patches;
rebuild/deploy the matching runtime. The client SDK version is independent.

For platform operators:

- Set `HIVEQ_L3_DATA_HOST_PATH` on the orchestrator worker to the Docker-host
  cache directory (default `/data/l3`). Backtest executors mount it at `/data/l3`
  and receive `HIVEQ_L3_DATA_DIR=/data/l3`.
- Provision write access for the executor user and the provider credential
  (`DATABENTO_API_KEY`) on the worker. Do not embed provider secrets in strategy
  code, YAML, or packages.
- The adapter reuses existing files and automatically downloads missing MBO
  symbol-days, subject to provider access. A typical resolved-contract path is
  `/data/l3/GLBX.MDP3/mbo/20261021.MESZ6.dbn.zst`.
- Continuous symbols such as `MES.c.0` resolve to the dated contract for each
  day. No manual file copying is required during ordinary runs.

The shared mount is for depth files. Recorded trades and optional analytics
continue through the HiveQ data API. Discover available schemas and coverage
with `hiveq datasets` and `hiveq datasets fields HIVEQ_US_FUT fut_trades`.

## Comparing with livesim

Keep strategy parameters, session, contract, fees and execution latency the
same. Check source coverage and event granularity: identical contract volume
does not imply identical trade events. Retain separate executions even when
their prices and timestamps match.

Recorded trades currently replay using their dataset timestamp; L3 updates are
merged by provider receive time. The broker's 20 ms latency does not align
these market-data clocks. Close P&L correlation therefore does not guarantee
identical queue position or fill timing. Compare fills, minute P&L changes,
minute-end positions and final flatness, rather than only cumulative P&L.
