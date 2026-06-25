# HiveQ SDK

**Institutional-grade quantitative trading and backtesting — in a few lines of Python.**

HiveQ is the platform that has executed billions of dollars in trades across
equities, futures, and options over years of live operation. The HiveQ SDK puts
that same engine behind a single Python import: you write a strategy on your
machine, and HiveQ runs it on its platform — sourcing the market data, simulating
execution against real market microstructure, and handing you back a full
performance report.

```bash
pip install hiveq-sdk
```

You write the *what* (your trading logic). The platform handles the *how* — data,
execution, settlement, analytics, and scale.

## Why HiveQ

- **A simulator built to mirror production.** Backtests run on a robust
  execution simulator that tracks real production order flow closely, so what you
  validate in research behaves like it will in live — no rewrite to go from
  research to live.
- **Every run is tracked and versioned.** Strategy code and config changes are
  captured and versioned per run, so a backtest that performed well is always
  reproducible — go back, inspect the exact code and configuration behind it, and
  build from there.
- **Realistic execution, not fills-at-close.** Orders clear against real market
  microstructure: tick-level data, exchange session windows, opening/closing
  auctions routed to the primary exchange, per-asset fee models, slippage, and
  tick-size rounding. What you see in a backtest is what you'd get.
- **Built-in execution algorithms.** Work orders with production execution
  algos — POV, TWAP, and market-on-open / market-on-close auctions — the same
  ones used in live trading, not approximations.
- **Multi-asset and multi-strategy.** Equities, futures (including continuous
  contracts with automatic rollover), and options (down to 0DTE) — trade them
  together, run a portfolio of strategies in one backtest.
- **Research → live in one click.** The strategy you backtest runs unchanged in
  live and paper trading — same code, just a live config. Promote a validated
  backtest to a live or paper simulation from the HiveQ platform with a single
  click; no rewrite, no redeploy dance.
- **Institutional analytics out of the box.** Performance reports, positions and
  trades over time, daily P&L, transaction-cost analysis (TCA), realtime metrics,
  and exportable PDF tearsheets.
- **Nothing to manage.** No clusters to provision, no engine to install — even
  your API key is generated for you. The platform fetches data and runs the
  compute; you just write strategies.
- **Run your own scripts — on a schedule, no dev help.** Beyond strategies, run
  arbitrary Python on the platform as a job: compute signals, scores, or any
  derived calculation your trading depends on, and publish them as a dataset your
  strategies subscribe to. Configure a script to run on a schedule (e.g. nightly
  or pre-open) so fresh signals are ready before the session — you manage these
  end-to-end yourself, no engineering team in the loop.
- **A reusable, versioned function registry.** Push a Python function once and
  reference it by name and version from any strategy or script. Indicators,
  signal models, and shared utilities live in one place, versioned — so research
  reuses production logic instead of re-implementing it, and you can roll forward
  or back with confidence.
- **Bring your own data.** Upload custom datasets to the platform and reference
  them from strategies the same way you reference market data.
- **One console for everything.** The HiveQ platform gives you a single place to
  track and manage all your work — backtests, live simulations, and your own
  scheduled scripts — with their results, logs, and versioned history side by
  side.
- **An AI-native platform.** HiveQ ships with an AI assistant fine-tuned for the
  platform — author and refine strategies, explain results, and build signals in
  natural language, with an assistant that already knows the HiveQ API.

## How it feels to use

A strategy is a plain Python class with one method per event. Subscribe to data
in `on_start`, react to it as it arrives, and place orders through the `ctx`
handle that every callback receives.

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig, AssetType

class BuyAndHold:
    def __init__(self):
        self.bought = False

    def on_start(self, ctx, event):
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                           asset_type=AssetType.EQUITY, interval='1m')

    def on_bar(self, ctx, event):
        bar = event.data()
        if not self.bought and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=100)
            self.bought = True

run = hf.run_backtest(
    strategy_configs=[StrategyConfig(name='BuyAndHold', type='BuyAndHold')],
    symbols=['AAPL'],
    start_date='2025-08-01',
    end_date='2025-08-31',
)

print(run.report().return_stats.to_string())
```

Run it like any script:

```bash
python my_strategy.py
```

That's the whole loop: author locally, call `run_backtest`, and the platform
deploys your strategy, runs it on the engine, and returns a **run handle** for
inspecting results.

## You get back a full performance report

`run_backtest` returns a `Run` — the single handle for everything the strategy
produced:

```python
run.report()          # full performance report (returns, drawdown, ratios)
run.positions()        # positions over time   (DataFrame)
run.trades()           # executed trades        (DataFrame)
run.daily_returns()    # daily P&L              (DataFrame)
run.logs()             # execution logs
```

The same handle works for any past run — `hf.get_run(run_id)` reattaches to it.

## The strategy model

- **One class, callback methods.** Implement the events you care about —
  `on_start`, `on_bar`, `on_trade`, `on_order`, `on_position`, `on_timer`, and
  more. `StrategyConfig.type` is just the class name as a string.
- **Subscribe in `on_start`.** Call `ctx.subscribe_bars(...)` (and friends)
  there so your data is registered before the run begins.
- **Place orders through `ctx`.** Market, limit, and stop orders, brackets,
  modify/cancel, plus production execution algorithms (POV, TWAP, auctions).
- **Everything flows through events.** Each callback receives an `event`; call
  `event.data()` for the bar, trade, order, or position that triggered it.

## Supported markets

Equities, futures (including continuous contracts with automatic rollover),
and options — plus your own custom data feeds, all tradable in one backtest.

## Learn more

- **[`examples/`](examples/)** — complete, runnable strategies: intraday
  momentum, bracket orders, pairs trading, 0DTE options, futures sessions,
  scheduled timers, custom data, and more.
- **[`docs/`](docs/)** — the complete API reference: every callback, order type,
  execution algorithm, data schema, and result accessor. Docs are versioned per
  release — read the one matching your installed SDK at
  **`docs/<version>/api-reference.md`** (or use
  [`docs/latest/api-reference.md`](docs/latest/api-reference.md) for the current version).
