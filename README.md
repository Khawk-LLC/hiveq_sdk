# HiveQ SDK

Write a trading strategy in Python, deploy it, and get back a full performance
report. You author the strategy on your machine; HiveQ runs the backtest on its
platform — fetching market data, executing your orders, and returning results.

```bash
pip install hiveq-sdk
```

The first time you run anything without being signed in, HiveQ opens your browser
to sign in (or sign up) and saves your access automatically — no copying, nothing
to configure. You can also trigger it with `hf.login()` or `hiveq-login`.

## Your first strategy

A strategy is a plain Python class with one method per event. Subscribe to data
in `on_start`, react to it in `on_bar`, and place orders through `ctx`.

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

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

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            fill = order.last_fill

run = hf.run_backtest(
    strategy_configs=[StrategyConfig(name='BuyAndHold', type='BuyAndHold')],
    symbols=['AAPL'],
    start_date='2025-08-01',
    end_date='2025-08-31',
)

report = run.report()
print(report.return_stats.to_string())
```

Run it:

```bash
python my_strategy.py
```

`run_backtest` deploys your strategy, runs it, and returns a **run handle** you
use to inspect results:

```python
run.report()          # full performance report
run.positions()       # positions over time   (DataFrame)
run.trades()          # executed trades        (DataFrame)
run.daily_returns()   # daily P&L              (DataFrame)
run.logs()            # execution logs
```

## How it works

- **One class, callback methods.** Implement the events you care about:
  `on_start`, `on_bar`, `on_order`, `on_position`, `on_timer`, and more.
  `StrategyConfig.type` is the class name as a string.
- **Subscribe in `on_start`.** Call `ctx.subscribe_bars(...)` (and friends) there
  so your data is registered before the run begins.
- **Place orders through `ctx`.** `ctx.buy_order`, `ctx.sell_order`,
  `ctx.short_order`, plus limit/stop orders, modify/cancel, and brackets.
- **Fills arrive in `on_order`.** Check `order.is_filled` — there is no separate
  fill callback.
- **Time is U.S. Eastern.** `ctx.now()` and every timestamp you receive are
  already in market time (ET). Compare wall-clock directly; no timezone math.

## Supported markets

Equities, futures (including continuous contracts with automatic rollover),
and options — plus your own custom data feeds. See the examples for each.

## Learn more

- **[`examples/`](examples/)** — complete, runnable strategies: intraday
  momentum, bracket orders, pairs trading, 0DTE options, futures sessions,
  scheduled timers, custom data, and more.
- **[`docs/`](docs/)** — the complete API reference: every callback, order
  type, data schema, and result accessor. Docs are versioned per release —
  read the one matching your installed SDK at **`docs/<version>/api-reference.md`**
  (current: [`docs/0.3.4/api-reference.md`](docs/0.3.4/api-reference.md)). Check your version with
  `python -c "import hiveq.flow as hf; print(hf.__version__)"`.
