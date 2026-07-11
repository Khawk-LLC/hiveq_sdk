## 1. Minimal working example (canonical)

```python
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()   # module-level — REQUIRED in every strategy (R10)

class BuyAndHold:
    def __init__(self):
        self.bought = False

    # PER-EVENT CALLBACKS (default contract). One focused method per event type.
    def on_start(self, ctx: hf.Context, event):
        ctx.subscribe_bars(ctx.strategy_config.symbols, asset_type=AssetType.EQUITY, interval='1m')
        logger.info(f"[START] subscribed 1m bars for {ctx.strategy_config.symbols}")

    def on_bar(self, ctx, event):
        bar = event.data()                           # -> SigmaBar (§7.1)
        logger.debug(f"[BAR] {bar.symbol} {bar.time} close={bar.close:.2f} bought={self.bought}")
        if not self.bought and ctx.is_flat(bar.symbol):
            logger.info(f"[ENTRY] buying 100 {bar.symbol} at {bar.close:.2f}")
            ctx.buy_order(bar.symbol, quantity=100)
            self.bought = True

    def on_order(self, ctx, event):                  # NOT on_order_filled — fills come here
        order = event.data()                         # -> SigmaOrder (§7.3)
        logger.info(f"[ORDER] {order.symbol} status={order.status} filled={order.is_filled}")
        if order.is_filled:
            fill = order.last_fill                   # -> SigmaFill (§7.4)

# run_backtest returns a Run HANDLE (§10.0) immediately, not a PerformanceReport.
run = hf.run_backtest(
    strategy_configs=[StrategyConfig(name='BuyAndHold', type='BuyAndHold')],
    symbols=['AAPL'],
    start_date='2025-08-01',
    end_date='2025-08-02',
    data_configs=[{'type': 'hiveq_historical', 'dataset': 'HIVEQ_US_EQ', 'schema': ['bars_1m']}],
)
run.wait(progress=False)                             # block quietly until done (R11)
report = run.report()                                # -> PerformanceReport (§10.1)
print(report.return_stats.to_string())
```

> **First time you run this:** if no key is saved yet, a browser opens for the user to sign in and this call waits ~5 min — that is expected. Just tell the user a browser is opening to sign in (and give the bare link if it didn't); wait, don't debug it, and never show internal commands or set a key by hand. See §3.1.

---

