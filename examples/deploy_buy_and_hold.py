#!/usr/bin/env python3
"""End-to-end example: author a strategy, deploy it, get results back.

Flow: this script captures your strategy (cloudpickle) and submits it to the
HiveQ platform; the platform runs it and returns results over REST. You get a
``Run`` handle back — read results with ``run.report()`` / ``run.status()``.

Prerequisites
-------------
1. Install the SDK:   pip install hiveq-sdk
2. Set your API key:  export HIVEQ_API_KEY=...   (the only required credential)

Run:
    python deploy_buy_and_hold.py
"""

# Credentials are read from the HIVEQ_API_KEY environment variable. Nothing else
# to set here — the platform resolves your user/org from the key.

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType


# --- strategy (authored against the SDK type surface) ----------------------
# Default contract: PER-EVENT CALLBACKS — one focused method per event type.
class BuyAndHold:
    """Buy 100 shares of each subscribed symbol once, then hold."""

    def __init__(self):
        self.bought = False

    def on_start(self, ctx: hf.Context, event):
        ctx.subscribe_bars(
            ctx.strategy_config.symbols,
            asset_type=AssetType.EQUITY,
            interval="1m",
        )

    def on_bar(self, ctx, event):
        bar = event.data()                           # -> SigmaBar
        if not self.bought and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=100)
            self.bought = True

    def on_order(self, ctx, event):                  # fills arrive here (NOT on_order_filled)
        order = event.data()                         # -> SigmaOrder
        if order.is_filled:
            print(f"  filled: {order.symbol} qty={order.filled_qty} @ {order.avg_px}")


# --- deploy + block until done (live progress line), then read results ------
if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="BuyAndHold", type="BuyAndHold")],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-02",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
        # silent=True,  # uncomment to return immediately with the Run handle
    )

    print(f"\nrun_id={run.run_id}  task_id={run.task_id}")
    print("status :", run.status())
    print("summary:", run.summary())

    report = run.report()
    rs = getattr(report, "return_stats", None)
    if rs is not None and not rs.empty:
        print("\nreturn stats:\n", rs.to_string())

