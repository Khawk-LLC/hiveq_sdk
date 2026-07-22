#!/usr/bin/env python3
"""End-to-end example: author a strategy, deploy it, get results back.

Flow: this script captures your strategy (cloudpickle) and submits it to the
HiveQ platform; the platform runs it and returns results over REST. You get a
``Run`` handle back — read results with ``run.report()`` / ``run.status()``.

Prerequisites
-------------
1. Install the SDK:   pip install hiveq-sdk
2. Have a HiveQ API key available — the only credential needed; the SDK warns
   if one isn't found.

Run:
    python deploy_buy_and_hold.py
"""

import hiveq.flow as hf
from datetime import time
from zoneinfo import ZoneInfo

from hiveq.flow import EventType, OrderStatus, StrategyConfig
from hiveq.flow.config import AssetType


# --- strategy (authored against the SDK type surface) ----------------------
# Default contract: PER-EVENT CALLBACKS — one focused method per event type.
class BuyAndHold:
    """Buy 100 shares of each subscribed symbol once, then hold."""

    _EASTERN = ZoneInfo("America/New_York")
    _MARKET_OPEN = time(9, 30)
    _MARKET_CLOSE = time(16, 0)

    def __init__(self):
        self.bought = False
        self.order_pending = False

    def on_start(self, ctx: hf.Context, event):
        ctx.subscribe_bars(
            ctx.strategy_config.symbols,
            asset_type=AssetType.EQUITY,
            interval="1m",
        )

    def on_bar(self, ctx, event):
        bar = event.data()                           # -> SigmaBar
        now_utc = ctx.now_utc()
        if now_utc is None:
            return
        now_et = now_utc.astimezone(self._EASTERN)
        in_regular_session = (
            now_et.weekday() < 5
            and self._MARKET_OPEN <= now_et.time() < self._MARKET_CLOSE
        )
        if (
            self.bought
            or self.order_pending
            or not in_regular_session
            or not ctx.is_flat(bar.symbol)
        ):
            return

        order = ctx.buy_order(bar.symbol, quantity=100)
        if order is not None:
            self.order_pending = True
            ctx.add_event_log(
                "Submitted initial buy-and-hold entry",
                symbol=bar.symbol,
            )

    def on_order(self, ctx, event):                  # fills arrive here (NOT on_order_filled)
        order = event.data()                         # -> SigmaOrder
        if order.is_filled:
            self.bought = True
            self.order_pending = False
            print(f"  filled: {order.symbol} qty={order.filled_qty} @ {order.avg_px}")
            return

        if (
            event.type in {EventType.ORDER_REJECTED, EventType.ORDER_CANCELED}
            or order.status in {OrderStatus.REJECTED, OrderStatus.CANCELED}
        ):
            self.order_pending = False
            ctx.add_event_log(
                f"Entry order {order.status}; waiting for the next regular-session bar",
                symbol=order.symbol,
            )


# --- deploy (returns immediately), block until done, then read results ------
if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="BuyAndHold", type="BuyAndHold")],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-02",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
        # silent=False,  # uncomment to block inside the call with a progress bar instead
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done

    print(f"\nrun_id={run.run_id}  task_id={run.task_id}")
    print("status :", run.status())
    print("summary:", run.summary())

    report = run.report()
    rs = getattr(report, "return_stats", None)
    if rs is not None and not rs.empty:
        print("\nreturn stats:\n", rs.to_string())
