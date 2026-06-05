#!/usr/bin/env python3
"""Thin SDK end-to-end example: author a strategy, deploy it, get results back.

Flow:
    this script (thin hiveq-flow-sdk)  -->  capture + cloudpickle strategy
        -->  submit to local orchestrator (:5010)
            -->  executor runs it on the FULL hiveq-flow engine
                -->  results returned over REST (:80)

Prerequisites
-------------
1. Local platform up:  orchestrator :5010, data gateway :80, auth :3001.
2. Thin SDK installed:  pip install hiveq-flow-sdk
3. API key on file:     echo 'HIVEQ_API_KEY=...' > ~/.hiveq/.env   (or export HIVEQ_API_KEY)

Run:
    python deploy_buy_and_hold.py
"""
import os

# Credentials are picked up automatically from ~/.hiveq/.env (HIVEQ_API_KEY=...),
# or the environment. Nothing to set here.

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
