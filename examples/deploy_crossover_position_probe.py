#!/usr/bin/env python3
"""Deploy an ES LiveSim probe that crosses a position through zero.

Steps (MARKET orders, one step at a time, each waits until the previous
target position is reached and STEP_SECONDS have passed):

    BUY 5   ->  +5   (opens LONG 5)
    SELL 10 ->  -5   (crossover: closes the LONG 5, opens SHORT 5)
    BUY 10  ->  +5   (crossover: closes the SHORT 5, opens LONG 5)
    SELL 5  ->   0   (closes the LONG 5)

Expected in the Positions grid: three Closed rows (LONG 5, SHORT 5, LONG 5),
each with its own open time / entry side, and nothing left Open.
"""

from __future__ import annotations

import sys
import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

SYMBOL = "ES.c.0"
CONTAINER_ID = "livesim-futures-2"
STEP_SECONDS = 20
# (side, qty, target net position after the fill)
STEPS = [("BUY", 5, 5), ("SELL", 10, -5), ("BUY", 10, 5), ("SELL", 5, 0)]


class CrossoverPositionProbe:
    def __init__(self):
        self.step = 0
        self.last_step_at = 0.0
        self.pending_target = None

    def on_start(self, ctx, event):
        ctx.subscribe_trades([SYMBOL], AssetType.FUTURES)
        ctx.add_event_log("CrossoverPositionProbe started", sub_event_type="INIT")

    def on_trade(self, ctx, event):
        if self.step >= len(STEPS):
            return
        trade_symbol = event.data().symbol
        if self.pending_target is not None:
            if abs(ctx.net_position(SYMBOL) - self.pending_target) > 1e-9:
                return  # previous step not filled yet
            self.pending_target = None
        now = time.time()
        if now - self.last_step_at < STEP_SECONDS:
            return
        side, qty, target = STEPS[self.step]
        order = (ctx.buy_order if side == "BUY" else ctx.sell_order)(SYMBOL, quantity=float(qty))
        self.step += 1
        self.last_step_at = now
        self.pending_target = target
        ctx.add_event_log(
            f"step {self.step}/{len(STEPS)}: {side} {qty} -> target {target:+d} "
            f"(net before {ctx.net_position(SYMBOL):+.0f}, trade sym {trade_symbol}) submitted={order is not None}",
            symbol=SYMBOL,
            sub_event_type="CROSSOVER_STEP",
        )

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            ctx.add_event_log(
                f"filled {order.side} {order.filled_qty} @ {order.avg_px}; net {ctx.net_position(SYMBOL):+.0f}",
                symbol=order.symbol,
                sub_event_type="FILL",
            )


def main() -> int:
    from hiveq.flow.livesim import _base_url

    instance_name = f"CrossoverProbe{int(time.time()) % 100000}"
    print(f"platform      : {_base_url()}")
    print(f"instance      : {instance_name}")

    deployment = hf.deploy_livesim(
        strategy_configs=[
            StrategyConfig(
                name="CrossoverPositionProbe",
                type="CrossoverPositionProbe",
                symbols=[SYMBOL],
                params={"assetType": "FUTURES"},
            )
        ],
        data_configs=[{"type": "live", "market_data_source": "Activ", "schema": ["fut_trades"]}],
        instance_name=instance_name,
        container_id=CONTAINER_ID,
    )
    print(f"deployment    : {deployment.deployment_id}")
    state = deployment.wait(timeout=180)
    print(f"status        : {state.get('status')}")
    print(f"container     : {state.get('container_id')}")
    if state.get("failure_reason"):
        print(f"failure       : {state['failure_reason']}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
