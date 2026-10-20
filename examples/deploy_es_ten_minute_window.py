#!/usr/bin/env python3
"""Deploy an ES LiveSim strategy that trades every ten minutes overnight.

The strategy subscribes to one-minute bars for ``ES.c.0`` and alternates
between long one contract and flat at 10-minute boundaries from 14:50 ET
through 13:20 ET.  At 13:30 ET it flattens defensively and places no more
orders until the next 14:50 ET window.

This deploys straight to LiveSim.  It does not submit a backtest.
"""

from __future__ import annotations

import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()

SYMBOL = "ES.c.0"
INTERVAL = "1m"
CONTAINER_ID = "livesim-futures-synthetic"
START_MINUTE = 14 * 60 + 50
STOP_MINUTE = 13 * 60 + 30


class EsTenMinuteWindow:
    """Alternate long/flat every ten minutes during the requested ET window."""

    def __init__(self):
        self.last_order_slot = None
        self.last_flatten_date = None

    def on_start(self, ctx, event):
        ctx.subscribe_bars([SYMBOL], AssetType.FUTURES, INTERVAL)
        ctx.add_event_log(
            "EsTenMinuteWindow subscribed to ES.c.0 1m bars",
            sub_event_type="INIT",
        )

    @staticmethod
    def _minutes_from_start(minute_of_day):
        if minute_of_day >= START_MINUTE:
            return minute_of_day - START_MINUTE
        return 24 * 60 - START_MINUTE + minute_of_day

    def on_bar(self, ctx, event):
        bar = event.data()
        now = ctx.now()  # Already ET; never convert it (R5).
        minute_of_day = now.hour * 60 + now.minute

        # At and after 13:30 ET, stop trading and flatten once for the date.
        if STOP_MINUTE <= minute_of_day < START_MINUTE:
            if minute_of_day == STOP_MINUTE and self.last_flatten_date != now.date():
                self.last_flatten_date = now.date()
                order = ctx.close_position(bar.symbol)
                ctx.add_event_log(
                    f"13:30 ET cutoff; flatten requested={order is not None}",
                    symbol=bar.symbol,
                    sub_event_type="CUTOFF",
                )
            return

        elapsed = self._minutes_from_start(minute_of_day)
        if elapsed % 10:
            return

        # One action per wall-clock slot even if duplicate bars are delivered.
        slot = (now.date().isoformat(), now.hour, now.minute)
        if slot == self.last_order_slot:
            return
        self.last_order_slot = slot

        if ctx.is_flat(bar.symbol):
            order = ctx.buy_order(bar.symbol, quantity=1.0)
            action = "BUY"
        elif ctx.is_net_long(bar.symbol):
            order = ctx.sell_order(bar.symbol, quantity=1.0)
            action = "SELL"
        else:
            order = ctx.close_position(bar.symbol)
            action = "FLATTEN"

        ctx.add_event_log(
            f"{now:%Y-%m-%d %H:%M} ET {action} submitted={order is not None}",
            symbol=bar.symbol,
            sub_event_type="SCHEDULED_ORDER",
        )

    def on_order(self, ctx, event):
        order = event.data()
        if order.is_filled:
            ctx.add_event_log(
                f"filled {order.side} {order.filled_qty} @ {order.avg_px}",
                symbol=order.symbol,
                sub_event_type="FILL",
            )


def main() -> int:
    from hiveq.flow.livesim import _base_url

    instance_name = f"EsTenMinuteWindow{int(time.time()) % 100000}"
    print(f"platform      : {_base_url()}")
    print(f"instance      : {instance_name}")

    deployment = hf.deploy_livesim(
        strategy_configs=[
            StrategyConfig(
                name="EsTenMinuteWindow",
                type="EsTenMinuteWindow",
                symbols=[SYMBOL],
                params={"assetType": "FUTURES"},
            )
        ],
        data_configs=[
            {
                "type": "live",
                "market_data_source": "SyntheticMarketData",
                "schema": ["fut_trades"],
            }
        ],
        instance_name=instance_name,
        container_id=CONTAINER_ID,
    )

    print(f"deployment    : {deployment.deployment_id}")
    print(f"artifact      : {deployment.artifact_id}")
    print(f"operation     : {deployment.operation_id}")

    state = deployment.wait(timeout=120)
    print(f"status        : {state.get('status')}")
    print(f"container     : {state.get('container_id')}")
    print(f"strategies    : {state.get('instance_names')}")
    if state.get("failure_reason"):
        print(f"failure       : {state['failure_reason']}")
        return 1

    logs = deployment.logs(tail=40)
    for line in logs.splitlines()[-10:]:
        print(f"log           : {line}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
