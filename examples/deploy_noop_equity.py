#!/usr/bin/env python3
"""Deploy a no-op AAPL strategy directly to Equity LiveSim.

The strategy subscribes to live AAPL trades and one-minute bars, records
lightweight liveness events, and deliberately places no orders.  The
deployment is left running unless ``--cleanup`` is passed.
"""

import argparse
import time

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()

SYMBOL = "AAPL"
INTERVAL = "1m"


class NoOpEquity:
    """Observe live AAPL market data without placing orders."""

    TRADE_LOG_EVERY = 500

    def __init__(self):
        self.trade_count = 0
        self.bar_count = 0

    def on_start(self, ctx, event):
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.EQUITY)
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval=INTERVAL)
        logger.info(f"[START] subscribed to AAPL trades and {INTERVAL} bars")
        ctx.add_event_log(
            f"NoOpEquity started: trades + {INTERVAL} bars {SYMBOL}",
            sub_event_type="INIT",
        )

    def on_trade(self, ctx, event):
        trade = event.data()
        self.trade_count += 1
        logger.debug(
            f"[TRADE] time={trade.time} symbol={trade.symbol} "
            f"price={trade.price} size={trade.size} count={self.trade_count}"
        )
        if self.trade_count % self.TRADE_LOG_EVERY == 0:
            logger.info(
                f"[TRADE {self.trade_count}] {trade.time} {trade.symbol} "
                f"price={trade.price} size={trade.size}"
            )
            ctx.add_event_log(
                f"trade #{self.trade_count} {trade.symbol} price={trade.price}",
                symbol=trade.symbol,
                state_variable={
                    "price": trade.price,
                    "size": trade.size,
                    "trades": self.trade_count,
                },
            )

    def on_bar(self, ctx, event):
        bar = event.data()
        self.bar_count += 1
        logger.debug(
            f"[BAR {self.bar_count}] {bar.time} {bar.symbol} "
            f"open={bar.open} high={bar.high} low={bar.low} close={bar.close} "
            f"volume={bar.volume}"
        )
        ctx.add_event_log(
            f"bar #{self.bar_count} {bar.symbol} close={bar.close}",
            symbol=bar.symbol,
            state_variable={
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "bars": self.bar_count,
                "trades": self.trade_count,
            },
        )

    def on_order(self, ctx, event):
        order = event.data()
        logger.info(
            f"[ORDER] unexpected order symbol={order.symbol} "
            f"status={order.status} filled={order.is_filled}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instance-name",
        default=f"NoOpEquity{int(time.time()) % 100000}",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Terminate the strategy after verifying the deployment.",
    )
    args = parser.parse_args()

    strategy = StrategyConfig(
        name="NoOpEquity",
        type="NoOpEquity",
        symbols=[SYMBOL],
        params={"assetType": "EQUITY"},
    )
    deployment = hf.deploy_livesim(
        [strategy],
        data_configs=[
            {
                "type": "live",
                "market_data_source": "Activ",
                "schema": ["eq_trades"],
            }
        ],
        instance_name=args.instance_name,
    )

    print(f"deployment_id={deployment.deployment_id}")
    print(f"artifact_id={deployment.artifact_id}")
    print(f"operation_id={deployment.operation_id}")

    state = deployment.wait(timeout=120)
    instance = (state.get("instance_names") or [args.instance_name])[0]
    print(f"status={state['status']}")
    print(f"container_id={state.get('container_id')}")
    print(f"instance_name={instance}")
    if state.get("failure_reason"):
        print(f"failure_reason={state['failure_reason']}")
        return 1

    events = deployment.events(limit=100)
    orders = deployment.orders(limit=100)
    print(f"event_rows={len(events)}")
    print(f"order_rows={len(orders)}")

    if args.cleanup:
        deployment.terminate(strategy=instance)
        print(f"terminated={instance}")
    else:
        print("deployment_left_running=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
