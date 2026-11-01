#!/usr/bin/env python3
"""One-day MES backtest: recorded trades + L3 queue simulation and quotes.

Run: python examples/run_l3_backtest.py --date 2026-10-21
The selected HiveQ profile supplies the endpoint; the platform fetches data.
This teaching strategy places one passive buy; session-close flattening exits it.
"""
import argparse
from datetime import datetime

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderType

logger = _get_logger()


class L3Example:
    def __init__(self):
        self.submitted = False

    def on_start(self, ctx, event):
        ctx.subscribe_trades(ctx.strategy_config.symbols, asset_type=AssetType.FUTURES)
        ctx.subscribe_quotes(ctx.strategy_config.symbols, asset_type=AssetType.FUTURES)
        logger.info("Subscribed to recorded futures trades and replay quotes")

    def on_quote(self, ctx, event):
        quote = event.data()
        now = ctx.now()
        logger.debug(f"quote {quote.symbol} bid={quote.bid_price} ask={quote.ask_price}")
        if self.submitted or now.hour != 14 or quote.bid_price <= 0:
            return
        if quote.ask_price <= quote.bid_price:
            return
        ctx.buy_order(quote.symbol, quantity=1.0, order_type=OrderType.LIMIT,
                      limit_price=quote.bid_price)
        self.submitted = True
        logger.info(f"Submitted one passive buy: {quote.symbol} at {quote.bid_price}")

    def on_trade(self, ctx, event):
        tick = event.data()
        logger.debug(f"recorded trade {tick.symbol} price={tick.price} size={tick.size}")

    def on_order(self, ctx, event):
        order = event.data()
        logger.info(f"order {order.symbol} filled={order.is_filled} quantity={order.filled_qty}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="One trading day, YYYY-MM-DD")
    args = parser.parse_args()
    datetime.strptime(args.date, "%Y-%m-%d")
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="L3Example_MES", type="L3Example",
                                         symbols=["MES.c.0"])],
        symbols=["MES.c.0"],
        start_date=args.date,
        end_date=args.date,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["fut_trades"]}],
        backtest_config=BacktestConfig(
            enable_auto_rollover=True, session_start="13:50", session_end="15:30",
            auto_flatten_at_close=True, futures_fee=0.50, initial_capital=1_000_000.0,
        ),
        engine_config=EngineConfig(timezone="America/New_York", params={
            "enable_mbo": True,
            "exchange_sim_adapter": "DepthPaperBroker",
            "databento.dataset": "GLBX.MDP3",
            "databento.publish_bbo": True,
            "latency_millis": 20,
        }),
        task_name=f"L3 example MES {args.date}",
    )
    print("Run ID:", run.run_id)
    run.wait(progress=False)
    print(run.report())


if __name__ == "__main__":
    main()
