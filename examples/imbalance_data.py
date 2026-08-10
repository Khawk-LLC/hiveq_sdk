#!/usr/bin/env python3
"""Auction imbalance data — receive real ``early_imbalance`` rows.

Demonstrates:
  - ``on_imbalance(ctx, event)`` and its ``ImbalanceData`` payload.
  - configuring the ``HIVEQ_US_EQ`` / ``early_imbalance`` historical source.

The selected symbol and date have verified data coverage.

Run:  python imbalance_data.py
"""

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class ImbalanceDataStrategy:
    def __init__(self):
        self.count = 0
        self.bar_count = 0

    def on_start(self, ctx, event):
        ctx.subscribe_bars(
            ctx.strategy_config.symbols,
            asset_type=AssetType.EQUITY,
            interval="1m",
        )
        logger.info(
            "[START] subscribed to bars_1m and waiting for early_imbalance data for "
            f"{ctx.strategy_config.symbols}"
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        self.bar_count += 1
        logger.debug(
            f"[BAR] count={self.bar_count} symbol={bar.symbol} "
            f"time={bar.time} close={bar.close} imbalances={self.count}"
        )

    def on_imbalance(self, ctx, event):
        imbalance = event.data()
        self.count += 1

        logger.debug(
            f"[IMBALANCE] count={self.count} symbol={imbalance.symbol} "
            f"side={imbalance.side} shares={imbalance.imbalance} "
            f"paired={imbalance.paired_shares} time={imbalance.time}"
        )

        # Keep a durable, queryable sample without writing one event-log row for
        # every feed update.
        if self.count <= 5 or self.count % 100 == 0:
            ctx.add_event_log(
                f"imbalance #{self.count}: side={imbalance.side} "
                f"shares={imbalance.imbalance:.0f} "
                f"paired={imbalance.paired_shares:.0f}",
                symbol=imbalance.symbol,
                state_variable={
                    "count": self.count,
                    "side": imbalance.side,
                    "imbalance": imbalance.imbalance,
                    "paired_shares": imbalance.paired_shares,
                    "ref_price": imbalance.ref_price,
                },
            )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(
                name="ImbalanceDataStrategy",
                type="ImbalanceDataStrategy",
            )
        ],
        symbols=["ABBV"],
        start_date="2026-04-30",
        end_date="2026-04-30",
        data_configs=[
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_EQ",
                "schema": ["bars_1m"],
            },
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_EQ",
                "schema": ["early_imbalance"],
            }
        ],
    )
    run.wait(progress=False)
    print("status:", run.status())
    print(run.event_logs().to_string(index=False))
