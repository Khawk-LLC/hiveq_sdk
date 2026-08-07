#!/usr/bin/env python3
"""NYSE auction imbalance data via ``on_imbalance`` and metadata routing."""

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class NyseImbalanceStrategy:
    def __init__(self):
        self.count = 0

    def on_start(self, ctx, event):
        ctx.subscribe_bars(
            ctx.strategy_config.symbols,
            asset_type=AssetType.EQUITY,
            interval="1m",
        )
        logger.info("[START] waiting for nyse_imbalance data")

    def on_imbalance(self, ctx, event):
        data = event.data()
        self.count += 1
        if self.count <= 5 or self.count % 100 == 0:
            ctx.add_event_log(
                f"NYSE imbalance #{self.count}: side={data.side} "
                f"shares={data.imbalance:.0f} paired={data.paired_shares:.0f}",
                symbol=data.symbol,
                state_variable={
                    "venue": "NYSE",
                    "count": self.count,
                    "side": data.side,
                    "imbalance": data.imbalance,
                    "paired_shares": data.paired_shares,
                    "ref_price": data.ref_price,
                },
            )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(name="NyseImbalanceStrategy", type="NyseImbalanceStrategy")
        ],
        symbols=["IBM"],
        start_date="2026-07-01",
        end_date="2026-07-01",
        data_configs=[
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_EQ",
                "schema": ["bars_1m"],
            },
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_EQ",
                "schema": ["nyse_imbalance"],
            },
        ],
    )
    run.wait(progress=False)
    print("status:", run.status())
    print(run.event_logs().to_string(index=False))
