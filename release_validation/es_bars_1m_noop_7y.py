"""No-op ES continuous-futures strategy over seven complete calendar years."""

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()
SYMBOL = "ES.c.0"
START_DATE = "2019-01-01"
END_DATE = "2025-12-31"


class EsBars1mNoOp7Y:
    """Subscribe to one-minute ES bars and intentionally place no orders."""

    def __init__(self):
        self.bar_count = 0
        self.first_bar = None
        self.last_bar = None

    def on_start(self, ctx, event):
        ctx.subscribe_futures_bars(symbols=[SYMBOL], interval="1m")
        logger.info(
            f"[START] subscribed {SYMBOL} bars_1m; trading_day={ctx.trading_day}"
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        self.bar_count += 1
        timestamp = bar.time.isoformat()
        if self.first_bar is None:
            self.first_bar = timestamp
        self.last_bar = timestamp

    def on_stop(self, ctx, event):
        logger.info(
            f"[STOP] {SYMBOL} bars_1m count={self.bar_count} "
            f"first={self.first_bar} last={self.last_bar}"
        )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="EsBars1mNoOp7Y",
            type="EsBars1mNoOp7Y",
            symbols=[SYMBOL],
        )],
        symbols=[SYMBOL],
        start_date=START_DATE,
        end_date=END_DATE,
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_FUT",
            "schema": ["bars_1m"],
        }],
    )
    logger.info(f"Submitted seven-year no-op backtest: {run}")
