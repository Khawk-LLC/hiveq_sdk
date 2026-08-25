#!/usr/bin/env python3
"""ES dynamic-bar smoke test for ACTIV-backed livesim.

The strategy itself names no market-data vendor. In livesim, the runtime's
ACTIV adapter supplies trades and builds the requested 1-second and 1-minute
bars dynamically. Do not add a Databento data source to the livesim deployment.

The ``__main__`` block is only a 30-minute historical backtest harness. Its
``data_configs`` are backtest inputs and are not part of the captured strategy.

Run:  python examples/activ_dynamic_bars_es.py
"""

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()

SYMBOL = "ES.v.0"
INTERVALS = ("1s", "1m")


class ActivDynamicBarsES:
    """Subscribe to two ES bar sizes and write every bar to event logs."""

    def __init__(self):
        self.bar_counts = {interval: 0 for interval in INTERVALS}

    def on_start(self, ctx: hf.Context, event):
        for interval in INTERVALS:
            ctx.subscribe_bars(
                symbols=[SYMBOL],
                asset_type=AssetType.FUTURES,
                interval=interval,
            )

        logger.info(f"[START] subscribed {SYMBOL} bars intervals={INTERVALS}")
        ctx.add_event_log(
            f"subscribed {SYMBOL} dynamic bars: 1s and 1m",
            sub_event_type="BAR_SUBSCRIPTION",
            symbol=SYMBOL,
            state_variable={"intervals": list(INTERVALS)},
        )

    def on_bar(self, ctx: hf.Context, event):
        bar = event.data()
        interval = bar.interval
        self.bar_counts[interval] = self.bar_counts.get(interval, 0) + 1

        logger.debug(
            f"[BAR] symbol={bar.symbol} interval={interval} time={bar.time} "
            f"open={bar.open} high={bar.high} low={bar.low} "
            f"close={bar.close} volume={bar.volume} "
            f"count={self.bar_counts[interval]}"
        )
        ctx.add_event_log(
            (
                f"{interval} bar {bar.symbol} {bar.time} "
                f"O={bar.open} H={bar.high} L={bar.low} "
                f"C={bar.close} V={bar.volume}"
            ),
            sub_event_type=f"BAR_{interval.upper()}",
            symbol=bar.symbol,
            state_variable={
                "interval": interval,
                "interval_millis": bar.interval_millis,
                "time": str(bar.time),
                "ts_event": bar.ts_event,
                "ts_init": bar.ts_init,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "count": self.bar_counts[interval],
            },
        )

    def on_stop(self, ctx: hf.Context, event):
        logger.info(f"[STOP] bar counts={self.bar_counts}")
        ctx.add_event_log(
            "dynamic bar smoke test stopped",
            sub_event_type="BAR_SUMMARY",
            symbol=SYMBOL,
            state_variable=dict(self.bar_counts),
        )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(name="ActivDynamicBarsES", type="ActivDynamicBarsES")
        ],
        symbols=[SYMBOL],
        start_date="2025-11-03",
        end_date="2025-11-03",
        data_configs=[
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_FUT",
                "schema": ["bars_1s", "bars_1m"],
            }
        ],
        backtest_config=BacktestConfig(
            session_start="10:00",
            session_end="10:30",
        ),
    )
    print(f"run {run.run_id} task {run.task_id}")
    run.wait(progress=False)
    print(run.event_logs().to_string(index=False))
