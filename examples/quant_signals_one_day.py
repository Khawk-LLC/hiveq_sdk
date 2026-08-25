#!/usr/bin/env python3
"""One-day validation of the hosted HiveQ Quant Signals custom-data feed."""

import json

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()

DATA_ID = "quant_signals"
SIGNAL_KEY = "Prillach_MC_ES"


def decode_signal_json(raw: str) -> dict:
    """Decode plain JSON or the Sigma CSV bridge's escaped representation."""
    return json.loads(raw.replace(r'\"', '"').replace("|", ","))


class QuantSignalsOneDay:
    def __init__(self):
        self.entered = False
        self.signal_count = 0

    def on_start(self, ctx, event):
        symbols = ctx.strategy_config.symbols
        ctx.subscribe_bars(symbols, asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(data_id=DATA_ID)
        logger.info(
            "[START] subscribed symbols=%s data_id=%s signal=%s",
            symbols,
            DATA_ID,
            SIGNAL_KEY,
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        logger.debug(
            "[BAR] symbol=%s time=%s close=%s entered=%s",
            bar.symbol,
            bar.time,
            bar.close,
            self.entered,
        )
        should_enter = not self.entered and ctx.is_flat(bar.symbol)
        logger.debug("[ENTRY_CHECK] symbol=%s should_enter=%s", bar.symbol, should_enter)
        if should_enter:
            logger.info("[ENTRY] buying symbol=%s quantity=1", bar.symbol)
            ctx.buy_order(bar.symbol, quantity=1.0)
            self.entered = True
        elif (
            self.entered
            and ctx.is_net_long(bar.symbol)
            and (bar.time.hour, bar.time.minute) >= (15, 55)
        ):
            logger.info("[EXIT] closing symbol=%s before session end", bar.symbol)
            ctx.close_position(bar.symbol)

    def on_custom_data(self, ctx, event):
        data = event.data()
        raw = data.column_data("signal_json", default=None)
        symbol = data.column_data("symbol", default=data.symbol or "UNKNOWN")
        self.signal_count += 1
        logger.debug(
            "[CUSTOM_DATA] count=%s symbol=%s time=%s has_signal_json=%s",
            self.signal_count,
            symbol,
            data.time,
            bool(raw),
        )

        decoded = None
        if raw:
            try:
                decoded = decode_signal_json(raw)
            except (json.JSONDecodeError, TypeError):
                logger.info("[CUSTOM_DATA] invalid signal_json symbol=%s", symbol)

        ctx.add_event_log(
            "quant signal received",
            sub_event_type="CUSTOM_DATA_RECEIVED",
            symbol=str(symbol),
            state_variable={"count": self.signal_count, "signal": decoded},
        )

    def on_order(self, ctx, event):
        order = event.data()
        logger.info(
            "[ORDER] symbol=%s status=%s filled=%s fill_price=%s",
            order.symbol,
            order.status,
            order.is_filled,
            order.avg_px,
        )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(name="QuantSignalsOneDay", type="QuantSignalsOneDay")
        ],
        symbols=["AAPL"],
        start_date="2024-08-27",
        end_date="2024-08-27",
        data_configs=[
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_EQ",
                "schema": ["bars_1m"],
            },
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_QUANT_SIGNALS",
                "schema": ["signals"],
                "id": DATA_ID,
                "symbols": [SIGNAL_KEY],
            },
        ],
    )
    print(f"run_id={run.run_id} task_id={run.task_id}")
    run.wait(progress=False)
    print(run.report().return_stats.to_string())
