"""LiveSim probe for the existing ES market-maker zones signal topic."""
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()
DATA_ID = "es_mm_zones"
SYMBOL = "ES.c.0"


class SignalWidgetLiveSimTest:
    def __init__(self):
        self.signal_count = 0

    def on_start(self, ctx, event):
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_data(DATA_ID)
        logger.info(
            f"[START] subscribed bars={SYMBOL} signal_data_id={DATA_ID}"
        )
        ctx.add_event_log(
            f"subscribed to LiveSim signal source {DATA_ID}",
            sub_event_type="SIGNAL_TEST_START",
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        logger.debug(f"[BAR] symbol={bar.symbol} time={bar.time} close={bar.close}")

    def on_custom_data(self, ctx, event):
        data = event.data()
        self.signal_count += 1
        payload_text = str(data)
        logger.info(
            f"[SIGNAL] data_id={DATA_ID} count={self.signal_count} payload={payload_text}"
        )
        ctx.add_event_log(
            f"LiveSim signal received from {DATA_ID}",
            sub_event_type="SIGNAL_TEST_DATA",
            state_variable={
                "data_id": DATA_ID,
                "count": self.signal_count,
                "payload": payload_text[:2000],
            },
        )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SignalWidgetLiveSimTest",
            type="SignalWidgetLiveSimTest",
            symbols=[SYMBOL],
        )],
        symbols=[SYMBOL],
        start_date="2025-09-23",
        end_date="2025-09-23",
        data_configs=[
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_FUT",
                "schema": ["bars_1m"],
            },
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_QUANT_CLUSTER_ZONE",
                "schema": ["cluster_zone"],
                "id": DATA_ID,
                "symbols": ["intraday_ES_zones_2_v1"],
            },
        ],
        backtest_config=BacktestConfig(
            session_start="10:00",
            session_end="10:05",
        ),
    )
    print(f"SOURCE_RUN_ID={run.run_id}", flush=True)
    run.wait(progress=False)
    print(f"SOURCE_RUN_STATUS={run.status()}", flush=True)
