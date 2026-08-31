"""Configured session_start determines StartEvent and first-bar time in ET."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT21:
    def on_start(self, ctx, event):
        self.state = {"start": event.time.strftime("%H:%M") if event.time else None,
                      "first_bar": None, "bars": 0}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info(f"[START] time={event.time}")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1
        if self.state["first_bar"] is None:
            self.state["first_bar"] = event.data().time.strftime("%H:%M")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t21_session_start_boundaries", self.state)


if __name__ == "__main__":
    def minute_of_day(value):
        hour, minute = map(int, value.split(":"))
        return hour * 60 + minute

    states = {}
    for start, end in [("09:30", "16:00"), ("10:00", "15:30"), ("11:30", "15:00")]:
        run = hf.run_backtest(
            strategy_configs=[StrategyConfig(name="SdkT21", type="SdkT21", symbols=["AAPL"])],
            symbols=["AAPL"], start_date="2025-09-15", end_date="2025-09-15",
            data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
            backtest_config=BacktestConfig(session_start=start, session_end=end))
        states[start] = completed_checkpoint(run, "t21_session_start_boundaries")
    finish("t21_session_start_boundaries", {
        f"start_event_{start}_ET": state["start"] == start
        for start, state in states.items()
    } | {
        f"data_begins_{start}_ET": state["bars"] > 0 and state["first_bar"] is not None
        and 0 <= minute_of_day(state["first_bar"]) - minute_of_day(start) <= 1
        for start, state in states.items()
    }, extra=str(states))
