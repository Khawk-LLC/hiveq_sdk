"""Remote translation of Flow QA t01: bar delivery and payload contract."""

from datetime import datetime
from pathlib import Path
import sys

# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (
    checkpoint,
    emit_checkpoint,
    finish,
    wait_for_final,
)
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

class SdkT01:
    def on_start(self, ctx, event):
        # on_start/on_stop fire once per calendar day. Preserve aggregate state
        # across those daily callbacks so the final checkpoint covers the full run.
        if not hasattr(self, "counts"):
            self.counts = {"1m": 0, "1d": 0}
            self.errors = []
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1d")
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        if bar.symbol == "AAPL" and bar.interval in self.counts:
            self.counts[bar.interval] += 1
        if len(self.errors) >= 5:
            return
        if not isinstance(event.ts_event, (int, float)) or event.ts_event <= 1e18:
            self.errors.append(f"ts_event not nanoseconds: {event.ts_event}")
        if bar.low > bar.high or not bar.low <= bar.open <= bar.high:
            self.errors.append("invalid OHLC range")
        if not bar.low <= bar.close <= bar.high or bar.volume < 0:
            self.errors.append("invalid close/volume")
        if not isinstance(bar.time, datetime) and not hasattr(bar.time, "hour"):
            self.errors.append(f"invalid bar.time: {type(bar.time)}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t01_bars_multi_interval", {**self.counts, "errors": self.errors})

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT01", type="SdkT01", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2025-06-02", end_date="2025-06-03",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1d"]},
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]},
        ],
    )
    wait_for_final(run)
    state = checkpoint(run, "t01_bars_multi_interval")
    finish("t01_bars_multi_interval", {
        "1m_bars_delivered": state["1m"] >= 700,
        "1d_bars_delivered": state["1d"] == 2,
        "payload_contract": not state["errors"],
    }, extra=f"1m={state['1m']}, 1d={state['1d']}, errors={state['errors'][:2]}")
