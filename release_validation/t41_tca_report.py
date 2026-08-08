"""Enabling TCA attaches populated per-fill and summary evidence to a completed round trip."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


class SdkT41:
    def on_start(self, ctx, event):
        self.bars = 0
        self.entry_sent = False
        self.exit_sent = False
        self.fills = 0
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        self.bars += 1
        if self.bars == 5:
            self.entry_sent = ctx.buy_order("AAPL", 10.0) is not None
        elif self.fills >= 1 and not self.exit_sent and self.bars >= 15:
            self.exit_sent = ctx.sell_order("AAPL", 10.0) is not None

    def on_order(self, ctx, event):
        if event.data().is_filled:
            self.fills += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t41_tca_report", {
            "bars": self.bars,
            "entry_sent": self.entry_sent,
            "exit_sent": self.exit_sent,
            "fills": self.fills,
            "final_position": float(ctx.net_position("AAPL")),
        })


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT41", type="SdkT41", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2025-12-02",
        end_date="2025-12-02",
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_EQ",
            "schema": ["bars_1m"],
        }],
        backtest_config=BacktestConfig(
            session_start="09:30", session_end="11:00", enable_tca=True
        ),
    )
    state = completed_checkpoint(run, "t41_tca_report")
    report = run.report()
    tca = report.tca_report if report is not None else None
    summary = None if tca is None else tca.summary
    fill_tca = None if tca is None else tca.fill_tca
    trade_tca = None if tca is None else tca.trade_tca
    finish("t41_tca_report", {
        "round_trip_filled": state["fills"] >= 2 and state["final_position"] == 0.0,
        "tca_attached": tca is not None,
        "tca_summary_present": summary is not None and not summary.empty,
        "per_fill_tca_present": fill_tca is not None and len(fill_tca) >= 2,
        "per_trade_tca_present": trade_tca is not None and len(trade_tca) >= 1,
    }, extra=(
        f"state={state}, summary_rows={0 if summary is None else len(summary)}, "
        f"fill_rows={0 if fill_tca is None else len(fill_tca)}, "
        f"trade_rows={0 if trade_tca is None else len(trade_tca)}"
    ))
