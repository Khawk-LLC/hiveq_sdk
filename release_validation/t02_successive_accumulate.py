"""Successive subscription calls accumulate across symbols and asset types."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig

class SdkT02:
    def on_start(self, ctx, event):
        self.counts = {"MSFT": 0, "AAPL": 0, "VIX": 0, "SPX": 0}
        ctx.subscribe_trades(["MSFT"], asset_type=AssetType.EQUITY)
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)
        ctx.subscribe_index(["VIX"]); ctx.subscribe_index(["SPX"])
    def on_trade(self, ctx, event):
        symbol = event.data().symbol
        if symbol in self.counts: self.counts[symbol] += 1
    def on_index_price(self, ctx, event):
        symbol = event.data().symbol
        if symbol in self.counts: self.counts[symbol] += 1
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t02_successive_accumulate", self.counts)

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT02", type="SdkT02", symbols=["MSFT", "AAPL"])],
        symbols=["MSFT", "AAPL"], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["eq_trades"]},
            {"type":"hiveq_historical","dataset":"HIVEQ_US_IND","schema":["indices_values"]},
        ], backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"))
    s = completed_checkpoint(run, "t02_successive_accumulate")
    finish("t02_successive_accumulate", {"trades_first_call_MSFT":s["MSFT"]>0,
        "trades_second_call_AAPL":s["AAPL"]>0,"index_first_call_VIX":s["VIX"]>0,
        "index_second_call_SPX":s["SPX"]>0}, extra=str(s))
