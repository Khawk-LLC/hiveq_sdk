"""Long, flat, and short transitions plus is_net_long/is_net_short helpers."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT29:
    def on_start(self, ctx, event):
        self.n=0; self.s={"long":False,"flat_after_long":False,"short":False,"positions":[]}
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        self.n += 1
        if self.n == 1: ctx.buy_order("AAPL", 10)
        elif self.n == 10:
            self.s["long"] = ctx.is_net_long("AAPL") and not ctx.is_net_short("AAPL")
            ctx.close_position("AAPL")
        elif self.n == 20:
            self.s["flat_after_long"] = ctx.is_flat("AAPL"); ctx.short_order("AAPL", 7)
        elif self.n == 30:
            self.s["short"] = ctx.is_net_short("AAPL") and not ctx.is_net_long("AAPL")
    def on_position(self, ctx, event):
        q=float(event.data().quantity)
        if q not in self.s["positions"]: self.s["positions"].append(q)
    def on_stop(self, ctx, event): emit_checkpoint(ctx,"t29_long_short_transitions",self.s)


if __name__ == "__main__":
    run=hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT29",type="SdkT29",symbols=["AAPL"])],
        symbols=["AAPL"],start_date="2025-06-02",end_date="2025-06-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="11:00"))
    state=completed_checkpoint(run,"t29_long_short_transitions")
    finish("t29_long_short_transitions",{
        "long_helper":state["long"],"flat_after_close":state["flat_after_long"],
        "short_helper":state["short"],"long_position_event":10.0 in state["positions"],
        "flat_position_event":0.0 in state["positions"],"short_position_event":-7.0 in state["positions"],
    },extra=str(state))
