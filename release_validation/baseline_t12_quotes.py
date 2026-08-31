"""Remote futures tick delivery (fut_trades) and non-crossed quote invariant."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType,BacktestConfig

class SdkT12:
    def on_start(self,ctx,event):self.s={"quotes":0,"crossed":0,"symbols":[]};ctx.subscribe_quotes(["ES.c.0"],asset_type=AssetType.FUTURES)
    def on_quote(self,ctx,event):
        q=event.data();self.s["quotes"]+=1
        if q.symbol not in self.s["symbols"]:self.s["symbols"].append(q.symbol)
        bid=float(getattr(q,"bid_price",0) or 0);ask=float(getattr(q,"ask_price",0) or 0)
        if bid>0 and ask>0 and bid>ask:self.s["crossed"]+=1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t12_quotes",self.s)

if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT12",type="SdkT12",symbols=["ES.c.0"])],symbols=["ES.c.0"],
        start_date="2025-06-02",end_date="2025-06-02",data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_FUT","schema":["fut_trades"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="10:00"))
    s=completed_checkpoint(run,"t12_quotes")
    finish("t12_quotes",{"quotes_delivered":s["quotes"]>0,"no_crossed_quotes":s["crossed"]==0},extra=str(s))
