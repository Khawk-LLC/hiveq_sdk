"""Remote timer cadence and timer payload contract."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import pandas as pd
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType,BacktestConfig

class SdkT07:
    def on_start(self,ctx,event):
        self.s={"timers":0,"ids":[],"bars":0,"cancel_result":None,"canceled_fires":0};ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
        ctx.set_timer(timer_id="qa_10min",timer_interval=pd.Timedelta(minutes=10))
        ctx.set_timer(timer_id="qa_canceled",timer_interval=pd.Timedelta(minutes=1))
        self.s["cancel_result"]=str(ctx.cancel_timer("qa_canceled"))
    def on_bar(self,ctx,event):self.s["bars"]+=1
    def on_timer(self,ctx,event):
        self.s["timers"]+=1; timer_id=getattr(event.data(),"timer_id",None)
        if timer_id not in self.s["ids"]:self.s["ids"].append(timer_id)
        if timer_id=="qa_canceled":self.s["canceled_fires"]+=1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t07_timers",self.s)

if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT07",type="SdkT07",symbols=["AAPL"])],symbols=["AAPL"],
        start_date="2025-06-02",end_date="2025-06-02",data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="11:30"))
    s=completed_checkpoint(run,"t07_timers")
    finish("t07_timers",{"timer_fired":s["timers"]>0,"cadence_sane":8<=s["timers"]<=16,"timer_id_payload":"qa_10min" in s["ids"],
        "cancel_was_exercised":s["cancel_result"] in ("None","True"),"canceled_timer_never_fired":s["canceled_fires"]==0},extra=str(s))
