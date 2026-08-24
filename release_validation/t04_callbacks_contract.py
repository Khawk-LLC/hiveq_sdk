"""Remote callback ordering, event type, timestamp, and context contract."""
from datetime import datetime
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType,BacktestConfig,EventType

class SdkT04:
    def on_start(self,ctx,event):
        if not hasattr(self, "s"):
            self.s={"starts":1,"bars":0,"stops":0,"fills":0,"first":"start","errors":[]}
        if event.type!=EventType.START:self.s["errors"].append(f"start={event.type}")
        ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):
        self.s["bars"]+=1
        if self.s["bars"]==1:
            if event.type!=EventType.BAR:self.s["errors"].append(f"bar={event.type}")
            if not isinstance(event.ts_event,(int,float)) or event.ts_event<=1e18:self.s["errors"].append("ts_not_ns")
            if not isinstance(ctx.now(),datetime):self.s["errors"].append("now_not_datetime")
            if ctx.trading_day!="2025-06-02":self.s["errors"].append(f"day={ctx.trading_day}")
            ctx.buy_order("AAPL", 1.0)
        elif self.s["bars"] == 2:
            ctx.sell_order("AAPL", 1.0)
    def on_order(self,ctx,event):
        if event.data().is_filled:self.s["fills"] += 1
    def on_stop(self,ctx,event):
        self.s["stops"]+=1
        if event.type!=EventType.STOP:self.s["errors"].append(f"stop={event.type}")
        emit_checkpoint(ctx,"t04_callbacks_contract",self.s)

if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT04",type="SdkT04",symbols=["AAPL"])],
        symbols=["AAPL"],start_date="2025-06-02",end_date="2025-06-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="10:30"))
    s=completed_checkpoint(run,"t04_callbacks_contract")
    finish("t04_callbacks_contract",{"on_start_fired_once":s["starts"]==1,"on_start_before_data":s["first"]=="start",
        "on_bar_fired":s["bars"]>0,"session_scoped_data_pull":55<=s["bars"]<=65,
        "on_stop_fired":s["stops"]>=1,"trade_round_trip":s["fills"]>=2,
        "event_contract":not s["errors"]},extra=str(s))
