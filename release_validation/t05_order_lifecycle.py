"""Remote market fill, resting limit, cancellation, and flatten contract."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType,BacktestConfig
from hiveq.flow.trading_types import OrderType

class SdkT05:
    def on_start(self,ctx,event):
        self.s={"bars":0,"statuses":[],"fill_qty":0.0,"fill_px":0.0,"limit_open":None,"after_cancel":None,"cancel":False,"flat":None}
        ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):
        self.s["bars"]+=1;b=event.data()
        if self.s["bars"]==1:ctx.buy_order("AAPL",quantity=10)
        elif self.s["bars"]==10:ctx.buy_order("AAPL",quantity=10,order_type=OrderType.LIMIT,limit_price=round(b.close*.5,2))
        elif self.s["bars"]==12:self.s["limit_open"]=ctx.has_open_order("AAPL");ctx.cancel_all_orders("AAPL")
        elif self.s["bars"]==20:self.s["after_cancel"]=ctx.has_open_order("AAPL");ctx.close_position("AAPL")
        elif self.s["bars"]==40:self.s["flat"]=ctx.is_flat("AAPL")
    def on_order(self,ctx,event):
        o=event.data();status=str(getattr(o,"status","")).upper()
        if status not in self.s["statuses"]:self.s["statuses"].append(status)
        if "FILL" in status and float(o.filled_qty or 0)>0 and str(o.side).upper().endswith("BUY") and not self.s["fill_qty"]:
            self.s["fill_qty"]=float(o.filled_qty);self.s["fill_px"]=float(o.avg_px or 0)
        if "CANCEL" in status and "REJECT" not in status:self.s["cancel"]=True
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t05_order_lifecycle",self.s)

if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT05",type="SdkT05",symbols=["AAPL"])],symbols=["AAPL"],
        start_date="2025-06-02",end_date="2025-06-02",data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="11:00"))
    s=completed_checkpoint(run,"t05_order_lifecycle")
    finish("t05_order_lifecycle",{"market_buy_filled":s["fill_qty"]==10 and s["fill_px"]>0,"limit_rested":s["limit_open"] is True,
        "cancel_event_seen":s["cancel"],"no_open_after_cancel":s["after_cancel"] is False,"flat_after_close":s["flat"] is True},extra=str(s))
