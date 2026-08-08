"""IOC expires immediately while an unfilled GTC limit persists through close."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderType

logger = _get_logger()


class SdkT30:
    def on_start(self,ctx,event):
        self.counts={"AAPL":0,"MSFT":0};self.ids={};self.s={"ioc_open_later":None,"gtc_open_later":None,"gtc_open_at_stop":None,"events":{},"fills":[]}
        ctx.subscribe_bars(["AAPL","MSFT"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):
        b=event.data();sym=b.symbol;self.counts[sym]+=1;n=self.counts[sym]
        if n==10 and sym=="AAPL":
            o=ctx.buy_order(sym,1,order_type=OrderType.LIMIT,limit_price=round(b.close*.5,2),time_in_force="IOC")
            if o:self.ids["ioc"]=o.order_id
        elif n==10 and sym=="MSFT":
            o=ctx.buy_order(sym,1,order_type=OrderType.LIMIT,limit_price=1.0,time_in_force="GTC")
            if o:self.ids["gtc"]=o.order_id
        elif n==20 and sym=="AAPL":self.s["ioc_open_later"]=ctx.has_open_order(sym)
        elif n==20 and sym=="MSFT":self.s["gtc_open_later"]=ctx.has_open_order(sym)
    def on_order(self,ctx,event):
        o=event.data()
        for leg,oid in self.ids.items():
            if o.order_id==oid:
                self.s["events"].setdefault(leg,[]).append(event.type.value)
                if event.type==EventType.ORDER_FILLED:self.s["fills"].append(leg)
    def on_stop(self,ctx,event):
        self.s["gtc_open_at_stop"]=ctx.has_open_order("MSFT")
        emit_checkpoint(ctx,"t30_ioc_gtc_limits",self.s|{"ids":self.ids})


if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT30",type="SdkT30",symbols=["AAPL","MSFT"])],
        symbols=["AAPL","MSFT"],start_date="2025-12-02",end_date="2025-12-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="16:00"))
    s=completed_checkpoint(run,"t30_ioc_gtc_limits")
    finish("t30_ioc_gtc_limits",{
        "both_orders_created":set(s["ids"])=={"ioc","gtc"},
        "ioc_not_open_later":s["ioc_open_later"] is False,
        "gtc_rests_intraday":s["gtc_open_later"] is True,
        "ioc_terminal_event":any(x in s["events"].get("ioc",[]) for x in ("ORDER_CANCELED","ORDER_EXPIRED")),
        "gtc_persists_through_session_close":s["gtc_open_at_stop"] is True,
        "gtc_not_auto_canceled":"ORDER_CANCELED" not in s["events"].get("gtc",[]),
        "neither_far_limit_filled":not s["fills"],
    },extra=str(s))
