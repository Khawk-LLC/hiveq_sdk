"""Remote position callbacks, exposures, equity identity, and realized PnL."""
from pathlib import Path
import sys
# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType,BacktestConfig
INITIAL=1_000_000.0

class SdkT06:
    def on_start(self,ctx,event):
        self.s={"bars":0,"position_events":0,"quantities":[],"hold":{},"entry":0.0,"exit":0.0,"flat":None,"realized":None}
        ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):
        self.s["bars"]+=1;b=event.data()
        if self.s["bars"]==1:ctx.buy_order("AAPL",quantity=100)
        elif self.s["bars"]==20:
            p=ctx.portfolio();gross=p.gross_exposure();net=p.net_exposure();approx=100*b.close
            self.s["hold"]={"net_position_100":p.net_position("AAPL")==100,"gross_exposure_sane":abs(gross-approx)/approx<.05,
                "net_equals_gross_long_only":abs(net-gross)<1e-6,"equity_identity":abs(p.equity-(INITIAL+p.realized_pnl()+p.unrealized_pnl()))<5}
        elif self.s["bars"]==30 and not ctx.has_open_order("AAPL"):ctx.close_position("AAPL")
        elif self.s["bars"]==50:self.s["flat"]=ctx.is_flat("AAPL");self.s["realized"]=ctx.portfolio().realized_pnl()
    def on_order(self,ctx,event):
        o=event.data()
        if "FILL" in str(getattr(o,"status","")).upper() and float(o.filled_qty or 0)>0:
            side=str(o.side).upper()
            if side.endswith("BUY") and not self.s["entry"]:self.s["entry"]=float(o.avg_px or 0)
            elif side.endswith("SELL") and not self.s["exit"]:self.s["exit"]=float(o.avg_px or 0)
    def on_position(self,ctx,event):
        self.s["position_events"]+=1;q=float(event.data().quantity)
        if q not in self.s["quantities"]:self.s["quantities"].append(q)
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t06_position_portfolio",self.s)

if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT06",type="SdkT06",symbols=["AAPL"])],symbols=["AAPL"],
        start_date="2025-06-02",end_date="2025-06-02",data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],backtest_config=BacktestConfig(initial_capital=INITIAL,session_start="09:30",session_end="11:00"))
    s=completed_checkpoint(run,"t06_position_portfolio");expected=(s["exit"]-s["entry"])*100
    realized=s["realized"] is not None and abs(s["realized"]-expected)<max(5,abs(expected)*.1+5)
    finish("t06_position_portfolio",{"position_events_fired":s["position_events"]>0,"position_qty_reported":100.0 in s["quantities"],
        **s["hold"],"flat_after_close":s["flat"] is True,"realized_matches_fills":realized},extra=str(s))
