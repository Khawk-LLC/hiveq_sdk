"""Multiple futures rollovers square old contracts and preserve integer positions."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig,StrategyConfig
from hiveq.flow.config import AssetType


class SdkT38:
    def on_start(self,ctx,event):
        if not hasattr(self,"s"):
            self.bought=set();self.pending=[];self.s={"bars":0,"rollovers":[],"fills":[],"fractional":[],"rollover_observations":[],"order_events":[]}
        ctx.subscribe_bars(["ES.v.0","NQ.v.0"],asset_type=AssetType.FUTURES,interval="1m")
    def _positions(self,ctx):
        return {p.symbol:float(p.quantity) for p in ctx.portfolio().positions() if abs(float(p.quantity))>1e-9}
    def on_bar(self,ctx,event):
        self.s["bars"]+=1;root=event.data().symbol[:2];continuous=f"{root}.v.0"
        for item in self.pending:
            if item["remaining"]>0:
                item["after"].append({"bar":self.s["bars"],"symbol":event.data().symbol,"positions":self._positions(ctx)})
                item["remaining"]-=1
        if continuous not in self.bought:
            self.bought.add(continuous);ctx.buy_order(continuous,1)
    def on_rollover(self,ctx,event):
        d=event.data();row=[d.continuous_symbol,d.prev_contract,d.current_contract];self.s["rollovers"].append(row)
        observation={"rollover":row,"before":self._positions(ctx),"after":[],"remaining":4}
        self.s["rollover_observations"].append(observation);self.pending.append(observation)
    def on_order(self,ctx,event):
        o=event.data()
        self.s["order_events"].append({"event":str(event.type),"symbol":o.symbol,"status":str(o.status),"side":str(o.side),"quantity":float(o.quantity),"filled_qty":float(o.filled_qty)})
        if o.is_filled and o.filled_qty:
            q=float(o.filled_qty);self.s["fills"].append([o.symbol,str(o.side),q])
            if abs(q-round(q))>1e-9:self.s["fractional"].append([o.symbol,q])
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t38_multi_rollover_squareoff",self.s)


if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT38",type="SdkT38",symbols=["ES.v.0","NQ.v.0"])],
        symbols=["ES.v.0","NQ.v.0"],start_date="2024-12-01",end_date="2025-06-24",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_FUT","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(enable_auto_rollover=True,session_start="18:00",session_end="17:00"))
    s=completed_checkpoint(run,"t38_multi_rollover_squareoff");balances={};order=[]
    for symbol,side,qty in s["fills"]:
        balances[symbol]=balances.get(symbol,0)+(qty if side.upper().endswith("BUY") else -qty)
        if symbol not in order:order.append(symbol)
    old_ok=True;final_ok=True;roots={}
    # Build the authoritative contract sequence from rollover callbacks, not
    # fills: a missing rollover fill is precisely what this test must detect.
    for continuous,prev,current in s["rollovers"]:
        root=continuous.split(".",1)[0]
        chain=roots.setdefault(root,[])
        for contract in (prev,current):
            if contract not in chain:chain.append(contract)
    for contracts in roots.values():
        old_ok=old_ok and all(abs(balances.get(x,0))<1e-9 for x in contracts[:-1])
        final_ok=final_ok and abs(balances.get(contracts[-1],0)-1)<1e-9
    finish("t38_multi_rollover_squareoff",{
        "bars_delivered":s["bars"]>0,
        "both_roots_filled":{"ES","NQ"}<=set(roots),
        "multiple_rollovers":len(s["rollovers"])>=4,
        "multiple_contracts_per_root":all(len(x)>=3 for x in roots.values()),
        "old_contracts_squared":old_ok,
        "final_contracts_long_one":final_ok,
        "integer_fills_only":not s["fractional"],
    },extra=f"rollovers={s['rollovers']}, chains={roots}, balances={balances}")
