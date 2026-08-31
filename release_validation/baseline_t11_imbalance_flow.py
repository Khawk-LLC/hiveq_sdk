"""Public SDK imbalance callback contract; absent QA rows are an explicit GAP."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()

class SdkT11:
    def on_start(self, ctx, event):
        self.state={"bars":0,"imbalances":0,"payload_ok":True,"event_type_ok":True,"sample":{}}
        ctx.subscribe_bars(["ABBV"],asset_type=AssetType.EQUITY,interval="1m")
        logger.info("[START] waiting for ABBV early_imbalance rows")
    def on_bar(self, ctx, event):
        self.state["bars"] += 1
        logger.debug(f"[BAR] count={self.state['bars']} time={event.data().time}")
    def on_imbalance(self, ctx, event):
        data=event.data(); self.state["imbalances"] += 1
        self.state["event_type_ok"] = self.state["event_type_ok"] and event.type==EventType.IMBALANCE
        self.state["payload_ok"] = self.state["payload_ok"] and data.symbol=="ABBV" and data.side in {"B","S"} and data.imbalance>=0 and data.paired_shares>=0 and isinstance(event.ts_event,(int,float)) and event.ts_event>1e18
        if not self.state["sample"]:
            self.state["sample"]={"symbol":data.symbol,"side":data.side,"imbalance":data.imbalance,"paired_shares":data.paired_shares,"ref_price":data.ref_price}
        logger.debug(f"[IMBALANCE] count={self.state['imbalances']} symbol={data.symbol} side={data.side}")
    def on_stop(self, ctx, event): emit_checkpoint(ctx,"t11_imbalance_flow",self.state)

if __name__ == "__main__":
    run=hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT11",type="SdkT11",symbols=["ABBV"])],
        symbols=["ABBV"],start_date="2026-04-30",end_date="2026-04-30",
        data_configs=[
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},
            {"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["early_imbalance"]},
        ])
    state=completed_checkpoint(run,"t11_imbalance_flow"); has_rows=state["imbalances"]>0
    checks={"run_with_imbalance_schema":state["bars"]>0}
    if has_rows: checks.update({"on_imbalance_fired":True,"event_type_IMBALANCE":state["event_type_ok"],"payload_contract":state["payload_ok"]})
    finish("t11_imbalance_flow",checks,extra=f"imbalance_events={state['imbalances']} (zero is QA data coverage)",gap=not has_rows)
