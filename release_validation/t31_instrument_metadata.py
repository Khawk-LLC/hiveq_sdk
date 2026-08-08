"""Registered equity and futures instruments expose multiplier and tick metadata."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading.price_utils import adjust_tick_size,get_min_tick

logger=_get_logger()


class SdkT31:
    def on_start(self,ctx,event):
        self.s={"bars":{},"meta":{}}
        ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
        ctx.subscribe_futures_bars(continuous="ES.c.0",interval="1m")
        for key in ("AAPL","ES.c.0"):
            inst=ctx.instrument(key)
            self.s["meta"][key]={"exists":inst is not None,
                "multiplier":inst.multiplier if inst else None,
                "min_tick":inst.min_tick if inst else None,
                "asset_type":str(inst.asset_type) if inst else None,
                "current_contract":inst.current_contract if inst else None,
                "helper_tick":get_min_tick(key),
                "rounded":adjust_tick_size(key,100.123 if key=="AAPL" else 6000.13)}
    def on_bar(self,ctx,event):
        sym=event.data().symbol;key="ES.c.0" if sym.startswith("ES") else sym
        self.s["bars"][key]=self.s["bars"].get(key,0)+1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t31_instrument_metadata",self.s)


if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT31",type="SdkT31",symbols=["AAPL","ES.c.0"])],
        symbols=["AAPL","ES.c.0"],start_date="2025-12-15",end_date="2025-12-15",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},
                      {"type":"hiveq_historical","dataset":"HIVEQ_US_FUT","schema":["fut_trades"]}])
    s=completed_checkpoint(run,"t31_instrument_metadata")
    eq=s["meta"].get("AAPL",{}); fut=s["meta"].get("ES.c.0",{})
    finish("t31_instrument_metadata",{
        "both_streams_delivered":all(s["bars"].get(x,0)>0 for x in ("AAPL","ES.c.0")),
        "both_instruments_registered":all(s["meta"].get(x,{}).get("exists") for x in ("AAPL","ES.c.0")),
        "equity_multiplier_positive":(eq.get("multiplier") or 0)>0,
        "futures_multiplier_positive":(fut.get("multiplier") or 0)>0,
        "ticks_positive":all((s["meta"].get(x,{}).get("min_tick") or 0)>0 for x in ("AAPL","ES.c.0")),
        "continuous_contract_resolved":bool(fut.get("current_contract")),
        "public_tick_helper_equity":eq.get("helper_tick")==.01 and eq.get("rounded")==100.12,
        "public_tick_helper_futures":fut.get("helper_tick")==.25 and fut.get("rounded")==6000.25,
    },extra=str(s))
