"""Hosted signal rows support static selectors and dynamic subscribe_data filters."""
from pathlib import Path
import json,sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig,StrategyConfig
from hiveq.flow.config import AssetType


class SdkT37A:
    def on_start(self,ctx,event):
        self.s={"bars":0,"signals":0,"parse_errors":0,"samples":[]};ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m");ctx.subscribe_data("StaticSignals")
    def on_bar(self,ctx,event):self.s["bars"]+=1
    def on_custom_data(self,ctx,event):self.read(event.data())
    def read(self,d):
        self.s["signals"]+=1;raw=d.column_data("signal_json")
        try:
            value=json.loads(raw.replace(r'\"','"').replace('|',',')) if raw else {}
            if len(self.s["samples"])<3:self.s["samples"].append({"symbol":d.symbol,"ticker":value.get("ticker"),"flag":value.get("flag")})
        except Exception:self.s["parse_errors"]+=1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t37_static_signals",self.s)


class SdkT37B(SdkT37A):
    def on_start(self,ctx,event):
        self.s={"bars":0,"signals":0,"parse_errors":0,"samples":[]};ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m");ctx.subscribe_data("DynamicSignals",signals=["Prillach_MC_ES"])
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t37_dynamic_signals",self.s)


def run_case(cls,name,data_id,symbols=None):
    config={"type":"hiveq_historical","dataset":"HIVEQ_QUANT_SIGNALS","schema":["signals"],"id":data_id}
    if symbols:config["symbols"]=symbols
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name=name,type=cls.__name__,symbols=["AAPL"])],
        symbols=["AAPL"],start_date="2024-08-27",end_date="2024-08-27",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},config],
        backtest_config=BacktestConfig(session_start="04:00",session_end="18:30"))
    return completed_checkpoint(run,"t37_static_signals" if cls is SdkT37A else "t37_dynamic_signals")


if __name__=="__main__":
    static=run_case(SdkT37A,"SdkT37A","StaticSignals",["Prillach_MC_ES"])
    dynamic=run_case(SdkT37B,"SdkT37B","DynamicSignals")
    has_rows=static["signals"]>0 and dynamic["signals"]>0
    checks={"control_bars_static":static["bars"]>0,"control_bars_dynamic":dynamic["bars"]>0}
    if has_rows:checks|={"static_selector_delivered":static["signals"]>0,"dynamic_selector_delivered":dynamic["signals"]>0,
        "static_json_parses":static["parse_errors"]==0,"dynamic_json_parses":dynamic["parse_errors"]==0,
        "static_samples":bool(static["samples"]),"dynamic_samples":bool(dynamic["samples"])}
    finish("t37_hosted_signals",checks,extra=f"static={static}, dynamic={dynamic}",gap=not has_rows)
