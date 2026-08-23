"""Uploaded CSV custom rows reach only the strategy subscribing to their id."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType


class SdkT36A:
    def on_start(self,ctx,event):
        if not hasattr(self,"s"):self.s={"bars":0,"custom":0,"samples":[]}
        ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m");ctx.subscribe_data("UserDataTest")
    def on_bar(self,ctx,event):self.s["bars"]+=1
    def on_custom_data(self,ctx,event):
        d=event.data();self.s["custom"]+=1
        if len(self.s["samples"])<3:self.s["samples"].append({"symbol":d.symbol,"action":d.column_data("action"),"weight":d.column_data("weight")})
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t36_custom_subscriber",self.s)


class SdkT36B:
    def on_start(self,ctx,event):
        if not hasattr(self,"s"):self.s={"bars":0,"custom":0}
        ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):self.s["bars"]+=1
    def on_custom_data(self,ctx,event):self.s["custom"]+=1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t36_bars_only",self.s)


if __name__=="__main__":
    fixture=Path(__file__).resolve().parent/"data"/"user_data.csv"
    # Local backtests consume the fixture directly; there is no upload step.
    run=hf.run_backtest(strategy_configs=[
        StrategyConfig(name="SdkT36A",type="SdkT36A",symbols=["AAPL"]),
        StrategyConfig(name="SdkT36B",type="SdkT36B",symbols=["AAPL"]),
    ],symbols=["AAPL"],start_date="2025-08-01",end_date="2025-08-02",
      data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]},
                    {"type":"csv","data_type":"custom","path":str(fixture),"id":"UserDataTest"}])
    run.wait(progress=False);status=run.status() or {};status_name=str(status.get("status","")).lower()
    if status_name in {"failed","error","terminated"}:
        log_text="\n".join(run.logs());missing="user_data.csv" in log_text and any(x in log_text.lower() for x in ("not found","no such file","missing"))
        finish("t36_custom_csv_routing",{"missing_uploaded_fixture_identified":missing},
               extra="upload examples/bt/userdata/user_data.csv as user_data.csv before release validation",gap=missing)
    else:
        subscriber=checkpoint(run,"t36_custom_subscriber");bars_only=checkpoint(run,"t36_bars_only")
        finish("t36_custom_csv_routing",{
            "control_bars_both_strategies":subscriber["bars"]>0 and bars_only["bars"]>0,
            "subscriber_received_rows":subscriber["custom"]>0,
            "payload_samples_recorded":bool(subscriber["samples"]),
            "mandatory_symbol_present":all(x["symbol"]=="AAPL" for x in subscriber["samples"]),
            "custom_columns_readable":all(x["action"] and x["weight"] for x in subscriber["samples"]),
            "non_subscriber_isolated":bars_only["custom"]==0,
        },extra=f"subscriber={subscriber}, bars_only={bars_only}")
