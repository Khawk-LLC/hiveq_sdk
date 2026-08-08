"""Adaptive parameter changes persist as structured PARAM_CHANGE events."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig,StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger=_get_logger()


class SdkT33:
    def on_start(self,ctx,event):
        self.n=0;self.size=100;self.s={"changes":0};ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):
        self.n+=1
        if self.n in (5,10,15):
            old=self.size;self.size+=25
            ctx.log_parameter_change("position_size",old,self.size,symbol="AAPL")
            self.s["changes"]+=1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t33_parameter_changes",self.s|{"final_size":self.size})


if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT33",type="SdkT33",symbols=["AAPL"])],
        symbols=["AAPL"],start_date="2025-09-02",end_date="2025-09-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30",session_end="10:30"))
    s=completed_checkpoint(run,"t33_parameter_changes");logs=run.event_logs()
    types=logs["event_log_type"].astype(str).str.upper() if len(logs) and "event_log_type" in logs else []
    param_rows=logs[types.str.contains("PARAM_CHANGE")] if hasattr(types,"str") else logs.iloc[0:0]
    finish("t33_parameter_changes",{
        "three_changes_emitted":s["changes"]==3,
        "adaptive_value_updated":s["final_size"]==175,
        "param_change_rows_persisted":len(param_rows)>=3,
        "parameter_name_visible":any("position_size" in str(x) for x in param_rows.to_dict("records")),
    },extra=f"state={s}, param_rows={len(param_rows)}")
