"""Sequential backtests keep event logs and state isolated by run id."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig,StrategyConfig
from hiveq.flow.config import AssetType


class SdkT34:
    def on_start(self,ctx,event):
        self.label=ctx.strategy_config.label;self.bars=0
        ctx.subscribe_bars(ctx.strategy_config.symbols,asset_type=AssetType.EQUITY,interval="1m")
        ctx.add_event_log(f"isolation-start-{self.label}",sub_event_type="ISOLATION")
    def on_bar(self,ctx,event):self.bars+=1
    def on_stop(self,ctx,event):emit_checkpoint(ctx,f"t34_run_isolation_{self.label}",{"label":self.label,"bars":self.bars})


if __name__=="__main__":
    runs=[];states=[];messages=[]
    for label,symbol in (("first","AAPL"),("second","MSFT")):
        run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT34",type="SdkT34",symbols=[symbol],params={"label":label})],
            symbols=[symbol],start_date="2025-08-19",end_date="2025-08-19",
            data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
            backtest_config=BacktestConfig(session_start="09:30",session_end="10:30"))
        states.append(completed_checkpoint(run,f"t34_run_isolation_{label}"));runs.append(run)
        logs=run.event_logs();messages.append(set(logs["message"].astype(str)) if len(logs) else set())
    finish("t34_run_isolation",{
        "distinct_run_ids":runs[0].run_id!=runs[1].run_id,
        "both_runs_received_data":all(s["bars"]>0 for s in states),
        "first_log_in_first_run":"isolation-start-first" in messages[0],
        "second_log_in_second_run":"isolation-start-second" in messages[1],
        "first_log_not_leaked":"isolation-start-first" not in messages[1],
        "second_log_not_backfilled":"isolation-start-second" not in messages[0],
    },extra=f"states={states}, run_ids={[r.run_id for r in runs]}")
