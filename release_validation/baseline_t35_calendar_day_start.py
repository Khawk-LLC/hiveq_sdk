"""on_start fires once per calendar day, including weekends and holidays."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint,emit_checkpoint,finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger=_get_logger()


class SdkT35:
    def __init__(self):self.starts=[];self.init_count=1
    def on_start(self,ctx,event):
        day=str(ctx.now().date());self.starts.append(day)
        ctx.subscribe_bars(["SPY"],asset_type=AssetType.EQUITY,interval="1d")
    def on_stop(self,ctx,event):emit_checkpoint(ctx,"t35_calendar_day_start",{"init_count":self.init_count,"starts":self.starts})


if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT35",type="SdkT35",symbols=["SPY"])],
        symbols=["SPY"],start_date="2023-01-01",end_date="2023-01-31",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1d"]}])
    s=completed_checkpoint(run,"t35_calendar_day_start");days=set(s["starts"])
    finish("t35_calendar_day_start",{
        "one_strategy_instance":s["init_count"]==1,
        "one_start_per_calendar_day":len(s["starts"])==31 and len(days)==31,
        "weekends_included":any(__import__("datetime").date.fromisoformat(d).weekday()>=5 for d in days),
        "MLK_holiday_included":"2023-01-16" in days,
        "range_endpoints_included":{"2023-01-01","2023-01-31"}<=days,
    },extra=f"count={len(s['starts'])}, first={s['starts'][:2]}, last={s['starts'][-2:]}")
