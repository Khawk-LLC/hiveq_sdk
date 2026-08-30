"""Intentional callback exceptions remain visible in complete executor logs."""
from pathlib import Path
import sys
sys.path[:0]=[str(Path(__file__).resolve().parent)]
from qa_common import finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger=_get_logger(); MARKER="SDK_T32_INTENTIONAL_ATTRIBUTE_ERROR"


class SdkT32:
    def on_start(self,ctx,event):
        self.raised=False;ctx.subscribe_bars(["AAPL"],asset_type=AssetType.EQUITY,interval="1m")
    def on_bar(self,ctx,event):
        if not self.raised:
            self.raised=True
            raise AttributeError(MARKER)


if __name__=="__main__":
    run=hf.run_backtest(strategy_configs=[StrategyConfig(name="SdkT32",type="SdkT32",symbols=["AAPL"])],
        symbols=["AAPL"],start_date="2026-04-01",end_date="2026-04-01",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}])
    run.wait(progress=False);status=run.status();lines=run.logs();text="\n".join(lines)
    final=bool(status.get("is_final") or str(status.get("status","")).lower() in {"completed","done"})
    if getattr(run,"is_local",False) and not lines:
        # Executor log lines are a deploy-mode surface: Run.logs() returns []
        # for an in-process engine run by design, so the log-scraping contract
        # cannot be evaluated here. Report the gap rather than a regression,
        # but still require the run to have survived the raised callback --
        # t71 covers in-process callback-failure dispatch in detail.
        finish("t32_callback_error_visibility",{
            "run_completed_despite_callback_error":final,
        },gap=True,extra=f"executor logs unavailable for in-memory runs; status={status}")
    else:
        finish("t32_callback_error_visibility",{
            "executor_log_available":bool(lines),
            "intentional_error_visible":MARKER in text,
            "standard_callback_error_tag":"STRATEGY_CALLBACK_ERROR" in text,
            "callback_name_visible":"on_bar" in text,
            "traceback_visible":"Traceback" in text or "AttributeError" in text,
        },extra=f"status={status}, log_lines={len(lines)}")
