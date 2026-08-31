"""Equity same-day and futures overnight sessions, including weekend and DST."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT27A:
    def __init__(self):
        self.s = {"sessions": [], "weekend_bars": 0, "outside": 0, "bars": 0}
    def on_start(self, ctx, event):
        self.s["sessions"].append({"start": event.time.isoformat() if event.time else None,
                                   "first": None, "count": 0})
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        bar = event.data(); session = self.s["sessions"][-1]; self.s["bars"] += 1; session["count"] += 1
        if session["first"] is None: session["first"] = bar.time.isoformat()
        if bar.time.weekday() >= 5: self.s["weekend_bars"] += 1
        minute = bar.time.hour * 60 + bar.time.minute
        if not 570 <= minute <= 960: self.s["outside"] += 1
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t27_equity_session", self.s)


class SdkT27B:
    def __init__(self):
        self.s = {"sessions": [], "saturday_ticks": 0, "ticks": 0, "offsets": []}
    def on_start(self, ctx, event):
        self.s["sessions"].append({"start": event.time.isoformat() if event.time else None,
                                   "first": None, "count": 0})
        ctx.subscribe_futures_trades(continuous="ES.c.0")
    def on_trade(self, ctx, event):
        trade = event.data(); session = self.s["sessions"][-1]; self.s["ticks"] += 1; session["count"] += 1
        if session["first"] is None: session["first"] = trade.time.isoformat()
        if trade.time.weekday() == 5: self.s["saturday_ticks"] += 1
        offset = trade.time.utcoffset().total_seconds() / 3600
        if offset not in self.s["offsets"]: self.s["offsets"].append(offset)
    def on_stop(self, ctx, event): emit_checkpoint(ctx, "t27_futures_session", self.s)


if __name__ == "__main__":
    equity_run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT27A", type="SdkT27A", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2026-04-02", end_date="2026-04-07",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="16:00"))
    equity = completed_checkpoint(equity_run, "t27_equity_session")
    futures_run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT27B", type="SdkT27B", symbols=["ES.c.0"])],
        symbols=["ES.c.0"], start_date="2026-03-05", end_date="2026-03-10",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_FUT","schema":["fut_trades"]}],
        backtest_config=BacktestConfig(session_start="18:00", session_end="17:00"))
    futures = completed_checkpoint(futures_run, "t27_futures_session")
    eq_data_sessions = [s for s in equity["sessions"] if s["count"]]
    fut_data_sessions = [s for s in futures["sessions"] if s["count"]]
    finish("t27_equity_futures_sessions", {
        "equity_bars_delivered": equity["bars"] > 0,
        "equity_same_day_starts_0930_ET": bool(eq_data_sessions) and all("T09:30:" in s["start"] for s in eq_data_sessions),
        "equity_no_weekend_bars": equity["weekend_bars"] == 0,
        "equity_bars_inside_session": equity["outside"] == 0,
        "futures_ticks_delivered": futures["ticks"] > 0,
        "futures_sessions_start_1800_ET": bool(fut_data_sessions) and all("T18:00:" in s["start"] for s in fut_data_sessions),
        "futures_no_saturday_ticks": futures["saturday_ticks"] == 0,
        "futures_spans_EST_and_EDT": sorted(futures["offsets"]) == [-5, -4],
    }, extra=f"equity={equity}, futures={futures}")
