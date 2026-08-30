"""Remote metric arithmetic independently recomputed from persisted fills."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig
INITIAL = 1_000_000.0

class SdkT10:
    def on_start(self, ctx, event):
        self.bars=0; self.fills=[]
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        self.bars += 1
        if self.bars == 1: ctx.buy_order("AAPL", quantity=100)
        elif self.bars == 60 and not ctx.has_open_order("AAPL"): ctx.close_position("AAPL")
    def on_order(self, ctx, event):
        order=event.data()
        if "FILL" in str(getattr(order,"status","")).upper() and float(order.filled_qty or 0)>0:
            self.fills.append([str(order.side).upper(),float(order.filled_qty),float(order.avg_px or 0)])
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t10_metrics_validation", {"fills":self.fills})

if __name__ == "__main__":
    run=hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT10",type="SdkT10",symbols=["AAPL"])],
        symbols=["AAPL"],start_date="2025-06-02",end_date="2025-06-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(initial_capital=INITIAL,session_start="09:30",session_end="12:00"))
    state=completed_checkpoint(run,"t10_metrics_validation"); report=run.report()
    buys=[x for x in state["fills"] if x[0].endswith("BUY")]; sells=[x for x in state["fills"] if x[0].endswith("SELL")]
    def weighted_average(rows): return sum(q*px for _,q,px in rows)/max(1e-9,sum(q for _,q,_ in rows))
    entry=weighted_average(buys); exit=weighted_average(sells); gross=(exit-entry)*100
    fees=float(report.total_fees or 0); expected=gross-fees
    daily=run.daily_returns(); return_ok=False; return_detail="no daily row"
    if len(daily):
        columns=[c for c in daily.columns if "return" in str(c).lower()]
        if columns:
            actual=float(daily[columns[0]].iloc[-1]); fraction=report.net_pnl/INITIAL
            return_ok=abs(actual-fraction)<5e-4 or abs(actual-fraction*100)<5e-2
            return_detail=f"{columns[0]}={actual}"
    trades=run.trades(); pnl_columns=[c for c in trades.columns if str(c).lower()=="pnl"]
    trades_ok=not pnl_columns or abs(float(trades[pnl_columns[0]].sum())-gross)<max(2,abs(gross)*.1)
    finish("t10_metrics_validation", {
        "round_trip_recorded":bool(buys) and bool(sells),
        "net_pnl_matches_fills":abs(report.net_pnl-expected)<max(2,abs(expected)*.05),
        "daily_return_consistent":return_ok,"trades_pnl_consistent":trades_ok,
        "fees_non_negative":fees>=0,
    }, extra=f"entry={entry}, exit={exit}, expected={expected}, report={report.net_pnl}, {return_detail}")
