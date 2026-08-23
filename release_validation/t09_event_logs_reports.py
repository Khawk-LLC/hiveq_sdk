"""Remote structured event logs and every public Run result surface."""
from pathlib import Path
import sys
# Assignment survives the engine's source graft; a bare sys.path.insert call does not.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig

class SdkT09:
    def on_start(self, ctx, event):
        self.bars = 0
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        self.bars += 1; bar = event.data()
        if self.bars == 1:
            ctx.add_event_log("qa_entry_signal", sub_event_type="SIGNAL", symbol="AAPL",
                              state_variable={"close": bar.close})
            ctx.buy_order("AAPL", quantity=50)
        elif self.bars == 30 and not ctx.has_open_order("AAPL"):
            ctx.add_event_log("qa_exit_signal", sub_event_type="SIGNAL", symbol="AAPL",
                              state_variable={"close": bar.close})
            ctx.close_position("AAPL")
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t09_event_logs_reports", {"bars": self.bars})

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT09", type="SdkT09", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="11:00", export_orders_csv=True))
    completed_checkpoint(run, "t09_event_logs_reports")
    report=run.report(); orders=run.orders(); trades=run.trades(); positions=run.positions()
    metrics=run.metrics(); equity=run.equity_curve(); daily=run.daily_returns()
    summary=run.summary(); logs=run.event_logs()
    messages=set(logs["message"].astype(str)) if len(logs) and "message" in logs else set()
    finish("t09_event_logs_reports", {
        "structured_event_logs": {"qa_entry_signal","qa_exit_signal"} <= messages,
        "report_net_pnl_present": isinstance(report.net_pnl, float),
        "orders_table": len(orders)>=2, "trades_table": len(trades)>=1,
        "positions_table": len(positions)>=1, "metrics_table": len(metrics)>=1,
        "daily_returns_table": len(daily)>=1, "equity_curve_table": len(equity)>=1,
        "summary_dict": isinstance(summary, dict),
    }, extra=f"orders={len(orders)}, trades={len(trades)}, logs={len(logs)}")
