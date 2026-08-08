"""Strategy-scoped portfolios remain separate while global portfolio aggregates."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import checkpoint, emit_checkpoint, finish, wait_for_final
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT25A:
    def on_start(self, ctx, event):
        self.sent = False; ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        if not self.sent: self.sent = ctx.buy_order("AAPL", 100) is not None
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t25_global_portfolio_a", {"sent": self.sent,
            "own": ctx.portfolio().net_position("AAPL"),
            "global": ctx.global_portfolio().net_position("AAPL")})


class SdkT25B:
    def on_start(self, ctx, event):
        self.sent = False; ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        if not self.sent: self.sent = ctx.buy_order("AAPL", 50) is not None
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t25_global_portfolio_b", {"sent": self.sent,
            "own": ctx.portfolio().net_position("AAPL"),
            "global": ctx.global_portfolio().net_position("AAPL")})


if __name__ == "__main__":
    run = hf.run_backtest(strategy_configs=[
        StrategyConfig(name="SdkT25A", type="SdkT25A", symbols=["AAPL"]),
        StrategyConfig(name="SdkT25B", type="SdkT25B", symbols=["AAPL"]),
    ], symbols=["AAPL"], start_date="2025-08-01", end_date="2025-08-01",
       data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
       backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"))
    wait_for_final(run)
    a = checkpoint(run, "t25_global_portfolio_a")
    b = checkpoint(run, "t25_global_portfolio_b")
    finish("t25_global_portfolio", {
        "both_orders_placed": a["sent"] and b["sent"],
        "strategy_a_position_isolated": a["own"] == 100,
        "strategy_b_position_isolated": b["own"] == 50,
        "global_position_aggregates_a": a["global"] == 150,
        "global_position_aggregates_b": b["global"] == 150,
        "orders_persisted": len(run.orders()) >= 2,
    }, extra=f"a={a}, b={b}")
