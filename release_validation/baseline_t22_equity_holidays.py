"""Equity holidays deliver no bars and therefore permit no strategy orders."""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()


class SdkT22:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.state = {"bar_dates": {}, "order_dates": {}}
            self.bought = set()
        ctx.subscribe_bars(["AAPL", "MSFT"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data(); day = str(bar.time.date()); key = f"{day}:{bar.symbol}"
        self.state["bar_dates"][day] = self.state["bar_dates"].get(day, 0) + 1
        if key not in self.bought:
            self.bought.add(key); ctx.buy_order(bar.symbol, 1)
            self.state["order_dates"][day] = self.state["order_dates"].get(day, 0) + 1
        logger.debug(f"[BAR] {key}")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t22_equity_holidays", self.state)


if __name__ == "__main__":
    cases = [
        ("2025-07-03", "2025-07-07", "2025-07-04"),
        ("2025-08-29", "2025-09-02", "2025-09-01"),
    ]
    states = {}
    for start, end, holiday in cases:
        run = hf.run_backtest(
            strategy_configs=[StrategyConfig(name="SdkT22", type="SdkT22", symbols=["AAPL", "MSFT"])],
            symbols=["AAPL", "MSFT"], start_date=start, end_date=end,
            data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
            backtest_config=BacktestConfig(session_start="09:30", session_end="16:00"))
        states[holiday] = completed_checkpoint(run, "t22_equity_holidays")
    checks = {}
    for holiday, state in states.items():
        checks[f"control_days_present_around_{holiday}"] = bool(state["bar_dates"])
        checks[f"no_bars_{holiday}"] = holiday not in state["bar_dates"]
        checks[f"no_orders_{holiday}"] = holiday not in state["order_dates"]
    finish("t22_equity_holidays", checks, extra=str(states))
