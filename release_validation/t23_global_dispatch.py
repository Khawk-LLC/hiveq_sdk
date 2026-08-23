"""A conventional SdkT23 strategy is captured, submitted, and dispatched on platform."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


class SdkT23:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.state = {"starts": 1, "bars": 0, "orders": 0, "fills": 0, "stops": 0}
            self.trade_open = False
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1
        if not self.trade_open:
            self.trade_open = ctx.buy_order(event.data().symbol, 10.0) is not None
            self.state["orders"] += int(self.trade_open)
        elif self.state["orders"] == 1:
            closed = ctx.sell_order(event.data().symbol, 10.0) is not None
            self.state["orders"] += int(closed)
            self.trade_open = not closed

    def on_order(self, ctx, event):
        if event.data().is_filled:
            self.state["fills"] += 1

    def on_stop(self, ctx, event):
        self.state["stops"] += 1
        emit_checkpoint(ctx, "t23_sdk_dispatch", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT23", type="SdkT23", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-01",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t23_sdk_dispatch")
    finish("t23_sdk_dispatch", {
        "start_dispatched": state["starts"] == 1,
        "bars_dispatched": state["bars"] > 0,
        "order_placed": state["orders"] == 2,
        "fill_dispatched": state["fills"] >= 2,
        "stop_dispatched": state["stops"] >= 1,
        "public_orders_recorded": len(run.orders()) >= 1,
    }, extra=str(state))
