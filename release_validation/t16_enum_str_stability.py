"""Platform validation of public trading enum string stability."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.trading_types import MarketCenter, OrderSide, OrderStatus, OrderType


class SdkT16:
    def on_start(self, ctx, event):
        cases = [
            (OrderSide.BUY, "BUY"), (OrderSide.SELL, "SELL"),
            (OrderType.MARKET, "MARKET"), (OrderType.LIMIT, "LIMIT"),
            (OrderType.STOP, "STOP"), (OrderType.STOP_LIMIT, "STOP_LIMIT"),
            (OrderType.MOO, "MOO"), (OrderType.MOC, "MOC"),
            (OrderType.LOO, "LOO"), (OrderType.LOC, "LOC"),
            (OrderStatus.FILLED, "FILLED"), (OrderStatus.REJECTED, "REJECTED"),
            (MarketCenter.NYSE, "NYSE"), (MarketCenter.NASDAQ, "NASDAQ"),
        ]
        self.state = {
            "checks": {f"str_{expected}": str(member) == expected for member, expected in cases},
            "members": len(cases),
            "bars": 0,
        }
        self.state["checks"].update({
            "f_string_uses_value": f"{OrderSide.BUY}" == "BUY",
            "name_and_value_stable": OrderSide.BUY.name == OrderSide.BUY.value == "BUY",
            "dispatch_map_compatible": {"BUY": 1, "SELL": -1}[str(OrderSide.SELL)] == -1,
        })
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        self.state["bars"] += 1

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t16_enum_str_stability", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT16", type="SdkT16", symbols=["AAPL"])],
        symbols=["AAPL"],
        start_date="2025-09-15",
        end_date="2025-09-15",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:00"),
    )
    state = completed_checkpoint(run, "t16_enum_str_stability")
    checks = dict(state["checks"])
    checks["platform_timeline_present"] = state["bars"] > 0
    finish("t16_enum_str_stability", checks, extra=f"members={state['members']}")
