"""STOP/STOP_LIMIT exits trigger and a manual bracket cancels its resting sibling."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.trading.price_utils import adjust_tick_size
from hiveq.flow.trading_types import OrderType


class SdkT46:
    def on_start(self, ctx, event):
        self.bars = {"AAPL": 0, "MSFT": 0}
        self.entry_ids = {}
        self.entry_filled = set()
        self.protective_ids = {}
        self.tp_id = ""
        self.state = {
            "stop_fills": [], "tp_cancel_requested": False, "tp_canceled": False,
            "rejects": [], "submitted_types": {}, "final_positions": {},
        }
        ctx.subscribe_bars(["AAPL", "MSFT"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = bar.symbol
        self.bars[symbol] += 1
        if self.bars[symbol] == 2:
            order = ctx.buy_order(symbol, 1.0)
            if order is not None:
                self.entry_ids[order.order_id] = symbol
        elif symbol in self.entry_filled and symbol not in self.protective_ids:
            stop = adjust_tick_size(symbol, float(bar.close) - 0.01)
            if symbol == "AAPL":
                protective = ctx.sell_order(
                    symbol, 1.0, order_type=OrderType.STOP, stop_price=stop,
                    time_in_force="GTC"
                )
                take_profit = ctx.sell_order(
                    symbol, 1.0, order_type=OrderType.LIMIT,
                    limit_price=adjust_tick_size(symbol, float(bar.close) * 2.0),
                    time_in_force="GTC"
                )
                if take_profit is not None:
                    self.tp_id = take_profit.order_id
            else:
                protective = ctx.sell_order(
                    symbol, 1.0, order_type=OrderType.STOP_LIMIT,
                    stop_price=stop,
                    limit_price=adjust_tick_size(symbol, float(bar.close) - 0.05),
                    time_in_force="GTC"
                )
            if protective is not None:
                self.protective_ids[symbol] = protective.order_id
                self.state["submitted_types"][symbol] = "STOP" if symbol == "AAPL" else "STOP_LIMIT"

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id in self.entry_ids and order.is_filled:
            self.entry_filled.add(self.entry_ids[order.order_id])
        for symbol, order_id in self.protective_ids.items():
            if order.order_id == order_id and order.is_filled:
                if symbol not in self.state["stop_fills"]:
                    self.state["stop_fills"].append(symbol)
                if symbol == "AAPL" and self.tp_id and not self.state["tp_cancel_requested"]:
                    self.state["tp_cancel_requested"] = bool(ctx.cancel_order(self.tp_id))
        if order.order_id == self.tp_id and "CANCEL" in str(order.status).upper():
            self.state["tp_canceled"] = True
        if order.order_id in set(self.protective_ids.values()) and "REJECT" in str(order.status).upper():
            self.state["rejects"].append({"symbol": order.symbol, "reason": str(order.reject_reason or "")})

    def on_stop(self, ctx, event):
        self.state["final_positions"] = {
            symbol: float(ctx.net_position(symbol)) for symbol in ("AAPL", "MSFT")
        }
        emit_checkpoint(ctx, "t46_stop_bracket_orders", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT46", type="SdkT46", symbols=["AAPL", "MSFT"])],
        symbols=["AAPL", "MSFT"],
        start_date="2025-04-04",
        end_date="2025-04-04",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="12:00"),
    )
    state = completed_checkpoint(run, "t46_stop_bracket_orders")
    finish("t46_stop_bracket_orders", {
        "both_protective_types_submitted": state["submitted_types"] == {
            "AAPL": "STOP", "MSFT": "STOP_LIMIT"
        },
        "stop_exit_filled": "AAPL" in state["stop_fills"],
        "stop_limit_exit_filled": "MSFT" in state["stop_fills"],
        "take_profit_sibling_canceled": state["tp_cancel_requested"] and state["tp_canceled"],
        "no_protective_rejections": not state["rejects"],
        "both_positions_flat": state["final_positions"] == {"AAPL": 0.0, "MSFT": 0.0},
    }, extra=str(state))
