"""MOO and MOC orders fill on official opening and closing auction prints."""
from datetime import time
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger
from hiveq.flow.trading_types import OrderType

logger = _get_logger()


class SdkT24:
    def on_start(self, ctx, event):
        self.state = {"trades": 0, "moo_placed": False, "moc_placed": False,
            "moo_fill": None, "moc_fill": None, "rejects": []}
        self.moo_id = None; self.moc_id = None
        ctx.subscribe_trades(["AAPL"], asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        trade = event.data(); clock = trade.time.time(); self.state["trades"] += 1
        if not self.state["moo_placed"] and time(8, 0) <= clock < time(8, 1):
            order = ctx.buy_order("AAPL", 100, order_type=OrderType.MOO)
            if order: self.moo_id = order.order_id; self.state["moo_placed"] = True
        if not self.state["moc_placed"] and time(15, 30) <= clock < time(15, 31):
            order = ctx.sell_order("AAPL", 100, order_type=OrderType.MOC)
            if order: self.moc_id = order.order_id; self.state["moc_placed"] = True

    def on_order(self, ctx, event):
        order = event.data()
        if event.type == EventType.ORDER_FILLED:
            value = {"time": order.time.strftime("%H:%M") if order.time else None,
                     "price": order.avg_px, "qty": order.filled_qty}
            if order.order_id == self.moo_id: self.state["moo_fill"] = value
            if order.order_id == self.moc_id: self.state["moc_fill"] = value
        elif event.type == EventType.ORDER_REJECTED:
            self.state["rejects"].append(order.reject_reason or "")

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t24_auction_orders", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT24", type="SdkT24", symbols=["AAPL"])],
        symbols=["AAPL"], start_date="2025-09-19", end_date="2025-09-19",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["eq_trades"]}],
        backtest_config=BacktestConfig(session_start="04:00", session_end="18:30"))
    state = completed_checkpoint(run, "t24_auction_orders")
    finish("t24_auction_orders", {
        "trade_ticks_delivered": state["trades"] > 0,
        "moo_placed_before_open": state["moo_placed"],
        "moo_filled_at_0930_ET": state["moo_fill"] is not None and state["moo_fill"]["time"] == "09:30",
        "moc_placed_before_close": state["moc_placed"],
        "moc_filled_at_1600_ET": state["moc_fill"] is not None and state["moc_fill"]["time"] == "16:00",
        "auction_orders_not_rejected": not state["rejects"],
        "two_public_fills": len(run.fills()) >= 2,
    }, extra=str(state))
