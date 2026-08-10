"""Single-order cancellation and modification, including invalid identifiers."""
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


class SdkT20:
    def on_start(self, ctx, event):
        self.counts = {"AAPL": 0, "MSFT": 0}
        self.ids = {}
        self.state = {"placed": [], "modified_price": [], "modified_qty_price": [],
            "canceled": [], "double_cancel": [], "bogus_cancel": None,
            "bogus_modify": None, "fills": [], "open_qty_before_cancel": {},
            "open_qty_after_cancel": {}, "order_events": [],
            "expected_modified_orders": {}}
        self.state["modify_rejections"] = []
        self.state["updates"] = []
        ctx.subscribe_bars(["AAPL", "MSFT"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("[START] subscribed for cancel/modify validation")

    def on_bar(self, ctx, event):
        bar = event.data(); symbol = bar.symbol
        self.counts[symbol] += 1; count = self.counts[symbol]
        if count == 5:
            order = ctx.buy_order(symbol, 1, order_type=OrderType.LIMIT,
                                  limit_price=round(bar.close * .5, 2))
            if order:
                self.ids[symbol] = order.order_id; self.state["placed"].append(symbol)
        elif count == 8 and symbol in self.ids:
            # One replacement per order: isolate price-only from quantity+price
            # and avoid stacking a second replace while the first is pending.
            if symbol == "AAPL":
                limit_price = round(bar.close * .6, 2)
                self.state["expected_modified_orders"][symbol] = {
                    "quantity": 1.0, "limit_price": limit_price}
                ok = ctx.modify_order(self.ids[symbol], limit_price=limit_price)
                self.state["modified_price"].append([symbol, bool(ok)])
            else:
                limit_price = round(bar.close * .55, 2)
                self.state["expected_modified_orders"][symbol] = {
                    "quantity": 2.0, "limit_price": limit_price}
                ok = ctx.modify_order(self.ids[symbol], quantity=2,
                                      limit_price=limit_price)
                self.state["modified_qty_price"].append([symbol, bool(ok)])
        elif count == 11 and symbol in self.ids:
            self.state["open_qty_before_cancel"][symbol] = float(ctx.open_order_qty(symbol))
        elif count == 12 and symbol in self.ids:
            oid = self.ids[symbol]
            self.state["canceled"].append([symbol, bool(ctx.cancel_order(oid))])
            self.state["double_cancel"].append([symbol, bool(ctx.cancel_order(oid))])
        elif count == 14 and symbol in self.ids:
            self.state["open_qty_after_cancel"][symbol] = float(ctx.open_order_qty(symbol))
        elif count == 15 and symbol == "AAPL":
            self.state["bogus_cancel"] = bool(ctx.cancel_order("BOGUS-CANCEL-ID"))
            self.state["bogus_modify"] = bool(ctx.modify_order(
                "BOGUS-MODIFY-ID", quantity=1, limit_price=100.0))

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id in self.ids.values():
            record = {
                "event": event.type.value if hasattr(event.type, "value") else str(event.type),
                "symbol": order.symbol,
                "quantity": float(order.quantity),
                "leaves_qty": float(order.leaves_qty),
                "limit_price": None if order.limit_price is None else float(order.limit_price),
                "status": order.status.value if hasattr(order.status, "value") else str(order.status),
                "reject_reason": order.reject_reason or "",
            }
            self.state["order_events"].append(record)
            if event.type == EventType.ORDER_MODIFY_REJECTED:
                self.state["modify_rejections"].append(record)
            elif event.type == EventType.ORDER_UPDATED:
                self.state["updates"].append(record)
        if order.is_filled and order.symbol in self.ids:
            self.state["fills"].append(order.order_id)

    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t20_cancel_modify_order", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT20", type="SdkT20", symbols=["AAPL", "MSFT"])],
        symbols=["AAPL", "MSFT"], start_date="2025-09-15", end_date="2025-09-15",
        data_configs=[{"type":"hiveq_historical","dataset":"HIVEQ_US_EQ","schema":["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="16:00"))
    state = completed_checkpoint(run, "t20_cancel_modify_order")
    finish("t20_cancel_modify_order", {
        "two_orders_placed": sorted(state["placed"]) == ["AAPL", "MSFT"],
        "price_modify_request_accepted": state["modified_price"] == [["AAPL", True]],
        "qty_price_modify_request_accepted": state["modified_qty_price"] == [["MSFT", True]],
        "no_modify_rejections": not state["modify_rejections"],
        "modified_order_state_observed": all(any(
            row["event"] == "ORDER_CANCELED"
            and row["symbol"] == symbol
            and row["quantity"] == expected["quantity"]
            and row["limit_price"] == expected["limit_price"]
            for row in state["order_events"]
        ) for symbol, expected in state["expected_modified_orders"].items())
        and set(state["expected_modified_orders"]) == {"AAPL", "MSFT"},
        "first_cancels_succeeded": len(state["canceled"]) == 2 and all(x[1] for x in state["canceled"]),
        "double_cancel_safe": len(state["double_cancel"]) == 2,
        "bogus_cancel_false": state["bogus_cancel"] is False,
        "bogus_modify_false": state["bogus_modify"] is False,
        "open_qty_reflects_modified_orders": state["open_qty_before_cancel"] == {"AAPL": 1.0, "MSFT": 2.0},
        "open_qty_zero_after_cancel": state["open_qty_after_cancel"] == {"AAPL": 0.0, "MSFT": 0.0},
        "resting_orders_never_filled": not state["fills"],
    }, extra=(f"open_qty_before_cancel={state['open_qty_before_cancel']}, "
              f"open_qty_after_cancel={state['open_qty_after_cancel']}, "
              f"modify_rejections={state['modify_rejections']}, "
              f"order_events={state['order_events']}, fills={state['fills']}"))
