"""Cancel and modify against orders that cannot accept them.

t20 covers the happy path plus an unknown order id. The failure directions --
cancelling an order that already filled, cancelling the same resting order
twice, modifying a terminal order, modifying a live one to an impossible
quantity -- are the ones an OMS gets wrong, and the ones that produce the
ORDER_CANCEL_REJECTED / ORDER_MODIFY_REJECTED event types the suite never
observes.

What matters is not only the boolean each call returns but that a refused
cancel or modify leaves the order and the position exactly as they were: a
cancel that returns False while still detaching the order, or a modify that
returns False after mutating quantity, is far worse than a clean refusal.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
    order_events,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402
from hiveq.flow.trading.price_utils import adjust_tick_size           # noqa: E402
from hiveq.flow.trading_types import OrderType                        # noqa: E402

SYMBOL = "AAPL"
RESTING_QTY = 20.0
MODIFIED_QTY = 12.0


class SdkT63:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = 0
            self.filled_id = ""
            self.resting_id = ""
            self.modify_id = ""
            self.state = {
                "bars": 0, "filled_qty": 0.0,
                "cancel_filled_order": None, "modify_filled_order": None,
                "cancel_unknown_id": None, "modify_unknown_id": None,
                "cancel_resting_first": None, "cancel_resting_again": None,
                "modify_resting_ok": None, "modify_invalid_qty": "",
                "open_qty_before_modify": None, "open_qty_after_modify": None,
                "open_qty_after_refused_modify": None,
                # A modify may be acknowledged synchronously and applied on a
                # later event, so the open quantity is sampled again a few bars
                # on before concluding the change was lost.
                "open_qty_deferred": None,
                "position_before": None, "position_after": None,
                "resting_statuses": [], "modify_statuses": [],
            }
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        self.bars += 1
        index = self.bars
        close = float(bar.close)

        if index == 3:
            order = ctx.buy_order(SYMBOL, 10.0)
            if order is not None:
                self.filled_id = order.order_id
        elif index == 8 and self.filled_id:
            # The order filled on bar 3; both operations must refuse it.
            self.state["position_before"] = float(ctx.net_position(SYMBOL))
            self.state["cancel_filled_order"] = bool(ctx.cancel_order(self.filled_id))
            self.state["modify_filled_order"] = bool(
                ctx.modify_order(self.filled_id, quantity=99.0)
            )
            self.state["position_after"] = float(ctx.net_position(SYMBOL))
            self.state["cancel_unknown_id"] = bool(ctx.cancel_order("no-such-order-id"))
            self.state["modify_unknown_id"] = bool(
                ctx.modify_order("no-such-order-id", quantity=1.0)
            )
        elif index == 12:
            # Well below the market so it rests for the rest of the session.
            order = ctx.buy_order(SYMBOL, RESTING_QTY, order_type=OrderType.LIMIT,
                                  limit_price=adjust_tick_size(SYMBOL, close * 0.90),
                                  time_in_force="GTC")
            if order is not None:
                self.resting_id = order.order_id
            order = ctx.buy_order(SYMBOL, RESTING_QTY, order_type=OrderType.LIMIT,
                                  limit_price=adjust_tick_size(SYMBOL, close * 0.89),
                                  time_in_force="GTC")
            if order is not None:
                self.modify_id = order.order_id
        elif index == 20 and self.modify_id:
            self.state["open_qty_before_modify"] = float(ctx.open_order_qty(SYMBOL))
            self.state["modify_resting_ok"] = bool(
                ctx.modify_order(self.modify_id, quantity=MODIFIED_QTY)
            )
            self.state["open_qty_after_modify"] = float(ctx.open_order_qty(SYMBOL))
            try:
                refused = ctx.modify_order(self.modify_id, quantity=0.0)
            except Exception as exc:                       # noqa: BLE001
                self.state["modify_invalid_qty"] = f"raised: {exc}"
            else:
                self.state["modify_invalid_qty"] = f"returned: {bool(refused)}"
            self.state["open_qty_after_refused_modify"] = float(
                ctx.open_order_qty(SYMBOL)
            )
        elif index == 26:
            self.state["open_qty_deferred"] = float(ctx.open_order_qty(SYMBOL))
        elif index == 30 and self.resting_id:
            self.state["cancel_resting_first"] = bool(ctx.cancel_order(self.resting_id))
        elif index == 36 and self.resting_id:
            self.state["cancel_resting_again"] = bool(ctx.cancel_order(self.resting_id))
        elif index == 50:
            ctx.cancel_all_orders(SYMBOL)
        elif index == 60 and not ctx.has_open_order(SYMBOL):
            ctx.close_position(SYMBOL)

    def on_order(self, ctx, event):
        order = event.data()
        status = str(order.status).upper()
        if order.order_id == self.filled_id and order.is_filled:
            self.state["filled_qty"] = float(order.filled_qty or 0)
        if order.order_id == self.resting_id:
            if status not in self.state["resting_statuses"]:
                self.state["resting_statuses"].append(status)
        if order.order_id == self.modify_id:
            if status not in self.state["modify_statuses"]:
                self.state["modify_statuses"].append(status)

    def on_stop(self, ctx, event):
        self.state["bars"] = self.bars
        emit_checkpoint(ctx, "t63_cancel_modify_rejects", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT63", type="SdkT63", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="11:00",
                                       export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t63_cancel_modify_rejects")
    events = order_events(run)
    event_types = (set(events["event_type"].astype(str)) if not events.empty
                   and "event_type" in events.columns else set())

    checks = {
        "bars_delivered": state["bars"] > 50,
        "control_round_trip": state["filled_qty"] == 10.0,
        "cancel_of_filled_order_refused": state["cancel_filled_order"] is False,
        "modify_of_filled_order_refused": state["modify_filled_order"] is False,
        "refusals_left_position_untouched": (
            state["position_before"] is not None
            and state["position_before"] == state["position_after"]
        ),
        "cancel_unknown_id_refused": state["cancel_unknown_id"] is False,
        "modify_unknown_id_refused": state["modify_unknown_id"] is False,
        "resting_cancel_accepted_once": state["cancel_resting_first"] is True,
        "second_cancel_refused": state["cancel_resting_again"] is False,
        "resting_order_reached_canceled": any(
            "CANCEL" in status for status in state["resting_statuses"]
        ),
        "modify_of_live_order_accepted": state["modify_resting_ok"] is True,
        "modify_changed_open_quantity": (
            state["open_qty_before_modify"] is not None
            and min(
                value for value in (state["open_qty_after_modify"],
                                    state["open_qty_deferred"])
                if value is not None
            ) < state["open_qty_before_modify"]
        ),
        "zero_quantity_modify_refused": (
            state["modify_invalid_qty"].startswith("raised:")
            or state["modify_invalid_qty"] == "returned: False"
        ),
        "refused_modify_left_open_quantity": (
            state["open_qty_after_refused_modify"] == state["open_qty_after_modify"]
        ),
    }
    checks.update(evidence_checks(run, orders=3, trades=1))
    finish("t63_cancel_modify_rejects", checks,
           extra=(f"captured_event_types={sorted(event_types)}; "
                  f"resting={state['resting_statuses']}; "
                  f"modified={state['modify_statuses']}; "
                  f"invalid_qty={state['modify_invalid_qty']!r}; "
                  f"open_qty={state['open_qty_before_modify']}->"
                  f"{state['open_qty_after_modify']}->"
                  f"{state['open_qty_after_refused_modify']}; "
                  f"open_qty_deferred={state['open_qty_deferred']}"))
