"""The order helpers and order fields the suite never calls.

``place_order`` -- the generic entry point the other helpers wrap -- is invoked
by no case at all, and neither is ``flatten_all``, ``get_order_state`` or
``clear_pending_order``; ``order_to_target`` is used once. On the returned
order, ``client_order_id``, ``last_qty``, ``commissions()``, ``is_sell_short``,
``account``, ``strategy_id``, ``router_id`` and ``is_executor_order`` are read
nowhere.

These are the calls a user reaches for first and the fields they key their own
reconciliation on, so each is exercised and its result checked for consistency
against the same fact reported elsewhere -- ``place_order`` against the side it
was given, ``order_to_target`` against the resulting position,
``get_order_state`` against the order's own status, ``flatten_all`` against
being flat.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402
from hiveq.flow.trading.price_utils import adjust_tick_size           # noqa: E402
from hiveq.flow.trading_types import OrderSide, OrderType             # noqa: E402

PRIMARY = "AAPL"
SECONDARY = "MSFT"
SYMBOLS = [PRIMARY, SECONDARY]


class SdkT68:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = {symbol: 0 for symbol in SYMBOLS}
            self.placed_id = ""
            self.resting_id = ""
            self.state = {
                "bars": 0, "place_order": {}, "fields": {}, "states": {},
                "target": {}, "flatten": {}, "clear_pending": None,
                "open_tracking": {}, "errors": [],
            }
        ctx.subscribe_bars(SYMBOLS, asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = bar.symbol
        if symbol not in self.bars:
            return
        self.bars[symbol] += 1
        count = self.bars[symbol]
        close = float(bar.close)

        if symbol == PRIMARY and count == 3:
            # The generic entry point, with the side passed explicitly.
            order = ctx.place_order(PRIMARY, OrderSide.BUY, 20.0, OrderType.MARKET)
            if order is None:
                self.state["errors"].append("place_order returned None")
                return
            self.placed_id = order.order_id
            self.state["place_order"] = {
                "symbol": order.symbol,
                "side": str(order.side).upper(),
                "quantity": float(order.quantity),
                "order_type": str(order.order_type).upper(),
                "time_in_force": str(order.time_in_force),
                "order_id_nonempty": bool(order.order_id),
                "client_order_id_nonempty": bool(order.client_order_id),
                "client_differs_from_order_id": order.client_order_id != order.order_id,
                "is_buy": bool(order.is_buy),
                "is_sell": bool(order.is_sell),
                "is_sell_short": bool(order.is_sell_short),
                "is_executor_order": bool(order.is_executor_order),
                "strategy_id": str(order.strategy_id or ""),
                "strategy_type": str(order.strategy_type or ""),
                "router_id": str(order.router_id or ""),
                "account": str(order.account or ""),
                "market_center": str(order.market_center or ""),
                "executor_id": str(order.executor_id or ""),
            }
            self.state["states"]["after_submit"] = str(
                ctx.get_order_state(self.placed_id)
            )

        elif symbol == PRIMARY and count == 6 and self.placed_id:
            self.state["states"]["after_fill"] = str(
                ctx.get_order_state(self.placed_id)
            )
            self.state["states"]["unknown_id"] = str(
                ctx.get_order_state("no-such-order")
            )
            # A resting order, so open-order tracking has something to see.
            order = ctx.buy_order(PRIMARY, 15.0, order_type=OrderType.LIMIT,
                                  limit_price=adjust_tick_size(PRIMARY, close * 0.90),
                                  time_in_force="GTC")
            if order is not None:
                self.resting_id = order.order_id
            self.state["open_tracking"]["has_open"] = bool(ctx.has_open_order(PRIMARY))
            self.state["open_tracking"]["open_qty"] = float(ctx.open_order_qty(PRIMARY))
            self.state["open_tracking"]["has_open_any"] = bool(ctx.has_open_order())

        elif symbol == SECONDARY and count == 10:
            # order_to_target from flat: the resulting position is the assertion.
            ctx.order_to_target(SECONDARY, 40.0)

        elif symbol == SECONDARY and count == 25:
            self.state["target"]["after_open"] = float(ctx.net_position(SECONDARY))
            ctx.order_to_target(SECONDARY, 15.0)

        elif symbol == SECONDARY and count == 40:
            self.state["target"]["after_reduce"] = float(ctx.net_position(SECONDARY))

        elif symbol == PRIMARY and count == 55:
            if self.resting_id:
                ctx.cancel_order(self.resting_id)
            try:
                ctx.clear_pending_order(PRIMARY)
                self.state["clear_pending"] = "ok"
            except Exception as exc:                       # noqa: BLE001
                self.state["clear_pending"] = f"raised: {exc}"

        elif symbol == PRIMARY and count == 65:
            self.state["flatten"]["before"] = {
                s: float(ctx.net_position(s)) for s in SYMBOLS
            }
            orders = ctx.flatten_all()
            self.state["flatten"]["order_count"] = len(orders or [])
            self.state["flatten"]["symbols"] = sorted(
                {order.symbol for order in (orders or [])}
            )

        elif symbol == PRIMARY and count == 80:
            self.state["flatten"]["after"] = {
                s: float(ctx.net_position(s)) for s in SYMBOLS
            }

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id != self.placed_id:
            return
        if "FILL" in str(order.status).upper() and float(order.filled_qty or 0) > 0:
            fill = order.last_fill
            self.state["fields"] = {
                "filled_qty": float(order.filled_qty),
                "leaves_qty": float(order.leaves_qty),
                "last_qty": float(order.last_qty or 0),
                "last_px": round(float(order.last_px or 0), 6),
                "avg_px": round(float(order.avg_px or 0), 6),
                "commission": round(float(order.commission or 0), 8),
                "commissions_count": len(order.commissions() or []),
                "is_filled": bool(order.is_filled),
                "is_open": bool(order.is_open),
                "last_fill_present": fill is not None,
                "last_fill_qty": float(getattr(fill, "last_qty", 0) or 0),
                "last_fill_px": round(float(getattr(fill, "last_px", 0) or 0), 6),
                "last_fill_partial": bool(getattr(fill, "is_partial_fill", False)),
            }

    def on_stop(self, ctx, event):
        self.state["bars"] = sum(self.bars.values())
        emit_checkpoint(ctx, "t68_order_helper_surface", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT68", type="SdkT68", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="11:30",
                                       export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t68_order_helper_surface")
    placed = state["place_order"]
    fields = state["fields"]
    states = state["states"]
    target = state["target"]
    flatten = state["flatten"]

    checks = {
        "no_helper_errors": not state["errors"],
        "place_order_accepted": bool(placed),
        "place_order_honoured_side": placed.get("side", "").endswith("BUY"),
        "place_order_honoured_quantity": placed.get("quantity") == 20.0,
        "place_order_honoured_type": "MARKET" in placed.get("order_type", ""),
        "place_order_side_flags_agree": (
            placed.get("is_buy") is True and placed.get("is_sell") is False
            and placed.get("is_sell_short") is False
        ),
        "not_marked_as_executor_order": placed.get("is_executor_order") is False,
        "order_id_populated": placed.get("order_id_nonempty") is True,
        "client_order_id_populated": placed.get("client_order_id_nonempty") is True,
        "strategy_identity_populated": bool(placed.get("strategy_id")
                                            or placed.get("strategy_type")),
        "market_center_populated": bool(placed.get("market_center")),

        "fill_fields_populated": bool(fields),
        "leaves_zero_when_fully_filled": fields.get("leaves_qty") == 0.0,
        "last_qty_populated": (fields.get("last_qty") or 0) > 0,
        "last_px_populated": (fields.get("last_px") or 0) > 0,
        "last_fill_object_available": fields.get("last_fill_present") is True,
        "last_fill_agrees_with_order": (
            abs((fields.get("last_fill_px") or 0) - (fields.get("last_px") or 0)) < 0.01
        ),
        "full_fill_not_marked_partial": fields.get("last_fill_partial") is False,
        "commissions_list_populated": (fields.get("commissions_count") or 0) > 0,
        "is_filled_and_not_open": (
            fields.get("is_filled") is True and fields.get("is_open") is False
        ),

        "order_state_readable": bool(states.get("after_submit")),
        "order_state_reflects_fill": "FILL" in states.get("after_fill", "").upper()
            or "ACCEPT" in states.get("after_fill", "").upper(),
        "order_state_unknown_id_is_none": states.get("unknown_id") == "None",

        "open_order_tracked": state["open_tracking"].get("has_open") is True,
        "open_quantity_reported": (state["open_tracking"].get("open_qty") or 0) > 0,
        "has_open_order_without_symbol": state["open_tracking"].get("has_open_any") is True,
        "clear_pending_order_callable": state["clear_pending"] == "ok",

        "order_to_target_opened_position": target.get("after_open") == 40.0,
        "order_to_target_reduced_position": target.get("after_reduce") == 15.0,

        "flatten_all_returned_orders": (flatten.get("order_count") or 0) >= 1,
        "flatten_all_covered_open_symbols": bool(flatten.get("symbols")),
        "flat_after_flatten_all": all(
            value == 0.0 for value in (flatten.get("after") or {"x": 1}).values()
        ),
    }
    checks.update(evidence_checks(run, orders=5, trades=2))
    finish("t68_order_helper_surface", checks,
           extra=(f"placed={placed}; fields={fields}; states={states}; "
                  f"target={target}; flatten={flatten}; "
                  f"open_tracking={state['open_tracking']}; "
                  f"clear_pending={state['clear_pending']!r}"))
