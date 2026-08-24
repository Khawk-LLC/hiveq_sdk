"""A partially filled order is accounted for correctly at every step.

Nothing else in the suite produces a partial fill, so the whole incremental
accounting path is untested: an order that fills in pieces must keep
``leaves_qty`` falling monotonically, must report an ``avg_px`` that is the
quantity-weighted mean of the pieces actually printed, must move the position
by exactly what filled and not by what was ordered, and must leave its
remainder cancellable.

The order is deliberately far larger than the liquidity available at its limit,
against trade prints rather than bars, because that is the only way to make the
engine fill in pieces. The intermediate states are read from the streamed order
capture -- ``run.orders()`` collapses an order to one terminal row, so a partial
fill is invisible there by construction.
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
# Far more than prints at one price in a minute, so the fill has to arrive in
# pieces or not at all.
BIG_QTY = 250_000.0
CONTROL_QTY = 10.0


class SdkT61:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.trades = 0
            self.big_id = ""
            self.control_id = ""
            self.state = {
                "trades": 0, "steps": [], "leaves": [], "cancel_requested": False,
                "position_after": None, "final": {}, "control_filled": 0.0,
                "avg_px_reported": 0.0, "order_qty": BIG_QTY,
                # steps is capped, so the running totals are tracked separately
                # rather than read off the last retained step.
                "filled_total": 0.0, "statuses_while_open": [],
            }
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        trade = event.data()
        if trade.symbol != SYMBOL:
            return
        self.trades += 1
        price = float(trade.price or 0)
        if price <= 0:
            return

        if self.trades == 50 and not self.control_id:
            # Independent control round trip, so the case still proves the
            # execution path end to end even if the engine declines to fill the
            # oversized order at all.
            order = ctx.buy_order(SYMBOL, CONTROL_QTY)
            if order is not None:
                self.control_id = order.order_id
        elif self.trades == 200 and not self.big_id:
            # At the market so it is immediately eligible, but far too large to
            # be satisfied by the prints available at that price.
            order = ctx.buy_order(SYMBOL, BIG_QTY, order_type=OrderType.LIMIT,
                                  limit_price=adjust_tick_size(SYMBOL, price * 1.002),
                                  time_in_force="GTC")
            if order is not None:
                self.big_id = order.order_id
        elif self.trades == 4000 and self.big_id and not self.state["cancel_requested"]:
            self.state["cancel_requested"] = bool(ctx.cancel_order(self.big_id))
            self.state["position_after"] = float(ctx.net_position(SYMBOL))

    def on_order(self, ctx, event):
        order = event.data()
        if order.order_id == self.control_id and order.is_filled:
            self.state["control_filled"] = float(order.filled_qty or 0)
        if order.order_id != self.big_id:
            return
        status = str(order.status).upper()
        filled = float(order.filled_qty or 0)
        leaves = float(order.leaves_qty or 0)
        last_qty = float(order.last_qty or 0)
        last_px = float(order.last_px or 0)
        if "FILL" in status and filled > 0:
            if len(self.state["steps"]) < 400:
                self.state["steps"].append(
                    [status, filled, leaves, last_qty, last_px,
                     round(float(order.avg_px or 0), 6)]
                )
            self.state["leaves"].append(leaves)
            self.state["filled_total"] = filled
            self.state["avg_px_reported"] = round(float(order.avg_px or 0), 6)
            if leaves > 0 and status not in self.state["statuses_while_open"]:
                # What the engine actually calls a partially filled order. The
                # public enum has PARTIALLY_FILLED, so recording this is the
                # difference between "partial fills work" and "code filtering on
                # status == PARTIALLY_FILLED would silently see nothing".
                self.state["statuses_while_open"].append(status)

    def on_stop(self, ctx, event):
        self.state["trades"] = self.trades
        self.state["final"] = {
            "position": float(ctx.net_position(SYMBOL)),
            "open_qty": float(ctx.open_order_qty(SYMBOL)),
            "has_open": bool(ctx.has_open_order(SYMBOL)),
        }
        emit_checkpoint(ctx, "t61_partial_fills", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT61", type="SdkT61", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]
        }],
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"),
    )
    state = completed_checkpoint(run, "t61_partial_fills")
    events = order_events(run)

    partial_rows = 0
    fill_pieces = []
    if not events.empty and "event_type" in events.columns:
        partial = events[events["event_type"].astype(str) == "ORDER_PARTIALLY_FILLED"]
        partial_rows = len(partial)
        fills = events[events["event_type"].astype(str).isin(
            ["ORDER_FILLED", "ORDER_PARTIALLY_FILLED"]
        )]
        for _, row in fills.iterrows():
            quantity = float(row.get("fill_qty") or 0)
            price = float(row.get("fill_price") or 0)
            if quantity > 0 and price > 0:
                fill_pieces.append((quantity, price))

    steps = state["steps"]
    leaves = state["leaves"]
    filled_total = float(state["filled_total"])
    pieces_total = sum(quantity for quantity, _ in fill_pieces)
    weighted = (sum(quantity * price for quantity, price in fill_pieces) / pieces_total
                if pieces_total else 0.0)

    partial_observed = partial_rows > 0 or len(steps) > 1
    checks = {
        "tick_data_delivered": state["trades"] > 1000,
        "control_round_trip": state["control_filled"] == CONTROL_QTY,
        "oversized_order_filled_partially": partial_observed,
        "leaves_never_increased": all(
            leaves[i] <= leaves[i - 1] + 1e-9 for i in range(1, len(leaves))
        ),
        "filled_plus_leaves_equals_order": bool(steps) and all(
            abs((step[1] + step[2]) - BIG_QTY) < 1e-6 for step in steps
        ),
        "position_matches_filled_not_ordered": (
            state["position_after"] is not None
            and abs(state["position_after"] - (filled_total + CONTROL_QTY)) < 1e-6
        ),
        "partial_state_visible_via_leaves_qty": bool(state["statuses_while_open"]),
        "avg_px_is_weighted_mean_of_pieces": (
            bool(fill_pieces)
            and abs(state["avg_px_reported"] - weighted) < 0.02
        ),
        "remainder_cancelled_and_flat_of_open": (
            state["cancel_requested"] and not state["final"]["has_open"]
            and state["final"]["open_qty"] == 0.0
        ),
    }
    checks.update(evidence_checks(run, orders=2, trades=0))
    finish("t61_partial_fills", checks,
           extra=(f"trades={state['trades']}; partial_events={partial_rows}; "
                  f"steps={len(steps)}; filled={filled_total}; "
                  f"pieces={len(fill_pieces)}/{pieces_total}; "
                  f"avg_px={state['avg_px_reported']} weighted={round(weighted, 6)}; "
                  f"final={state['final']}; "
                  f"status_while_partially_filled={state['statuses_while_open']}; "
                  f"first_steps={steps[:3]}"))
