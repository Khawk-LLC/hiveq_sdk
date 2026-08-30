"""A partially filled order is accounted for correctly at every step.

Nothing else in the suite produces a partial fill, so the whole incremental
accounting path is untested: an order that fills in pieces must keep
``leaves_qty`` falling monotonically, must report an ``avg_px`` that is the
quantity-weighted mean of the pieces actually printed, must move the position
by exactly what filled and not by what was ordered, and must leave its
remainder cancellable.

The order is deliberately far larger than the liquidity available at its limit,
against trade prints rather than bars, because that is the only way to make the
engine fill in pieces. The intermediate states come from the strategy's own
per-event capture -- ``run.orders()`` collapses an order to one terminal row, so
a partial fill is invisible there by construction. The streamed order-event file
is read too when it exists, but it is written only by an in-process run, so no
assertion may depend on it: a platform run has to be checkable from the
checkpointed steps alone.
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
# Far more than the session can absorb, so the order fills in pieces and still
# has a remainder left to cancel. 250_000 was ~6% of AAPL's volume in this
# window and filled completely, which left `remainder_cancelled_and_flat_of_open`
# asserting against an order that no longer had a remainder.
BIG_QTY = 5_000_000.0
CONTROL_QTY = 10.0


class SdkT61:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.trades = 0
            self.big_id = ""
            self.big_order = None
            self.control_id = ""
            self.state = {
                "trades": 0, "steps": [], "leaves": [], "cancel_requested": False,
                "position_after": None, "final": {}, "control_filled": 0.0,
                "avg_px_reported": 0.0, "order_qty": BIG_QTY,
                # steps is capped, so the running totals are tracked separately
                # rather than read off the last retained step.
                "filled_total": 0.0, "statuses_while_open": [],
                "partial_event_types": [],
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

        # The in-memory paper broker updates the returned zero-copy order on
        # every execution, but currently emits only accepted and terminal
        # callbacks for this path. Observe that same public order surface here
        # so the test does not confuse callback delivery with fill accounting.
        if self.big_order is not None:
            self._record_partial_step(self.big_order, source="poll")

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
                self.big_order = order
        elif self.trades == 4000 and self.big_id and not self.state["cancel_requested"]:
            self.state["cancel_requested"] = bool(ctx.cancel_order(self.big_id))
            self.state["position_after"] = float(ctx.net_position(SYMBOL))

    def on_order(self, ctx, event):
        order = event.data()
        # A marketable limit can begin filling synchronously inside buy_order(),
        # before that call returns and assigns self.big_id.  The first callback
        # with both executed and remaining quantity uniquely identifies the
        # oversized order; retain its ID before applying the normal ID filter.
        if (not self.big_id and order.symbol == SYMBOL
                and float(order.filled_qty or 0) > 0
                and float(order.leaves_qty or 0) > 0):
            self.big_id = order.order_id
        if order.order_id == self.control_id and order.is_filled:
            self.state["control_filled"] = float(order.filled_qty or 0)
        if order.order_id != self.big_id:
            return
        if "PARTIALLY_FILLED" in str(event.type).upper():
            self.state["partial_event_types"].append(str(event.type))
        self._record_partial_step(order, source="event")

    def _record_partial_step(self, order, source="poll"):
        status = str(order.status).upper()
        filled = float(order.filled_qty or 0)
        leaves = float(order.leaves_qty or 0)
        last_qty = float(order.last_qty or 0)
        last_px = float(order.last_px or 0)
        previous_filled = float(self.state["filled_total"])
        if filled > previous_filled:
            if len(self.state["steps"]) < 400:
                self.state["steps"].append(
                    [status, filled, leaves, last_qty, last_px,
                     round(float(order.avg_px or 0), 6), source]
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
        symbols=[SYMBOL], start_date="2026-08-12", end_date="2026-08-12",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]
        }],
        # The local suite enables export_orders_csv centrally in qa_common.
        # Remote runs publish order history through the platform instead.
        backtest_config=BacktestConfig(session_start="09:30", session_end="10:30"),
    )
    state = completed_checkpoint(run, "t61_partial_fills")
    events = order_events(run)

    partial_rows = 0
    fill_pieces = []
    captured_piece_qty_matches_increment = None
    if not events.empty and "event_type" in events.columns:
        partial = events[events["event_type"].astype(str) == "ORDER_PARTIALLY_FILLED"]
        partial_rows = len(partial)
        fills = events[events["event_type"].astype(str).isin(
            ["ORDER_FILLED", "ORDER_PARTIALLY_FILLED"]
        )]
        if "order_qty" in fills.columns:
            fills = fills[
                (fills["order_qty"].astype(float) - BIG_QTY).abs() < 1e-6
            ]
        previous_cumulative = 0.0
        captured_piece_checks = []
        for _, row in fills.iterrows():
            quantity = float(row.get("fill_qty") or 0)
            price = float(row.get("fill_price") or 0)
            cumulative = float(row.get("filled_qty") or 0)
            if quantity > 0 and price > 0:
                fill_pieces.append((quantity, price))
                captured_piece_checks.append(
                    abs((cumulative - previous_cumulative) - quantity) < 1e-6
                )
                previous_cumulative = cumulative
        if captured_piece_checks:
            captured_piece_qty_matches_increment = all(captured_piece_checks)

    steps = state["steps"]
    leaves = state["leaves"]

    def weighted_mean_error(rows):
        """Largest gap between the reported avg_px and the pieces printed.

        Each captured step carries the cumulative filled quantity, the piece
        just printed (``last_qty``/``last_px``) and the avg_px reported at that
        moment, so the quantity-weighted mean can be rebuilt step by step from
        the checkpoint itself.  ``avg_px`` is asserted against that, not
        against the local capture file, which a platform run never writes.
        """
        running_notional = 0.0
        previous_filled = 0.0
        worst = None
        for _status, filled, _leaves, last_qty, last_px, avg_px, *_source in rows:
            piece = float(filled) - previous_filled
            if piece <= 0 or float(last_px) <= 0:
                return None
            running_notional += piece * float(last_px)
            expected = running_notional / float(filled)
            gap = abs(float(avg_px) - expected)
            worst = gap if worst is None else max(worst, gap)
            previous_filled = float(filled)
        return worst

    step_avg_px_error = weighted_mean_error(steps)
    # Only a real fill callback can prove "each event reports its own piece".
    # An on_trade poll reads the zero-copy order's cumulative state, so when
    # several executions land between two polls the increment legitimately
    # exceeds the single last_qty on view -- asserting equality there measures
    # the sampling rate, not the engine. Assert equality on event-sourced steps
    # when the broker emits them, and otherwise assert the weaker property a
    # poll can actually establish: no step reports a piece larger than the
    # cumulative increase it accompanies.
    def _increment(index, step):
        return float(step[1]) - (0.0 if index == 0 else float(steps[index - 1][1]))

    event_steps = [(i, x) for i, x in enumerate(steps) if len(x) > 6 and x[6] == "event"]
    if event_steps:
        checkpoint_piece_qty_matches_increment = all(
            abs(_increment(i, x) - float(x[3])) < 1e-6 for i, x in event_steps
        )
    else:
        checkpoint_piece_qty_matches_increment = bool(steps) and all(
            float(step[3]) <= _increment(index, step) + 1e-6
            for index, step in enumerate(steps)
        )
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
        # Compared at the end of the run, not at the cancel: `position_after` is
        # a snapshot taken the moment cancel_order() returns, while fills already
        # in flight keep landing afterwards, so the two are sampled at different
        # instants and diverge for any order large enough to still be working.
        "position_matches_filled_not_ordered": (
            bool(state["final"])
            and abs(
                float(state["final"]["position"]) - (filled_total + CONTROL_QTY)
            ) < 1e-6
        ),
        "partial_state_visible_via_leaves_qty": bool(state["statuses_while_open"]),
        # Rebuilt from the captured steps; the local event file is used only
        # as a second opinion when an in-process run happened to write it.
        "avg_px_is_weighted_mean_of_pieces": (
            step_avg_px_error is not None and step_avg_px_error < 0.02
            and (not fill_pieces
                 or abs(state["avg_px_reported"] - weighted) < 0.02)
        ),
        "each_fill_event_reports_its_own_piece": (
            captured_piece_qty_matches_increment
            if captured_piece_qty_matches_increment is not None
            else checkpoint_piece_qty_matches_increment
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
                  f"step_avg_px_error={step_avg_px_error}; "
                  f"avg_px={state['avg_px_reported']} weighted={round(weighted, 6)}; "
                  f"final={state['final']}; "
                  f"partial_event_types={state['partial_event_types'][:3]}; "
                  f"status_while_partially_filled={state['statuses_while_open']}; "
                  f"first_steps={steps[:3]}"))
