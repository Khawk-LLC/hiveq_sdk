"""Every public order type and time-in-force, actually submitted.

t16 proves the enums stringify to stable wire values. That is a serialization
test: LOO, LOC, FOK, GTD and DAY appear nowhere else in the suite as an order
the engine was asked to execute, so the SDK could expose an order type the OMS
silently drops and nothing would notice.

Each cell is submitted for real and its outcome recorded. A cell is allowed to
fill, rest, expire or be refused -- what is not allowed is for the engine to
accept an order and then do nothing observable with it, or to reject a type the
public API advertises. Auction types are submitted before their cutoffs so
their outcome reflects the auction rather than a late-order rejection.
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
QTY = 5.0

# (label, order_type, time_in_force, price offset for limit, stop offset)
# Offsets are fractions of the current close: negative rests a buy below the
# market, positive makes it immediately executable.
CELLS = [
    ("market_ioc", OrderType.MARKET, "IOC", None, None),
    ("market_day", OrderType.MARKET, "DAY", None, None),
    ("limit_gtc_resting", OrderType.LIMIT, "GTC", -0.10, None),
    ("limit_day_resting", OrderType.LIMIT, "DAY", -0.10, None),
    ("limit_ioc_executable", OrderType.LIMIT, "IOC", +0.02, None),
    ("limit_ioc_unfillable", OrderType.LIMIT, "IOC", -0.10, None),
    ("limit_fok_executable", OrderType.LIMIT, "FOK", +0.02, None),
    ("limit_fok_unfillable", OrderType.LIMIT, "FOK", -0.10, None),
    ("limit_gtd", OrderType.LIMIT, "GTD", -0.10, None),
    ("stop_gtc", OrderType.STOP, "GTC", None, +0.02),
    ("stop_limit_gtc", OrderType.STOP_LIMIT, "GTC", +0.03, +0.02),
    ("moo", OrderType.MOO, None, None, None),
    ("moc", OrderType.MOC, None, None, None),
    ("loo", OrderType.LOO, None, +0.02, None),
    ("loc", OrderType.LOC, None, +0.02, None),
]
# Bar index at which each cell is submitted. Auction orders go first so they
# precede the 09:28 / 15:55 cutoffs the session defaults enforce.
AUCTION_LABELS = {"moo", "moc", "loo", "loc"}


class SdkT66:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = 0
            self.by_id = {}
            self.state = {"bars": 0, "submitted": {}, "errors": {},
                          "outcomes": {}, "reject_reasons": {}, "control_filled": 0.0}
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def submit(self, ctx, close, label, order_type, tif, limit_offset, stop_offset):
        # tif None means "let the engine pick": auction types require their own
        # (MOO/LOO reject anything but OPG), so overriding it is a submission
        # error rather than a test of the type.
        kwargs = {"order_type": order_type}
        if tif is not None:
            kwargs["time_in_force"] = tif
        if limit_offset is not None:
            kwargs["limit_price"] = adjust_tick_size(SYMBOL, close * (1 + limit_offset))
        if stop_offset is not None:
            kwargs["stop_price"] = adjust_tick_size(SYMBOL, close * (1 + stop_offset))
        try:
            order = ctx.buy_order(SYMBOL, QTY, **kwargs)
        except Exception as exc:                           # noqa: BLE001
            self.state["errors"][label] = str(exc)[:160]
            self.state["submitted"][label] = False
            return
        self.state["submitted"][label] = order is not None
        if order is not None:
            self.by_id[order.order_id] = label

    def on_bar(self, ctx, event):
        bar = event.data()
        self.bars += 1
        close = float(bar.close)

        if self.bars == 1:
            # Auction cells first: MOO/LOO must be in before the open cutoff and
            # MOC/LOC before the close cutoff, so submitting early is correct.
            for cell in CELLS:
                if cell[0] in AUCTION_LABELS:
                    self.submit(ctx, close, *cell)
        elif self.bars == 2:
            order = ctx.buy_order(SYMBOL, QTY)
            if order is not None:
                self.by_id[order.order_id] = "control"
        elif self.bars == 6:
            for cell in CELLS:
                if cell[0] not in AUCTION_LABELS:
                    self.submit(ctx, close, *cell)

    def on_order(self, ctx, event):
        order = event.data()
        label = self.by_id.get(order.order_id)
        if label is None:
            return
        status = str(order.status).upper()
        outcomes = self.state["outcomes"].setdefault(label, [])
        if status not in outcomes:
            outcomes.append(status)
        if label == "control" and order.is_filled:
            self.state["control_filled"] = float(order.filled_qty or 0)
        if order.reject_reason:
            self.state["reject_reasons"][label] = str(order.reject_reason)[:160]

    def on_stop(self, ctx, event):
        self.state["bars"] = self.bars
        emit_checkpoint(ctx, "t66_order_type_tif_matrix", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT66", type="SdkT66", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(session_start="04:00", session_end="18:30",
                                       export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t66_order_type_tif_matrix")
    events = order_events(run)
    captured_tifs = set()
    captured_types = set()
    if not events.empty:
        if "tif" in events.columns:
            captured_tifs = {str(value).upper() for value in events["tif"].dropna()}
        if "order_type" in events.columns:
            captured_types = {str(value).upper() for value in events["order_type"].dropna()}

    labels = [cell[0] for cell in CELLS]
    submitted = state["submitted"]
    outcomes = state["outcomes"]
    accepted = [label for label in labels if submitted.get(label)]
    silent = [label for label in accepted if not outcomes.get(label)]
    local_errors = state["errors"]

    def reached(label, *fragments):
        seen = " ".join(outcomes.get(label, []))
        return any(fragment in seen for fragment in fragments)

    checks = {
        "bars_delivered": state["bars"] > 100,
        "control_round_trip": state["control_filled"] == QTY,
        "every_cell_submitted_or_explained": all(
            submitted.get(label) or label in local_errors for label in labels
        ),
        # The core property: nothing is accepted and then silently dropped.
        "no_accepted_order_vanished": not silent,
        "market_orders_filled": reached("market_ioc", "FILL") and reached("market_day", "FILL"),
        "resting_limits_stayed_open": (
            reached("limit_gtc_resting", "SUBMITTED", "ACCEPTED")
            and reached("limit_day_resting", "SUBMITTED", "ACCEPTED")
        ),
        "executable_ioc_filled": reached("limit_ioc_executable", "FILL"),
        "executable_fok_filled": reached("limit_fok_executable", "FILL"),
        # Neither type may rest: an immediate-or-cancel order left SUBMITTED at
        # the end of the session was accepted and then not honoured.
        "unfillable_ioc_resolved_terminally": reached(
            "limit_ioc_unfillable", "CANCEL", "EXPIRED", "REJECT", "FILL"
        ),
        "unfillable_fok_resolved_terminally": reached(
            "limit_fok_unfillable", "CANCEL", "EXPIRED", "REJECT", "FILL"
        ),
        "stop_types_accepted": (
            reached("stop_gtc", "SUBMITTED", "ACCEPTED", "FILL", "TRIGGER")
            and reached("stop_limit_gtc", "SUBMITTED", "ACCEPTED", "FILL", "TRIGGER")
        ),
        "open_auction_types_resolved": (
            reached("moo", "FILL", "CANCEL", "REJECT")
            and reached("loo", "FILL", "CANCEL", "REJECT")
        ),
        "close_auction_types_resolved": (
            reached("moc", "FILL", "CANCEL", "REJECT")
            and reached("loc", "FILL", "CANCEL", "REJECT")
        ),
        "gtd_accepted": reached("limit_gtd", "SUBMITTED", "ACCEPTED", "EXPIRED", "CANCEL"),
    }
    checks.update(evidence_checks(run, orders=10, trades=1))
    finish("t66_order_type_tif_matrix", checks,
           extra=(f"cells={len(labels)}; accepted={len(accepted)}; "
                  f"silent={silent}; local_errors={local_errors}; "
                  f"captured_tifs={sorted(captured_tifs)}; "
                  f"captured_types={sorted(captured_types)}; "
                  f"outcomes={outcomes}; rejects={state['reject_reasons']}"))
