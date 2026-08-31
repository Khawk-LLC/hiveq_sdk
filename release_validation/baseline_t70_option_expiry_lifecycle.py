"""A 0DTE option position carried into expiry.

t26 filters option snaps, t40 fills one option limit, t45 trades four-leg
baskets -- all of them exit before the contract dies. Nothing asks what happens
to an option still held at expiry, which is where the interesting behaviour
lives: the position has to resolve rather than linger as a phantom holding, and
whatever cash it settles for has to appear in the account.

Both directions are covered from one 0DTE chain: a long call held to expiry and
a short call held to expiry, alongside a control option round trip that closes
normally so the case still proves the option execution path end to end.
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
from hiveq.flow.trading.price_utils import adjust_tick_size           # noqa: E402
from hiveq.flow.trading_types import OrderType                        # noqa: E402

UNDERLYING = "SPXW"
QTY = 1.0
INITIAL = 1_000_000.0


class SdkT70:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.snaps = 0
            self.held = {}
            self.control = ""
            self.state = {
                "snaps": 0, "symbols_seen": 0, "held": {}, "control_symbol": "",
                "control_round_trip": False, "fills": [], "errors": [],
                "at_expiry": {}, "after_expiry": {}, "cash": {},
            }
        ctx.subscribe_option_snaps(UNDERLYING, option_type="C",
                                   expiration_type="0dte", interval="1s")

    def snapshot(self, ctx):
        portfolio = ctx.portfolio()
        return {
            "cash": round(portfolio.cash, 4),
            "equity": round(portfolio.equity, 4),
            "realized": round(portfolio.realized_pnl(), 4),
            "unrealized": round(portfolio.unrealized_pnl(), 4),
            "positions": {
                symbol: float(ctx.net_position(symbol)) for symbol in self.held
            },
        }

    def on_snap(self, ctx, event):
        snap = event.data()
        self.snaps += 1
        # The tradable option identifier is `chain`; `symbol` is the underlying.
        symbol = getattr(snap, "chain", "") or ""
        bid = float(getattr(snap, "bid_px", 0) or 0)
        ask = float(getattr(snap, "ask_px", 0) or 0)
        if not symbol or bid <= 0 or ask <= 0:
            return

        # First tradable contract becomes the long-to-expiry leg, the second the
        # short-to-expiry leg, the third the control round trip.
        if "long" not in self.held and self.snaps > 200:
            price = adjust_tick_size(symbol, ask)
            try:
                order = ctx.buy_order(symbol, QTY, order_type=OrderType.LIMIT,
                                      limit_price=price, time_in_force="DAY")
            except Exception as exc:                       # noqa: BLE001
                self.state["errors"].append(f"long: {str(exc)[:140]}")
                return
            if order is not None:
                self.held["long"] = symbol
        elif "short" not in self.held and symbol not in self.held.values():
            price = adjust_tick_size(symbol, bid)
            try:
                order = ctx.short_order(symbol, QTY, order_type=OrderType.LIMIT,
                                        limit_price=price, time_in_force="DAY")
            except Exception as exc:                       # noqa: BLE001
                self.state["errors"].append(f"short: {str(exc)[:140]}")
                return
            if order is not None:
                self.held["short"] = symbol
        elif not self.control and symbol not in self.held.values():
            price = adjust_tick_size(symbol, ask)
            try:
                order = ctx.buy_order(symbol, QTY, order_type=OrderType.LIMIT,
                                      limit_price=price, time_in_force="DAY")
            except Exception as exc:                       # noqa: BLE001
                self.state["errors"].append(f"control: {str(exc)[:140]}")
                return
            if order is not None:
                self.control = symbol
                self.held["control"] = symbol
        elif self.control and ctx.net_position(self.control) != 0 and self.snaps > 5000:
            if not ctx.has_open_order(self.control):
                ctx.close_position(self.control)
                self.state["control_round_trip"] = True

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(order.status).upper() and float(order.filled_qty or 0) > 0:
            if len(self.state["fills"]) < 40:
                self.state["fills"].append([
                    order.symbol, str(order.side).upper(), float(order.filled_qty),
                    round(float(order.avg_px or 0), 6),
                ])

    def on_stop(self, ctx, event):
        self.state["snaps"] = self.snaps
        self.state["held"] = dict(self.held)
        self.state["control_symbol"] = self.control
        self.state["at_expiry"] = self.snapshot(ctx)
        emit_checkpoint(ctx, "t70_option_expiry_lifecycle", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT70", type="SdkT70",
                                         symbols=[UNDERLYING])],
        symbols=[UNDERLYING], start_date="2025-06-02", end_date="2025-06-03",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_OPT", "schema": ["snaps_1s"]
        }],
        backtest_config=BacktestConfig(initial_capital=INITIAL,
                                       session_start="09:30", session_end="16:15"),
    )
    state = completed_checkpoint(run, "t70_option_expiry_lifecycle")
    at_expiry = state["at_expiry"]
    positions = at_expiry.get("positions") or {}
    held = state["held"]
    fills = state["fills"]

    if state["snaps"] == 0:
        finish("t70_option_expiry_lifecycle",
               {"option_snaps_available": False}, gap=True,
               extra="no 0dte option snaps delivered for the window")
        raise SystemExit(0)

    long_symbol = held.get("long", "")
    short_symbol = held.get("short", "")
    filled_symbols = {row[0] for row in fills}

    checks = {
        "option_snaps_delivered": state["snaps"] > 1000,
        "no_option_order_errors": not state["errors"],
        "expiry_legs_submitted": bool(long_symbol) and bool(short_symbol),
        "option_orders_filled": bool(filled_symbols),
        # The property under test: nothing may still be held once the contracts
        # have expired and the run has ended.
        "no_position_survives_expiry": all(
            quantity == 0.0 for quantity in positions.values()
        ),
        "cash_settled_into_account": at_expiry.get("cash", 0) != 0,
        "equity_identity_at_expiry": abs(
            at_expiry.get("equity", 0)
            - (INITIAL + at_expiry.get("realized", 0)
               + at_expiry.get("unrealized", 0))
        ) < 50.0,
        "expired_legs_left_no_unrealized": abs(at_expiry.get("unrealized", 0)) < 1e-6,
    }
    checks.update(evidence_checks(run, orders=2, trades=0))
    finish("t70_option_expiry_lifecycle", checks,
           extra=(f"snaps={state['snaps']}; held={held}; "
                  f"filled_symbols={sorted(filled_symbols)[:4]}; "
                  f"fills={fills[:6]}; at_expiry={at_expiry}; "
                  f"control_round_trip={state['control_round_trip']}; "
                  f"errors={state['errors'][:2]}"))
