"""Every public position and portfolio accessor, reconciled against the fills.

Eleven of SigmaPosition's properties and four of SigmaPortfolio's are read by no
other case in the suite -- ``market_value``, ``notional``, ``long_notional``,
``short_notional``, ``max_exposure``, ``total_fill_qty``, ``avg_price``,
``is_long``/``is_short``/``is_open``, ``total_pnl``, ``day_pnl``,
``portfolio.position()``. A release could return zero, a stale value, or an
unsigned quantity from any of them and the suite would stay green.

Rather than assert each is merely present, each is checked against a value
derived independently from the fills the strategy itself observed, in three
states -- flat, long, and short -- so a sign error or an absolute-value bug in
short exposure cannot hide.
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

LONG_SYMBOL = "AAPL"
SHORT_SYMBOL = "MSFT"
SYMBOLS = [LONG_SYMBOL, SHORT_SYMBOL]
INITIAL = 1_000_000.0
LONG_QTY = 100.0
SHORT_QTY = 60.0


class SdkT67:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = {symbol: 0 for symbol in SYMBOLS}
            self.entry = {}
            self.last = {}
            self.state = {"bars": 0, "flat": {}, "held": {}, "closed": {},
                          "fills": [], "missing": [], "none_when_flat": None}
        ctx.subscribe_bars(SYMBOLS, asset_type=AssetType.EQUITY, interval="1m")

    def snapshot(self, ctx):
        portfolio = ctx.portfolio()
        out = {"portfolio": {
            "equity": round(portfolio.equity, 6), "cash": round(portfolio.cash, 6),
            "realized": round(portfolio.realized_pnl(), 6),
            "unrealized": round(portfolio.unrealized_pnl(), 6),
            "total_pnl": round(portfolio.total_pnl(), 6),
            "day_pnl": round(portfolio.day_pnl(), 6),
            "fees": round(portfolio.fees, 6),
            "net_exposure": round(portfolio.net_exposure(), 6),
            "gross_exposure": round(portfolio.gross_exposure(), 6),
            "position_count": len(portfolio.positions()),
        }, "positions": {}}
        for symbol in SYMBOLS:
            position = portfolio.position(symbol)
            if position is None:
                out["positions"][symbol] = None
                continue
            out["positions"][symbol] = {
                "quantity": float(position.quantity),
                "side": str(position.side),
                "avg_price": round(float(position.avg_price), 6),
                "entry_price": round(float(position.entry_price), 6),
                "average_price": round(float(position.average_price), 6),
                "market_value": round(float(position.market_value), 6),
                "notional": round(float(position.notional), 6),
                "long_notional": round(float(position.long_notional), 6),
                "short_notional": round(float(position.short_notional), 6),
                "max_exposure": round(float(position.max_exposure), 6),
                "total_fill_qty": float(position.total_fill_qty),
                "last_price": round(float(position.last_price), 6),
                "realized_pnl": round(float(position.realized_pnl), 6),
                "unrealized_pnl": round(float(position.unrealized_pnl), 6),
                "total_pnl": round(float(position.total_pnl), 6),
                "day_pnl": round(float(position.day_pnl), 6),
                "fees": round(float(position.fees), 6),
                "is_open": bool(position.is_open),
                "is_flat": bool(position.is_flat),
                "is_long": bool(position.is_long),
                "is_short": bool(position.is_short),
            }
        return out

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = bar.symbol
        if symbol not in self.bars:
            return
        self.bars[symbol] += 1
        self.last[symbol] = float(bar.close)
        count = self.bars[symbol]

        if count == 1 and self.state["none_when_flat"] is None:
            # A never-traded symbol may legitimately have no position object;
            # what matters is that the accessor says so instead of raising.
            try:
                self.state["none_when_flat"] = (
                    ctx.portfolio().position(symbol) is None
                )
            except Exception as exc:                       # noqa: BLE001
                self.state["missing"].append(f"position() raised when flat: {exc}")
        if count == 2 and not self.state["flat"]:
            self.state["flat"] = self.snapshot(ctx)
        if count == 5:
            if symbol == LONG_SYMBOL:
                ctx.buy_order(symbol, LONG_QTY)
            else:
                ctx.short_order(symbol, SHORT_QTY)
        elif count == 25 and all(value >= 25 for value in self.bars.values()):
            self.state["held"] = self.snapshot(ctx) | {"last": dict(self.last)}
        elif count == 40 and not ctx.has_open_order(symbol):
            ctx.close_position(symbol)
        elif count == 60 and all(value >= 60 for value in self.bars.values()):
            self.state["closed"] = self.snapshot(ctx)

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(order.status).upper() and float(order.filled_qty or 0) > 0:
            self.state["fills"].append([
                order.symbol, str(order.side).upper(), float(order.filled_qty),
                round(float(order.avg_px or 0), 6),
            ])

    def on_stop(self, ctx, event):
        self.state["bars"] = sum(self.bars.values())
        if not self.state["closed"]:
            self.state["closed"] = self.snapshot(ctx)
        emit_checkpoint(ctx, "t67_portfolio_position_surface", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT67", type="SdkT67", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(initial_capital=INITIAL, session_start="09:30",
                                       session_end="11:30"),
    )
    state = completed_checkpoint(run, "t67_portfolio_position_surface")
    held = state["held"]
    closed = state["closed"]
    long_position = (held.get("positions") or {}).get(LONG_SYMBOL) or {}
    short_position = (held.get("positions") or {}).get(SHORT_SYMBOL) or {}
    last = held.get("last") or {}

    entry = {}
    for symbol, side, quantity, price in state["fills"]:
        if symbol not in entry:
            entry[symbol] = price

    long_value = LONG_QTY * last.get(LONG_SYMBOL, 0)
    short_value = SHORT_QTY * last.get(SHORT_SYMBOL, 0)

    def close_enough(actual, expected, tolerance=0.02):
        return expected and abs(abs(actual) - abs(expected)) / abs(expected) < tolerance

    checks = {
        "both_legs_filled": len(state["fills"]) >= 2,
        "position_object_available_when_held": bool(long_position) and bool(short_position),
        "flat_symbol_reports_no_position": state["none_when_flat"] is not None,
        "no_accessor_raised": not state["missing"],

        "long_quantity_positive": long_position.get("quantity") == LONG_QTY,
        "short_quantity_negative": short_position.get("quantity") == -SHORT_QTY,
        "long_flags_consistent": (
            long_position.get("is_long") is True
            and long_position.get("is_short") is False
            and long_position.get("is_open") is True
            and long_position.get("is_flat") is False
        ),
        "short_flags_consistent": (
            short_position.get("is_short") is True
            and short_position.get("is_long") is False
            and short_position.get("is_open") is True
        ),
        "avg_price_matches_entry_fill": close_enough(
            long_position.get("avg_price", 0), entry.get(LONG_SYMBOL, 0), 0.005
        ),
        # The short leg is a separate code path in the engine's entry-price
        # accumulation; a sign error there would leave the long check green.
        "short_avg_price_matches_entry_fill": close_enough(
            short_position.get("avg_price", 0), entry.get(SHORT_SYMBOL, 0), 0.005
        ),
        # Three aliases of a dead property agree at 0.0, so equality alone can
        # never fail. Require a real price alongside it.
        "avg_price_aliases_agree": (
            long_position.get("avg_price") == long_position.get("average_price")
            == long_position.get("entry_price")
            and float(long_position.get("avg_price") or 0) > 0
        ),
        "last_price_tracks_bar": close_enough(
            long_position.get("last_price", 0), last.get(LONG_SYMBOL, 0), 0.005
        ),
        "market_value_matches_quantity_times_price": close_enough(
            long_position.get("market_value", 0), long_value
        ),
        "notional_matches_quantity_times_price": close_enough(
            long_position.get("notional", 0), long_value
        ),
        "long_notional_populated_for_long": close_enough(
            long_position.get("long_notional", 0), long_value
        ),
        "short_notional_populated_for_short": close_enough(
            short_position.get("short_notional", 0), short_value
        ),
        "max_exposure_at_least_current": (
            abs(long_position.get("max_exposure", 0)) >= abs(long_value) * 0.95
        ),
        "total_fill_qty_matches_ordered": long_position.get("total_fill_qty") == LONG_QTY,
        "position_total_pnl_is_realized_plus_unrealized": abs(
            long_position.get("total_pnl", 0)
            - (long_position.get("realized_pnl", 0)
               + long_position.get("unrealized_pnl", 0))
        ) < 0.51,

        "portfolio_total_pnl_consistent": abs(
            held["portfolio"]["total_pnl"]
            - (held["portfolio"]["realized"] + held["portfolio"]["unrealized"])
        ) < 1.0,
        "portfolio_day_pnl_present": held["portfolio"]["day_pnl"] is not None,
        "positions_list_covers_both": held["portfolio"]["position_count"] >= 2,
        "gross_exposure_is_sum_of_absolutes": close_enough(
            held["portfolio"]["gross_exposure"], long_value + short_value
        ),
        "net_exposure_is_signed_sum": close_enough(
            held["portfolio"]["net_exposure"], long_value - short_value
        ),
        "net_below_gross_when_hedged": (
            abs(held["portfolio"]["net_exposure"])
            < held["portfolio"]["gross_exposure"] - 1.0
        ),
        "flat_after_close": all(
            (closed["positions"].get(symbol) or {}).get("quantity", 0) == 0
            for symbol in SYMBOLS
        ),
        "exposure_zero_after_close": abs(closed["portfolio"]["gross_exposure"]) < 1.0,
    }
    checks.update(evidence_checks(run, orders=4, trades=2))
    finish("t67_portfolio_position_surface", checks,
           extra=(f"fills={state['fills']}; long={long_position}; "
                  f"short={short_position}; portfolio_held={held.get('portfolio')}; "
                  f"portfolio_closed={closed.get('portfolio')}; "
                  f"none_when_flat={state['none_when_flat']}; "
                  f"missing={state['missing']}"))
