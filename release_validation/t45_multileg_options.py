"""One-year 0DTE option baskets open and close four long/short legs daily."""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.trading.price_utils import adjust_tick_size
from hiveq.flow.trading_types import OrderType


class SdkT45:
    def on_start(self, ctx, event):
        self.quotes = {"C": {}, "P": {}}
        self.legs = {}
        self.order_phase = {}
        self.entry_fills = set()
        self.exit_fills = set()
        self.entries_submitted = False
        self.exits_submitted = False
        self.state = {"snaps": 0, "entry_orders": 0, "exit_orders": 0, "rejects": []}
        ctx.subscribe_option_snaps("SPXW", option_type="C", expiration_type="0dte", interval="1s")
        ctx.subscribe_option_snaps("SPXW", option_type="P", expiration_type="0dte", interval="1s")

    def _submit(self, ctx, symbol, side, price, phase):
        px = adjust_tick_size(symbol, price)
        if side == "BUY":
            order = ctx.buy_order(symbol, 1.0, order_type=OrderType.LIMIT, limit_price=px)
        elif side == "SHORT":
            order = ctx.short_order(symbol, 1.0, order_type=OrderType.LIMIT, limit_price=px)
        else:
            order = ctx.sell_order(symbol, 1.0, order_type=OrderType.LIMIT, limit_price=px)
        if order is not None:
            self.order_phase[order.order_id] = (phase, symbol)
            self.state[f"{phase}_orders"] += 1

    def on_snap(self, ctx, event):
        snap = event.data()
        self.state["snaps"] += 1
        if snap.bid_px is None or snap.ask_px is None or snap.bid_px <= 0 or snap.ask_px <= 0:
            return
        kind = str(snap.option_type).upper()
        self.quotes[kind][float(snap.strike)] = (
            snap.chain, float(snap.bid_px), float(snap.ask_px)
        )

        if not self.entries_submitted and len(self.quotes["C"]) >= 2 and len(self.quotes["P"]) >= 2:
            calls = sorted(self.quotes["C"])
            puts = sorted(self.quotes["P"])
            call_mid = max(0, len(calls) // 2 - 1)
            put_mid = max(0, len(puts) // 2 - 1)
            c_short, c_long = calls[call_mid], calls[call_mid + 1]
            p_long, p_short = puts[put_mid], puts[put_mid + 1]
            chosen = {
                "long_put": ("P", p_long, "BUY"),
                "short_put": ("P", p_short, "SHORT"),
                "short_call": ("C", c_short, "SHORT"),
                "long_call": ("C", c_long, "BUY"),
            }
            self.entries_submitted = True
            for role, (option_type, strike, side) in chosen.items():
                symbol, bid, ask = self.quotes[option_type][strike]
                self.legs[role] = {"symbol": symbol, "side": side, "strike": strike}
                self._submit(ctx, symbol, side, ask if side == "BUY" else bid, "entry")

        if (
            self.entries_submitted and len(self.entry_fills) == 4
            and not self.exits_submitted
            and all(leg["symbol"] in {q[0] for side in self.quotes.values() for q in side.values()}
                    for leg in self.legs.values())
        ):
            self.exits_submitted = True
            by_symbol = {q[0]: q for side in self.quotes.values() for q in side.values()}
            for leg in self.legs.values():
                symbol = leg["symbol"]
                _, bid, ask = by_symbol[symbol]
                if leg["side"] == "BUY":
                    self._submit(ctx, symbol, "SELL", bid, "exit")
                else:
                    self._submit(ctx, symbol, "BUY", ask, "exit")

    def on_order(self, ctx, event):
        order = event.data()
        phase_symbol = self.order_phase.get(order.order_id)
        if phase_symbol and order.is_filled:
            phase, symbol = phase_symbol
            (self.entry_fills if phase == "entry" else self.exit_fills).add(symbol)
        if phase_symbol and "REJECT" in str(order.status).upper():
            self.state["rejects"].append({
                "symbol": order.symbol, "reason": str(order.reject_reason or "")
            })

    def on_stop(self, ctx, event):
        self.state.update({
            "legs": self.legs,
            "entry_fills": sorted(self.entry_fills),
            "exit_fills": sorted(self.exit_fills),
            "positions": {
                role: float(ctx.net_position(leg["symbol"])) for role, leg in self.legs.items()
            },
        })
        emit_checkpoint(ctx, "t45_multileg_options", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT45", type="SdkT45", symbols=["SPXW"])],
        symbols=["SPXW"],
        start_date="2025-06-02",
        end_date="2025-06-06",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_OPT", "schema": ["snaps_1s"]
        }],
        backtest_config=BacktestConfig(session_start="15:30", session_end="16:00"),
    )
    state = completed_checkpoint(run, "t45_multileg_options")
    roles = set(state["legs"])
    finish("t45_multileg_options", {
        "call_and_put_wings_selected": roles == {"long_put", "short_put", "short_call", "long_call"},
        "four_entry_orders": state["entry_orders"] == 4,
        "four_entry_fills": len(state["entry_fills"]) == 4,
        "four_exit_orders": state["exit_orders"] == 4,
        "four_exit_fills": len(state["exit_fills"]) == 4,
        "no_rejections": not state["rejects"],
        "all_legs_flat": len(state["positions"]) == 4 and all(v == 0.0 for v in state["positions"].values()),
        # Five sessions x four legs x entry+exit = 40 orders and 20 round
        # trips; the floors stay tolerant of a single thin chain.
        "week_has_at_least_30_orders": len(run.orders()) >= 30,
        "week_has_at_least_15_round_trips": len(run.trades()) >= 15,
    }, extra=str(state))
