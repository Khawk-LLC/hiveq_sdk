"""Ten-symbol MOO-to-MOC auctions across a holiday-shortened trading week.

Endurance here is about session variety rather than raw span: a full market
closure and a 13:00 early close in the same window exercise the auction
cutoffs that a run of ordinary sessions never reaches, at a fraction of the
tick volume a year of `eq_trades` costs.

Christmas week, not Thanksgiving week: 2025-11-24 has no `eq_trades` rows before
09:47:19 ET -- the opening auction print and the first 17 minutes of the session
are absent for every symbol (verified in ClickHouse, table-wide for that date).
Auction orders cannot fill against a print that was never ingested, and a
validation that fails on missing data tests the ingest, not the framework.
2025-12-22..26 has the same structure -- 12-25 closed, 12-24 an early close --
with the session data intact.

A symbol that does not fill is a failure, not an exemption: the MOO-to-MOC round
trip is the contract under test. Sessions with no ingested data are excluded by
choosing the window; a symbol that had data and still did not fill is a defect,
in the engine's auction matching or in the ingested print coverage, and the
per-session breakdown in the result line says which.

The strategy's state is rebuilt by ``on_start``, which the engine calls once per
session, so the checkpoint used to carry only the *last* session's tally while
the checks read as if they covered the week -- and the order/fill floors were
counts no single-session strategy could reach.  Each session is now snapshotted
as it ends and every floor is derived from the sessions actually run.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (
    completed_checkpoint,
    emit_checkpoint,
    finish,
    open_positions,
    orders_frame,
)
from baseline_t24_auction_orders import SYMBOLS, SdkT24

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig


class SdkT57(SdkT24):
    """The proven ten-symbol auction round trip, tallied session by session."""

    def on_start(self, ctx, event):
        if not hasattr(self, "sessions"):
            self.sessions = []
        else:
            # The session that just ended, before SdkT24.on_start resets it.
            self.sessions.append(self._snapshot(ctx))
        super().on_start(ctx, event)
        moment = ctx.now()
        self.state["session_date"] = str(moment.date()) if moment else ""
        self.state["session_open_time"] = moment.isoformat() if moment else ""

    def _snapshot(self, ctx):
        state = dict(self.state)
        state["moo_placed"] = sorted(self.state["moo_placed"])
        state["moc_placed"] = sorted(self.state["moc_placed"])
        return state

    def on_stop(self, ctx, event):
        self.sessions.append(self._snapshot(ctx))
        emit_checkpoint(ctx, "t57_auction_orders_endurance",
                        {"sessions": self.sessions})


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT57", type="SdkT57", symbols=SYMBOLS)],
        symbols=SYMBOLS,
        start_date="2025-12-22",
        end_date="2025-12-26",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]
        }],
        backtest_config=BacktestConfig(session_start="04:00", session_end="16:30"),
    )
    state = completed_checkpoint(run, "t57_auction_orders_endurance")
    orders = orders_frame(run)
    fills = run.fills()
    trades = run.trades()
    positions = run.positions()

    # Christmas week: 12-22/23 regular, 12-24 a 13:00 early close, 12-25 closed,
    # 12-26 regular -- both a full closure and a shortened auction day.
    sessions = [session for session in state["sessions"] if session.get("trades")]
    universe = set(SYMBOLS)
    moo_fills = sum(len(x["moo_fills"]) for x in sessions)
    moc_fills = sum(len(x["moc_fills"]) for x in sessions)
    expected_round_trips = 10 * len(sessions)
    finish("t57_auction_orders_endurance", {
        "multiple_sessions_observed": len(sessions) >= 3,
        "tick_data_in_every_session": all(x["trades"] > 0 for x in sessions),
        "every_session_placed_ten_moo": bool(sessions) and all(
            set(x["moo_placed"]) == universe for x in sessions),
        "every_session_filled_ten_moo": bool(sessions) and all(
            set(x["moo_fills"]) == universe for x in sessions),
        "every_session_placed_ten_moc": bool(sessions) and all(
            set(x["moc_placed"]) == universe for x in sessions),
        "every_session_filled_ten_moc": bool(sessions) and all(
            set(x["moc_fills"]) == universe for x in sessions),
        # A MOO entered hours before the cutoff must never be rejected: it fills
        # on its venue's print or is canceled when the auction passes.
        "no_session_rejections": all(not x["rejects"] for x in sessions),
        "every_moo_resolved_every_session": bool(sessions) and all(
            set(x["moo_terminal"]) == universe
            and all(status in {"FILLED", "CANCELED"}
                    for status, _ in x["moo_terminal"].values())
            for x in sessions),
        "round_trip_per_symbol_per_session": len(trades) >= expected_round_trips,
        "two_orders_per_symbol_per_session": len(orders) >= 2 * expected_round_trips,
        "two_fills_per_symbol_per_session": len(fills) >= 2 * expected_round_trips,
        "all_symbols_flat": open_positions(positions).empty,
    }, extra=(f"sessions={len(sessions)}; orders={len(orders)}, fills={len(fills)}, "
              f"trades={len(trades)}, moo_fills={moo_fills}, moc_fills={moc_fills}; "
              f"per_session=" + str([{
                  "date": x.get("session_date"), "trades": x["trades"],
                  "moo_placed": len(x["moo_placed"]), "moo_fills": len(x["moo_fills"]),
                  "moc_placed": len(x["moc_placed"]), "moc_fills": len(x["moc_fills"]),
                  "rejects": len(x["rejects"]),
                  "reject_reasons": sorted({r["reason"] for r in x["rejects"]}),
              } for x in sessions])))
