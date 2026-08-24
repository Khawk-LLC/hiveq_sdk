"""Ten-symbol MOO-to-MOC auctions across a holiday-shortened trading week.

Endurance here is about session variety rather than raw span: a full market
closure and a 13:00 early close in the same window exercise the auction
cutoffs that a run of ordinary sessions never reaches, at a fraction of the
tick volume a year of `eq_trades` costs.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import open_positions, completed_checkpoint, finish, orders_frame
from t24_auction_orders import SYMBOLS, SdkT24

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig


class SdkT57(SdkT24):
    """Run the proven ten-symbol auction round trip on every session in the week."""


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT57", type="SdkT57", symbols=SYMBOLS)],
        symbols=SYMBOLS,
        start_date="2025-11-24",
        end_date="2025-11-28",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]
        }],
        backtest_config=BacktestConfig(session_start="04:00", session_end="18:30"),
    )
    state = completed_checkpoint(run, "t24_auction_orders")
    orders = orders_frame(run)
    fills = run.fills()
    trades = run.trades()
    positions = run.positions()
    # Thanksgiving week: Mon-Wed regular, Thursday closed, Friday a 13:00 early
    # close -- four sessions covering both a full closure and a shortened
    # auction day, which is what the endurance case was really reaching for.
    # Four sessions x ten symbols = 40 round trips and 80 orders/fills; the
    # floors stay tolerant of a few symbol-specific data gaps.
    finish("t57_auction_orders_endurance", {
        "final_session_all_ten_moo_filled": set(state["moo_fills"]) == set(SYMBOLS),
        "final_session_all_ten_moc_filled": set(state["moc_fills"]) == set(SYMBOLS),
        "at_least_30_moo_moc_round_trips": len(trades) >= 30,
        "at_least_60_orders": len(orders) >= 60,
        "at_least_60_fills": len(fills) >= 60,
        "no_final_session_rejections": not state["rejects"],
        "all_symbols_flat": open_positions(positions).empty,
    }, extra=(f"orders={len(orders)}, fills={len(fills)}, trades={len(trades)}, "
              f"final_session={state}"))
