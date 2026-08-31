"""The same backtest, submitted twice, produces the same execution.

Run isolation (t34) proves two runs do not contaminate each other. It does not
prove the engine is deterministic: an unstable data ordering, an iteration over
a hash-ordered container, or a clock read in a fill decision all keep runs
isolated while making them disagree. Nothing else in the suite would notice,
because every other case asserts one run against a threshold rather than
against a second run.

So this replays one fixed configuration and requires the two runs to agree on
the whole execution path -- every order's side, quantity, fill quantity, fill
price and sequence, the trade count, the realized PnL, and the closing
portfolio -- not merely on a summary statistic.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    evidence_checks,
    finish,
    orders_frame,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402
from hiveq.flow.trading_types import OrderType                        # noqa: E402

SYMBOLS = ["AAPL", "MSFT"]
INITIAL = 250_000.0
# A fixed bar-count schedule, so the only thing that can make two runs differ
# is the engine -- not a threshold the strategy itself evaluates differently.
ENTRY_BAR = {"AAPL": 3, "MSFT": 7}
EXIT_BAR = {"AAPL": 25, "MSFT": 31}
REENTRY_BAR = {"AAPL": 40, "MSFT": 45}


class SdkT59:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = {symbol: 0 for symbol in SYMBOLS}
            self.state = {"fills": [], "bars": {}, "final": {}}
        ctx.subscribe_bars(SYMBOLS, asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = bar.symbol
        if symbol not in self.bars:
            return
        self.bars[symbol] += 1
        count = self.bars[symbol]
        if count == ENTRY_BAR[symbol]:
            ctx.buy_order(symbol, 25.0)
        elif count == EXIT_BAR[symbol] and not ctx.has_open_order(symbol):
            ctx.close_position(symbol)
        elif count == REENTRY_BAR[symbol] and not ctx.has_open_order(symbol):
            # A limit that rests then fills, so the replay also covers a path
            # where the fill decision depends on subsequent bars rather than on
            # the bar that placed the order.
            ctx.buy_order(symbol, 10.0, order_type=OrderType.LIMIT,
                          limit_price=round(float(bar.close) * 1.01, 2))

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(order.status).upper() and float(order.filled_qty or 0) > 0:
            self.state["fills"].append([
                order.symbol, str(order.side).upper(), str(order.order_type).upper(),
                float(order.filled_qty), round(float(order.avg_px or 0), 6),
                int(order.ts_event or 0),
            ])

    def on_stop(self, ctx, event):
        portfolio = ctx.portfolio()
        self.state["bars"] = dict(self.bars)
        self.state["final"] = {
            "realized": round(portfolio.realized_pnl(), 6),
            "equity": round(portfolio.equity, 6),
            "fees": round(portfolio.fees, 6),
            "positions": {symbol: ctx.net_position(symbol) for symbol in SYMBOLS},
        }
        emit_checkpoint(ctx, "t59_determinism_replay", self.state)


def replay():
    return hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT59", type="SdkT59", symbols=SYMBOLS)],
        symbols=SYMBOLS, start_date="2025-06-02", end_date="2025-06-02",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]
        }],
        backtest_config=BacktestConfig(initial_capital=INITIAL, session_start="09:30",
                                       session_end="11:30"),
    )


def order_signature(run):
    """The comparable execution path from the public orders table."""
    frame = orders_frame(run)
    if frame.empty:
        return []
    lowered = {str(column).lower(): column for column in frame.columns}

    def column(*names):
        for name in names:
            if name in lowered:
                return lowered[name]
        return None

    fields = [column("symbol"), column("side"),
              column("fillqty", "filled_qty"), column("fillprice", "avg_fill_price"),
              column("status"), column("ordertype", "order_type")]
    fields = [name for name in fields if name]
    rows = frame[fields].astype(str).values.tolist()
    # Order of arrival is itself part of the execution path, but the two runs
    # write separate capture files -- sort so a stable set comparison is not
    # defeated by file ordering, and compare arrival order separately via fills.
    return sorted("|".join(row) for row in rows)


if __name__ == "__main__":
    first = replay()
    first_state = completed_checkpoint(first, "t59_determinism_replay")
    second = replay()
    second_state = completed_checkpoint(second, "t59_determinism_replay")

    first_fills = [row[:5] for row in first_state["fills"]]
    second_fills = [row[:5] for row in second_state["fills"]]
    first_orders = order_signature(first)
    second_orders = order_signature(second)
    first_trades = first.trades()
    second_trades = second.trades()

    checks = {
        "distinct_run_ids": first.run_id != second.run_id,
        "traded_both_runs": bool(first_fills) and bool(second_fills),
        "bar_counts_identical": first_state["bars"] == second_state["bars"],
        "fill_sequence_identical": first_fills == second_fills,
        "orders_table_identical": bool(first_orders) and first_orders == second_orders,
        "trade_count_identical": len(first_trades) == len(second_trades),
        "final_portfolio_identical": first_state["final"] == second_state["final"],
    }
    checks.update(evidence_checks(first, orders=2, trades=1))
    finish("t59_determinism_replay", checks,
           extra=(f"runs=({first.run_id},{second.run_id}); "
                  f"fills={len(first_fills)}/{len(second_fills)}; "
                  f"orders={len(first_orders)}/{len(second_orders)}; "
                  f"final_a={first_state['final']}; final_b={second_state['final']}"))
