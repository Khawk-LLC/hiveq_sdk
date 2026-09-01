"""Remote metric arithmetic on a futures round trip, reconciled from fills.

Equity's t10 uses an implicit multiplier of 1 and never exercises the code
that applies the contract multiplier to notional and PnL. Any bug where the
multiplier is dropped or hardcoded to 1 passes t10 silently but corrupts
every futures backtest. This case buys 1 ES contract, closes it later in the
same session, and reconciles ``report.net_pnl`` against
``(exit - entry) * qty * multiplier - fees`` computed here.
"""
from pathlib import Path
import sys
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, BacktestConfig

SYMBOL = "ES.c.0"
INITIAL = 1_000_000.0

class SdkT10b:
    def on_start(self, ctx, event):
        self.bars = 0
        self.fills = []
        self.multiplier = None
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")
    def on_bar(self, ctx, event):
        self.bars += 1
        if self.bars == 1:
            ctx.buy_order(SYMBOL, quantity=1)
        elif self.bars == 60 and not ctx.has_open_order(SYMBOL):
            ctx.close_position(SYMBOL)
    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" in str(getattr(order, "status", "")).upper() and float(order.filled_qty or 0) > 0:
            self.fills.append([str(order.side).upper(), float(order.filled_qty), float(order.avg_px or 0)])
    def on_stop(self, ctx, event):
        # Capture multiplier from the instrument. This is the value the engine
        # itself should be using to size notional and PnL -- if it disagrees
        # with the report's implicit multiplier, that's the bug we're catching.
        try:
            self.multiplier = float(ctx.instrument(SYMBOL).multiplier)
        except Exception:
            self.multiplier = None
        emit_checkpoint(ctx, "t10b_metrics_validation_futures",
                        {"fills": self.fills, "multiplier": self.multiplier})

if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT10b", type="SdkT10b", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2026-05-07", end_date="2026-05-07",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT", "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(initial_capital=INITIAL, session_start="09:30", session_end="12:00"))
    state = completed_checkpoint(run, "t10b_metrics_validation_futures")
    report = run.report()
    buys = [x for x in state["fills"] if x[0].endswith("BUY")]
    sells = [x for x in state["fills"] if x[0].endswith("SELL")]
    def weighted_average(rows):
        return sum(q * px for _, q, px in rows) / max(1e-9, sum(q for _, q, _ in rows))
    mult = state["multiplier"]
    entry = weighted_average(buys) if buys else 0.0
    exit = weighted_average(sells) if sells else 0.0
    qty = sum(q for _, q, _ in buys) if buys else 0.0
    fees = float(report.total_fees or 0)
    # Multiplier must be > 1 for futures -- if it comes back 1.0 that itself
    # is the bug (contract security metadata missing or engine defaulting).
    multiplier_present = isinstance(mult, float) and mult > 1.0
    gross = (exit - entry) * qty * (mult or 1.0)
    expected = gross - fees
    trades = run.trades()
    pnl_columns = [c for c in trades.columns if str(c).lower() == "pnl"]
    trades_ok = not pnl_columns or abs(float(trades[pnl_columns[0]].sum()) - gross) < max(2, abs(gross) * .1)
    finish("t10b_metrics_validation_futures", {
        "round_trip_recorded": bool(buys) and bool(sells),
        "multiplier_present_and_gt_one": multiplier_present,
        "net_pnl_matches_fills_with_multiplier":
            abs(report.net_pnl - expected) < max(2, abs(expected) * .05),
        "trades_pnl_consistent": trades_ok,
        "fees_non_negative": fees >= 0,
    }, extra=(f"symbol={SYMBOL}, entry={entry}, exit={exit}, qty={qty}, "
             f"multiplier={mult}, gross={gross}, expected={expected}, "
             f"report={report.net_pnl}"))
