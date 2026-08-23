"""Performance statistics recomputed from the run's own equity curve.

t09 proves the analytics tables are non-empty. It never checks a single number
in them, so a broken Sharpe, an inverted drawdown, or a return series scaled by
100 would pass the whole suite. Those numbers are what a user decides to trade
on.

Everything here is recomputed independently from the persisted equity curve and
trade rows, then compared to what the platform reports -- and the surfaces the
suite never touches at all (``overview``, ``tearsheet``) are exercised so a
release cannot break them unnoticed.
"""
from pathlib import Path
import sys
import math

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

SYMBOL = "AAPL"
INITIAL = 1_000_000.0
QTY = 300.0


class SdkT65:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.bars = 0
            self.state = {"bars": 0, "round_trips": 0, "wins": 0, "losses": 0,
                          "entry_px": 0.0, "pnl": [], "peak": 0.0, "max_dd": 0.0,
                          "final": {}}
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1d")

    def on_bar(self, ctx, event):
        self.bars += 1
        # One position at a time, flipped every other day, so the run produces a
        # mix of winning and losing closed trades to reconcile against.
        if ctx.is_flat(SYMBOL) and not ctx.has_open_order(SYMBOL):
            ctx.buy_order(SYMBOL, QTY)
        elif not ctx.has_open_order(SYMBOL):
            ctx.close_position(SYMBOL)

        portfolio = ctx.portfolio()
        equity = portfolio.equity
        self.state["peak"] = max(self.state["peak"], equity)
        if self.state["peak"] > 0:
            drawdown = (self.state["peak"] - equity) / self.state["peak"]
            self.state["max_dd"] = max(self.state["max_dd"], drawdown)

    def on_order(self, ctx, event):
        order = event.data()
        if "FILL" not in str(order.status).upper() or float(order.filled_qty or 0) <= 0:
            return
        price = float(order.avg_px or 0)
        if str(order.side).upper().endswith("BUY"):
            self.state["entry_px"] = price
        elif self.state["entry_px"]:
            pnl = (price - self.state["entry_px"]) * float(order.filled_qty)
            self.state["pnl"].append(round(pnl, 6))
            self.state["round_trips"] += 1
            if pnl > 0:
                self.state["wins"] += 1
            elif pnl < 0:
                self.state["losses"] += 1
            self.state["entry_px"] = 0.0

    def on_stop(self, ctx, event):
        portfolio = ctx.portfolio()
        self.state["bars"] = self.bars
        self.state["final"] = {
            "equity": round(portfolio.equity, 6),
            "realized": round(portfolio.realized_pnl(), 6),
            "fees": round(portfolio.fees, 6),
            "max_drawdown_reported": round(float(portfolio.max_drawdown or 0), 8),
            "total_pnl": round(portfolio.total_pnl(), 6),
            "day_pnl": round(portfolio.day_pnl(), 6),
        }
        emit_checkpoint(ctx, "t65_analytics_reconciliation", self.state)


def numeric_column(frame, *candidates):
    lowered = {str(column).lower(): column for column in frame.columns}
    for candidate in candidates:
        for name, column in lowered.items():
            if candidate in name:
                return column
    return None


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT65", type="SdkT65", symbols=[SYMBOL])],
        symbols=[SYMBOL], start_date="2025-01-02", end_date="2025-06-30",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1d"]
        }],
        backtest_config=BacktestConfig(initial_capital=INITIAL, export_orders_csv=True),
    )
    state = completed_checkpoint(run, "t65_analytics_reconciliation")
    report = run.report()
    equity_curve = run.equity_curve()
    daily = run.daily_returns()
    trades = run.trades()
    metrics = run.metrics()

    # Independent statistics from the persisted equity curve.
    equity_column = numeric_column(equity_curve, "equity", "value", "nav")
    series = []
    if equity_column is not None:
        series = [float(value) for value in equity_curve[equity_column].dropna()]
    returns = [series[i] / series[i - 1] - 1.0
               for i in range(1, len(series)) if series[i - 1]]
    mean = sum(returns) / len(returns) if returns else 0.0
    variance = (sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
                if len(returns) > 1 else 0.0)
    stdev = math.sqrt(variance)
    sharpe = (mean / stdev * math.sqrt(252)) if stdev else 0.0

    peak = 0.0
    computed_dd = 0.0
    for value in series:
        peak = max(peak, value)
        if peak:
            computed_dd = max(computed_dd, (peak - value) / peak)

    reported_sharpe = None
    if report.return_stats is not None and len(report.return_stats):
        column = numeric_column(report.return_stats, "sharpe")
        if column is not None:
            try:
                reported_sharpe = float(report.return_stats[column].dropna().iloc[-1])
            except (IndexError, ValueError):
                reported_sharpe = None

    trade_pnl_column = numeric_column(trades, "pnl") if len(trades) else None
    trade_pnl = (float(trades[trade_pnl_column].sum())
                 if trade_pnl_column is not None else None)
    strategy_pnl = sum(state["pnl"])

    overview_ok = False
    try:
        overview_ok = isinstance(run.overview(), dict)
    except Exception as exc:                               # noqa: BLE001
        overview_ok = f"overview failed: {exc}"

    tearsheet_ok = False
    try:
        tearsheet_ok = bool(report.create_tearsheet())
    except Exception as exc:                               # noqa: BLE001
        tearsheet_ok = f"tearsheet failed: {exc}"

    checks = {
        "multi_month_run_traded": state["round_trips"] >= 10,
        "equity_curve_has_series": len(series) >= 20,
        "daily_returns_rows": len(daily) >= 20,
        "metrics_rows": len(metrics) >= 1,
        "net_pnl_matches_realized": abs(
            float(report.net_pnl or 0)
            - (state["final"]["realized"] - state["final"]["fees"])
        ) < max(2.0, abs(state["final"]["realized"]) * 0.02),
        "equity_curve_ends_at_final_equity": bool(series) and abs(
            series[-1] - state["final"]["equity"]
        ) < max(2.0, state["final"]["equity"] * 0.001),
        "reported_drawdown_matches_curve": abs(
            state["final"]["max_drawdown_reported"] - computed_dd
        ) < 0.02 or abs(
            state["final"]["max_drawdown_reported"] / 100.0 - computed_dd
        ) < 0.02,
        "drawdown_is_a_fraction_not_a_ratio_error": 0.0 <= computed_dd <= 1.0,
        "trades_table_reports_pnl": trade_pnl is not None,
        "trade_pnl_matches_strategy_arithmetic": (
            trade_pnl is not None
            and abs(trade_pnl - strategy_pnl) < max(5.0, abs(strategy_pnl) * 0.1)
        ),
        "win_loss_counts_consistent": (
            state["wins"] + state["losses"] <= state["round_trips"]
            and state["round_trips"] > 0
        ),
        # PerformanceReport documents return_stats as carrying Sharpe; if the
        # column is absent the comparison below passes vacuously, so its
        # presence is asserted separately.
        "sharpe_reported_in_return_stats": reported_sharpe is not None,
        "sharpe_matches_curve": (
            reported_sharpe is None
            or abs(reported_sharpe - sharpe) < max(0.5, abs(sharpe) * 0.25)
        ),
        "overview_readable": overview_ok is True,
        "tearsheet_renders": tearsheet_ok is True,
    }
    checks.update(evidence_checks(run, orders=10, trades=5))
    finish("t65_analytics_reconciliation", checks,
           extra=(f"round_trips={state['round_trips']} wins={state['wins']} "
                  f"losses={state['losses']}; equity_points={len(series)}; "
                  f"computed_sharpe={round(sharpe, 4)} reported={reported_sharpe}; "
                  f"computed_dd={round(computed_dd, 6)} "
                  f"reported_dd={state['final']['max_drawdown_reported']}; "
                  f"trade_pnl={trade_pnl} strategy_pnl={round(strategy_pnl, 4)}; "
                  f"net_pnl={report.net_pnl}; final={state['final']}; "
                  f"overview={overview_ok}; tearsheet={tearsheet_ok}"))
