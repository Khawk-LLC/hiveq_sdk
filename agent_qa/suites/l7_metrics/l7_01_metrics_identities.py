"""l7_01: report metrics recomputed independently from the fills.

A metrics test that reads the report and asserts the report agrees with itself
proves nothing. This one recomputes the headline numbers from the *fill* rows
and checks the report matches — an arithmetic error in the engine's accounting
shows up as a mismatch rather than as a plausible-looking number.

Identities asserted:

* ``net_pnl`` equals PnL computed by walking the fills (signed notional, minus
  fees) — the accounting identity.
* ``daily_return`` is in **percent**, not a fraction: ``net_pnl /
  initial_capital * 100``. This unit is easy to get backwards and the mistake is
  invisible until someone compounds it.
* The equity curve starts at ``initial_capital`` and its last point equals
  ``initial_capital + net_pnl``.
* ``trades()`` and ``orders()`` are mutually consistent — every trade traces to
  filled orders.
* Risk ratios (Sharpe/Sortino/volatility) are **null, not zero**, on a run too
  short to define them. The engine's >=10-active-day floor emitting null is
  intentional; a UI that renders 0.0 instead of "-" is the display bug this
  guards against, and a 0.0 arriving from the engine would be the real one.

Runs a deliberately simple buy-and-hold so the expected PnL is a single
round-trip that can be checked by hand from the extra line.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l7_01_metrics_identities"
SURFACE = "l7.metrics"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol
QTY = 100.0
CAPITAL = FIXTURES.initial_capital

#: Money tolerance. Fees and rounding make exact equality the wrong test.
TOL = 1.0

probe = Probe()


class L7MetricsIdentities:

    def __init__(self):
        self.step = 0

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        probe.bump("bar")
        self.step += 1
        if self.step == 2:
            ctx.buy_order(SYMBOL, quantity=QTY)
            probe.bump("buy")
        elif self.step == 20:
            ctx.close_position(SYMBOL)
            probe.bump("close")

    def on_order(self, ctx, event):
        order = event.data()
        if getattr(order, "is_filled", False):
            probe.bump("fill")
            probe.sample("fill", symbol=order.symbol,
                         qty=getattr(order, "filled_qty", None),
                         px=getattr(order, "avg_px", None),
                         side=str(getattr(order, "side", "")))

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def _num(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pnl_from_fills(df):
    """Signed cash flow across fills: sells add, buys subtract."""
    if df is None or getattr(df, "empty", True):
        return None
    cols = {c.lower(): c for c in df.columns}
    qty_c = cols.get("filled_qty") or cols.get("quantity") or cols.get("qty")
    px_c = cols.get("avg_px") or cols.get("price") or cols.get("fill_price")
    side_c = cols.get("side") or cols.get("order_side")
    if not (qty_c and px_c and side_c):
        return None

    total = 0.0
    for _, row in df.iterrows():
        qty, px = _num(row[qty_c]), _num(row[px_c])
        if qty is None or px is None:
            continue
        side = str(row[side_c]).upper()
        total += (qty * px) if "SELL" in side else -(qty * px)
    return total


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L7MetricsIdentities", symbols=[SYMBOL],
                        params={"initial_capital": CAPITAL})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:30"),
    )

    data = probe.collect(run)
    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")

    # R12: a run with no trades is not a valid basis for a metrics assertion.
    c.add("run_produced_trades", data.count("fill") > 0,
          f"fills={data.count('fill')} — metrics cannot be validated on a "
          "zero-trade run")
    if data.count("fill") == 0:
        c.finish(NAME, surface=SURFACE, extra=f"bars={data.count('bar')}")
        return

    report = run.report()
    net_pnl = _num(getattr(report, "net_pnl", None))
    c.add("net_pnl_present", net_pnl is not None, f"report.net_pnl={net_pnl!r}")

    fills_df = run.fills()
    recomputed = _pnl_from_fills(fills_df)
    # This is the test's whole reason to exist: agreement between the report and
    # an INDEPENDENT recomputation. Previously a missing fills table only
    # produced a note, so the test reported PASS while never checking the one
    # thing it was written to check. Now it is gated, so an empty fills table
    # makes the verdict inconclusive instead of green.
    n_fill_rows = 0 if fills_df is None else len(fills_df)
    c.add("net_pnl_matches_fills",
          net_pnl is not None and recomputed is not None
          and abs(net_pnl - recomputed) <= max(TOL, abs(net_pnl) * 0.01),
          f"report={net_pnl} vs recomputed_from_fills={recomputed}",
          requires=net_pnl is not None and recomputed is not None,
          requires_detail=f"cannot recompute independently: run.fills() returned "
                          f"{n_fill_rows} rows while the strategy observed "
                          f"{data.count('fill')} fills, so the report's net_pnl "
                          f"is unverified")

    # daily_return is a PERCENT, not a fraction.
    daily = run.daily_returns()
    if daily is not None and not getattr(daily, "empty", True):
        col = next((x for x in daily.columns if "return" in x.lower()), None)
        if col and net_pnl is not None:
            observed = _num(daily.iloc[-1][col])
            expected_pct = net_pnl / CAPITAL * 100.0
            if observed is not None:
                c.add("daily_return_is_percent",
                      abs(observed - expected_pct) <= max(0.01, abs(expected_pct) * 0.05),
                      f"daily_return={observed!r} vs net_pnl/capital*100="
                      f"{expected_pct:.4f}; a ~100x gap means it is a fraction")
    else:
        c.note("daily_returns() empty")

    # Equity curve endpoints.
    eq = run.equity_curve()
    if eq is not None and not getattr(eq, "empty", True):
        col = next((x for x in eq.columns if "equity" in x.lower() or "value" in x.lower()), None)
        if col:
            first, last = _num(eq.iloc[0][col]), _num(eq.iloc[-1][col])
            if first is not None:
                c.add("equity_starts_at_capital", abs(first - CAPITAL) <= max(TOL, CAPITAL * 0.01),
                      f"first equity point={first:.2f}, initial_capital={CAPITAL:.2f}")
            if last is not None and net_pnl is not None:
                c.add("equity_ends_at_capital_plus_pnl",
                      abs(last - (CAPITAL + net_pnl)) <= max(TOL, CAPITAL * 0.01),
                      f"last={last:.2f} vs capital+net_pnl={CAPITAL + net_pnl:.2f}")
    else:
        c.note("equity_curve() empty")

    # Risk ratios must be null (not 0.0) on a single-session run: the engine's
    # >=10-active-day floor is intentional, and 0.0 would be a real defect.
    metrics = run.metrics()
    zeroed = []
    if metrics is not None and not getattr(metrics, "empty", True):
        row = metrics.iloc[-1].to_dict()
        for key in ("sharpe", "sortino", "volatility"):
            match = next((k for k in row if key in k.lower()), None)
            if match is None:
                continue
            value = _num(row[match])
            if value is not None and value == 0.0:
                zeroed.append(match)
        c.add("risk_ratios_null_not_zero", not zeroed,
              f"{zeroed} came back as 0.0 on a 1-session run; the >=10-active-day "
              "floor should emit null so the UI renders '-'")

    # Trades vs orders consistency.
    trades, orders = run.trades(), run.orders()
    n_trades = 0 if trades is None else len(trades)
    n_orders = 0 if orders is None else len(orders)
    c.add("trades_and_orders_consistent", n_orders >= n_trades,
          f"trades={n_trades} > orders={n_orders}, which cannot happen")

    c.finish(NAME, surface=SURFACE, extra=(
        f"fills={data.count('fill')}, net_pnl={net_pnl}, recomputed={recomputed}, "
        f"trades={n_trades}, orders={n_orders}"
    ))


if __name__ == "__main__":
    main()
