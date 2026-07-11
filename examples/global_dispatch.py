#!/usr/bin/env python3
"""Global single-dispatch contract — the alternative to per-event callbacks.

Demonstrates the OPT-IN single-dispatch form (§4): instead of separate
``on_start``/``on_bar``/``on_order`` methods, you write ONE entry point that
branches on ``event.type`` (an ``EventType`` — see §7.0 EventType->payload map).
Per-event callbacks remain the canonical/default contract; use this only when
you specifically want one dispatch point.

Two equivalent shapes (both shown here, only one is used per run):
  1. MODULE-LEVEL function ``def on_hiveq_event(ctx, event):`` — auto-discovered
     when ``run_backtest(strategy_configs=[], ...)`` is called with NO classes.
  2. CLASS method ``def on_hiveq_event(self, ctx, event):`` — still needs a
     ``StrategyConfig(name=..., type=...)`` naming the class.

Strategy logic (buy-and-hold): subscribe in START, buy 100 shares on the first
bar per symbol, then hold. Fills are observed in the ORDER_FILLED branch — note
there is NO on_order_filled callback; the single-dispatch form sees the raw
EventType (§7.0).

Run:  python global_dispatch.py
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType, EventType


# --- Shape 1: module-level global dispatch (auto-discovered when strategy_configs=[]) ---
def on_hiveq_event(ctx, event):
    if event.type == EventType.START:
        ctx.subscribe_bars(symbols=ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")

    elif event.type == EventType.BAR:
        bar = event.data()                                   # -> SigmaBar (§7.1)
        if ctx.is_flat(bar.symbol):                          # buy once, then hold
            ctx.buy_order(bar.symbol, quantity=100)
            ctx.add_event_log(f"buy-and-hold {bar.symbol} @ {bar.close}", symbol=bar.symbol)

    elif event.type == EventType.ORDER_FILLED:
        o = event.data()                                     # -> SigmaOrder (§7.3)
        ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}", symbol=o.symbol)


# --- Shape 2: the same single-dispatch contract as a class method ---
# Run this form by passing strategy_configs=[StrategyConfig(name="BuyHold",
# type="BuyHoldDispatch")] instead of the empty list below.
class BuyHoldDispatch:
    def on_hiveq_event(self, ctx, event):
        on_hiveq_event(ctx, event)                           # delegate to the same logic


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[],                                 # empty -> module-level on_hiveq_event is used
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-02",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    logs = run.event_logs()
    if not logs.empty:
        print(logs.to_string())
