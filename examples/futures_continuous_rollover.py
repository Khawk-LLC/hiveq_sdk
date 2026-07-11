#!/usr/bin/env python3
"""Futures, continuous contract + auto-rollover (ES) — Donchian-style breakout.

Demonstrates (verified against the SDK surface):
  - subscribing to a CONTINUOUS futures series by its SYMBOL STRING via
    ``ctx.subscribe_futures_bars(symbols=['ES.c.0'])`` (§5.1).
  - ``BacktestConfig(enable_auto_rollover=True)``: this is what turns on
    continuous-contract rollover — the engine rolls the position when the contract
    rolls and you observe each roll in ``on_rollover``. No ``data_configs`` flag.
  - the FUTURES SESSION window (R6): 18:00 -> 17:00 ET (CME Globex). Times are ET.

Continuous symbol forms (§5.1): "ES.c.0" = front by calendar roll, "ES.v.0" = volume-roll.

Run:  python futures_continuous_rollover.py
"""
from collections import deque

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig

CONT = "ES.c.0"              # front continuous ES
LOOKBACK = 30                # bars for the breakout channel


class FuturesBreakout:
    def __init__(self):
        self.win = {}                                        # symbol -> deque[float] of closes

    def on_start(self, ctx, event):
        ctx.subscribe_futures_bars(symbols=[CONT], interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        w = self.win.setdefault(bar.symbol, deque(maxlen=LOOKBACK))
        if len(w) == w.maxlen:
            hi, lo = max(w), min(w)
            if bar.close > hi and ctx.is_flat(bar.symbol):
                ctx.buy_order(bar.symbol, quantity=1)        # 1 contract
            elif bar.close < lo and ctx.is_net_long(bar.symbol):
                ctx.close_position(bar.symbol)
        w.append(bar.close)

    def on_rollover(self, ctx, event):
        roll = event.data()                                  # -> Rollover (§7.12)
        ctx.add_event_log(
            f"rollover {roll.continuous_symbol}: {roll.prev_contract} -> {roll.current_contract}")

    def on_order(self, ctx, event):
        o = event.data()
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}", symbol=o.symbol)


if __name__ == "__main__":
    cfg = BacktestConfig(
        enable_auto_rollover=True,                           # roll position when the contract rolls
        session_start="18:00",                               # ET — CME Globex session (R6)
        session_end="17:00",
    )
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="FuturesBreakout", type="FuturesBreakout")],
        start_date="2024-01-02",
        end_date="2024-03-01",
        data_configs=[{
            "type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
            "schema": ["bars_1m"]}],
        backtest_config=cfg,
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
