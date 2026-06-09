#!/usr/bin/env python3
"""Intraday momentum (equity) — fast/slow SMA crossover, EST time window, EOD flat.

Demonstrates (every ctx call verified against the SDK type surface):
  - per-event callbacks + per-symbol state in a rolling ``deque`` — there is no
    engine-side history buffer, you keep your own window (§16.2).
  - an indicator computed by hand with ``numpy`` (no built-in TA library — §16.3).
  - TIME IS EST/EDT: ``ctx.now()`` is already the configured-tz (ET) datetime.
    Compare wall-clock directly; never convert to/from UTC (R5/R6).
  - sizing + exits: ``buy_order`` to enter, ``close_position`` to flatten.

Shape: long-only intraday. Enter on a fast>slow SMA cross during the entry window;
exit on the reverse cross or force-flat near the 16:00 ET close.

Run:  python intraday_momentum_equity.py
"""
from collections import deque

import numpy as np

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

FAST, SLOW = 10, 30          # SMA lengths, in 1-minute bars
ENTRY_OPEN = (9, 35)         # ET (hour, minute): skip the first few minutes
FLAT_BY = (15, 55)           # ET: flatten before the 16:00 close


class IntradayMomentum:
    def __init__(self):
        self.closes = {}                                     # symbol -> deque[float]

    def on_start(self, ctx, event):                          # subscribe in START (R3)
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        w = self.closes.setdefault(bar.symbol, deque(maxlen=SLOW))
        w.append(bar.close)

        now = ctx.now()                                      # ET datetime (R5/R6) — no tz math
        if (now.hour, now.minute) >= FLAT_BY:                # force-flat near the close
            if not ctx.is_flat(bar.symbol):
                ctx.close_position(bar.symbol)
            return

        if len(w) < SLOW:
            return                                           # still warming up
        closes = np.asarray(w)
        fast, slow = closes[-FAST:].mean(), closes.mean()

        if fast > slow and (now.hour, now.minute) >= ENTRY_OPEN and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=100)
            ctx.add_event_log(f"enter long {bar.symbol} fast={fast:.2f} slow={slow:.2f}",
                              symbol=bar.symbol)
        elif fast < slow and ctx.is_net_long(bar.symbol):
            ctx.close_position(bar.symbol)
            ctx.add_event_log(f"exit long {bar.symbol}", symbol=bar.symbol)

    def on_order(self, ctx, event):                          # fills arrive here, not on_order_filled
        o = event.data()                                     # -> SigmaOrder (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="IntradayMomentum", type="IntradayMomentum")],
        symbols=["AAPL", "MSFT"],
        start_date="2025-08-01",
        end_date="2025-08-08",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
