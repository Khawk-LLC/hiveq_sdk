#!/usr/bin/env python3
"""Statistical-arbitrage pairs — z-score of the log-spread between two symbols.

Demonstrates (every ctx call verified against the SDK type surface):
  - a 2-symbol pairs/spread strategy with PER-SYMBOL state: you keep the latest
    close of each leg yourself (there is no engine history buffer — §16.2), and
    only act once BOTH legs have printed for the current bar.
  - a rolling z-score of the log-spread computed by hand with ``numpy`` (no
    built-in TA — §16.3): enter when |z| crosses ``ENTRY_Z`` (long the cheap leg,
    short the rich leg), unwind when |z| mean-reverts below ``EXIT_Z``.
  - long/short legs via ``buy_order`` / ``short_order`` and flatten via
    ``close_position``; position checks via ``ctx.is_flat`` (§8).

Shape: spread = log(A) - beta*log(B). When z < -ENTRY_Z the spread is cheap ->
long A / short B; when z > +ENTRY_Z it's rich -> short A / long B; close both
legs when |z| < EXIT_Z. ``beta`` is a fixed hedge ratio here for clarity.

Run:  python pairs_stat_arb.py
"""
from collections import deque

import numpy as np

import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

SYM_A, SYM_B = "KO", "PEP"     # the pair (correlated consumer-staples names)
BETA = 1.0                     # fixed hedge ratio: spread = log(A) - BETA*log(B)
WINDOW = 60                    # bars in the rolling z-score window
ENTRY_Z, EXIT_Z = 2.0, 0.5     # enter beyond +/-2 sigma, unwind inside +/-0.5
QTY_A, QTY_B = 100, 100        # per-leg share clips


class PairsStatArb:
    def __init__(self):
        self.last = {}                                       # symbol -> latest close
        self.spreads = deque(maxlen=WINDOW)                  # rolling log-spread
        self.direction = 0                                   # +1 long A/short B, -1 inverse, 0 flat

    def on_start(self, ctx, event):                          # subscribe in START (R3)
        ctx.subscribe_bars([SYM_A, SYM_B], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        self.last[bar.symbol] = bar.close
        if SYM_A not in self.last or SYM_B not in self.last:
            return                                           # wait for both legs

        spread = np.log(self.last[SYM_A]) - BETA * np.log(self.last[SYM_B])
        self.spreads.append(spread)
        if len(self.spreads) < WINDOW:
            return                                           # still warming up

        arr = np.asarray(self.spreads)
        std = arr.std()
        if std < 1e-9:
            return
        z = (spread - arr.mean()) / std

        if self.direction != 0:                              # manage the open spread
            if abs(z) < EXIT_Z:
                ctx.close_position(SYM_A)
                ctx.close_position(SYM_B)
                self.direction = 0
                ctx.add_event_log(f"exit pair z={z:.2f}", symbol=SYM_A)
            return

        if z < -ENTRY_Z and ctx.is_flat(SYM_A) and ctx.is_flat(SYM_B):
            ctx.buy_order(SYM_A, quantity=QTY_A)             # spread cheap: long A, short B
            ctx.short_order(SYM_B, quantity=QTY_B)
            self.direction = 1
            ctx.add_event_log(f"long spread z={z:.2f}", symbol=SYM_A)
        elif z > ENTRY_Z and ctx.is_flat(SYM_A) and ctx.is_flat(SYM_B):
            ctx.short_order(SYM_A, quantity=QTY_A)           # spread rich: short A, long B
            ctx.buy_order(SYM_B, quantity=QTY_B)
            self.direction = -1
            ctx.add_event_log(f"short spread z={z:.2f}", symbol=SYM_A)

    def on_order(self, ctx, event):                          # fills arrive here (§7.3)
        o = event.data()
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}", symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="PairsStatArb", type="PairsStatArb",
                                         symbols=[SYM_A, SYM_B])],
        symbols=[SYM_A, SYM_B],
        start_date="2025-08-01",
        end_date="2025-08-08",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
