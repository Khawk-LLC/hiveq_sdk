#!/usr/bin/env python3
"""0DTE SPXW iron condor — option snaps subscription + multi-leg structure.

Demonstrates (every ctx call verified against the SDK type surface):
  - ``ctx.subscribe_option_snaps(symbol, expiration_type='0dte', interval='1s')``
    to stream the live 0DTE chain, plus ``ctx.subscribe_index(['SPX'])`` for the
    spot underlying (§5.5 / §6 subscriptions).
  - the SNAP callback ``on_snap(ctx, event)`` with ``event.data()`` -> SigmaSnapData
    (§7.7): cache each leg by ``(option_type, strike)`` using ``chain`` (the OSI
    symbol you actually trade), ``bid_px``/``ask_px``/``mid_price``.
  - a 4-leg structure placed all-or-nothing: sell the nearest OTM call & put,
    buy the wings two strikes further out, as LIMIT orders at the snap mid.
  - TIME IS EST/EDT: ``ctx.now()`` is already ET — compare wall-clock directly,
    never convert UTC (R5/R6). Throttle to one entry per clock-minute.

Run:  python options_0dte_iron_condor.py
"""
from datetime import time

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.trading_types import OrderType

UNDERLYING = "SPXW"            # 0DTE SPX Weeklys chain root (NOT 'SPX', which is monthly AM-settled)
ENTRY_TIME = time(15, 30)      # ET window to sell condors
EXIT_TIME = time(15, 57)
CONTRACTS = 1
DEFAULT_TICK = 0.05           # SPX/SPXW options trade on a $0.05 grid


class ZeroDTEIronCondor:
    def __init__(self):
        self.spx = None                                      # latest SPX index price
        self.snaps = {}                                      # (option_type, strike) -> dict
        self.last_minute = -1                                # one entry per clock-minute

    def on_start(self, ctx, event):
        ctx.subscribe_option_snaps(UNDERLYING, expiration_type="0dte", interval="1s")
        ctx.subscribe_index(["SPX"])

    def on_index_price(self, ctx, event):                    # INDEX_PRICE -> IndexPrice (§7.11)
        idx = event.data()
        if idx.symbol == "SPX":
            self.spx = float(idx.price)

    def on_snap(self, ctx, event):                           # SNAP -> SigmaSnapData (§7.7)
        snap = event.data()
        self.snaps[(str(snap.option_type).upper(), float(snap.strike))] = {
            "chain": str(snap.chain),                        # OSI symbol -> trade THIS, verbatim
            "bid": snap.bid_px,
            "ask": snap.ask_px,
        }

        now = ctx.now()                                      # ET datetime (R5/R6) — no tz math
        if now is None or self.spx is None:
            return
        minute = now.hour * 60 + now.minute
        if ENTRY_TIME <= now.time() <= EXIT_TIME and minute != self.last_minute:
            self._enter(ctx, minute)

    def _mid(self, ctx, option_type, strike):
        d = self.snaps.get((option_type, strike))
        if not d or d["bid"] is None or d["ask"] is None or d["bid"] <= 0 or d["ask"] <= 0:
            return None                                      # worthless far-OTM wings quote 0 -> skip
        tick = getattr(ctx.instrument(d["chain"]), "min_tick", DEFAULT_TICK) or DEFAULT_TICK
        mid = round(round((d["bid"] + d["ask"]) / 2.0 / tick) * tick, 6)
        return mid if mid > 0 else None

    def _enter(self, ctx, minute):
        self.last_minute = minute                            # throttle FIRST — a failed leg must not retry this minute
        calls = sorted(s for ot, s in self.snaps if ot == "C")
        puts = sorted(s for ot, s in self.snaps if ot == "P")

        ci = next((i for i, s in enumerate(calls) if s >= self.spx), None)
        pi = next((i for i in range(len(puts) - 1, -1, -1) if puts[i] <= self.spx), None)
        if ci is None or ci + 2 >= len(calls) or pi is None or pi - 2 < 0:
            ctx.add_event_log(f"[{minute}] not enough OTM strikes around SPX={self.spx:.2f}",
                              sub_event_type="ERROR")
            return

        # (action, option_type, strike): sell nearest OTM, buy the wing two strikes out
        legs = [("SELL", "C", calls[ci]), ("BUY", "C", calls[ci + 2]),
                ("SELL", "P", puts[pi]),  ("BUY", "P", puts[pi - 2])]

        # Validate ALL legs first — an iron condor is all-or-nothing; never leave a naked short.
        prepared = []
        for action, ot, strike in legs:
            d = self.snaps.get((ot, strike))
            mid = self._mid(ctx, ot, strike)
            if not d or mid is None:
                ctx.add_event_log(f"[{minute}] unpriced {ot} {strike} — abort entry",
                                  sub_event_type="ERROR")
                return
            prepared.append((action, d["chain"], mid))

        for action, sym, mid in prepared:
            if action == "SELL":
                ctx.sell_order(sym, CONTRACTS, order_type=OrderType.LIMIT, limit_price=mid)
            else:
                ctx.buy_order(sym, CONTRACTS, order_type=OrderType.LIMIT, limit_price=mid)
        ctx.add_event_log(f"[{minute}] IC entered SPX={self.spx:.2f}", sub_event_type="ENTRY_TRADE")

    def on_order(self, ctx, event):                          # fills arrive here (§7.3)
        o = event.data()
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}", symbol=o.symbol)

    def on_stop(self, ctx, event):
        pf = ctx.portfolio()
        ctx.add_event_log(f"day end total_pnl={pf.total_pnl():.2f} unrealized={pf.unrealized_pnl():.2f}",
                          sub_event_type="TEARDOWN")


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="ZeroDTEIronCondor", type="ZeroDTEIronCondor")],
        start_date="2026-04-24",
        end_date="2026-04-24",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_OPT", "schema": ["snaps_1s"]}],
        backtest_config=BacktestConfig(start_date="2026-04-24", end_date="2026-04-24",
                                       session_start="15:30", session_end="16:00"),
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    logs = run.event_logs()
    if not logs.empty:
        print(logs.to_string())
