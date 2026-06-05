#!/usr/bin/env python3
"""Auction orders — MOO (Market-On-Open) & MOC (Market-On-Close) via OrderType (§5.2.1).

Demonstrates (every ctx call verified against the SDK type surface):
  - auction order types: ``order_type=OrderType.MOO`` joins the opening cross and
    fills at the 09:30 ET open print; ``OrderType.MOC`` joins the closing cross
    and fills at the 16:00 ET close print (§5.2.1).
  - exchange entry cutoffs: submit MOO BEFORE 09:28 ET and MOC BEFORE 15:50 ET
    (NYSE's close cutoff is earlier than Nasdaq's — 15:50 is venue-agnostic).
  - TIME IS EST/EDT: payload ``tick.time`` is already ET (R5); compare wall-clock
    directly, never convert to/from UTC.

IMPORTANT — data schema: MOO/MOC fill against the **auction prints**
(MCOfficialOpen / MCOfficialClose), which exist only in tick-level trade data.
Use the ``eq_trades`` schema (futures: ``fut_trades``) and
``ctx.subscribe_trade_ticks(...)`` — minute bars (``bars_1m``) and quotes
(``tbbo``) carry NO auction print, so auction orders would never fill on them.
See API doc §5.2.1 (auction orders) and §9.1 (schemas).

Auctions route to the symbol's primary listing exchange; when ``market_center``
is omitted they default to NASDAQ (AAPL's primary venue) — §5.2.1.

Run:  python auction_moo_moc.py   (needs HIVEQ_API_KEY)
"""
from datetime import time as dtime

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.trading_types import OrderType

MOO_OPEN, MOO_CUTOFF = dtime(8, 0), dtime(9, 28)     # ET: place MOO in [08:00, 09:28)
MOC_OPEN, MOC_CUTOFF = dtime(15, 30), dtime(15, 50)  # ET: place MOC in [15:30, 15:50)


class AuctionMooMoc:
    def __init__(self):
        self.moo_done = False
        self.moc_done = False

    def on_start(self, ctx, event):                              # subscribe in START (R3)
        # Trade ticks (eq_trades) carry the auction prints MOO/MOC fill against.
        ctx.subscribe_trade_ticks(ctx.strategy_config.symbols, asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        tick = event.data()                                      # -> SigmaTradeTick (§7.5)
        if tick.time is None:
            return
        sym = tick.symbol
        t = tick.time.time()                                     # ET wall-clock (R5) — no tz math

        # MOO and MOC are placed INDEPENDENTLY — a missing/empty pre-open window
        # must NOT suppress the closing auction (and vice versa).
        if not self.moo_done and MOO_OPEN <= t < MOO_CUTOFF:
            ctx.buy_order(sym, quantity=100, order_type=OrderType.MOO)    # fills at the 09:30 open cross
            self.moo_done = True
            ctx.add_event_log(f"MOO buy 100 {sym} placed {t:%H:%M} (fills at open)", symbol=sym)

        if not self.moc_done and MOC_OPEN <= t < MOC_CUTOFF:
            ctx.sell_order(sym, quantity=100, order_type=OrderType.MOC)   # fills at the 16:00 close cross
            self.moc_done = True
            ctx.add_event_log(f"MOC sell 100 {sym} placed {t:%H:%M} (fills at close)", symbol=sym)

    def on_order(self, ctx, event):                              # auction fills arrive here
        o = event.data()                                         # -> SigmaOrder (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"auction fill {o.symbol} {o.side} {o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="AuctionMooMoc", type="AuctionMooMoc")],
        symbols=["AAPL"],
        start_date="2025-09-19",
        end_date="2025-09-19",
        # eq_trades = tick-level trades INCLUDING the opening/closing auction prints.
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]}],
        # Pre-market (04:00) through after-hours (20:00) so the 08:00 MOO window has data.
        backtest_config=BacktestConfig(session_start="04:00", session_end="20:00")
    )
    print("status:", run.status())

    orders = run.orders()
    print("\n=== orders ===")
    print(orders.to_string() if orders is not None and not orders.empty else "(no orders)")

    elogs = run.event_logs()
    print("\n=== event logs ===")
    print(elogs.to_string() if elogs is not None and not elogs.empty else "(no event logs)")
