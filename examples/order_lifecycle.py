#!/usr/bin/env python3
"""Order lifecycle — market + limit orders, modify/cancel, states, fills in on_order.

Demonstrates (every ctx call verified against the SDK type surface):
  - placing a MARKET order (immediate fill) and a resting LIMIT order (§5.2).
  - ``modify_order`` (re-price the resting limit), ``cancel_order`` (cancel one by
    id), ``cancel_all_orders`` (cancel everything for a symbol).
  - reading order state with ``has_open_order`` / ``open_order_qty`` and tracking
    fills in ``on_order`` — fills arrive there as ``order.is_filled``, not a
    separate callback (§7.3).

Shape: place a market buy early, rest a limit sell 1% above, re-price it to the
market a few bars later (so it fills), then park a far-away order and cancel it.

Run:  python order_lifecycle.py
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.trading_types import OrderType


class OrderLifecycle:
    def __init__(self):
        self.bar = 0
        self.limit_id = None          # resting limit-sell order id
        self.cancel_id = None         # far-away order placed only to cancel

    def on_start(self, ctx, event):                              # subscribe in START (R3)
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()                                       # -> SigmaBar (§7.1)
        sym = bar.symbol
        self.bar += 1

        if self.bar == 2:                                        # MARKET buy -> fills now
            ctx.buy_order(sym, quantity=50)
            ctx.add_event_log(f"market buy 50 {sym} @ ~{bar.close:.2f}", symbol=sym)

        elif self.bar == 12 and ctx.is_net_long(sym):            # rest a LIMIT sell 1% up
            o = ctx.sell_order(sym, quantity=25,
                               order_type=OrderType.LIMIT, limit_price=bar.close * 1.01)
            self.limit_id = o.order_id if o else None
            ctx.add_event_log(f"limit sell 25 {sym} @ {bar.close * 1.01:.2f}", symbol=sym)

        elif self.bar == 22 and self.limit_id and ctx.has_open_order(sym):
            ctx.modify_order(self.limit_id, limit_price=bar.close)  # re-price to mkt -> fills
            ctx.add_event_log(f"modify {self.limit_id} -> {bar.close:.2f}", symbol=sym)

        elif self.bar == 37 and ctx.is_net_long(sym):            # park a far order to cancel
            o = ctx.sell_order(sym, quantity=10,
                               order_type=OrderType.LIMIT, limit_price=bar.close * 1.10)
            self.cancel_id = o.order_id if o else None

        elif self.bar == 40 and self.cancel_id:                  # cancel that one by id
            ctx.cancel_order(self.cancel_id)
            ctx.add_event_log(f"cancel_order {self.cancel_id}", symbol=sym)
            ctx.cancel_all_orders(sym)                           # ...and any other open orders

    def on_order(self, ctx, event):                              # fills + state changes here
        o = event.data()                                         # -> SigmaOrder (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"fill {o.symbol} {o.side} {o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="OrderLifecycle", type="OrderLifecycle")],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-01",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
