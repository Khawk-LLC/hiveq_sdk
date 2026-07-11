#!/usr/bin/env python3
"""Bracket: stop-loss + take-profit with OCO emulation, plus a trailing stop.

The engine has NO native bracket/OCO order and NO trailing-stop order type (§16.4).
The idiom shown here:
  1. place the entry (market);
  2. on its fill (``on_order``), attach protective STOP + LIMIT child orders;
  3. when one leg fills the position goes flat -> cancel the sibling yourself to
     emulate OCO (done in ``on_position``);
  4. trail the stop up in ``on_bar`` via ``modify_order`` against a high-water mark.

Run:  python bracket_stop_take_profit.py
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.trading_types import OrderType
from hiveq.flow.trading.price_utils import adjust_tick_size

STOP_PCT, TAKE_PCT, TRAIL_PCT = 0.02, 0.04, 0.015


class Bracket:
    def __init__(self):
        self.entered = set()                                 # symbols we've entered
        self.stop_id = {}                                    # symbol -> live stop order_id
        self.hwm = {}                                        # symbol -> high-water mark since entry

    def on_start(self, ctx, event):
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                            asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        # Toy entry trigger: first bar we see for a symbol, while flat.
        if bar.symbol not in self.entered and ctx.is_flat(bar.symbol):
            self.entered.add(bar.symbol)
            ctx.buy_order(bar.symbol, quantity=100)          # market entry; protect on fill
            return
        # Trail the stop up as price makes new highs (emulated trailing stop).
        if ctx.is_net_long(bar.symbol) and bar.symbol in self.stop_id:
            if bar.close > self.hwm.get(bar.symbol, bar.close):
                self.hwm[bar.symbol] = bar.close
                ctx.modify_order(self.stop_id[bar.symbol],
                                 stop_price=adjust_tick_size(bar.symbol, bar.close * (1 - TRAIL_PCT)))

    def on_order(self, ctx, event):
        o = event.data()                                     # -> SigmaOrder (§7.3)
        if not o.is_filled:
            return
        # Entry fill -> attach the protective children (the "bracket").
        if o.symbol in self.entered and o.symbol not in self.stop_id and ctx.is_net_long(o.symbol):
            entry = o.avg_px
            self.hwm[o.symbol] = entry
            stop_px = adjust_tick_size(o.symbol, entry * (1 - STOP_PCT))
            take_px = adjust_tick_size(o.symbol, entry * (1 + TAKE_PCT))
            stop = ctx.sell_order(o.symbol, quantity=o.filled_qty,
                                  order_type=OrderType.STOP, stop_price=stop_px)
            ctx.sell_order(o.symbol, quantity=o.filled_qty,
                           order_type=OrderType.LIMIT, limit_price=take_px)
            if stop is not None:
                self.stop_id[o.symbol] = stop.order_id
            ctx.add_event_log(
                f"bracket {o.symbol}: stop@{stop_px:.2f} target@{take_px:.2f}",
                symbol=o.symbol)

    def on_position(self, ctx, event):
        # One protective leg filled -> position flat -> cancel the sibling (OCO).
        pos = event.data()                                   # -> SigmaPosition (§7.2)
        if pos.symbol in self.stop_id and ctx.is_flat(pos.symbol):
            ctx.cancel_all_orders(pos.symbol)
            self.stop_id.pop(pos.symbol, None)
            self.hwm.pop(pos.symbol, None)
            self.entered.discard(pos.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="Bracket", type="Bracket")],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-05",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
