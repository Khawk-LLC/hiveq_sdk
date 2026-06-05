#!/usr/bin/env python3
"""Executor-driven entry (POV) — slice a sizeable order at a % of volume.

When the strategy needs managed execution (large/sliced orders, live order-chasing,
auction routing) let an EXECUTOR work the order instead of hand-managing
``buy_order``/``sell_order`` + replaces/cancels (§5.10, §16.6). For a one-shot
market order this is overkill — use a direct order there.

Driven off TRADE ticks (``eq_trades`` schema, ``on_trade``) — a POV executor
participates in live trade flow, so it needs trade data, not bars.

Idiom (verified against the SDK surface):
  - hold ONE executor handle per (symbol, role);
  - check ``ctx.executor_state(handle)`` before starting another;
  - re-target IN PLACE with ``replace_executor_params_by_id`` — never stack a
    second executor on the same target.

Run:  python example.py   (needs HIVEQ_API_KEY)
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType

TARGET_QTY = 1000
# executor_state strings that mean "no longer working this target" (§5.10).
DONE = ("FILLED", "STOPPED", "STOPPING", "INVALID", "UNDEFINED")


class PovEntry:
    def __init__(self):
        self.entry = {}                                      # symbol -> executor handle

    def on_start(self, ctx, event):
        # eq_trades gives the POV executor live tra
        # de flow to work against.
        ctx.subscribe_trades(ctx.strategy_config.symbols, asset_type=AssetType.EQUITY)

    def on_trade(self, ctx, event):
        t = event.data()                                     # -> SigmaTradeTick (§7.5)
        sym = t.symbol
        ex = self.entry.get(sym)
        working = ex is not None and ctx.executor_state(ex) not in DONE
        # No signal gate: start a POV BUY once per symbol, then let the executor work.
        if ctx.is_flat(sym) and not working:
            params = ctx.build_executor_params(
                symbol=sym, quantity=TARGET_QTY, side="BUY",
                executor_type="POV", participate_pct=10,     # work at 10% of traded volume
            )
            self.entry[sym] = ctx.add_executor(params)
            ctx.add_event_log(f"start POV BUY {TARGET_QTY} {sym}", symbol=sym)

    def on_executor(self, ctx, event):
        # EXECUTOR_EVENT lifecycle updates (§7.13): state transitions, partial fills,
        # completion. The payload is opaque; track progress via executor_state above.
        pass

    def on_order(self, ctx, event):
        o = event.data()                                     # child fills from the executor (§7.3)
        if o.is_filled:
            ctx.add_event_log(f"child fill {o.symbol} qty={o.filled_qty} @ {o.avg_px}",
                              symbol=o.symbol)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="PovEntry", type="PovEntry")],
        symbols=["AAPL"],
        start_date="2025-08-01",
        end_date="2025-08-01",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]}],
    )
    print("status:", run.status())
    rs = getattr(run.report(), "return_stats", None)
    if rs is not None and not rs.empty:
        print(rs.to_string())
