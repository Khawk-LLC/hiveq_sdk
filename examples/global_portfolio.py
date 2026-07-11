#!/usr/bin/env python3
"""Global portfolio accessors — strategy-scoped vs account-wide position views.

Demonstrates (every ctx call verified against the SDK type surface):
  - ``ctx.portfolio()`` -> ``SigmaPortfolio``: YOUR strategy's positions/PnL only.
  - ``ctx.global_portfolio()`` -> ``SigmaGlobalPortfolio``: the account-wide
    aggregate across ALL strategies running in the same run (§8, R9).
  - run two strategies on overlapping symbols so the same name accumulates a
    bigger position at the account level than either strategy sees alone.
  - per-event callbacks (``on_start``/``on_bar``/``on_order``) — the canonical
    contract; fills arrive in ``on_order`` (there is no ``on_order_filled``).

Shape: StrategyA buys AAPL on any up-bar; StrategyB buys AAPL (smaller clip) on
a 0.2% up-move. Each logs its own net_position/net_exposure next to the account
total so you can watch them diverge (§7.2, §8 portfolio API).

Run:  python global_portfolio.py
"""
import hiveq.flow as hf
from hiveq.flow import StrategyConfig
from hiveq.flow.config import AssetType


class StrategyA:
    """Buys AAPL whenever the current bar closes above its open."""

    def on_start(self, ctx, event):
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()                                   # -> SigmaBar (§7.1)
        mine = ctx.portfolio()                               # strategy-scoped (§8)
        if bar.close > bar.open and mine.is_flat("AAPL"):
            ctx.buy_order("AAPL", quantity=100)

    def on_order(self, ctx, event):
        o = event.data()                                     # -> SigmaOrder (§7.3)
        if not o.is_filled:
            return
        mine, acct = ctx.portfolio(), ctx.global_portfolio()
        ctx.add_event_log("StrategyA filled", symbol="AAPL", state_variable={
            "my_position": mine.net_position("AAPL"),
            "account_position": acct.net_position("AAPL"),
            "my_exposure": mine.net_exposure(),
            "account_exposure": acct.net_exposure(),
        })


class StrategyB:
    """Buys a smaller AAPL clip only on a sharper 0.2% up-move."""

    def on_start(self, ctx, event):
        ctx.subscribe_bars(["AAPL", "MSFT"], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        mine = ctx.portfolio()
        if bar.symbol == "AAPL" and bar.close > bar.open * 1.002 and mine.is_flat("AAPL"):
            ctx.buy_order("AAPL", quantity=50)

    def on_order(self, ctx, event):
        o = event.data()
        if not o.is_filled:
            return
        mine, acct = ctx.portfolio(), ctx.global_portfolio()
        ctx.add_event_log("StrategyB filled", symbol="AAPL", state_variable={
            "my_position": mine.net_position("AAPL"),
            "account_position": acct.net_position("AAPL"),   # >= my_position
            "my_exposure": mine.net_exposure(),
            "account_exposure": acct.net_exposure(),
        })


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(name="StrategyA", type="StrategyA", symbols=["AAPL"]),
            StrategyConfig(name="StrategyB", type="StrategyB", symbols=["AAPL", "MSFT"]),
        ],
        symbols=["AAPL", "MSFT"],
        start_date="2025-08-01",
        end_date="2025-08-08",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1m"]}],
    )
    run.wait()  # deploy returns immediately; block (progress bar) until done
    print("status:", run.status())
    logs = run.event_logs()
    if not logs.empty:
        print(logs.to_string())
