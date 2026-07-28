"""l2_02: one strategy, every asset class, each into its own callback.

Ports and widens ``qa_validation/t03_all_asset_types.py``. The question is not
"does equity data work" — l1 answers that — but whether a *single* strategy can
hold subscriptions across equities, futures, indices and options simultaneously
and have the engine route each to the correct handler.

This is the routing test. Cross-wiring (an index price arriving in ``on_bar``, a
snap arriving as a trade) is invisible in any single-asset run and corrupts
strategies in ways that look like data errors.

Per §4 the mapping asserted here is:

    on_bar          <- EventType.BAR          (equities, futures)
    on_trade        <- EventType.TRADE
    on_index_price  <- EventType.INDEX_PRICE  (indices)
    on_snap         <- EventType.SNAP         (option snapshots)

Asset classes with no rows on the probed date are reported as a data gap; the
routing assertion still holds for whatever did arrive.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l2_02_all_asset_types"
SURFACE = "l2.callbacks"
DAY = FIXTURES.futures_day
EQUITY = FIXTURES.equity_symbol
FUTURE = FIXTURES.futures_continuous
INDEX = FIXTURES.index_symbol
OPTION_ROOT = FIXTURES.option_underlying

probe = Probe()


class L2AllAssetTypes:

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([EQUITY], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_trades([EQUITY], asset_type=AssetType.EQUITY)
        ctx.subscribe_futures_bars(symbols=[FUTURE], interval="1m")
        ctx.subscribe_bars([INDEX], asset_type=AssetType.INDEX, interval="1m")
        ctx.subscribe_bars([OPTION_ROOT], asset_type=AssetType.OPTIONS, interval="1m")

    def on_bar(self, ctx, event):
        from hiveq.flow.config import EventType

        bar = event.data()
        probe.bump("bar")
        if event.type not in (EventType.BAR, EventType.BAR_1_MIN, EventType.BAR_1_DAY):
            probe.error(f"on_bar received event.type={event.type!r}")
        bucket = "bar:equity" if bar.symbol == EQUITY else "bar:other"
        probe.bump(bucket)
        if probe.counters[bucket] == 1:
            probe.sample(bucket, symbol=bar.symbol, close=bar.close)

    def on_trade(self, ctx, event):
        from hiveq.flow.config import EventType

        probe.bump("trade")
        if event.type != EventType.TRADE:
            probe.error(f"on_trade received event.type={event.type!r}")
        if probe.counters["trade"] == 1:
            tick = event.data()
            probe.sample("trade", symbol=tick.symbol, price=tick.price)

    def on_index_price(self, ctx, event):
        from hiveq.flow.config import EventType

        probe.bump("index")
        if event.type != EventType.INDEX_PRICE:
            probe.error(f"on_index_price received event.type={event.type!r}")
        if probe.counters["index"] == 1:
            px = event.data()
            probe.sample("index", symbol=getattr(px, "symbol", None),
                         price=getattr(px, "price", None))

    def on_snap(self, ctx, event):
        from hiveq.flow.config import EventType

        probe.bump("snap")
        if event.type != EventType.SNAP:
            probe.error(f"on_snap received event.type={event.type!r}")
        if probe.counters["snap"] == 1:
            snap = event.data()
            probe.sample("snap", symbol=getattr(snap, "symbol", None))

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    symbols = [EQUITY, FUTURE, INDEX, OPTION_ROOT]
    run = backtest.run(
        [StrategyConfig(name=NAME, type="L2AllAssetTypes", symbols=symbols,
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=symbols,
        start_date=DAY,
        end_date=DAY,
        data_configs=[
            backtest.historical(FIXTURES.dataset_equity, ["bars_1m", "eq_trades"]),
            backtest.historical(FIXTURES.dataset_futures, "bars_1m"),
        ],
        backtest_config=BacktestConfig(
            start_date=DAY, end_date=DAY,
            session_start=FIXTURES.futures_session_start,
            session_end=FIXTURES.futures_session_end,
        ),
    )

    data = probe.collect(run)
    streams = {
        "bars": data.count("bar"),
        "trades": data.count("trade"),
        "index": data.count("index"),
        "snaps": data.count("snap"),
    }
    live = [k for k, n in streams.items() if n > 0]
    empty = [k for k, n in streams.items() if n == 0]

    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]))
    c.add("multi_asset_run_delivers", bool(live), f"every stream was empty: {streams}")

    # The actual routing assertion: nothing arrived at the wrong handler.
    # Routing can only be shown correct if something was routed.
    c.add("no_cross_wired_callbacks", not data.errors, f"errors={data.errors[:3]}",
          requires=bool(live), requires_detail="no stream delivered, nothing was routed")

    c.finish(
        NAME,
        surface=SURFACE,
        gap=bool(empty) and bool(live),
        extra=f"{streams}" + (f"; no rows for {empty} on {DAY}" if empty else ""),
    )


if __name__ == "__main__":
    main()
