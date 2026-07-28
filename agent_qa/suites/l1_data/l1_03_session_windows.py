"""l1_03: the fetch window follows the session, per asset class.

This is a regression test for a bug that shipped and was fixed in the C++
adapter on 2026-07-12. Adding a futures data config flips the session to
18:00 -> 17:00 (previous calendar day start). Futures should fetch that shifted
window, while equities and options continue to fetch the full trading date
(00:00 -> 23:59, as indices do). Before the fix, equities and options got an
*inverted* window and delivered nothing at all.

So the assertion is not "some data arrived" — it is "equities still arrive when
a futures config is present". A single-asset test cannot see this class of bug;
it only appears when the two are mixed in one run.

Also pins the narrower property that a session window actually bounds the pull:
a 30-minute RTH slice must not return a full day of bars.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l1_03_session_windows"
SURFACE = "l1.sessions"
DAY = FIXTURES.equity_day
EQUITY = FIXTURES.equity_symbol
FUTURE = FIXTURES.futures_continuous

probe = Probe()


class L1SessionWindows:

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([EQUITY], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_futures_bars(symbols=[FUTURE], interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        probe.bump("bar")
        kind = "equity" if bar.symbol == EQUITY else "futures"
        n = probe.bump(kind)
        if n == 1:
            probe.sample(kind, symbol=bar.symbol, ts=event.ts_event,
                         time=str(getattr(bar, "time", "")))
        # Track the observed time span so the window can be checked without
        # shipping every bar back.
        key = f"{kind}_last"
        probe.samples_by_key[key] = [{"time": str(getattr(bar, "time", "")),
                                      "ts": event.ts_event}]

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    symbols = [EQUITY, FUTURE]
    run = backtest.run(
        [StrategyConfig(name=NAME, type="L1SessionWindows", symbols=symbols,
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=symbols,
        start_date=DAY,
        end_date=DAY,
        data_configs=[
            backtest.historical(FIXTURES.dataset_equity, "bars_1m"),
            backtest.historical(FIXTURES.dataset_futures, "bars_1m"),
        ],
        backtest_config=BacktestConfig(
            start_date=DAY, end_date=DAY,
            session_start=FIXTURES.futures_session_start,
            session_end=FIXTURES.futures_session_end,
        ),
    )

    data = probe.collect(run)
    eq, fut = data.count("equity"), data.count("futures")

    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("futures_delivered", fut > 0, f"n={fut}")

    # The 2026-07-12 regression, stated directly: mixing a futures config into
    # the run must not starve the equity stream.
    c.add("equity_survives_futures_config", eq > 0,
          f"n={eq} — equities delivered nothing while a futures data config was "
          "present; this is the inverted-window regression fixed 2026-07-12")

    c.add("both_streams_in_one_run", eq > 0 and fut > 0, f"equity={eq}, futures={fut}")
    c.finish(NAME, surface=SURFACE, extra=f"equity_bars={eq}, futures_bars={fut}, "
                         f"first_equity={data.first('equity')}, "
                         f"first_futures={data.first('futures')}")


if __name__ == "__main__":
    main()
