"""l4_02: the requested bar interval is actually honoured.

``l4_01`` claimed to cover multi-interval subscriptions but did not: its check was
``bars > 10``, which 1-minute bars alone satisfy ~200x over, and the payload
carries only ``EventType.BAR`` with no ``BAR_1_DAY`` specialisation, so 1m and 1d
bars are indistinguishable once delivered. A green check there proved nothing
about intervals.

This proves it by **cardinality**, which needs no way to tell one bar from
another: over the same multi-session window, the bar COUNT is determined by the
interval.

    interval="1d"  over N sessions  ->  ~N bars
    interval="1m"  over N sessions  ->  ~N * 390 bars   (regular equity session)

Two runs, each subscribing exactly one interval, then compared. If a ``1d``
subscription returns ~1950 bars it is being silently served minute data — which
is the defect worth catching, and it is invisible to any single-run test.

The ratio assertion is the real one: minute bars must outnumber daily bars by
roughly two orders of magnitude. Absolute counts are checked with wide bands so a
half-day or holiday inside the window does not fail the run.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l4_02_interval_honoured"
SURFACE = "l4.bars"
START = FIXTURES.equity_week_start
END = FIXTURES.equity_week_end
SYMBOL = FIXTURES.equity_symbol

#: 2025-06-02..06-06 is Mon-Fri: five regular sessions.
SESSIONS = 5
#: 09:30-16:00 inclusive of the closing print.
MINUTES_PER_SESSION = 390

daily_probe = Probe()
minute_probe = Probe()


class L4DailyOnly:
    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        daily_probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1d")

    def on_bar(self, ctx, event):
        bar = event.data()
        n = daily_probe.bump("bar")
        if n <= 8:
            daily_probe.sample("bar", symbol=bar.symbol, close=bar.close,
                               ts=event.ts_event, time=str(getattr(bar, "time", "")))

    def on_stop(self, ctx, event):
        daily_probe.flush(ctx)


class L4MinuteOnly:
    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        minute_probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        n = minute_probe.bump("bar")
        if n <= 3:
            minute_probe.sample("bar", symbol=bar.symbol, close=bar.close,
                                ts=event.ts_event, time=str(getattr(bar, "time", "")))

    def on_stop(self, ctx, event):
        minute_probe.flush(ctx)


def _run(cls_name: str, schema: str):
    from hiveq.flow import BacktestConfig, StrategyConfig

    return backtest.run(
        [StrategyConfig(name=f"{NAME}_{schema}", type=cls_name, symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=START,
        end_date=END,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, schema)],
        backtest_config=BacktestConfig(start_date=START, end_date=END,
                                       session_start=FIXTURES.equity_session_start,
                                       session_end=FIXTURES.equity_session_end),
    )


def main():
    install_crash_handler(NAME, SURFACE)

    daily_run = _run("L4DailyOnly", "bars_1d")
    minute_run = _run("L4MinuteOnly", "bars_1m")

    d = daily_probe.collect(daily_run)
    m = minute_probe.collect(minute_run)
    n_daily, n_minute = d.count("bar"), m.count("bar")
    expected_minute = SESSIONS * MINUTES_PER_SESSION

    c = Checks()
    c.note(f"daily evidence via {d.source}, minute via {m.source}; "
           f"window {START}..{END} ({SESSIONS} sessions)")
    c.add("daily_run_completed", backtest.completed_ok(daily_run),
          f"status={backtest.status_of(daily_run)}")
    c.add("minute_run_completed", backtest.completed_ok(minute_run),
          f"status={backtest.status_of(minute_run)}")

    c.add("daily_bars_delivered", n_daily > 0, f"n={n_daily}")
    c.add("minute_bars_delivered", n_minute > 0, f"n={n_minute}")

    # One bar per session, give or take a holiday/half-day.
    c.add("daily_count_matches_sessions", SESSIONS - 2 <= n_daily <= SESSIONS + 2,
          f"interval='1d' produced {n_daily} bars over {SESSIONS} sessions, "
          f"expected ~{SESSIONS}",
          requires=n_daily > 0, requires_detail="no daily bars to count")

    c.add("minute_count_matches_session_minutes",
          expected_minute * 0.85 <= n_minute <= expected_minute * 1.15,
          f"interval='1m' produced {n_minute} bars, expected ~{expected_minute} "
          f"({SESSIONS}x{MINUTES_PER_SESSION})",
          requires=n_minute > 0, requires_detail="no minute bars to count")

    # The load-bearing assertion: the two intervals must differ by ~2 orders of
    # magnitude. If they are close, one subscription is being served the other's
    # granularity and neither absolute band would necessarily catch it.
    ratio = (n_minute / n_daily) if n_daily else 0
    c.add("interval_changes_cardinality", ratio >= 50,
          f"minute/daily bar ratio is {ratio:.1f} (n_minute={n_minute}, "
          f"n_daily={n_daily}); expected ~{MINUTES_PER_SESSION}. A ratio near 1 "
          f"means the interval argument is being ignored",
          requires=n_daily > 0 and n_minute > 0,
          requires_detail="one of the two runs delivered nothing, so the "
                          "intervals cannot be compared")

    c.finish(NAME, surface=SURFACE, extra=(
        f"daily={n_daily}, minute={n_minute}, ratio={ratio:.1f}, "
        f"first_daily={d.first('bar')}, daily_samples={len(d.samples('bar'))}"
    ))


if __name__ == "__main__":
    main()
