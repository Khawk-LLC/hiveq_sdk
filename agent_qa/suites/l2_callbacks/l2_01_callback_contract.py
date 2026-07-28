"""l2_01: callback firing order and the Event object's contract.

Widens ``hiveq-flow/qa_validation/t04_callbacks_contract.py`` and moves it onto
``Probe``, so the same assertions hold whether the callbacks ran in this process
or in the platform executor.

What is asserted (§0/R4, R5; §4; §6):

* ``on_start`` fires, and fires *before* any data callback.
* ``event.type`` is the documented ``EventType`` member for each callback.
* ``ts_event`` is int **nanoseconds** (R5), not seconds or milliseconds — the
  single most common payload misreading.
* ``ctx.now()`` returns a datetime and ``ctx.trading_day`` is the session date.
* ``on_stop`` fires, and fires last.
* Data is pulled by SESSION window, not the whole calendar day: a 09:30-10:30
  window on one equity yields ~61 one-minute bars, not ~950.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l2_01_callback_contract"
SURFACE = "l2.callbacks"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol

probe = Probe()


class L2CallbackContract:
    """Records the shape of every callback it receives; trades nothing."""

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType, EventType

        probe.bump("start")
        # "first" is written once and never overwritten, so it records which
        # callback the engine actually delivered first.
        if probe.counters.get("bar", 0) == 0:
            probe.bump("start_before_any_bar")
        if event.type != EventType.START:
            probe.error(f"on_start event.type={event.type!r}")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        from datetime import datetime

        from hiveq.flow.config import EventType

        n = probe.bump("bar")
        if n > 1:
            return  # contract only needs inspecting once; keep the run cheap

        if event.type != EventType.BAR:
            probe.error(f"on_bar event.type={event.type!r}")

        # R5: nanoseconds. 1e18 ns ~= 2001-09-09, so anything smaller is a unit
        # error (seconds or millis) rather than a genuinely old timestamp.
        ts = event.ts_event
        if not isinstance(ts, (int, float)) or ts < 1e18:
            probe.error(f"ts_event not nanoseconds: {ts!r}")

        now = ctx.now()
        if not isinstance(now, datetime):
            probe.error(f"ctx.now() is {type(now).__name__}, expected datetime")
        if str(ctx.trading_day) != DAY:
            probe.error(f"ctx.trading_day={ctx.trading_day!r}, expected {DAY}")

        bar = event.data()
        if not hasattr(bar, "time"):
            probe.error("bar payload has no .time")
        if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high):
            probe.error(f"OHLC inconsistent: o={bar.open} h={bar.high} "
                        f"l={bar.low} c={bar.close}")

        probe.sample("bar", symbol=bar.symbol, close=bar.close, ts_event=ts,
                     trading_day=str(ctx.trading_day))

    def on_stop(self, ctx, event):
        from hiveq.flow.config import EventType

        probe.bump("stop")
        if event.type != EventType.STOP:
            probe.error(f"on_stop event.type={event.type!r}")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L2CallbackContract", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:30"),
    )

    data = probe.collect(run)
    bars = data.count("bar")

    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]))
    c.add("on_start_fired", data.count("start") >= 1, f"n={data.count('start')}")
    c.add("on_start_before_data", data.count("start_before_any_bar") >= 1,
          "a bar arrived before the first on_start")
    c.add("on_bar_fired", bars > 0, f"n={bars}")
    # A 09:30-10:30 window is 61 one-minute bars inclusive; allow for a missing
    # opening or closing print rather than pinning an exact count.
    c.add("session_scoped_data_pull", 55 <= bars <= 65,
          f"n={bars}; expected ~61 for a 09:30-10:30 window, not a full day")
    c.add("on_stop_fired", data.count("stop") >= 1, f"n={data.count('stop')}")
    # Gated: an empty error list proves the contract held only if a bar was
    # actually inspected. On a zero-bar run this check was passing while every
    # other check failed — a false green.
    c.add("event_contract_clean", not data.errors, f"errors={data.errors[:3]}",
          requires=bars > 0, requires_detail="no bar was inspected")
    c.finish(NAME, surface=SURFACE, extra=f"bars={bars}, first={data.first('bar')}")


if __name__ == "__main__":
    main()
