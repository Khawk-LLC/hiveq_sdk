"""l3_01: timers fire on cadence, carry their id, and can be cancelled.

Timers are the first non-data event source in the ladder, so this is where
"the engine can generate events on its own clock" gets established.

Asserted (§5.8, §7.8):

* ``set_timer(id, interval)`` produces ``EventType.TIMER`` events.
* The payload carries the ``timer_id`` that was registered — with two timers
  running, events must be attributable to the right one.
* Two timers at different intervals fire in the expected *ratio*, which is the
  real cadence check; an absolute count depends on session length.
* ``cancel_timer`` stops delivery — after cancelling, the count freezes.

Registered in ``on_start``. Note ``on_start`` fires once per **calendar** day
(§4), so re-registration must be idempotent; a ``self._armed`` guard is the
documented pattern and is used here.
"""

import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l3_01_timer_cadence"
SURFACE = "l3.timers"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol

FAST_ID = "qa_fast"
SLOW_ID = "qa_slow"
CANCEL_ID = "qa_cancel"
#: Cancel after this many fast ticks, then assert the cancelled timer is frozen.
CANCEL_AFTER = 3

probe = Probe()


class L3TimerCadence:

    def __init__(self):
        self._armed = False
        self._cancelled = False

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

        # on_start re-runs every calendar day; arming twice would double-register.
        if self._armed:
            return
        self._armed = True
        ctx.set_timer(FAST_ID, timedelta(minutes=1))
        ctx.set_timer(SLOW_ID, timedelta(minutes=5))
        ctx.set_timer(CANCEL_ID, timedelta(minutes=1))

    def on_bar(self, ctx, event):
        probe.bump("bar")

    def on_timer(self, ctx, event):
        from hiveq.flow.config import EventType

        probe.bump("timer")
        if event.type != EventType.TIMER:
            probe.error(f"on_timer event.type={event.type!r}")

        payload = event.data()
        timer_id = getattr(payload, "timer_id", None)
        if not timer_id:
            probe.error("timer payload has no timer_id")
            return

        probe.bump(f"timer:{timer_id}")
        if probe.counters[f"timer:{timer_id}"] == 1:
            probe.sample(f"timer:{timer_id}", timer_id=timer_id, ts=event.ts_event)

        if timer_id not in (FAST_ID, SLOW_ID, CANCEL_ID):
            probe.error(f"unregistered timer_id fired: {timer_id!r}")

        # Cancel one timer mid-run, then record how many events arrive after.
        if self._cancelled and timer_id == CANCEL_ID:
            probe.bump("fired_after_cancel")

        if (not self._cancelled
                and timer_id == FAST_ID
                and probe.counters[f"timer:{FAST_ID}"] >= CANCEL_AFTER):
            ctx.cancel_timer(CANCEL_ID)
            self._cancelled = True
            probe.bump("cancel_issued")
            probe.sample("cancel", at_fast_tick=probe.counters[f"timer:{FAST_ID}"],
                         cancel_timer_count=probe.counters.get(f"timer:{CANCEL_ID}", 0))

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L3TimerCadence", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="11:30"),
    )

    data = probe.collect(run)
    fast = data.count(f"timer:{FAST_ID}")
    slow = data.count(f"timer:{SLOW_ID}")
    after_cancel = data.count("fired_after_cancel")

    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("timers_fired", data.count("timer") > 0, f"n={data.count('timer')}")
    c.add("timer_id_delivered", fast > 0, f"no events carried timer_id={FAST_ID!r}")
    c.add("multiple_timers_distinguished", fast > 0 and slow > 0,
          f"fast={fast}, slow={slow}")

    # A 1m and a 5m timer over the same window should differ by roughly 5x.
    # Allow a wide band: the point is that the intervals are honoured, not that
    # the engine's clock is exact at the boundaries.
    if fast and slow:
        ratio = fast / slow
        c.add("cadence_ratio_plausible", 3.0 <= ratio <= 8.0,
              f"fast/slow={ratio:.1f}, expected ~5")

    c.add("cancel_issued", data.count("cancel_issued") > 0,
          "the run never reached the cancel point; window may be too short")
    # Both gated on the cancelled timer having actually fired BEFORE the
    # cancel: otherwise "nothing arrived after cancel" is true because nothing
    # ever arrived, which says nothing about cancel_timer.
    fired_before_cancel = data.count(f"timer:{CANCEL_ID}") > 0
    c.add("cancel_stops_delivery", after_cancel == 0,
          f"{after_cancel} events arrived on {CANCEL_ID!r} after cancel_timer",
          requires=data.count("cancel_issued") > 0 and fired_before_cancel,
          requires_detail=f"{CANCEL_ID} never fired before the cancel "
                          f"(fired={data.count(f'timer:{CANCEL_ID}')}), so cancel "
                          "had nothing to stop")
    c.add("no_timer_errors", not data.errors, f"errors={data.errors[:3]}",
          requires=data.count("timer") > 0, requires_detail="no timer fired")

    c.finish(NAME, surface=SURFACE, extra=f"fast={fast}, slow={slow}, "
                         f"cancelled={data.count(f'timer:{CANCEL_ID}')}, "
                         f"after_cancel={after_cancel}, bars={data.count('bar')}")


if __name__ == "__main__":
    main()
