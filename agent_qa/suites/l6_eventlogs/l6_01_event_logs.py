"""l6_01: structured event logging and the result accessors.

``ctx.add_event_log`` is the platform's durable, queryable observability channel
— the one thing that keeps working at the default ``WARNING`` level (R10), which
is why this whole suite's ``Probe`` is built on it. So it is worth testing
directly rather than trusting transitively.

Two behaviours, and they differ by engine on purpose:

* ``ctx.add_event_log(...)`` must never raise, in either engine.
* ``run.event_logs()`` returns rows on a **remote** run and, by explicit design,
  an **empty DataFrame** on a local one (``runs.py``: ``if self.is_local: return
  pd.DataFrame()``). This test asserts the documented behaviour in each case
  rather than treating the local emptiness as a bug — which is exactly the
  distinction ``qa_validation/t09`` recorded as a known gap.

Also exercises ``ctx.log_parameter_change``, the typed helper that produces a
``PARAM_CHANGE`` row — the same row type l9 looks for after a livesim param
hot-update.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES, detect_engine
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l6_01_event_logs"
SURFACE = "l6.eventlogs"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol
MARKER = "QA_L6_MARKER"

probe = Probe()


class L6EventLogs:

    def __init__(self):
        self._logged = False

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        probe.bump("bar")
        if self._logged:
            return
        self._logged = True

        # Plain message.
        try:
            ctx.add_event_log(f"{MARKER} plain message")
            probe.bump("add_event_log_plain")
        except Exception as exc:  # noqa: BLE001
            probe.error(f"add_event_log(message) raised: {exc}")

        # Full form: sub_event_type + symbol + structured state.
        try:
            ctx.add_event_log(
                f"{MARKER} structured",
                sub_event_type=MARKER,
                symbol=bar.symbol,
                state_variable={"close": float(bar.close), "phase": "l6"},
            )
            probe.bump("add_event_log_structured")
        except Exception as exc:  # noqa: BLE001
            probe.error(f"add_event_log(full) raised: {exc}")

        # Typed parameter-change helper -> EventLogType.PARAM_CHANGE.
        try:
            ctx.log_parameter_change("qa_threshold", 1.0, 2.5, symbol=bar.symbol)
            probe.bump("log_parameter_change")
        except Exception as exc:  # noqa: BLE001
            probe.error(f"log_parameter_change raised: {exc}")

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L6EventLogs", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:00"),
    )

    data = probe.collect(run)
    engine = detect_engine()

    c = Checks()
    c.note(f"evidence via {data.source}, engine={engine}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]))
    c.add("add_event_log_plain_ok", data.count("add_event_log_plain") > 0)
    c.add("add_event_log_structured_ok", data.count("add_event_log_structured") > 0)
    c.add("log_parameter_change_ok", data.count("log_parameter_change") > 0)
    c.add("no_logging_exceptions", not data.errors, f"errors={data.errors[:3]}")

    # The engine-dependent half.
    try:
        df = run.event_logs()
        reachable = True
    except Exception as exc:  # noqa: BLE001
        df, reachable = None, False
        c.note(f"event_logs() raised: {exc}")

    c.add("event_logs_accessor_callable", reachable)

    if engine == "inproc":
        # Documented: local runs serve everything from an in-process report and
        # never touch the REST API, so this is empty by design, not by failure.
        empty = df is None or getattr(df, "empty", True)
        c.add("local_event_logs_empty_by_design", empty,
              f"expected an empty DataFrame on a local run, got {len(df)} rows")
        c.note("run.event_logs() is empty for local runs by design (runs.py); "
               "event logs are persisted only for platform runs")
    else:
        n = 0 if df is None else len(df)
        c.add("remote_event_logs_populated", n > 0,
              f"n={n} — a remote run wrote no event-log rows")
        if n:
            text = df.to_string()
            c.add("marker_row_present", MARKER in text,
                  "the rows written by this strategy are not in event_logs()")

    c.finish(NAME, surface=SURFACE, extra=f"bars={data.count('bar')}")


if __name__ == "__main__":
    main()
