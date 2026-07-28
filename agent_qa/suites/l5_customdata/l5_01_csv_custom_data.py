"""l5_01: CSV custom data reaches ``on_custom_data`` with readable columns.

The user-data path, which is how strategies consume their own signals. Per §9.2
the engine requires three columns by header name (case-insensitive, any order):
``date`` (YYYY-MM-DD), ``time`` (HH:MM:SS) and ``sym``. Everything else is
user-defined and arrives as a **string**, read via ``column_data(name)``.

Asserted here:

* Rows fire ``EventType.CUSTOM_DATA`` into ``on_custom_data``.
* ``date`` + ``time`` determine *when* the row fires — rows must arrive in
  timestamp order and inside the session window, not all at once at startup.
* ``sym`` is exposed both as ``data.symbol`` and ``column_data("sym")``.
* Arbitrary columns round-trip as strings, including the pipe-escape convention
  (the engine splits on commas, so a value containing a comma is written with
  ``|`` and decoded in strategy code).
* ``column_data`` honours its ``default`` for an absent column instead of
  raising — strategies rely on this for optional fields.

The CSV lives in ``agent_qa/fixtures/`` and is referenced by relative path, which
also exercises the source bundler's "adjacent config files" capture on remote
runs.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l5_01_csv_custom_data"
SURFACE = "l5.customdata"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol
DATA_ID = "qa_signals"

_HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(_HERE, "..", "..", "fixtures", "qa_signals.csv")

#: Rows in the fixture, so the test knows what complete delivery looks like.
EXPECTED_ROWS = 5

probe = Probe()


class L5CsvCustomData:

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(data_id=DATA_ID)

    def on_bar(self, ctx, event):
        probe.bump("bar")

    def on_custom_data(self, ctx, event):
        from hiveq.flow.config import EventType

        n = probe.bump("custom")
        if event.type != EventType.CUSTOM_DATA:
            probe.error(f"on_custom_data event.type={event.type!r}")

        data = event.data()

        sym_attr = getattr(data, "symbol", None)
        sym_col = data.column_data("sym")
        if sym_attr and sym_col and str(sym_attr) != str(sym_col):
            probe.error(f"data.symbol={sym_attr!r} != column_data('sym')={sym_col!r}")
        if not sym_col:
            probe.error("column_data('sym') empty — mandatory column not exposed")

        # Values are strings; a strategy is expected to cast.
        raw_prob = data.column_data("zone_prob", default="")
        if raw_prob and not isinstance(raw_prob, str):
            probe.error(f"column_data returned {type(raw_prob).__name__}, expected str")

        # Absent column must fall back rather than raise.
        sentinel = data.column_data("definitely_not_a_column", default="__default__")
        if sentinel != "__default__":
            probe.error(f"column_data default ignored, got {sentinel!r}")

        # Rows must arrive in timestamp order.
        ts = event.ts_event
        prev = probe.counters.get("_last_ts", 0)
        if prev and ts < prev:
            probe.error(f"custom rows out of order: {ts} after {prev}")
        probe.counters["_last_ts"] = ts

        # Pipe-escape convention for embedded commas.
        note = data.column_data("note", default="")
        if "|" in note:
            probe.bump("pipe_escaped_value")

        probe.sample("custom", n=n, symbol=str(sym_col), signal=data.column_data("signal"),
                     zone_prob=raw_prob, label=data.column_data("label"),
                     ts=ts, now=str(ctx.now()))

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    if not os.path.exists(CSV_PATH):
        from agent_qa.core.result import finish
        finish(NAME, {"fixture_present": False}, surface=SURFACE,
               extra=f"missing CSV fixture at {CSV_PATH}")
        return

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L5CsvCustomData", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[
            backtest.historical(FIXTURES.dataset_equity, "bars_1m"),
            # stage_csv uploads to the persistent-data store on remote runs and
            # returns the store-relative path; in-process it just returns the
            # local path. An absolute path here silently delivers zero rows
            # remotely (§9.2).
            backtest.csv_source(DATA_ID, backtest.stage_csv(CSV_PATH), data_type="custom"),
        ],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="11:00"),
    )

    data = probe.collect(run)
    rows = data.count("custom")

    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]))
    c.add("custom_rows_delivered", rows > 0, f"n={rows}")
    c.add("all_fixture_rows_delivered", rows == EXPECTED_ROWS,
          f"got {rows}, fixture has {EXPECTED_ROWS} rows in the 09:30-11:00 window")
    c.add("column_contract_clean", not data.errors, f"errors={data.errors[:3]}")
    c.add("pipe_escape_preserved", data.count("pipe_escaped_value") > 0,
          "the row containing a pipe-escaped comma did not arrive intact")

    c.finish(NAME, surface=SURFACE, extra=f"rows={rows}, first={data.first('custom')}")


if __name__ == "__main__":
    main()
