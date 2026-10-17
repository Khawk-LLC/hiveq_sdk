"""The 2013-2014 ES/GC roll matrix on trade prints, across session configurations.

These are the rolls the session-boundary rollover rework was verified on:
quarterly ES (Mar/Sep/Dec 2013) and monthly GC (2013-2014, including the case
that used to leave the position stuck on GCQ3 for the rest of the run, and the
entry that used to land on the contract after the roll target). They run on
``fut_trades`` -- 1-minute bars for GC do not reach back to 2013 -- with the
session configurations that exposed the original defects:

* overnight 18:00-16:00 ET (a late expiring-contract close used to leave the
  roll exits minutes to fill);
* calendar-day 00:00:01-23:59:59 (the boundary is midnight, not the evening);
* a 09:30-09:45 intraday window (the roll happens far outside the session).

Expected rolls come from ``data/futures_roll_definitions.json``; the checks are
those of ``rollover_checks.analyze`` (see baseline t75), with the roll required
to fall outside each case's own session on the ET wall clock.

Run one case: ``python baseline_t76_rollover_2013_trades_matrix.py --case gc_feb_mar14_dst``.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys
import traceback

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, finish
from rollover_checks import RolloverProbe, analyze

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig


class SdkT76Rollover2013Trades(RolloverProbe):
    CHECKPOINT = "t76_rollover_2013_trades"


OVERNIGHT = ("18:00", "16:00")
CALENDAR_DAY = ("00:00:01", "23:59:59")
OPENING_RANGE = ("09:30", "09:45")

# name, series, start, end, (session_start, session_end), what the case pins down
CASES = [
    ("es_mar13", "ES.c.0", "2013-03-11", "2013-03-20", OVERNIGHT,
     "ESH3->ESM3 the week after DST starts"),
    ("es_sep13", "ES.c.0", "2013-09-16", "2013-09-24", OVERNIGHT,
     "ESU3->ESZ3 in EDT"),
    ("es_dec13", "ES.c.0", "2013-12-16", "2013-12-24", OVERNIGHT,
     "ESZ3->ESH4 in EST; Christmas Eve early close"),
    ("es_dec13_calendar_day", "ES.c.0", "2013-12-17", "2013-12-23", CALENDAR_DAY,
     "ESZ3->ESH4 with midnight session boundaries"),
    ("gc_feb13_late_start", "GC.c.0", "2013-02-21", "2013-03-01", OVERNIGHT,
     "GCG3->GCH3, window opens three sessions before the roll"),
    ("gc_feb13_from_new_year", "GC.c.0", "2013-01-01", "2013-03-01", OVERNIGHT,
     "starts on New Year's Day; GCF3->GCG3 then GCG3->GCH3"),
    ("gc_feb14", "GC.c.0", "2014-02-19", "2014-03-05", OVERNIGHT,
     "GCG4->GCH4 in EST"),
    ("gc_feb_mar14_dst", "GC.c.0", "2014-02-24", "2014-04-02", OVERNIGHT,
     "GCG4->GCH4 in EST, DST starts 3/9, GCH4->GCJ4 in EDT -- one position through both"),
    ("gc_apr14_weekend", "GC.c.0", "2014-04-22", "2014-05-02", OVERNIGHT,
     "GCJ4->GCK4 first session Mon 4/28: Friday close -> Sunday open"),
    ("gc_oct_nov13_dst_end", "GC.c.0", "2013-10-24", "2013-12-03", ("18:00", "17:00"),
     "GCV3->GCX3 in EDT, DST ends 11/3, GCX3->GCZ3 in EST, Thanksgiving week"),
    ("gc_feb14_opening_range", "GC.c.0", "2014-02-19", "2014-03-05", OPENING_RANGE,
     "GCG4->GCH4 with a 09:30-09:45 session: the roll happens between windows"),
]


def run_case(name, series, start, end, session):
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT76Rollover2013Trades", type="SdkT76Rollover2013Trades",
            symbols=[series], params={"schema": "fut_trades"},
        )],
        symbols=[series], start_date=start, end_date=end,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["fut_trades"]}],
        backtest_config=BacktestConfig(
            initial_capital=5_000_000.0, enable_auto_rollover=True,
            auto_flatten_at_close=False,
            session_start=session[0], session_end=session[1],
        ),
        engine_config=EngineConfig(timezone="America/New_York"),
    )
    state = completed_checkpoint(run, SdkT76Rollover2013Trades.CHECKPOINT)
    return analyze(state, start, end, *session)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append",
                        help="run only these cases (repeatable)")
    args = parser.parse_args()
    selected = [case for case in CASES if not args.case or case[0] in args.case]
    if not selected:
        raise SystemExit(f"unknown case(s) {args.case}; known: {[c[0] for c in CASES]}")

    checks: dict[str, bool] = {}
    details = []
    for name, series, start, end, session, purpose in selected:
        print(f"[t76] case {name}: {series} {start}..{end} session={session} -- {purpose}",
              flush=True)
        try:
            case_checks, detail = run_case(name, series, start, end, session)
        except Exception as exc:                            # noqa: BLE001
            traceback.print_exc()
            checks[f"{name}_ran"] = False
            details.append(f"{name}: {type(exc).__name__}: {exc}")
            continue
        failed = [key for key, ok in case_checks.items() if not ok]
        print(f"[t76] case {name}: {'PASS' if not failed else 'FAIL ' + ', '.join(failed)}",
              flush=True)
        print(f"[t76] case {name} detail: {detail}", flush=True)
        checks[f"{name}_ran"] = True
        checks.update({f"{name}.{key}": ok for key, ok in case_checks.items()})
        if failed:
            details.append(f"{name}: {detail}")
    finish("t76_rollover_2013_trades_matrix", checks, extra=" | ".join(details))


if __name__ == "__main__":
    main()
