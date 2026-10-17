"""Futures rolls at the session boundary: EST/EDT, DST weekends, holidays, window edges.

Each case is a short backtest (1-minute bars; trade prints for GC/CL, which
have no bars) around one or more real rolls of
``fut_continuous_def`` (the expected prev/current pairs come from
``data/futures_roll_definitions.json``, never from the engine). A buy-and-hold
probe (``rollover_checks.RolloverProbe``) records every delivered contract,
``on_rollover`` payload, ROLLOVER_DUE / ROLLOVER_COMPLETE event, position and
``ctx.instrument(series).current_contract``; ``rollover_checks.analyze`` then
requires, per series:

* exactly the defined rolls, in order, and no other contract delivered;
* ``on_rollover`` with a complete payload, before any print of the new contract
  and after the last print of the old one (callback order);
* the roll stamped at the 17:00 New York close before the new contract's first
  session -- 22:00 UTC in standard time, 21:00 UTC in daylight time -- so a
  stamp pinned to a fixed UTC hour fails in one of the seasons;
* the position moved: one lot on the new contract and nothing on the old one on
  the first print after the roll; the continuous instrument re-resolved;
* ROLLOVER_DUE before and ROLLOVER_COMPLETE after each roll, no ROLLOVER_FAILED.

Session times are pinned to America/New_York (EngineConfig.timezone) so the
result does not depend on the host clock: a UTC container and an ET laptop must
agree.

Run one case: ``python baseline_t75_rollover_session_boundary_matrix.py --case es_dst_week_mar13``.
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


class SdkT75RolloverBoundary(RolloverProbe):
    CHECKPOINT = "t75_rollover_session_boundary"


SESSION = ("18:00", "17:00")

# name, series, start, end, schema, what the case pins down. GC and CL have no
# 1-minute bars in HIVEQ_US_FUT (only fut_trades), so their cases run on trades.
B, T = "bars_1m", "fut_trades"
CASES = [
    # --- standard vs daylight time -------------------------------------
    ("es_dst_week_mar13", ["ES.c.0"], "2013-03-06", "2013-03-20", B,
     "DST starts Sun 3/10 inside the window; ESH3->ESM3 rolls in EDT (first session Fri 3/15)"),
    ("es_edt_sep13", ["ES.c.0"], "2013-09-16", "2013-09-24", B,
     "summer roll ESU3->ESZ3 (EDT)"),
    ("es_est_dec13_christmas", ["ES.c.0"], "2013-12-16", "2013-12-27", B,
     "winter roll ESZ3->ESH4 (EST), then the Christmas Eve early close and Christmas"),
    ("gc_dst_end_oct_nov24", ["GC.c.0"], "2024-10-23", "2024-12-02", T,
     "GCV4->GCX4 in EDT, DST ends Sun 11/3, GCX4->GCZ4 in EST, then Thanksgiving"),
    # --- weekend boundaries --------------------------------------------
    ("esv_dst_weekend_mar23", ["ES.v.0"], "2023-03-08", "2023-03-16", B,
     "volume roll ESH3->ESM3 first session Mon 3/13: the boundary is the DST-start weekend"),
    ("esv_weekend_dec22", ["ES.v.0"], "2022-12-07", "2022-12-14", B,
     "volume roll ESZ2->ESH3 first session Mon 12/12 (standard-time weekend)"),
    ("gc_weekend_jan24", ["GC.c.0"], "2024-01-24", "2024-02-01", T,
     "GCF4->GCG4 first session Mon 1/29 (Friday close -> Sunday open)"),
    ("es_start_saturday_jun24", ["ES.c.0"], "2024-06-15", "2024-06-25", B,
     "window starts on a Saturday; ESM4->ESU4 first session Fri 6/21"),
    # --- holidays ------------------------------------------------------
    ("es_juneteenth_jun24", ["ES.c.0"], "2024-06-17", "2024-06-25", B,
     "Juneteenth (Wed 6/19) two days before ESM4->ESU4"),
    ("cl_presidents_day_feb24", ["CL.c.0"], "2024-02-14", "2024-02-23", T,
     "CLH4->CLJ4 first session is Presidents Day Mon 2/19 (weekend + holiday boundary)"),
    ("cl_mlk_jan13", ["CL.c.0"], "2013-01-16", "2013-01-24", T,
     "CLG3->CLH3 first session is MLK Day Mon 1/21"),
    ("cl_good_friday_apr25", ["CL.c.0"], "2025-04-15", "2025-04-25", T,
     "Good Friday 4/18 closed; CLK5->CLM5 first session Tue 4/22"),
    ("gcv_thanksgiving_nov24", ["GC.v.0"], "2024-11-20", "2024-12-04", T,
     "bi-monthly GCZ4->GCG5 first session Wed 11/27, Thanksgiving the next day"),
    ("gc_christmas_new_year_dec24", ["GC.c.0"], "2024-12-19", "2025-01-06", T,
     "GCZ4->GCF5 first session Fri 12/27 after Christmas; New Year in the window"),
    ("gc_start_new_year_jan25", ["GC.c.0"], "2025-01-01", "2025-01-31", T,
     "window starts on New Year's Day; GCF5->GCG5 first session Wed 1/29"),
    # --- window edges --------------------------------------------------
    ("es_start_on_roll_session_jun24", ["ES.c.0"], "2024-06-21", "2024-06-26", B,
     "first session is the new contract's first session: no roll, ESU4 from the start"),
    ("es_end_before_roll_jun24", ["ES.c.0"], "2024-06-14", "2024-06-20", B,
     "last session is ESM4's last: no roll, nothing delivered on ESU4"),
    # --- several roots in one run --------------------------------------
    ("multi_root_jun24", ["ES.c.0", "NQ.c.0", "CL.c.0", "GC.c.0"], "2024-06-17", "2024-06-27", T,
     "quarterly ES/NQ, monthly CL (day after Juneteenth) and GC roll independently in one run"),
]


def run_case(name, series, start, end, schema):
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT75RolloverBoundary", type="SdkT75RolloverBoundary",
            symbols=series, params={"schema": schema},
        )],
        symbols=series, start_date=start, end_date=end,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": [schema]}],
        backtest_config=BacktestConfig(
            initial_capital=5_000_000.0, enable_auto_rollover=True,
            auto_flatten_at_close=False,
            session_start=SESSION[0], session_end=SESSION[1],
        ),
        engine_config=EngineConfig(timezone="America/New_York"),
    )
    state = completed_checkpoint(run, SdkT75RolloverBoundary.CHECKPOINT)
    return analyze(state, start, end)


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
    for name, series, start, end, schema, purpose in selected:
        print(f"[t75] case {name}: {series} {start}..{end} {schema} -- {purpose}", flush=True)
        try:
            case_checks, detail = run_case(name, series, start, end, schema)
        except Exception as exc:                            # noqa: BLE001
            traceback.print_exc()
            checks[f"{name}_ran"] = False
            details.append(f"{name}: {type(exc).__name__}: {exc}")
            continue
        failed = [key for key, ok in case_checks.items() if not ok]
        print(f"[t75] case {name}: {'PASS' if not failed else 'FAIL ' + ', '.join(failed)}",
              flush=True)
        print(f"[t75] case {name} detail: {detail}", flush=True)
        checks[f"{name}_ran"] = True
        checks.update({f"{name}.{key}": ok for key, ok in case_checks.items()})
        if failed:
            details.append(f"{name}: {detail}")
    finish("t75_rollover_session_boundary_matrix", checks, extra=" | ".join(details))


if __name__ == "__main__":
    main()
