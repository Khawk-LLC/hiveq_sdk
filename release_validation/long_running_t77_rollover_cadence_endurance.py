"""Years of continuous rolls per cadence: quarterly, monthly and bi-monthly series.

One buy-and-hold lot per case, held across every roll of the window. The roll
chain must match ``fut_continuous_def`` exactly (``data/futures_roll_definitions.json``):
same count, same prev/current pairs, same order, the position always on the
current contract and nothing left behind on an expired one. Every roll is
reconciled the same way as the short matrices (baseline t75/t76): on_rollover
payload and timestamp, ROLLOVER_DUE / ROLLOVER_COMPLETE, the roll outside the
session on the ET wall clock through every DST change of the window, and the
continuous instrument re-resolving to the new contract.

Cadences:

* quarterly  -- ES.c.0 1m bars 2013-2025 (H/M/U/Z, rolls on expiry Friday; 52 rolls);
* monthly    -- GC.c.0 trades 2013-2025 (every month, calendar roll; 156 rolls,
  including the 2013 run that used to stay stuck on GCQ3 after a failed roll
  exit) and CL.c.0 trades 2024-2025 (24 rolls, a few days before the 25th);
* bi-monthly -- GC.v.0 trades 2024-2025 (volume roll through G/J/M/Q/Z; 10 rolls).

Run one case: ``python long_running_t77_rollover_cadence_endurance.py --case monthly_gc``.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import traceback

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, finish
from rollover_checks import RolloverProbe, analyze, expected_schedule

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig


class SdkT77RolloverCadence(RolloverProbe):
    CHECKPOINT = "t77_rollover_cadence"


SESSION = ("18:00", "17:00")

# name, series, start, end, schema. GC and CL have no 1-minute bars in
# HIVEQ_US_FUT, so they run on trade prints: GC.c.0's calendar front month is
# thin enough to replay 2013-2025; CL and the GC volume roll are liquid, so they
# cover two years.
CASES = [
    ("quarterly_es", "ES.c.0", "2013-01-01", "2025-12-31", "bars_1m"),
    ("monthly_gc", "GC.c.0", "2013-01-01", "2025-12-31", "fut_trades"),
    ("monthly_cl", "CL.c.0", "2024-01-01", "2025-12-31", "fut_trades"),
    ("bimonthly_gc_volume", "GC.v.0", "2024-01-01", "2025-12-31", "fut_trades"),
]


def run_case(series, start, end, schema):
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT77RolloverCadence", type="SdkT77RolloverCadence",
            symbols=[series], params={"schema": schema},
        )],
        symbols=[series], start_date=start, end_date=end,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": [schema]}],
        backtest_config=BacktestConfig(
            initial_capital=5_000_000.0, enable_auto_rollover=True,
            auto_flatten_at_close=False,
            session_start=SESSION[0], session_end=SESSION[1],
        ),
        engine_config=EngineConfig(timezone="America/New_York"),
    )
    print(f"run_id={getattr(run, 'run_id', None)}", flush=True)
    state = completed_checkpoint(run, SdkT77RolloverCadence.CHECKPOINT)
    return analyze(state, start, end)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append",
                        help="run only these cases (repeatable)")
    parser.add_argument("--start", help="override every selected case's start date")
    parser.add_argument("--end", help="override every selected case's end date")
    args = parser.parse_args()
    selected = [case for case in CASES if not args.case or case[0] in args.case]
    if not selected:
        raise SystemExit(f"unknown case(s) {args.case}; known: {[c[0] for c in CASES]}")

    checks: dict[str, bool] = {}
    details = []
    for name, series, start, end, schema in selected:
        start, end = args.start or start, args.end or end
        expected = expected_schedule(series, start, end)
        print(f"[t77] case {name}: {series} {start}..{end} {schema} "
              f"expected_rolls={len(expected['rolls'])} "
              f"{expected['initial']}->{expected['final']}", flush=True)
        try:
            case_checks, detail = run_case(series, start, end, schema)
        except Exception as exc:                            # noqa: BLE001
            traceback.print_exc()
            checks[f"{name}_ran"] = False
            details.append(f"{name}: {type(exc).__name__}: {exc}")
            continue
        failed = [key for key, ok in case_checks.items() if not ok]
        print(f"[t77] case {name}: {'PASS' if not failed else 'FAIL ' + ', '.join(failed)}",
              flush=True)
        print(f"[t77] case {name} detail: {json.dumps(detail, default=str)}", flush=True)
        checks[f"{name}_ran"] = True
        checks.update({f"{name}.{key}": ok for key, ok in case_checks.items()})
        if failed:
            details.append(f"{name}: {detail}")
    finish("t77_rollover_cadence_endurance", checks, extra=" | ".join(details))


if __name__ == "__main__":
    main()
