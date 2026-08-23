"""Ten-year rollover reconciliation across 37 volume-continuous futures."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import export_run_artifacts, wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


DEFAULT_SYMBOLS = [
    "6A.v.0", "6B.v.0", "6E.v.0", "6J.v.0", "6S.v.0", "BTC.v.0",
    "BZ.v.0", "CL.v.0", "ES.v.0", "ETH.v.0", "GC.v.0", "HG.v.0",
    "HH.v.0", "KE.v.0", "MBT.v.0", "MCL.v.0", "MES.v.0", "MET.v.0",
    "MNQ.v.0", "MYM.v.0", "NG.v.0", "NIY.v.0", "NQ.v.0", "PL.v.0",
    "RTY.v.0", "SI.v.0", "VX.v.0", "YM.v.0", "ZB.v.0", "ZC.v.0",
    "ZF.v.0", "ZL.v.0", "ZM.v.0", "ZN.v.0", "ZS.v.0", "ZT.v.0",
    "ZW.v.0",
]


class SdkT52MultiSymbolLongRollover:
    def __init__(self):
        self.entered = set()
        self.rollovers = {}

    def on_start(self, ctx, event):
        ctx.subscribe_bars(
            ctx.strategy_config.symbols, asset_type=AssetType.FUTURES, interval="1m"
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        for continuous in ctx.strategy_config.symbols:
            if ctx.instrument(continuous).current_contract != bar.symbol:
                continue
            if continuous not in self.entered:
                ctx.buy_order(bar.symbol, quantity=1.0)
                self.entered.add(continuous)
                ctx.add_event_log(
                    f"ENTRY {continuous}->{bar.symbol}", sub_event_type="ENTRY",
                    state_variable={"continuous_symbol": continuous,
                                    "contract": bar.symbol,
                                    "time": bar.time.isoformat()},
                )
            break

    def on_security_event(self, ctx, event):
        data = event.data()
        phase = str(data.event_type)
        if phase.startswith("ROLLOVER_"):
            ctx.add_event_log(
                f"{phase} {data.symbol}->{data.rollover_symbol}",
                sub_event_type=phase,
                state_variable={
                    "phase": phase, "symbol": data.symbol,
                    "rollover_symbol": data.rollover_symbol,
                    "payload_ts_event": int(data.ts_event),
                    "event_ts_event": int(event.ts_event),
                },
            )

    def on_rollover(self, ctx, event):
        data = event.data()
        sequence = self.rollovers.get(data.continuous_symbol, 0) + 1
        self.rollovers[data.continuous_symbol] = sequence
        ctx.add_event_log(
            f"ROLLOVER_DONE {data.continuous_symbol} "
            f"{data.prev_contract}->{data.current_contract}",
            sub_event_type="ROLLOVER_DONE",
            state_variable={
                "sequence": sequence,
                "continuous_symbol": data.continuous_symbol,
                "prev_contract": data.prev_contract,
                "current_contract": data.current_contract,
                "payload_ts_event": int(data.ts_event),
                "event_ts_event": int(event.ts_event),
            },
        )


def _state(value):
    if isinstance(value, str):
        return json.loads(value or "{}")
    return value if isinstance(value, dict) else {}


def analyze(run, symbols: list[str]) -> dict:
    orders = run.orders()
    positions = run.positions()
    events = run.event_logs().sort_values("ts_event")
    entries = [_state(x) for x in events.loc[
        events["sub_event_type"] == "ENTRY", "state_variables"
    ]]
    rolls = [_state(x) for x in events.loc[
        events["sub_event_type"] == "ROLLOVER_DONE", "state_variables"
    ]]
    entered = {x.get("continuous_symbol") for x in entries}
    per_symbol = {}
    for symbol in symbols:
        chain = [x for x in rolls if x.get("continuous_symbol") == symbol]
        pairs_complete = all(
            x.get("prev_contract") and x.get("current_contract")
            and x.get("prev_contract") != x.get("current_contract") for x in chain
        )
        continuity = all(
            chain[i - 1].get("current_contract") == chain[i].get("prev_contract")
            for i in range(1, len(chain))
        )
        per_symbol[symbol] = {
            "entered": symbol in entered,
            "rollover_done_events": len(chain),
            "payload_pairs_complete": pairs_complete,
            "contract_chain_continuous": continuity,
        }
    open_positions = positions[positions["quantity"] != 0]
    all_filled = bool(len(orders)) and bool((orders["status"] == "FILLED").all())
    passed = (
        all_filled and len(open_positions) == len(symbols)
        and all(x["entered"] and x["rollover_done_events"] > 0
                and x["payload_pairs_complete"] and x["contract_chain_continuous"]
                for x in per_symbol.values())
    )
    return {
        "symbols": symbols, "orders": len(orders), "all_orders_filled": all_filled,
        "open_positions": len(open_positions),
        "expected_open_positions": len(symbols),
        "stale_open_positions": max(0, len(open_positions) - len(symbols)),
        "per_symbol": per_symbol, "passed": bool(passed),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--start", default="2016-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--timeout", type=float, default=14400.0)
    args = parser.parse_args()
    if len(args.symbols) < 10 or any(".v." not in x for x in args.symbols):
        raise ValueError("provide at least ten volume-continuous (.v.) futures symbols")

    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT52MultiSymbolLongRollover",
            type="SdkT52MultiSymbolLongRollover", symbols=args.symbols,
        )],
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(
            symbols=args.symbols, start_date=args.start, end_date=args.end,
            initial_capital=25_000_000.0, session_start="18:00", session_end="17:00",
            enable_auto_rollover=True, auto_flatten_at_close=False,
        ),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}", flush=True)
    wait_for_final(run, timeout=args.timeout)
    validation = analyze(run, args.symbols)
    artifacts = export_run_artifacts(run, validation=validation)
    print(json.dumps(validation, indent=2), flush=True)
    print(f"run_artifacts={artifacts}", flush=True)
    if not validation["passed"]:
        raise AssertionError(f"multi-symbol rollover validation failed: {validation}")


if __name__ == "__main__":
    main()
