"""Ten-year rollover reconciliation across 37 volume-continuous futures."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import finish_validation, open_positions as open_position_rows, export_run_artifacts, wait_for_final

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


# An outright contract is ROOT + month code + year digits: ESZ5, 6AZ5, MCLF26.
OUTRIGHT = re.compile(r"^([0-9A-Z]+?)([FGHJKMNQUVXZ])(\d{1,2})$")


def outright_root(symbol: str) -> str:
    match = OUTRIGHT.match(str(symbol))
    return match.group(1) if match else str(symbol)


class SdkT52MultiSymbolLongRollover:
    def __init__(self):
        self.entered = set()
        self.rollovers = {}
        self.by_root = {}
        self.bars = {}
        self.unmatched = {}

    def on_start(self, ctx, event):
        symbols = ctx.strategy_config.symbols
        # Continuous alias -> root ("6A.v.0" -> "6A"), so a delivered outright
        # can be matched back to the subscription that produced it.
        self.by_root = {continuous.split(".")[0]: continuous for continuous in symbols}
        ctx.subscribe_bars(symbols, asset_type=AssetType.FUTURES, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        # Entry is keyed on the contract's root, not on equality with
        # ``instrument(continuous).current_contract``: for most of these roots
        # that accessor did not name the contract the callbacks were being
        # given, so thirty of the thirty-seven symbols were never entered and
        # the rollover reconciliation ran on seven of them. What
        # current_contract reported at entry is logged instead of gating on it.
        continuous = self.by_root.get(outright_root(bar.symbol))
        if continuous is None:
            # A delivered contract whose root is not in the subscription: worth
            # naming, because it would otherwise look like a missing symbol.
            self.unmatched[str(bar.symbol)] = self.unmatched.get(
                str(bar.symbol), 0) + 1
            return
        self.bars[continuous] = self.bars.get(continuous, 0) + 1
        if continuous in self.entered:
            return
        self.entered.add(continuous)
        ctx.buy_order(bar.symbol, quantity=1.0)
        try:
            reported = str(getattr(ctx.instrument(continuous),
                                   "current_contract", "") or "")
        except Exception as exc:                           # noqa: BLE001
            reported = f"raised: {str(exc)[:60]}"
        ctx.add_event_log(
            f"ENTRY {continuous}->{bar.symbol}", sub_event_type="ENTRY",
            state_variable={"continuous_symbol": continuous,
                            "contract": str(bar.symbol),
                            "reported_current_contract": reported,
                            "time": bar.time.isoformat()},
        )

    def on_stop(self, ctx, event):
        # Which subscriptions actually produced bars. Without this a symbol that
        # was never entered cannot be told apart from a symbol the engine never
        # delivered data for -- and for most of these roots it is the latter.
        ctx.add_event_log(
            "BAR_COVERAGE", sub_event_type="BAR_COVERAGE",
            state_variable={"bars": self.bars, "unmatched": self.unmatched},
        )

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
    # In-process runs serialize state_variables with orjson, so this column
    # holds bytes; without decoding, every payload silently reads as {}.
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
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
    coverage = [_state(x) for x in events.loc[
        events["sub_event_type"] == "BAR_COVERAGE", "state_variables"
    ]]
    bars_by_symbol = coverage[-1].get("bars", {}) if coverage else {}
    unmatched = coverage[-1].get("unmatched", {}) if coverage else {}
    entered = {x.get("continuous_symbol") for x in entries}
    contract_agreed = {
        x.get("continuous_symbol"): (
            x.get("reported_current_contract") == x.get("contract")
        )
        for x in entries
    }
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
            "bars_delivered": int(bars_by_symbol.get(symbol, 0)),
            "entered": symbol in entered,
            "current_contract_agreed_at_entry": contract_agreed.get(symbol),
            "rollover_done_events": len(chain),
            "payload_pairs_complete": pairs_complete,
            "contract_chain_continuous": continuity,
        }
    open_positions = open_position_rows(positions)
    all_filled = bool(len(orders)) and bool((orders["status"] == "FILLED").all())

    def failing(predicate):
        return sorted(symbol for symbol, row in per_symbol.items()
                      if not predicate(row))

    # Each property is its own named check, so the scorecard says which one
    # broke and for which symbols instead of one opaque "passed".
    return {
        "symbols": symbols, "orders": len(orders), "all_orders_filled": all_filled,
        "open_positions": len(open_positions),
        "expected_open_positions": len(symbols),
        "stale_open_positions": max(0, len(open_positions) - len(symbols)),
        # Split by cause: a symbol the engine delivered no bar for is a data
        # gap, and a symbol that got bars but was never entered is an execution
        # defect. Reported as separate checks so the scorecard names the right
        # one.
        "every_symbol_received_bars": not failing(
            lambda row: row["bars_delivered"] > 0),
        "every_symbol_with_bars_entered": not failing(
            lambda row: row["entered"] or not row["bars_delivered"]),
        "every_symbol_entered": not failing(lambda row: row["entered"]),
        "every_symbol_rolled_over": not failing(
            lambda row: row["rollover_done_events"] > 0),
        "rollover_payload_pairs_complete": not failing(
            lambda row: row["payload_pairs_complete"]),
        "rollover_chain_continuous": not failing(
            lambda row: row["contract_chain_continuous"]),
        "open_position_per_symbol": len(open_positions) == len(symbols),
        "no_bars_delivered": failing(lambda row: row["bars_delivered"] > 0),
        "unmatched_contracts": unmatched,
        "not_entered": failing(lambda row: row["entered"]),
        "never_rolled": failing(lambda row: row["rollover_done_events"] > 0),
        "discontinuous_chain": failing(lambda row: row["contract_chain_continuous"]),
        "current_contract_disagreed_at_entry": sorted(
            symbol for symbol, row in per_symbol.items()
            if row["entered"] and row["current_contract_agreed_at_entry"] is False
        ),
        "per_symbol": per_symbol,
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
    finish_validation("t52_multi_symbol_long_rollover", validation)


if __name__ == "__main__":
    main()
