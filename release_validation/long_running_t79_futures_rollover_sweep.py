"""Full 2013-2026 rollover sweep across every defined continuous futures root.

One backtest subscribes to a universe of ``.v.0`` continuous futures and buys
one contract of each on the first bar it receives. From there the strategy
only observes — ``on_rollover`` events are logged with the prev/current pair,
the running tally per root is kept as a tiny counter dict, and nothing else is
retained. Memory stays flat with time because the strategy carries no
per-bar / per-rolls accumulators.

The engine's ``enable_auto_rollover`` is deliberately left off: the test
confirms the roll **signal** fires at the right moment with the right pair.
Position management on the roll is not under test here and is the
responsibility of other validations (t49, t52, t75, t76).

Thirty-one roots are run.  See the "status A" set in the hand-off table for
the derivation: continuous_def start <= 2013-01-02 AND bars_1m data from
2012 or earlier.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import event_logs_until, export_run_artifacts, finish, wait_for_final
from rollover_checks import root_of

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig
from hiveq.flow.config import AssetType


SERIES = [f"{root}.v.0" for root in [
    "6A", "6B", "6E", "6J", "6S",
    "BZ", "CL", "ES", "GC", "HG",
    "HH", "KE", "MGC", "NG", "NIY",
    "NQ", "PL", "SI", "UB", "YM",
    "ZB", "ZC", "ZF", "ZL", "ZM",
    "ZN", "ZS", "ZT", "ZW",
]]

START_DATE = "2013-01-02"
END_DATE = "2026-10-19"


class SdkT79FuturesRolloverSweep:
    """Buy one of every subscribed continuous series on first bar; observe only.

    State is three small objects:
    * ``bought`` -- a set of outright symbols the strategy has already placed
      an entry order for. Caps at the number of unique outrights the engine
      ever delivers (one per root at a time under ``.v.0``), so this is bounded.
    * ``rolls`` -- ``{root: count}``. Thirty-one keys, int values.
    * ``last_pair`` -- ``{root: (prev, current)}``, overwritten each roll.

    No per-bar list appends. No per-roll detail dicts. ``on_rollover`` fires
    ``ctx.add_event_log(..., state_variable=...)`` with the pair, which the
    engine persists to its own log table rather than keeping in Python RAM.
    """

    def __init__(self):
        # Entry is keyed on ROOT (ES, 6E, ...) not outright (ESH3, 6EM3, ...),
        # otherwise each new contract delivered after a roll looks like an
        # unseen symbol and the strategy fires another buy_order, ending up
        # with one long per contract per root (~1160 by 2026) instead of one
        # per root.
        self.entered = set()
        self.rolls = {}
        self.last_pair = {}

    def on_start(self, ctx, event):
        # Repeated daily subscriptions are deduplicated by the engine.
        ctx.subscribe_bars(
            ctx.strategy_config.symbols,
            asset_type=AssetType.FUTURES,
            interval="1m",
        )

    def on_bar(self, ctx, event):
        symbol = str(event.data().symbol)
        root = root_of(symbol)
        if root in self.entered:
            return
        self.entered.add(root)
        ctx.buy_order(symbol, 1)

    def on_rollover(self, ctx, event):
        data = event.data()
        root = str(data.continuous_symbol).split(".", 1)[0]
        self.rolls[root] = self.rolls.get(root, 0) + 1
        self.last_pair[root] = (str(data.prev_contract), str(data.current_contract))
        ctx.add_event_log(
            f"ROLLOVER {data.continuous_symbol} "
            f"{data.prev_contract}->{data.current_contract}",
            sub_event_type="ROLLOVER",
            state_variable={
                "continuous_symbol": str(data.continuous_symbol),
                "prev_contract": str(data.prev_contract),
                "current_contract": str(data.current_contract),
                "sequence": self.rolls[root],
                "ts_event": int(event.ts_event),
            },
        )


def _decode(value):
    if value is None:
        return {}
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value or "{}")
    return value if isinstance(value, dict) else {}


def analyze(run) -> dict:
    logs = event_logs_until(run, "ROLLOVER", timeout=180.0)
    rolls_by_root: dict[str, list[dict]] = {}
    if logs is not None and not getattr(logs, "empty", True):
        for _, row in logs.iterrows():
            if str(row.get("sub_event_type")) != "ROLLOVER":
                continue
            state = _decode(row.get("state_variables"))
            root = str(state.get("continuous_symbol", "")).split(".", 1)[0]
            if root:
                rolls_by_root.setdefault(root, []).append(state)
    for root, rolls in rolls_by_root.items():
        rolls.sort(key=lambda r: r.get("sequence", 0))

    per_root = {}
    checks: dict[str, bool] = {}
    for series in SERIES:
        root = series.split(".", 1)[0]
        rolls = rolls_by_root.get(root, [])
        chain_continuous = all(
            rolls[i - 1].get("current_contract") == rolls[i].get("prev_contract")
            for i in range(1, len(rolls))
        )
        pairs_complete = all(
            r.get("prev_contract") and r.get("current_contract")
            and r.get("prev_contract") != r.get("current_contract")
            for r in rolls
        )
        per_root[root] = {
            "rollovers": len(rolls),
            "first_pair": (rolls[0].get("prev_contract"), rolls[0].get("current_contract")) if rolls else None,
            "last_pair": (rolls[-1].get("prev_contract"), rolls[-1].get("current_contract")) if rolls else None,
            "chain_continuous": chain_continuous,
            "pairs_complete": pairs_complete,
        }
        checks[f"{root}_rollover_fired"] = len(rolls) >= 1
        checks[f"{root}_chain_continuous"] = chain_continuous
        checks[f"{root}_pairs_complete"] = pairs_complete
    return {"checks": checks, "per_root": per_root}


def main() -> int:
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="SdkT79FuturesRolloverSweep",
            type="SdkT79FuturesRolloverSweep",
            symbols=SERIES,
        )],
        symbols=SERIES, start_date=START_DATE, end_date=END_DATE,
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(
            initial_capital=25_000_000.0,
            # auto_rollover=True keeps the single long position live across
            # every roll (matches t49/t52/t76); without it the position sits
            # on the first delivered outright and the PnL curve goes flat
            # the moment that contract expires.
            enable_auto_rollover=True,
            auto_flatten_at_close=False,
            session_start="18:00", session_end="17:00",
        ),
        engine_config=EngineConfig(timezone="America/New_York"),
    )
    print(f"[t79] run_id={run.run_id} task_id={run.task_id}", flush=True)
    wait_for_final(run, timeout=21600.0)
    report = analyze(run)
    export_run_artifacts(run, validation=report)
    print(json.dumps(report, indent=2, default=str)[:4000], flush=True)
    finish("t79_futures_rollover_sweep", report["checks"],
           extra=f"series={len(SERIES)}; per_root={json.dumps(report['per_root'], default=str)[:300]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
