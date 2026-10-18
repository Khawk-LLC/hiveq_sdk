"""Strategy and global portfolios across several ES contracts, long and short, through a roll.

Three strategies on one account each hold several ES contracts at once -- explicit
outrights (ESM4, ESU4, ESZ4) plus orders on the continuous ``ES.c.0``, long and
short --
across the June 2024 roll (ESM4 -> ESU4, first ESU4 session Fri 2024-06-21).
Expected positions are fixed in advance, not derived from the engine:

    before the roll  A: ESM4 +5 (2 explicit + 3 via ES.c.0), ESU4 +1, ESZ4 -1
                     B: ESM4 +2 (via ES.c.0),                ESU4 +1
                     C: ESM4 -4 (short via ES.c.0),                    ESZ4 +1
                     global: ESM4 +3, ESU4 +2, ESZ4 flat (A's short cancels C's long)
    after the roll   the series' old contract (ESM4) is rolled into ESU4, the
                     short as buy-to-close ESM4 / sell ESU4:
                     A: ESU4 +6, ESZ4 -1   B: ESU4 +3   C: ESU4 -4, ESZ4 +1
                     global: ESU4 +5; ESM4 and ESZ4 flat

and, on every bar of both strategies:

* every contract position equals that strategy's own fill ledger (before the roll);
* ``portfolio().net_position("ES.c.0")`` equals the sum of the strategy's ES
  contracts, and ``position("ES.c.0")`` is exactly the current contract;
* ``global_portfolio()`` equals A + B + C per contract, in ``net_position`` and
  in ``position(contract).quantity``
  (snapshots of all strategies at the same bar time);
* net ES exposure is unchanged by the roll (A 5, B 3, C -3, global 5).
"""
from __future__ import annotations

from pathlib import Path
import re
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, EngineConfig, StrategyConfig
from hiveq.flow.config import AssetType

SERIES = "ES.c.0"
CONTRACTS = ["ESM4", "ESU4", "ESZ4"]
OUTRIGHT = re.compile(r"^ES[FGHJKMNQUVXZ]\d{1,2}$")

PLAN = {
    "SdkT78A": [("ESM4", 2), ("ESU4", 1), ("ESZ4", -1), (SERIES, 3)],
    "SdkT78B": [("ESU4", 1), (SERIES, 2)],
    "SdkT78C": [(SERIES, -4), ("ESZ4", 1)],
}
EXPECTED_BEFORE = {"SdkT78A": {"ESM4": 5, "ESU4": 1, "ESZ4": -1},
                   "SdkT78B": {"ESM4": 2, "ESU4": 1},
                   "SdkT78C": {"ESM4": -4, "ESZ4": 1}}
EXPECTED_AFTER = {"SdkT78A": {"ESU4": 6, "ESZ4": -1},
                  "SdkT78B": {"ESU4": 3},
                  "SdkT78C": {"ESU4": -4, "ESZ4": 1}}
EXPECTED_GLOBAL_BEFORE = {"ESM4": 3, "ESU4": 2}
EXPECTED_GLOBAL_AFTER = {"ESU4": 5}
ROLL_PAIR = ["ESM4", "ESU4"]


def es_positions(portfolio) -> dict[str, float]:
    held = {}
    for position in portfolio.positions():
        quantity = float(position.quantity)
        if abs(quantity) > 1e-9 and OUTRIGHT.match(str(position.symbol)):
            held[str(position.symbol)] = held.get(str(position.symbol), 0.0) + quantity
    return held


class SdkT78AggregationProbe:
    NAME = ""

    def on_start(self, ctx, event):
        if not hasattr(self, "s"):
            self.sent = False
            self.ledger: dict[str, float] = {}
            self.s = {"orders": [], "fills": [], "rejects": [], "rolls": [],
                      "snapshots": [], "violations": {}, "examples": [], "bars": 0}
            self.last_minute = None
        ctx.subscribe_bars([SERIES] + CONTRACTS, asset_type=AssetType.FUTURES, interval="1m")

    def _violation(self, name, detail):
        self.s["violations"][name] = self.s["violations"].get(name, 0) + 1
        if len(self.s["examples"]) < 12:
            self.s["examples"].append([name, detail])

    def on_bar(self, ctx, event):
        ts = int(event.ts_event)
        self.s["bars"] += 1
        if not self.sent:
            self.sent = True
            for symbol, quantity in PLAN[self.NAME]:
                order = (ctx.buy_order(symbol, abs(quantity)) if quantity > 0
                         else ctx.sell_order(symbol, abs(quantity)))
                self.s["orders"].append([symbol, quantity, order is not None])
        own = ctx.portfolio()
        held = es_positions(own)
        current = str(getattr(ctx.instrument(SERIES), "current_contract", "") or "")

        net_series = float(own.net_position(SERIES))
        if abs(net_series - sum(held.values())) > 1e-9:
            self._violation("net_position_not_sum_of_contracts", [ts, net_series, held])
        series_position = own.position(SERIES)
        series_qty = float(series_position.quantity) if series_position is not None else 0.0
        if abs(series_qty - held.get(current, 0.0)) > 1e-9:
            self._violation("position_series_not_current_contract", [ts, series_qty, current, held])
        for contract in CONTRACTS:
            contract_position = own.position(contract)
            qty = float(contract_position.quantity) if contract_position is not None else 0.0
            if abs(qty - held.get(contract, 0.0)) > 1e-9:
                self._violation("position_contract_mismatch", [ts, contract, qty, held])
            if abs(float(own.net_position(contract)) - held.get(contract, 0.0)) > 1e-9:
                self._violation("net_position_contract_mismatch", [ts, contract, held])
        if not self.s["rolls"]:
            ledger = {k: v for k, v in self.ledger.items() if abs(v) > 1e-9}
            if ledger != held:
                self._violation("positions_differ_from_fill_ledger", [ts, ledger, held])

        glob = ctx.global_portfolio()
        global_held = es_positions(glob)
        global_net = float(glob.net_position(SERIES))
        global_contract_net = {c: float(glob.net_position(c)) for c in CONTRACTS}
        global_contract_position = {}
        for contract in CONTRACTS:
            gp = glob.position(contract)
            global_contract_position[contract] = float(gp.quantity) if gp is not None else 0.0
        if abs(global_net - sum(global_held.values())) > 1e-9:
            self._violation("global_net_not_sum_of_contracts", [ts, global_net, global_held])

        # One snapshot per minute: both strategies see the same bar times, so the
        # analysis can check global == A + B at identical instants.
        minute = ts // 60_000_000_000
        if minute != self.last_minute:
            self.last_minute = minute
            self.s["snapshots"].append({"ts": ts, "own": held, "global": global_held,
                                        "global_contract_net": global_contract_net,
                                        "global_contract_position": global_contract_position,
                                        "own_net": net_series, "global_net": global_net,
                                        "current": current})

    def on_rollover(self, ctx, event):
        data = event.data()
        self.s["rolls"].append([str(data.prev_contract), str(data.current_contract),
                                int(data.ts_event)])

    def on_order(self, ctx, event):
        order = event.data()
        status = str(order.status).upper()
        if "REJECT" in status:
            self.s["rejects"].append([str(order.symbol), status])
        if order.is_filled and order.filled_qty:
            symbol = str(order.symbol)
            if not OUTRIGHT.match(symbol):
                symbol = str(getattr(ctx.instrument(symbol), "current_contract", "") or symbol)
            signed = float(order.filled_qty) * (1 if str(order.side).upper().endswith("BUY") else -1)
            self.ledger[symbol] = self.ledger.get(symbol, 0.0) + signed
            self.s["fills"].append([str(order.symbol), symbol, signed, int(event.ts_event)])

    def on_stop(self, ctx, event):
        self.s["final_own"] = es_positions(ctx.portfolio())
        self.s["final_global"] = es_positions(ctx.global_portfolio())
        self.s["final_own_net"] = float(ctx.portfolio().net_position(SERIES))
        self.s["final_global_net"] = float(ctx.global_portfolio().net_position(SERIES))
        emit_checkpoint(ctx, f"t78_{self.NAME}", self.s)


class SdkT78A(SdkT78AggregationProbe):
    NAME = "SdkT78A"


class SdkT78B(SdkT78AggregationProbe):
    NAME = "SdkT78B"


class SdkT78C(SdkT78AggregationProbe):
    NAME = "SdkT78C"


def _close(a: dict, b: dict) -> bool:
    keys = set(a) | set(b)
    return all(abs(a.get(k, 0.0) - b.get(k, 0.0)) < 1e-9 for k in keys)


def _sum(*books: dict) -> dict:
    out: dict[str, float] = {}
    for book in books:
        for key, value in book.items():
            out[key] = out.get(key, 0.0) + value
    return {k: v for k, v in out.items() if abs(v) > 1e-9}


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(name="SdkT78A", type="SdkT78A", symbols=[SERIES]),
            StrategyConfig(name="SdkT78B", type="SdkT78B", symbols=[SERIES]),
            StrategyConfig(name="SdkT78C", type="SdkT78C", symbols=[SERIES]),
        ],
        symbols=[SERIES], start_date="2024-06-17", end_date="2024-06-25",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(initial_capital=5_000_000.0, enable_auto_rollover=True,
                                       auto_flatten_at_close=False,
                                       session_start="18:00", session_end="17:00"),
        engine_config=EngineConfig(timezone="America/New_York"),
    )
    states = {name: completed_checkpoint(run, f"t78_{name}") for name in PLAN}

    # Positions just before and just after the roll, from each strategy's snapshots.
    def around_roll(state):
        roll_ts = state["rolls"][0][2] if state["rolls"] else None
        before = [s for s in state["snapshots"] if roll_ts is None or s["ts"] < roll_ts]
        after = [s for s in state["snapshots"] if roll_ts is not None and s["ts"] > roll_ts]
        return (before[-1] if before else None), (after[0] if after else None)

    checks: dict[str, bool] = {}
    for name, state in states.items():
        before, after = around_roll(state)
        checks[f"{name}_all_orders_sent"] = all(sent for _, _, sent in state["orders"])
        checks[f"{name}_no_rejects"] = not state["rejects"]
        checks[f"{name}_one_roll_esm4_to_esu4"] = [r[:2] for r in state["rolls"]] == [ROLL_PAIR]
        checks[f"{name}_positions_before_roll"] = bool(before) and _close(before["own"], EXPECTED_BEFORE[name])
        checks[f"{name}_positions_after_roll"] = bool(after) and _close(after["own"], EXPECTED_AFTER[name])
        checks[f"{name}_final_positions"] = _close(state["final_own"], EXPECTED_AFTER[name])
        checks[f"{name}_net_unchanged_by_roll"] = (
            bool(before) and bool(after)
            and abs(before["own_net"] - after["own_net"]) < 1e-9
            and abs(after["own_net"] - sum(EXPECTED_AFTER[name].values())) < 1e-9)
        for violation in ("net_position_not_sum_of_contracts", "position_series_not_current_contract",
                          "position_contract_mismatch", "net_position_contract_mismatch", "positions_differ_from_fill_ledger",
                          "global_net_not_sum_of_contracts"):
            checks[f"{name}_no_{violation}"] = not state["violations"].get(violation)

    # Global == A + B + C at the same instants (minute snapshots present in all).
    by_ts = {name: {snap["ts"]: snap for snap in state["snapshots"]} for name, state in states.items()}
    common = sorted(set.intersection(*(set(index) for index in by_ts.values())))
    global_mismatch = []
    global_contract_net_mismatch = []
    global_position_not_total = []
    for ts in common:
        total = _sum(*(by_ts[name][ts]["own"] for name in states))
        total_net = sum(by_ts[name][ts]["own_net"] for name in states)
        for name in states:
            snap = by_ts[name][ts]
            if not _close(snap["global"], total) or abs(snap["global_net"] - total_net) > 1e-9:
                global_mismatch.append([ts, name, snap["global"], total])
            # Per symbol: global net_position(contract) is the account total.
            if not _close({c: q for c, q in snap["global_contract_net"].items() if abs(q) > 1e-9}, total):
                global_contract_net_mismatch.append([ts, name, snap["global_contract_net"], total])
            # global position(contract) is the account total too.
            if any(abs(snap["global_contract_position"][c] - total.get(c, 0.0)) > 1e-9 for c in CONTRACTS):
                global_position_not_total.append([ts, name, snap["global_contract_position"], total])
    roll_ts = states["SdkT78A"]["rolls"][0][2] if states["SdkT78A"]["rolls"] else None
    before_ts = [ts for ts in common if roll_ts is not None and ts < roll_ts]
    after_ts = [ts for ts in common if roll_ts is not None and ts > roll_ts]
    checks["paired_snapshots_exist"] = len(common) > 100
    checks["global_equals_sum_of_strategies_every_minute"] = not global_mismatch
    checks["global_net_position_per_contract_is_account_total"] = not global_contract_net_mismatch
    checks["global_position_per_contract_is_account_total"] = not global_position_not_total
    checks["global_before_roll_expected"] = bool(before_ts) and _close(
        by_ts["SdkT78A"][before_ts[-1]]["global"], EXPECTED_GLOBAL_BEFORE)
    checks["global_after_roll_expected"] = bool(after_ts) and _close(
        by_ts["SdkT78A"][after_ts[0]]["global"], EXPECTED_GLOBAL_AFTER)
    final_total = _sum(*(state["final_own"] for state in states.values()))
    checks["final_global_equals_sum_of_strategies"] = all(
        _close(state["final_global"], final_total) for state in states.values())
    checks["final_global_expected"] = _close(states["SdkT78A"]["final_global"], EXPECTED_GLOBAL_AFTER)
    checks["final_global_net_is_5"] = abs(states["SdkT78A"]["final_global_net"] - 5) < 1e-9

    detail = {
        name: {"orders": s["orders"], "fills": s["fills"][:12], "rolls": s["rolls"],
               "before_after_roll": around_roll(s), "final_own": s["final_own"],
               "final_global": s["final_global"], "violations": s["violations"],
               "examples": s["examples"], "bars": s["bars"]}
        for name, s in states.items()
    }
    detail["global_mismatch_examples"] = global_mismatch[:5]
    detail["global_contract_net_mismatch_examples"] = global_contract_net_mismatch[:5]
    detail["global_position_not_total_count"] = len(global_position_not_total)
    detail["global_position_not_total_examples"] = global_position_not_total[:3]
    finish("t78_continuous_portfolio_aggregation", checks, extra=str(detail))
