"""Shared probe and reconciliation for the futures rollover validations.

``RolloverProbe`` is a buy-and-hold strategy base that records, per continuous
series, everything a roll has to get right:

* the contract sequence actually delivered to the strategy (bar or trade
  prints), as segments ``[contract, first_ts, last_ts, prints]``;
* each ``on_rollover`` payload, with the positions held when it fired;
* the first print after each roll: its contract, what
  ``ctx.instrument(series).current_contract`` reports, and the positions;
* ``ROLLOVER_DUE`` / ``ROLLOVER_COMPLETE`` / ``ROLLOVER_FAILED`` security events;
* fills and rejects.

``analyze`` reconciles that record against the roll schedule in
``data/futures_roll_definitions.json`` (``fut_continuous_def``): exact
prev/current pairs, the roll instant against the delivered prints, the roll
stamped at the 17:00 New York close in both EST and EDT, the
position following the roll, and the instrument re-resolving to the new
contract. Every check is a named boolean so a failure says which rule broke.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from qa_common import emit_checkpoint

from hiveq.flow.config import AssetType

ET = ZoneInfo("America/New_York")
DEFINITIONS_PATH = Path(__file__).resolve().parent / "data" / "futures_roll_definitions.json"
# An outright contract is ROOT + month code + year digits: ESZ5, GCF26, 6EM6.
OUTRIGHT = re.compile(r"^([0-9A-Z]+?)([FGHJKMNQUVXZ])(\d{1,2})$")
SECURITY_PHASES = ("ROLLOVER_DUE", "ROLLOVER_COMPLETE", "ROLLOVER_FAILED")
# Snapshots taken on the first prints after a roll (the roll fills and the
# instrument switch must already be visible on the first one).
POST_ROLL_PRINTS = 3


def root_of(symbol: str) -> str:
    match = OUTRIGHT.match(str(symbol))
    return match.group(1) if match else str(symbol).split(".", 1)[0]


class RolloverProbe:
    """Buy one lot of every subscribed continuous series and record its rolls.

    Subclasses only set ``CHECKPOINT`` (and are defined in the entry script, so
    the strategy loader finds them). The data schema decides the callback:
    ``bars_1m`` cases subscribe bars, ``fut_trades`` cases subscribe trades.
    """

    CHECKPOINT = "rollover_probe"

    def on_start(self, ctx, event):
        if not hasattr(self, "s"):
            series = list(ctx.strategy_config.symbols)
            params = dict(getattr(ctx.strategy_config, "params", None) or {})
            self.by_root = {root_of(name): name for name in series}
            self.entered = set()
            self.pending = {}
            self.s = {
                "series": {name: {"segments": [], "rolls": [], "post": [],
                                  "instrument_mismatch": 0,
                                  "instrument_mismatch_examples": [],
                                  "unmatched": 0} for name in series},
                "security_events": [],
                "fills": [],
                "rejects": [],
                "schema": params.get("schema", "bars_1m"),
                "sessions": 0,
            }
        self.s["sessions"] += 1
        series = list(self.s["series"])
        # Repeated daily subscriptions are deduplicated by the engine.
        if self.s["schema"] == "fut_trades":
            ctx.subscribe_trades(series, asset_type=AssetType.FUTURES)
        else:
            ctx.subscribe_bars(series, asset_type=AssetType.FUTURES, interval="1m")

    def _positions(self, ctx, root):
        held = {}
        for position in ctx.portfolio().positions():
            quantity = float(position.quantity)
            if abs(quantity) > 1e-9 and root_of(position.symbol) == root:
                held[str(position.symbol)] = quantity
        return held

    def _current_contract(self, ctx, series):
        try:
            return str(getattr(ctx.instrument(series), "current_contract", "") or "")
        except Exception as exc:                            # noqa: BLE001
            return f"raised: {str(exc)[:60]}"

    def _on_print(self, ctx, symbol, ts):
        symbol = str(symbol)
        series = self.by_root.get(root_of(symbol))
        if series is None:
            return
        record = self.s["series"][series]
        # Callback order, independent of any timestamp: every print is on the
        # contract named by the latest on_rollover (or the first contract
        # delivered). An old-contract print after the callback, or a new-contract
        # print before it, counts here.
        expected = (record["rolls"][-1]["current_contract"] if record["rolls"]
                    else record["segments"][0][0] if record["segments"] else symbol)
        if symbol != expected:
            record["off_contract_prints"] = record.get("off_contract_prints", 0) + 1
            examples = record.setdefault("off_contract_examples", [])
            if len(examples) < 5:
                examples.append([ts, symbol, expected])
        segments = record["segments"]
        new_segment = not segments or segments[-1][0] != symbol
        if new_segment:
            segments.append([symbol, ts, ts, 1])
        else:
            segments[-1][2] = ts
            segments[-1][3] += 1
        if new_segment:
            # The continuous instrument must name the contract being delivered.
            current = self._current_contract(ctx, series)
            if current != symbol:
                record["instrument_mismatch"] += 1
                if len(record["instrument_mismatch_examples"]) < 5:
                    record["instrument_mismatch_examples"].append([ts, symbol, current])
        pending = self.pending.get(series)
        if pending is not None and pending["remaining"] > 0:
            pending["prints"].append({
                "ts": ts, "symbol": symbol,
                "instrument_current": self._current_contract(ctx, series),
                "positions": self._positions(ctx, root_of(symbol)),
                "net_series": float(ctx.net_position(series)),
            })
            pending["remaining"] -= 1
        if series not in self.entered:
            self.entered.add(series)
            ctx.buy_order(series, 1)

    def on_bar(self, ctx, event):
        self._on_print(ctx, event.data().symbol, int(event.ts_event))

    def on_trade(self, ctx, event):
        self._on_print(ctx, event.data().symbol, int(event.ts_event))

    def on_rollover(self, ctx, event):
        data = event.data()
        series = str(data.continuous_symbol)
        row = {
            "continuous_symbol": series,
            "prev_contract": str(data.prev_contract),
            "current_contract": str(data.current_contract),
            "payload_ts": int(data.ts_event),
            "event_ts": int(event.ts_event),
            "positions_at_callback": self._positions(ctx, root_of(data.current_contract)),
            "instrument_current_at_callback": self._current_contract(ctx, series),
        }
        record = self.s["series"].get(series)
        if record is None:
            self.s.setdefault("unknown_rollovers", []).append(row)
            return
        record["rolls"].append(row)
        observation = {"roll": len(record["rolls"]) - 1, "prints": [],
                       "remaining": POST_ROLL_PRINTS}
        record["post"].append(observation)
        self.pending[series] = observation
        ctx.add_event_log(
            f"ROLLOVER {series} {row['prev_contract']}->{row['current_contract']}",
            sub_event_type="ROLLOVER", state_variable=row,
        )

    def on_security_event(self, ctx, event):
        data = event.data()
        phase = str(data.event_type)
        if phase.startswith("ROLLOVER_"):
            self.s["security_events"].append({
                "phase": phase, "symbol": str(data.symbol),
                "rollover_symbol": str(getattr(data, "rollover_symbol", "") or ""),
                "continuous_symbol": str(getattr(data, "continuous_symbol", "") or ""),
                "ts": int(data.ts_event),
            })

    def on_order(self, ctx, event):
        order = event.data()
        status = str(order.status).upper()
        if "REJECT" in status:
            self.s["rejects"].append([str(order.symbol), status,
                                      str(getattr(order, "reject_reason", "") or "")[:120]])
        if order.is_filled and order.filled_qty:
            self.s["fills"].append([str(order.symbol), str(order.side),
                                    float(order.filled_qty), int(event.ts_event)])

    def on_stop(self, ctx, event):
        for series, record in self.s["series"].items():
            record["final_positions"] = self._positions(ctx, root_of(series))
            record["final_instrument_current"] = self._current_contract(ctx, series)
        emit_checkpoint(ctx, self.CHECKPOINT, self.s)


# --------------------------------------------------------------------------
# Expectations
# --------------------------------------------------------------------------

def load_definitions() -> dict[str, list[tuple[str, str]]]:
    raw = json.loads(DEFINITIONS_PATH.read_text(encoding="utf-8"))
    return {key: [tuple(row) for row in rows] for key, rows in raw.items()
            if not key.startswith("_")}


def expected_schedule(series: str, start: str, end: str) -> dict[str, Any]:
    """The contract trading at ``start`` and every roll inside ``(start, end]``.

    A definition row's date is the first session (trade date) on the new
    contract. A backtest whose first session is that date starts on the new
    contract with no roll; one whose last session is the day before never
    reaches the boundary.
    """
    rows = load_definitions()[series]
    initial = [row for row in rows if row[0] <= start]
    if not initial:
        raise ValueError(f"{series}: no definition at or before {start}")
    rolls = []
    previous = initial[-1][1]
    for first_session, contract in rows:
        if start < first_session <= end and contract != previous:
            rolls.append({"prev": previous, "current": contract,
                          "first_session": first_session})
            previous = contract
    # A run whose first session is a new contract's first session: with an
    # overnight session the series resolves on the evening before (the old
    # contract) and rolls at the very first boundary, before any data.
    start_roll = None
    if initial[-1][0] == start and len(initial) > 1 and initial[-2][1] != initial[-1][1]:
        start_roll = {"prev": initial[-2][1], "current": initial[-1][1], "first_session": start}
    return {"initial": initial[-1][1], "rolls": rolls, "final": previous,
            "start_roll": start_roll}


# --------------------------------------------------------------------------
# Reconciliation
# --------------------------------------------------------------------------

def close_stamp(first_session: str) -> dt.datetime:
    """When a roll is stamped: 17:00 New York (the CME close) on the last weekday
    before the new contract's first session (Friday for a Monday first session)."""
    day = dt.date.fromisoformat(first_session) - dt.timedelta(days=1)
    while day.weekday() >= 5:
        day -= dt.timedelta(days=1)
    return dt.datetime.combine(day, dt.time(17, 0), tzinfo=ET)


def et_label(ts_ns: int) -> str:
    local = dt.datetime.fromtimestamp(ts_ns / 1e9, tz=ET)
    return local.strftime("%Y-%m-%d %H:%M:%S %Z")


def analyze_series(state: dict[str, Any], series: str, start: str,
                   end: str) -> tuple[dict[str, bool], dict[str, Any]]:
    """Checks for one continuous series; returns (checks, detail)."""
    record = state["series"][series]
    expected = expected_schedule(series, start, end)
    rolls = record["rolls"]
    post_observations = record["post"]
    segments = record["segments"]
    tag = series.replace(".", "_")
    # Accept the start-boundary roll (see expected_schedule) only as the first
    # callback, before the first delivered print, and set it aside: nothing was
    # held and nothing was delivered on its old contract.
    start_roll_ok = True
    start_roll = expected["start_roll"]
    if (start_roll and rolls
            and [rolls[0]["prev_contract"], rolls[0]["current_contract"]]
            == [start_roll["prev"], start_roll["current"]]):
        start_roll_ok = bool(segments) and rolls[0]["payload_ts"] <= segments[0][1]
        rolls = rolls[1:]
        post_observations = post_observations[1:]
    pairs = [[row["prev_contract"], row["current_contract"]] for row in rolls]
    expected_pairs = [[row["prev"], row["current"]] for row in expected["rolls"]]
    delivered = [segment[0] for segment in segments]
    expected_contracts = [expected["initial"]] + [row["current"] for row in expected["rolls"]]

    # The roll is stamped at the market close (17:00 New York, 22:00 UTC in standard
    # time and 21:00 UTC in daylight time) before the new contract's first session,
    # whatever session the run is configured with; data and position switch at the
    # run's own session boundary (checked by callback order above and below).
    stamp_ok = True
    roll_times = []
    for index, row in enumerate(rolls):
        roll_ts = row["payload_ts"]
        roll_times.append(et_label(roll_ts) if roll_ts else "0")
        if index < len(expected["rolls"]):
            expected_stamp = close_stamp(expected["rolls"][index]["first_session"])
            if roll_ts != int(expected_stamp.timestamp()) * 1_000_000_000:
                stamp_ok = False

    post_ok = True
    post_detail = []
    for observation, row in zip(post_observations, rolls):
        prints = observation["prints"]
        first = prints[0] if prints else None
        post_detail.append(first)
        if first is None:
            # A roll at the very end of the window has nothing after it.
            post_ok = post_ok and observation["roll"] == len(rolls) - 1 and row is rolls[-1]
            continue
        current = row["current_contract"]
        if (first["symbol"] != current
                or first["instrument_current"] != current
                or first["positions"] != {current: 1.0}
                or abs(first["net_series"] - 1.0) > 1e-9):
            post_ok = False

    events = state.get("security_events", [])
    due = [e for e in events if e["phase"] == "ROLLOVER_DUE"
           and series in (e["continuous_symbol"], e["symbol"])]
    complete = [e for e in events if e["phase"] == "ROLLOVER_COMPLETE"
                and series in (e["continuous_symbol"], e["symbol"])]
    failed = [e for e in events if e["phase"] == "ROLLOVER_FAILED"
              and root_of(e["symbol"]) == root_of(series)]
    due_ok = all(
        any(e["rollover_symbol"] == row["current_contract"] and 0 < e["ts"] <= row["payload_ts"]
            for e in due)
        for row in rolls
    )
    complete_ok = all(
        any(e["symbol"] in (row["current_contract"], series) and e["ts"] >= row["payload_ts"]
            for e in complete)
        for row in rolls
    )

    final_positions = record.get("final_positions", {})
    checks = {
        f"{tag}_prints_delivered": bool(segments),
        f"{tag}_start_boundary_roll_before_data": start_roll_ok,
        f"{tag}_roll_count_exact": len(rolls) == len(expected_pairs),
        f"{tag}_roll_pairs_match_definitions": pairs == expected_pairs,
        f"{tag}_contract_sequence_matches": delivered == expected_contracts,
        f"{tag}_every_contract_traded": all(segment[3] > 0 for segment in segments),
        f"{tag}_payload_complete": all(
            row["continuous_symbol"] == series and row["prev_contract"]
            and row["current_contract"] and row["prev_contract"] != row["current_contract"]
            for row in rolls),
        f"{tag}_timestamps_set": all(
            row["payload_ts"] > 0 and row["payload_ts"] == row["event_ts"] for row in rolls),
        f"{tag}_every_print_on_callback_contract": record.get("off_contract_prints", 0) == 0,
        f"{tag}_roll_stamped_at_close_ny": stamp_ok,
        f"{tag}_position_and_instrument_follow_roll": post_ok,
        f"{tag}_instrument_names_delivered_contract": record["instrument_mismatch"] == 0,
        f"{tag}_rollover_due_before_each_roll": due_ok,
        f"{tag}_rollover_complete_after_each_roll": complete_ok,
        f"{tag}_no_rollover_failed": not failed,
        f"{tag}_final_position_on_final_contract": final_positions == {expected["final"]: 1.0},
        f"{tag}_final_instrument_current": record.get("final_instrument_current") == expected["final"],
    }
    detail = {
        "series": series,
        "expected_pairs": expected_pairs,
        "rolls": pairs,
        "roll_times_et": roll_times,
        "delivered": [[s[0], et_label(s[1]), et_label(s[2]), s[3]] for s in segments],
        "post_roll_first_print": post_detail,
        "instrument_mismatch_examples": record["instrument_mismatch_examples"],
        "off_contract_examples": record.get("off_contract_examples", []),
        "due": len(due), "complete": len(complete), "failed": failed[:3],
        "final_positions": final_positions,
    }
    return checks, detail


def analyze(state: dict[str, Any], start: str,
            end: str) -> tuple[dict[str, bool], dict[str, Any]]:
    checks: dict[str, bool] = {}
    detail: dict[str, Any] = {}
    for series in state["series"]:
        series_checks, series_detail = analyze_series(state, series, start, end)
        checks.update(series_checks)
        detail[series] = series_detail
    fractional = [fill for fill in state.get("fills", [])
                  if abs(fill[2] - round(fill[2])) > 1e-9]
    checks["no_unknown_rollovers"] = not state.get("unknown_rollovers")
    checks["no_rejected_orders"] = not state.get("rejects")
    checks["integer_fills_only"] = not fractional
    detail["rejects"] = state.get("rejects", [])[:5]
    detail["unknown_rollovers"] = state.get("unknown_rollovers", [])[:5]
    return checks, detail
