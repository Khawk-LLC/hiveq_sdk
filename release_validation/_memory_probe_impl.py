"""Probe: a full week of `eq_trades` plus `nasd_imbalance` across a 100-name universe.

Report-only.  It measures what the platform actually delivers for a wide
universe over a multi-day range — per-symbol tick counts, per-day counts,
Nasdaq imbalance coverage, and payload sanity — instead of gating a release.
Symbols that deliver nothing are reported as coverage gaps, not failures.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import checkpoint, emit_checkpoint, export_run_artifacts, wait_for_final

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType, EventType
from hiveq.flow.logger import logger as _get_logger

logger = _get_logger()

NAME = "probe_universe_trades_imbalance"


def _fmt_ns_ts(ns: int) -> str:
    """Format an event timestamp (ns since epoch) as an ISO string in UTC."""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).isoformat(timespec="seconds")

SYMBOLS = [
    "AAL", "AAOI", "AAPL", "ACWI", "ADBE", "ADI", "ADSK", "ALAB", "AMAT", "AMD",
    "AMGN", "AMKR", "AMZN", "APLD", "APP", "ARM", "ASML", "ASTS", "AVGO", "AXTI",
    "BKNG", "CBRS", "CEG", "CIFR", "CMCSA", "CME", "COIN", "COST", "CRDO", "CRWD",
    "CRWV", "CSCO", "DDOG", "FLEX", "FTNT", "GILD", "GOOG", "GOOGL", "HON", "HOOD",
    "IBIT", "IEF", "INTC", "INTU", "IREN", "ISRG", "KLAC", "LIN", "LITE", "LRCX",
    "MCHP", "MELI", "META", "MPWR", "MRVL", "MSFT", "MSTR", "MU", "MUU", "NBIS",
    "NFLX", "NUVL", "NVDA", "NVTS", "NXPI", "ON", "PANW", "PEP", "PLTR", "QCOM",
    "QQQ", "QQQM", "RGTI", "RKLB", "ROKU", "ROST", "SATS", "SHOP", "SMCI", "SMH",
    "SNDK", "SOFI", "SOXX", "SQQQ", "STX", "TER", "TLT", "TMUS", "TQQQ", "TSLA",
    "TSLL", "TTD", "TXN", "UAL", "VCSH", "WDAY", "WDC", "WMT", "XOVR", "ZS",
]


class SdkProbeUniverse:
    """Counts every equity trade tick and Nasdaq imbalance row it is handed.

    Per-callback work stays to dict/int updates: a week of tick data for 100
    names is tens of millions of callbacks, so formatting or object building
    per event would dominate the run.
    """

    # Sim-time cadence for the liveness heartbeat: 30 min of *market time*
    # advanced by the tick stream, NOT wall-clock. Sim time is what actually
    # measures backtest progress — a slow machine can't fake it, and a hang
    # shows up as heartbeats stopping (because ticks stop being delivered).
    # WARNING level so it survives the engine's default WARNING filter.
    _HEARTBEAT_INTERVAL_NS = 30 * 60 * 1_000_000_000

    def __init__(self):
        # State lives on the instance (not in on_start) because on_start fires
        # once per calendar day and the counts must accumulate across the week.
        self.state = {
            "requested_symbols": len(SYMBOLS),
            "starts": [],
            "trades": 0,
            "imbalances": 0,
            "bad_trades": 0,
            "bad_imbalances": 0,
            "trade_counts": {},
            "trade_volume": {},
            "imbalance_counts": {},
            "imbalance_sides": {},
            "imbalance_cross_types": {},
            "imbalance_ref_price_populated": 0,
            "imbalance_near_price_populated": 0,
            "imbalance_far_price_populated": 0,
            "days": [],
            "off_universe_symbols": [],
            "first_trade": None,
            "last_trade_ts": None,
            "first_imbalance": None,
            "last_imbalance_ts": None,
            "imbalance_samples": [],
        }
        self._universe = set(SYMBOLS)
        self._day = None
        self._run_start_wall = time.monotonic()
        self._last_heartbeat_ts = None
        self._last_heartbeat_wall = self._run_start_wall
        self._events_at_last_heartbeat = 0

    def _maybe_heartbeat(self, ctx, ts_event):
        """Emit a heartbeat every _HEARTBEAT_INTERVAL_NS of *sim* (tick) time.

        Cheap enough to call on every event: an int subtract and compare, no
        syscalls. Wall-time is also captured on emit so we can spot slow-throughput
        runs (long wall gap between two heartbeats that are 30 min sim-time apart).
        """
        if self._last_heartbeat_ts is None:
            self._last_heartbeat_ts = ts_event
            return
        if ts_event - self._last_heartbeat_ts < self._HEARTBEAT_INTERVAL_NS:
            return
        state = self.state
        total = state["trades"] + state["imbalances"]
        delta = total - self._events_at_last_heartbeat
        now = time.monotonic()
        wall_window = now - self._last_heartbeat_wall
        rate = delta / wall_window if wall_window > 0 else 0.0
        wall_total = int(now - self._run_start_wall)
        sim_now_iso = _fmt_ns_ts(ts_event)
        day = (self._day or {}).get("day")
        logger.warning(
            f"[HEARTBEAT] sim_now={sim_now_iso} day={day} "
            f"wall={wall_total // 3600:d}h{(wall_total % 3600) // 60:02d}m "
            f"trades={state['trades']:,} imbalances={state['imbalances']:,} "
            f"events_since_last={delta:,} rate={rate:,.0f}/s"
        )
        self._last_heartbeat_ts = ts_event
        self._last_heartbeat_wall = now
        self._events_at_last_heartbeat = total

    def on_start(self, ctx, event):
        day = str(ctx.now().date())
        self.state["starts"].append(day)
        self._day = {"day": day, "trades": 0, "imbalances": 0, "symbols_seen": 0}
        self.state["days"].append(self._day)
        self._day_symbols = set()
        # Re-subscribing each session day is the proven multi-day pattern; the
        # engine treats successive subscriptions as a union, not a duplicate feed.
        ctx.subscribe_trades(SYMBOLS, asset_type=AssetType.EQUITY)
        logger.info(f"[START] {day}: subscribed {len(SYMBOLS)} equity trade streams")

    def on_trade(self, ctx, event):
        trade = event.data()
        symbol = trade.symbol
        state = self.state
        state["trades"] += 1
        counts = state["trade_counts"]
        counts[symbol] = counts.get(symbol, 0) + 1
        volume = state["trade_volume"]
        volume[symbol] = volume.get(symbol, 0.0) + float(trade.size)
        state["last_trade_ts"] = int(trade.ts_event)
        if trade.price <= 0 or trade.size <= 0 or trade.ts_event <= 0:
            state["bad_trades"] += 1
        if symbol not in self._universe and symbol not in state["off_universe_symbols"]:
            state["off_universe_symbols"].append(symbol)
        if self._day is not None:
            self._day["trades"] += 1
            if symbol not in self._day_symbols:
                self._day_symbols.add(symbol)
                self._day["symbols_seen"] = len(self._day_symbols)
        if state["first_trade"] is None:
            state["first_trade"] = {
                "symbol": symbol, "price": float(trade.price),
                "size": float(trade.size), "ts_event": int(trade.ts_event),
                "aggressor_side": str(trade.aggressor_side),
            }
        self._maybe_heartbeat(ctx, int(trade.ts_event))

    def on_imbalance(self, ctx, event):
        data = event.data()
        symbol = data.symbol
        state = self.state
        state["imbalances"] += 1
        counts = state["imbalance_counts"]
        counts[symbol] = counts.get(symbol, 0) + 1
        side = str(data.side)
        sides = state["imbalance_sides"]
        sides[side] = sides.get(side, 0) + 1
        cross = str(data.cross_type)
        cross_types = state["imbalance_cross_types"]
        cross_types[cross] = cross_types.get(cross, 0) + 1
        if data.ref_price is not None:
            state["imbalance_ref_price_populated"] += 1
        if data.near_price is not None:
            state["imbalance_near_price_populated"] += 1
        if data.far_price is not None:
            state["imbalance_far_price_populated"] += 1
        state["last_imbalance_ts"] = int(event.ts_event)
        valid = (
            event.type == EventType.IMBALANCE
            and bool(side)
            and isinstance(data.imbalance, (int, float))
            and isinstance(data.paired_shares, (int, float)) and data.paired_shares >= 0
            and isinstance(event.ts_event, int) and event.ts_event > 0
        )
        if not valid:
            state["bad_imbalances"] += 1
        if self._day is not None:
            self._day["imbalances"] += 1
        sample = None
        if state["first_imbalance"] is None or len(state["imbalance_samples"]) < 10:
            sample = {
                "row": state["imbalances"], "symbol": symbol, "side": side,
                "imbalance": data.imbalance, "paired_shares": data.paired_shares,
                "ref_price": data.ref_price, "near_price": data.near_price,
                "far_price": data.far_price, "cross_type": data.cross_type,
                "ts_event": int(event.ts_event),
            }
        if state["first_imbalance"] is None:
            state["first_imbalance"] = sample
        elif sample is not None and state["imbalances"] % 500 == 0:
            state["imbalance_samples"].append(sample)
        self._maybe_heartbeat(ctx, int(event.ts_event))

    def on_stop(self, ctx, event):
        # on_stop fires once, when the engine STOPS (not per session day), so
        # this single emission carries the whole accumulated week.
        elapsed = int(time.monotonic() - self._run_start_wall)
        logger.warning(
            f"[HEARTBEAT/FINAL] elapsed={elapsed // 3600:d}h{(elapsed % 3600) // 60:02d}m "
            f"trades={self.state['trades']:,} imbalances={self.state['imbalances']:,} "
            f"days={len(self.state['starts'])}"
        )
        emit_checkpoint(ctx, NAME, self.state)


def _report(state: dict, start: str, end: str) -> dict:
    trade_counts = state.get("trade_counts", {})
    imbalance_counts = state.get("imbalance_counts", {})
    silent_trades = [s for s in SYMBOLS if not trade_counts.get(s)]
    silent_imbalance = [s for s in SYMBOLS if not imbalance_counts.get(s)]
    top = sorted(trade_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
    return {
        "probe": NAME,
        "start_date": start,
        "end_date": end,
        "requested_symbols": len(SYMBOLS),
        "session_days": len(state.get("starts", [])),
        "trades": state.get("trades", 0),
        "imbalances": state.get("imbalances", 0),
        "symbols_with_trades": len(SYMBOLS) - len(silent_trades),
        "symbols_with_imbalance": len(SYMBOLS) - len(silent_imbalance),
        "symbols_without_trades": silent_trades,
        "symbols_without_imbalance": silent_imbalance,
        "off_universe_symbols": state.get("off_universe_symbols", []),
        "bad_trade_payloads": state.get("bad_trades", 0),
        "bad_imbalance_payloads": state.get("bad_imbalances", 0),
        "top_symbols_by_trades": top,
        "per_day": state.get("days", []),
        "imbalance_sides": state.get("imbalance_sides", {}),
        "imbalance_cross_types": state.get("imbalance_cross_types", {}),
        "imbalance_ref_price_populated": state.get("imbalance_ref_price_populated", 0),
        "imbalance_near_price_populated": state.get("imbalance_near_price_populated", 0),
        "imbalance_far_price_populated": state.get("imbalance_far_price_populated", 0),
        "first_trade": state.get("first_trade"),
        "first_imbalance": state.get("first_imbalance"),
        "imbalance_samples": state.get("imbalance_samples", []),
        "trade_counts": trade_counts,
        "trade_volume": state.get("trade_volume", {}),
        "imbalance_counts": imbalance_counts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    # One trading week (Mon-Fri) by default; override for a different window.
    parser.add_argument("--start", default="2026-08-10", help="start date YYYY-MM-DD")
    parser.add_argument("--end", default="2026-08-14", help="end date YYYY-MM-DD")
    parser.add_argument("--session-start", default=None,
                        help="ET HH:MM; default keeps the equity 04:00-18:30 window "
                             "so pre-open and closing-cross imbalance rows arrive")
    parser.add_argument("--session-end", default=None, help="ET HH:MM")
    parser.add_argument("--timeout", type=float, default=14400.0,
                        help="seconds to wait for the run (default 4h)")
    parser.add_argument("--out", default=None, help="report JSON path")
    args = parser.parse_args()

    backtest_config = None
    if args.session_start or args.session_end:
        backtest_config = BacktestConfig(session_start=args.session_start,
                                         session_end=args.session_end)

    print(f"PROBE: submitting {NAME} {args.start}..{args.end} "
          f"symbols={len(SYMBOLS)} schemas=eq_trades,nasd_imbalance", flush=True)
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkProbeUniverse",
                                         type="SdkProbeUniverse", symbols=SYMBOLS)],
        symbols=SYMBOLS,
        start_date=args.start,
        end_date=args.end,
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["eq_trades"]},
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["nasd_imbalance"]},
        ],
        backtest_config=backtest_config,
    )
    print(f"PROBE: run_id={getattr(run, 'run_id', None)} "
          f"task_id={getattr(run, 'task_id', None)}", flush=True)

    try:
        wait_for_final(run, timeout=args.timeout)
        state = checkpoint(run, NAME, timeout=180.0)
    except (AssertionError, TimeoutError) as exc:
        # A partial checkpoint is the useful artifact of a probe that died or
        # ran long, so surface it instead of only the failure.
        print(f"PROBE: run did not complete cleanly: {exc}", flush=True)
        try:
            state = checkpoint(run, NAME, timeout=10.0)
            print("PROBE: reporting the last partial checkpoint", flush=True)
        except Exception:
            raise SystemExit(1)

    report = _report(state, args.start, args.end)
    out = Path(args.out) if args.out else (
        Path(__file__).resolve().parent / "probe_reports"
        / f"{NAME}_{args.start}_{args.end}.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str))
    artifacts = export_run_artifacts(run, validation=report)

    print(f"PROBE: days={report['session_days']} "
          f"trades={report['trades']:,} imbalances={report['imbalances']:,}", flush=True)
    print(f"PROBE: coverage trades={report['symbols_with_trades']}/{len(SYMBOLS)} "
          f"imbalance={report['symbols_with_imbalance']}/{len(SYMBOLS)}", flush=True)
    print(f"PROBE: payload_errors trades={report['bad_trade_payloads']} "
          f"imbalance={report['bad_imbalance_payloads']}", flush=True)
    if report["symbols_without_trades"]:
        print(f"PROBE: no trade ticks for {report['symbols_without_trades']}", flush=True)
    if report["symbols_without_imbalance"]:
        print(f"PROBE: no nasd_imbalance rows for "
              f"{report['symbols_without_imbalance']}", flush=True)
    if report["off_universe_symbols"]:
        print(f"PROBE: symbols outside the request: "
              f"{report['off_universe_symbols']}", flush=True)
    for day in report["per_day"]:
        print(f"PROBE: {day['day']} trades={day['trades']:,} "
              f"imbalances={day['imbalances']:,} symbols={day.get('symbols_seen')}",
              flush=True)
    print(f"PROBE: top_by_trades={report['top_symbols_by_trades']}", flush=True)
    print(f"PROBE: imbalance_sides={report['imbalance_sides']} "
          f"cross_types={report['imbalance_cross_types']}", flush=True)
    print(f"PROBE: report={out}", flush=True)
    print(f"PROBE: run_artifacts={artifacts}", flush=True)


if __name__ == "__main__":
    main()
