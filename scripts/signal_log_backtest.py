#!/usr/bin/env python3
"""Signal log backtest — dump every HIVEQ_QUANT_SIGNALS row the engine delivers.

A no-orders probe. It subscribes to one hosted signal feed and writes every
custom-data row it receives to the event log, field by field, so the signal
path can be confirmed end to end before the run is promoted by hand.

    python scripts/signal_log_backtest.py

Reading the result: the SIGNAL / SIGNAL_ROW / SIGNAL_JSON lines land in the
run's event log (``run.logs()`` / the runs API), not on stdout.

Why the config looks the way it does
------------------------------------
Three names in the signal path are separate and are NOT interchangeable:

1. ``id`` on the ``data_configs`` entry is the *logical source id*. It is the
   only one ``ctx.subscribe_data(data_id=...)`` matches on, so the two strings
   must be identical.
2. ``symbols`` on that entry is the *historical selector* — the registered
   signal name in ``HIVEQ_QUANT_SIGNALS`` (e.g. ``Prillach_MC_ES``), not the
   tradable ticker. Omit it and the dataset's own default selection applies.
3. The tradable instrument lives inside the row: the signal fields are packed
   into the ``signal_json`` column, and ``ticker`` there is the instrument.
   ``data.symbol`` is the row's key (the signal name), so never assume it is a
   ticker — both are logged so a divergence stays visible.

Delivery is not symbol-routed: ``on_custom_data`` is filtered by ``event_id``
(what ``subscribe_data`` names), so every row on the subscribed source reaches
the callback.

The bars config is required as well as useful: the venue is initialized from
market data, and promotion re-reads dataset/schemas out of this same payload.
"""

import json
import os

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

SIGNAL_ID = "quant_signals"                       # must match subscribe_data
# The historical selector: a registered signal name in HIVEQ_QUANT_SIGNALS.
SIGNAL_KEYS = [k for k in os.environ.get(
    "SIGLOG_KEY", "Spy-ClusterDecay-5m-v2").split(",") if k]
SYMBOL = os.environ.get("SIGLOG_SYMBOL", "AAPL")
START = os.environ.get("SIGLOG_START", "2024-08-27")
END = os.environ.get("SIGLOG_END", START)
INSTANCE = os.environ.get("SIGLOG_INSTANCE", "signal_log")
# A busy feed is thousands of rows a day; dump this many in full, then count.
MAX_ROWS = int(os.environ.get("SIGLOG_MAX", "50"))
# Past the cap the log would otherwise go dark for the rest of the session, and
# a sparse feed and a hung strategy look identical from the outside. One compact
# line per this many seconds of feed time keeps the difference visible.
HEARTBEAT_SECS = int(os.environ.get("SIGLOG_HEARTBEAT", "300"))
# Fallback pulse when a row carries no usable ts_event.
HEARTBEAT_ROWS = int(os.environ.get("SIGLOG_HEARTBEAT_ROWS", "250"))

# Pulled out by name in the one-line summary when the feed carries them; a
# feed with different field names falls back to its own first few fields.
# Either way the full row still lands in the dump beneath it.
SUMMARY_FIELDS = ("ticker", "norm_ticker", "side", "size", "score", "flag",
                  "signal1", "weight1", "stop_px", "target_px", "text")
# Row plumbing, not signal content: never worth a slot in the one-liner.
META_FIELDS = ("Unnamed: 0", "date", "time", "sym", "symbol", "signal_json",
               "ts_event", "recv_time", "kx_recv_time", "kx_recv_date")


def summarize(fields, limit=8):
    """One line of the named signal fields, or of whatever the feed does send."""
    named = {k: v for k, v in fields.items() if k in SUMMARY_FIELDS}
    if not named:
        named = {k: v for k, v in fields.items() if k not in META_FIELDS}
    return " ".join(f"{k}={v}" for k, v in list(named.items())[:limit])


def decode_signal_json(raw):
    """Decode plain JSON or the Sigma CSV bridge's escaped representation."""
    return json.loads(raw.replace(r'\"', '"').replace("|", ","))


def _ts_ns(ts_event):
    """ts_event as nanoseconds since epoch; 0 when the row carries no clock."""
    try:
        return int(ts_event)
    except (TypeError, ValueError):
        return 0


class SignalLog:
    """Logs every custom-data row on SIGNAL_ID. Places no orders."""

    def __init__(self):
        self.rows = 0
        self.suppressed = 0       # rows seen after the cap, never dumped
        self.beats = 0
        self.last_beat_ns = 0

    def on_start(self, ctx, event):
        # Bars initialize the venue; the probe itself never trades.
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_data(data_id=SIGNAL_ID)
        ctx.add_event_log(
            f"signal log started id={SIGNAL_ID} keys={SIGNAL_KEYS}",
            sub_event_type="INIT")

    def on_custom_data(self, ctx, event):
        data = event.data()
        self.rows += 1
        if self.rows > MAX_ROWS:          # keep counting, stop flooding the log
            self.suppressed += 1
            ts = _ts_ns(data.ts_event)
            if ts:
                due = ts - self.last_beat_ns >= HEARTBEAT_SECS * 1_000_000_000
            else:                         # no clock on the row: count instead
                due = self.suppressed % HEARTBEAT_ROWS == 1
            if not due:
                return
            self.last_beat_ns = ts
            self.beats += 1
            ctx.add_event_log(
                f"HEARTBEAT rows={self.rows} suppressed={self.suppressed} "
                f"last={data.time} sym={data.symbol}",
                sub_event_type="HEARTBEAT", symbol=str(data.symbol))
            return
        row = data.data or {}
        summary = summarize(row)

        ctx.add_event_log(
            f"SIGNAL event_id={data.event_id} sym={data.symbol} "
            f"ts_event={data.ts_event} time={data.time} | {summary}",
            sub_event_type="SIGNAL", symbol=str(data.symbol))
        ctx.add_event_log(f"SIGNAL_ROW {row}",
                          sub_event_type="SIGNAL_ROW", symbol=str(data.symbol))

        # The tradable instrument and the signal fields live inside this column,
        # not in the row's own columns.
        raw = data.column_data("signal_json", default=None)
        if not raw:
            return
        try:
            sig = decode_signal_json(raw)
        except (json.JSONDecodeError, TypeError, AttributeError):
            ctx.add_event_log(f"SIGNAL_JSON unparsed {raw!r}",
                              sub_event_type="ERROR", symbol=str(data.symbol))
            return
        ticker = sig.get("ticker", data.symbol)
        detail = summarize(sig)
        ctx.add_event_log(f"SIGNAL_JSON ticker={ticker} | {detail} | {sig}",
                          sub_event_type="SIGNAL_JSON", symbol=str(ticker))

    def on_stop(self, ctx, event):
        ctx.add_event_log(f"signal log done rows={self.rows} logged="
                          f"{min(self.rows, MAX_ROWS)} suppressed={self.suppressed} "
                          f"beats={self.beats} id={SIGNAL_ID}",
                          sub_event_type="SUMMARY",
                          state_variable={"rows": self.rows})


if __name__ == "__main__":
    signal_config = {
        "type": "hiveq_historical",
        "dataset": "HIVEQ_QUANT_SIGNALS",
        "schema": ["signals"],
        "id": SIGNAL_ID,                       # == subscribe_data(data_id=...)
    }
    if SIGNAL_KEYS:
        signal_config["symbols"] = SIGNAL_KEYS

    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name=INSTANCE, type="SignalLog",
                                         symbols=[SYMBOL])],
        symbols=[SYMBOL],
        start_date=START,
        end_date=END,
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ",
             "schema": ["bars_1m"]},
            signal_config,
        ],
        backtest_config=BacktestConfig(start_date=START, end_date=END),
    )
    print(f"run_id={run.run_id} task_id={run.task_id}")
    run.wait(progress=False)
    print(f"status={run.status()}")
    print("promote this run_id by hand to carry the strategy onto a liveSim")
