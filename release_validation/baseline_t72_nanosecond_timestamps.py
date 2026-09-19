"""Nanosecond fidelity of the market-data timestamps handed to a strategy.

Every event surface documents ``ts_event`` as "Event timestamp in nanoseconds",
and the upstream feeds really do carry nanoseconds -- DataBento natively, the
HiveQ/ClickHouse API as nine fractional digits, Activ via
``Time::GetTotal<Nanoseconds>()``. Nothing in the suite checks that the
resolution survives the trip to the strategy, so a silent truncation anywhere in
the chain would look exactly like a correct run.

Truncation is easy to reintroduce and invisible without a test like this. The
engine carries event time in a single field, ``Tau::DateTime``, which reaches
nanosecond resolution only because boost posix_time is built with
``BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG``. Drop that macro from any one target
and boost falls back to microsecond ticks: ``timeSinceEpoch()`` then returns a
microsecond value multiplied by 1000, which *looks* like nanoseconds while its
low three digits are structurally always zero. The feed parsers can regress the
same way by rounding on the way in, as each of them once did.

The check is statistical rather than per-event, because a genuinely
nanosecond-precise timestamp may land on a round microsecond by chance. Across
thousands of events, though, *every* timestamp ending in three zeros is
conclusive. So each stream is classified by the finest unit that explains every
timestamp observed:

    nanosecond   some ts_event % 1_000 != 0
    microsecond  none of the above, but some ts_event % 1_000_000 != 0
    millisecond  none of the above, but some ts_event % 1_000_000_000 != 0
    second       every timestamp lands on a whole second

Magnitude is verified separately: a microsecond value mislabelled as
nanoseconds is ~1e15 for a contemporary date and a millisecond value ~1e12,
both far below the ~1.7e18 a real epoch-nanosecond timestamp must be. That
catches a unit mix-up which the resolution test alone would report merely as
"second" resolution.

Bars are sampled for magnitude only. A bar timestamp is an aggregation
boundary, so it is legitimately round and carries no feed resolution.
"""
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import (                                               # noqa: E402
    completed_checkpoint,
    emit_checkpoint,
    finish,
)

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import BacktestConfig, StrategyConfig                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402

SYMBOL = "ES.c.0"

# Resolution can only be asserted against a feed that actually stores
# sub-millisecond values. Measured in ClickHouse (2026-09):
#   market_data_futures.fut_trades   millisecond -- every value ends in 6 zeros
#   market_data_equity.eq_trades     millisecond -- 242M rows over 2026-09-09/10,
#                                    zero sub-microsecond timestamps
#   prod_imbalances_001.*            millisecond
#   prod_quant_001.signals           MICROSECOND (DateTime64(6)) -- the only
#                                    surface HiveQ actually consumes that
#                                    carries sub-millisecond detail
# So the trade/quote streams are kept for delivery and magnitude only, and the
# resolution assertion runs against the hosted-signal stream. A signal row is
# custom data (on_custom_data), a different surface from on_trade/on_quote, but
# it shares Tau::DateTime and therefore still catches the boost microsecond-tick
# regression this test exists to find.
SIGNAL_DATA_ID = "NanoSignals"
SIGNAL_NAME = "imb_decay_v2_hiveq_test"

# The signal feed is DateTime64(6); microsecond is the finest it can be. Anything
# coarser than this means something truncated on the way to the strategy.
SUB_MILLISECOND = ("nanosecond", "microsecond")

# Epoch nanoseconds must be ~1.7e18 for a contemporary date. 1e18 is 2001-09-09,
# which is below any date this suite runs and far above a microsecond (~1e15) or
# millisecond (~1e12) value that has been mislabelled as nanoseconds.
NANOS_FLOOR = 1_000_000_000_000_000_000

# Timestamps are only conclusive in bulk; a handful could be round by chance.
MIN_SAMPLE = 200


def new_stream():
    return {"count": 0, "sub_micro": 0, "sub_milli": 0, "sub_second": 0,
            "bad_magnitude": 0, "zero": 0, "samples": []}


def observe(stream, ts):
    """Fold one timestamp into a stream's resolution tally."""
    stream["count"] += 1
    ts = int(ts or 0)
    if ts == 0:
        stream["zero"] += 1
        return
    if ts < NANOS_FLOOR:
        stream["bad_magnitude"] += 1
    if ts % 1_000:
        stream["sub_micro"] += 1
    if ts % 1_000_000:
        stream["sub_milli"] += 1
    if ts % 1_000_000_000:
        stream["sub_second"] += 1
    if len(stream["samples"]) < 5:
        stream["samples"].append(ts)


def resolution_of(stream):
    """Finest unit that explains every timestamp seen on this stream."""
    if stream["count"] == 0:
        return "no-data"
    if stream["count"] == stream["zero"]:
        return "always-zero"
    if stream["sub_micro"]:
        return "nanosecond"
    if stream["sub_milli"]:
        return "microsecond"
    if stream["sub_second"]:
        return "millisecond"
    return "second"


class SdkT72:
    def on_start(self, ctx, event):
        if not hasattr(self, "state"):
            self.state = {"streams": {}, "errors": []}
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.FUTURES)
        ctx.subscribe_quotes([SYMBOL], asset_type=AssetType.FUTURES)
        ctx.subscribe_data(SIGNAL_DATA_ID)

    def record(self, name, ts):
        stream = self.state["streams"].setdefault(name, new_stream())
        observe(stream, ts)

    def capture(self, kind, event):
        """Sample both timestamp surfaces: the Event wrapper and its payload.

        They are separate code paths, so one can be truncated while the other
        is not -- checking only the payload would miss that.
        """
        try:
            self.record(kind + ".event", getattr(event, "ts_event", 0))
            data = event.data()
            self.record(kind + ".data", getattr(data, "ts_event", 0))
        except Exception as exc:                       # pragma: no cover
            if len(self.state["errors"]) < 5:
                self.state["errors"].append(f"{kind}: {exc!r}")

    def on_trade(self, ctx, event):
        self.capture("trade", event)

    def on_quote(self, ctx, event):
        self.capture("quote", event)

    def on_bar(self, ctx, event):
        self.capture("bar", event)

    def on_custom_data(self, ctx, event):
        """Sample the signal row's event time.

        Unlike a trade or quote, a custom-data payload is a column bag and is
        not guaranteed to expose ts_event, so a missing payload surface is
        skipped rather than recorded as a zero -- recording 0 would trip
        no_zero_timestamps for a shape difference rather than a real defect.
        """
        try:
            self.record("signal.event", getattr(event, "ts_event", 0))
            data = event.data()
            payload_ts = getattr(data, "ts_event", None)
            if payload_ts:
                self.record("signal.data", payload_ts)
        except Exception as exc:                       # pragma: no cover
            if len(self.state["errors"]) < 5:
                self.state["errors"].append(f"signal: {exc!r}")

    def on_stop(self, ctx, event):
        for name, stream in self.state["streams"].items():
            stream["resolution"] = resolution_of(stream)
        emit_checkpoint(ctx, "t72_nanosecond_timestamps", self.state)


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT72", type="SdkT72",
                                         symbols=[SYMBOL])],
        symbols=[SYMBOL],
        start_date="2026-03-16", end_date="2026-03-16",
        data_configs=[
            {"type": "hiveq_historical", "dataset": "HIVEQ_US_FUT",
             "schema": ["fut_trades"]},
            # 1240 rows that session, 871 of them carrying sub-millisecond
            # detail -- well past MIN_SAMPLE and conclusive in bulk.
            {"type": "hiveq_historical", "dataset": "HIVEQ_QUANT_SIGNALS",
             "schema": ["signals"], "id": SIGNAL_DATA_ID,
             "symbols": [SIGNAL_NAME]},
        ],
        # Signals run 17:59-20:00 UTC that day; futures cover the same window.
        backtest_config=BacktestConfig(session_start="13:55",
                                       session_end="16:00"),
    )
    state = completed_checkpoint(run, "t72_nanosecond_timestamps")
    streams = state.get("streams", {})

    def stream(name):
        return streams.get(name, new_stream())

    # Resolution is only asserted for streams that carry a feed timestamp.
    # Bars are aggregation boundaries and are checked for magnitude only.
    tick_streams = {name: data for name, data in streams.items()
                    if not name.startswith("bar.")}
    sampled = {name: data for name, data in tick_streams.items()
               if data["count"] >= MIN_SAMPLE}
    # Only the signal feed stores sub-millisecond values; asserting resolution
    # on a millisecond-grade trades feed would fail a perfectly healthy engine.
    resolution_streams = {name: data for name, data in sampled.items()
                          if name.startswith("signal.")}

    checks = {
        "trades_delivered": stream("trade.data")["count"] > 0,
        "quotes_delivered": stream("quote.data")["count"] > 0,
        # Stated separately so "the signal feed produced nothing here" cannot be
        # misread as "the engine truncated". Without this, both surface only as
        # nanosecond_resolution_preserved=FAIL.
        "signals_delivered": stream("signal.event")["count"] > 0,
        "sample_large_enough": bool(resolution_streams),
        "no_capture_errors": not state.get("errors"),
        # A zero timestamp means the field was never populated at all.
        "no_zero_timestamps": all(data["zero"] == 0
                                  for data in tick_streams.values()),
        # Catches a microsecond or millisecond value mislabelled as nanoseconds.
        "ts_event_is_epoch_nanos": all(data["bad_magnitude"] == 0
                                       for data in streams.values()),
        # The regression detector: whatever the source stores must reach the
        # strategy intact. This is the check that caught HiveQUserDataAdapter
        # capping the fraction at "HH:MM:SS.mmm" and losing 871 of 1240
        # microsecond-bearing signal rows.
        "sub_millisecond_resolution_preserved": bool(resolution_streams) and all(
            resolution_of(data) in SUB_MILLISECOND
            for data in resolution_streams.values()),
        # Strict on purpose: a PASS here must mean nanosecond fidelity was
        # actually observed end to end. No feed HiveQ consumes stores
        # nanoseconds today -- eq_trades/fut_trades are millisecond in storage
        # (242M rows over 2026-09-09/10 with zero sub-microsecond values) and
        # hosted signals are DateTime64(6) -- so this stays red until the
        # ingestion write path stops rounding. Deliberately NOT relaxed to the
        # source's ceiling: a green line naming nanoseconds would assert
        # something that was never verified.
        "nanosecond_resolution_preserved": bool(resolution_streams) and all(
            resolution_of(data) == "nanosecond"
            for data in resolution_streams.values()),
    }

    detail = {name: {"n": data["count"], "resolution": resolution_of(data),
                     "sub_micro": data["sub_micro"], "samples": data["samples"][:2]}
              for name, data in sorted(streams.items())}
    finish("t72_nanosecond_timestamps", checks,
           extra=f"errors={state.get('errors')}; {detail}")
