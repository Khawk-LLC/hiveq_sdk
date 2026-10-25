"""Print every ES.c.0 / GC.c.0 one-minute OHLC bar over a short 2013 window.

This is a diagnostic probe, not a numbered release-validation case.  It uses
the public remote SDK path and records each received bar as a structured event
log, then prints those rows locally after the run finishes.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import event_logs_until

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()

SYMBOLS = ["ES.c.0", "GC.c.0"]
START_DATE = "2013-01-02"
END_DATE = "2013-01-04"
BAR_EVENT = "ES_GC_2013_BAR"
SUMMARY_EVENT = "ES_GC_2013_SUMMARY"
OUTPUT = Path(__file__).resolve().parent / "probe_reports" / "es_gc_2013_bars.csv"


class EsGc2013BarProbe:
    def __init__(self):
        self.counts = {symbol: 0 for symbol in SYMBOLS}

    def on_start(self, ctx, event):
        ctx.subscribe_bars(SYMBOLS, asset_type=AssetType.FUTURES, interval="1m")
        logger.info(
            "[START] subscribed 1m futures bars for %s; counts=%s",
            SYMBOLS,
            self.counts,
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        # Continuous subscriptions deliver the resolved outright in bar.symbol
        # (ESH3/GCF3 here), never the subscribed ES.c.0/GC.c.0 alias.
        series = next(
            (item for item in SYMBOLS if bar.symbol.startswith(item.split(".")[0])),
            bar.symbol,
        )
        self.counts[series] = self.counts.get(series, 0) + 1
        row = {
            "time": bar.time.isoformat(),
            "series": series,
            "contract": bar.symbol,
            "ts_event": int(bar.ts_event),
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
        }
        logger.debug(
            "[BAR] %s %s O=%s H=%s L=%s C=%s",
            row["time"], row["contract"], row["open"], row["high"],
            row["low"], row["close"],
        )
        ctx.add_event_log(
            json.dumps(row, separators=(",", ":")),
            sub_event_type=BAR_EVENT,
            symbol=bar.symbol,
            state_variable=row,
        )

    def on_stop(self, ctx, event):
        logger.info("[STOP] received bar counts=%s", self.counts)
        ctx.add_event_log(
            "bar probe summary",
            sub_event_type=SUMMARY_EVENT,
            state_variable={"counts": dict(self.counts)},
        )


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="EsGc2013BarProbe",
            type="EsGc2013BarProbe",
            symbols=SYMBOLS,
        )],
        symbols=SYMBOLS,
        start_date=START_DATE,
        end_date=END_DATE,
        data_configs=[{
            "type": "hiveq_historical",
            "dataset": "HIVEQ_US_FUT",
            "schema": ["bars_1m"],
        }],
        backtest_config=BacktestConfig(
            session_start="18:00",
            session_end="17:00",
        ),
    )
    run.wait(progress=False)

    event_logs = event_logs_until(run, SUMMARY_EVENT, timeout=180.0)
    bars = event_logs[event_logs["sub_event_type"] == BAR_EVENT].copy()
    bars = bars.sort_values(["ts_event", "symbol"])

    rows = [json.loads(message) for message in bars["message"]]
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=["time", "series", "contract", "open", "high", "low", "close"],
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

    print("time,series,contract,open,high,low,close")
    for row in rows:
        print(
            f'{row["time"]},{row["series"]},{row["contract"]},{row["open"]},'
            f'{row["high"]},{row["low"]},{row["close"]}'
        )

    counts = {
        symbol: sum(row["series"] == symbol for row in rows)
        for symbol in SYMBOLS
    }
    print(f"BAR_COUNTS={json.dumps(counts, sort_keys=True)}")
    print(f"CSV={OUTPUT}")
    print(f"RUN_ID={run.run_id}")
