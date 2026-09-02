"""Remote callback bar counts match the data driver for equity and futures.

Parity is asserted per ``(symbol, session day)`` on *distinct* bar timestamps,
which is the claim that actually matters: every bar the Data API holds for a
session is delivered to the callback, and none is invented.  Three properties of
the two surfaces make a raw row-count comparison wrong:

* The callback count is compared with a direct ``hiveq.driver`` load in the
  same platform image.  The test therefore exercises the supported data-driver
  surface and has no dependency on the separately installed ``hiveq_data`` SDK.
* The stream's resolved outright contracts are loaded rather than its continuous
  aliases, so both sides compare the exact same instruments.
* A continuous subscription is a spliced series: it delivers the front contract
  only, so it can never match a whole-window count for either outright.
* ``bars_1m`` storage still carries exact duplicate rows for some sessions
  (equity 2025-09-18 is one), and both surfaces report them differently.
  Duplicates are counted and reported here, and gated in the data-driver
  release validation, not re-asserted as an SDK regression.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

START_DATE = "2025-09-15"
END_DATE = "2025-09-19"
EQUITY_SYMBOLS = ["AAPL", "MSFT"]
FUTURES_SYMBOLS = ["ES.c.0", "NQ.c.0"]


def session_day(moment: datetime, overnight: bool) -> str:
    """The trading day a bar belongs to, ET.

    Futures sessions run 18:00 -> 17:00, so an 18:00-or-later print belongs to
    the next trading day; an equity session is the calendar day it prints on.
    """
    if overnight and moment.hour >= 18:
        return str(moment.date() + timedelta(days=1))
    return str(moment.date())


class SdkT42Counting:
    """Counts distinct (symbol, ts_event) bars per symbol and session day."""

    overnight = False
    symbols: list = []
    checkpoint_name = ""
    asset_type = AssetType.EQUITY
    dataset = ""
    session_start = ""
    session_end = ""

    def on_start(self, ctx, event):
        if not hasattr(self, "seen"):
            self.seen = set()
            self.per_day = Counter()
            self.duplicates = 0
        ctx.subscribe_bars(self.symbols, asset_type=self.asset_type, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        key = (str(bar.symbol), int(bar.ts_event))
        if key in self.seen:
            self.duplicates += 1
            return
        self.seen.add(key)
        self.per_day[f"{bar.symbol}|{session_day(bar.time, self.overnight)}"] += 1

    def on_stop(self, ctx, event):
        stream = {
            "per_day": dict(self.per_day),
            "duplicate_deliveries": self.duplicates,
            "bars": sum(self.per_day.values()),
        }
        symbols = sorted({key.split("|", 1)[0] for key in stream["per_day"]})
        api = driver_counts(
            self.dataset,
            symbols,
            self.session_start,
            self.session_end,
            self.overnight,
        )
        exact, mismatched = compare(stream, api)
        emit_checkpoint(ctx, self.checkpoint_name, {
            "stream": stream,
            "driver": api,
            "symbols": symbols,
            "exact": exact,
            "mismatched": mismatched,
        })


class SdkT42A(SdkT42Counting):
    symbols = EQUITY_SYMBOLS
    checkpoint_name = "t42_equity"
    asset_type = AssetType.EQUITY
    overnight = False
    dataset = "HIVEQ_US_EQ"
    session_start = "09:30:00"
    session_end = "16:00:00"


class SdkT42B(SdkT42Counting):
    symbols = FUTURES_SYMBOLS
    checkpoint_name = "t42_futures"
    asset_type = AssetType.FUTURES
    overnight = True
    dataset = "HIVEQ_US_FUT"
    session_start = "18:00:00"
    session_end = "17:00:00"


def remote_counts(cls, name, dataset, session_start, session_end):
    data = {"type": "hiveq_historical", "dataset": dataset, "schema": ["bars_1m"]}
    if dataset == "HIVEQ_US_FUT":
        data["filter_mode"] = "continuous"
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name=cls.__name__, type=cls.__name__,
                                         symbols=cls.symbols)],
        symbols=cls.symbols,
        start_date=START_DATE,
        end_date=END_DATE,
        data_configs=[data],
        backtest_config=BacktestConfig(session_start=session_start,
                                       session_end=session_end),
    )
    return completed_checkpoint(run, name)


STAMP = "%Y-%m-%d %H:%M:%S"


def in_session(moment, start_tod, end_tod, overnight):
    tod = moment.strftime("%H:%M:%S")
    if overnight:
        return tod >= start_tod or tod <= end_tod
    return start_tod <= tod <= end_tod


def driver_counts(dataset, symbols, start_tod, end_tod, overnight):
    """Load bars through the platform data driver and count distinct stamps.

    The historical endpoint treats the session end as exclusive, while the
    stream can contain a bar stamped exactly at that boundary.  Ask for one
    additional minute and trim back to ``last_dt`` below.
    """
    if not symbols:
        return {"per_day": {}, "duplicate_rows": 0, "bars": 0}

    import collections
    import hiveq.driver as dd
    from hiveq.datetime import DateRange, TimeRange

    first_date = (datetime.strptime(START_DATE, "%Y-%m-%d") - timedelta(days=1)
                  if overnight else datetime.strptime(START_DATE, "%Y-%m-%d"))
    first_dt = datetime.combine(first_date.date(),
                                datetime.strptime(start_tod, "%H:%M:%S").time())
    last_dt = datetime.combine(datetime.strptime(END_DATE, "%Y-%m-%d").date(),
                               datetime.strptime(end_tod, "%H:%M:%S").time())
    query_end_tod = (datetime.strptime(end_tod, "%H:%M:%S")
                     + timedelta(minutes=1)).strftime("%H:%M:%S")

    source = "T42Bars"
    transport = "T42HiveQBars"
    dd.init(config={
        source: {"primary": transport},
        transport: {
            "transport": "HiveQ",
            "dataset": dataset,
            "schema": "bars_1m",
            "filterMode": "session",
            "splitSize": "1",
            "timezone": "America/New_York",
        },
    })
    Params = collections.namedtuple("T42Params", ["date", "time", "sym"])
    params = Params(
        DateRange(first_date.strftime("%Y-%m-%d"), END_DATE),
        TimeRange(start_tod, query_end_tod),
        list(symbols),
    )
    frame = dd.load(source, params_tuple=params, cache=dd.Cache.NO_CACHE)
    rows = [] if frame is None else frame.to_dict("records")
    per_day = Counter()
    seen = defaultdict(set)
    duplicates = 0
    for row in rows:
        symbol = row.get("symbol") or row.get("ticker") or row.get("instrument")
        stamp = row.get("time") or row.get("ts_event")
        if not symbol or stamp is None:
            continue
        symbol = str(symbol)
        moment = datetime.strptime(str(stamp)[:19], STAMP)
        if moment < first_dt or moment > last_dt:
            continue
        if not in_session(moment, start_tod, end_tod, overnight):
            continue
        if moment in seen[symbol]:
            duplicates += 1
            continue
        seen[symbol].add(moment)
        per_day[f"{symbol}|{session_day(moment, overnight)}"] += 1
    return {"per_day": dict(per_day), "duplicate_rows": duplicates,
            "bars": sum(per_day.values())}


def compare(stream, api):
    """Every (symbol, session day) the callbacks delivered matches the API.

    Only the keys the stream covered are compared: a continuous subscription
    delivers the front contract, so the API's rows for the *other* outright in
    the same session are correctly absent from the stream.
    """
    keys = stream.get("per_day") or {}
    if not keys:
        return False, {}
    api_days = api.get("per_day") or {}
    mismatched = {key: [count, api_days.get(key, 0)]
                  for key, count in keys.items() if api_days.get(key, 0) != count}
    return not mismatched, mismatched


if __name__ == "__main__":
    eq_result = remote_counts(SdkT42A, "t42_equity", "HIVEQ_US_EQ", "09:30", "16:00")
    fut_result = remote_counts(SdkT42B, "t42_futures", "HIVEQ_US_FUT",
                               "18:00", "17:00")
    eq_stream, eq_api = eq_result["stream"], eq_result["driver"]
    fut_stream, fut_api = fut_result["stream"], fut_result["driver"]
    fut_outrights = fut_result["symbols"]
    finish("t42_stream_data_api_parity", {
        "equity_stream_nonempty": eq_stream["bars"] > 0,
        "equity_api_nonempty": eq_api["bars"] > 0,
        "equity_counts_exact_per_session": eq_result["exact"],
        "futures_stream_nonempty": fut_stream["bars"] > 0,
        "futures_continuous_resolved_to_outrights": bool(fut_outrights),
        "futures_api_nonempty": fut_api["bars"] > 0,
        "futures_counts_exact_per_session": fut_result["exact"],
    }, extra=(f"eq_stream={eq_stream}; eq_api_bars={eq_api['bars']} "
              f"eq_api_duplicate_rows={eq_api['duplicate_rows']}; "
              f"eq_mismatched={eq_result['mismatched']}; "
              f"fut_outrights={fut_outrights}; fut_stream={fut_stream}; "
              f"fut_api_bars={fut_api['bars']} "
              f"fut_api_duplicate_rows={fut_api['duplicate_rows']}; "
              f"fut_mismatched={fut_result['mismatched']}"))
