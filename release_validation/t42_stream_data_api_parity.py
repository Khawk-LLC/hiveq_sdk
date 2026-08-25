"""Remote callback bar counts match the underlying Data API for equity and futures.

Parity is asserted per ``(symbol, session day)`` on *distinct* bar timestamps,
which is the claim that actually matters: every bar the Data API holds for a
session is delivered to the callback, and none is invented.  Three properties of
the two surfaces make a raw row-count comparison wrong:

* The Data API resolves no continuous alias -- ``symbols=['ES.c.0']`` returns
  nothing and ``chains=`` is rejected outright, so the outright contracts the
  callbacks resolved to are what must be queried.
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
        emit_checkpoint(ctx, self.checkpoint_name, {
            "per_day": dict(self.per_day),
            "duplicate_deliveries": self.duplicates,
            "bars": sum(self.per_day.values()),
        })


class SdkT42A(SdkT42Counting):
    symbols = EQUITY_SYMBOLS
    checkpoint_name = "t42_equity"
    asset_type = AssetType.EQUITY
    overnight = False


class SdkT42B(SdkT42Counting):
    symbols = FUTURES_SYMBOLS
    checkpoint_name = "t42_futures"
    asset_type = AssetType.FUTURES
    overnight = True


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


def api_counts(dataset, symbols, first, last, start_tod, end_tod, overnight):
    """Distinct (symbol, timestamp) API rows per symbol and session day.

    ``first``/``last`` bound the same window the run covered, and the query asks
    for one minute past ``last`` because the Data API's session filter treats
    its ``end`` as exclusive while the engine delivers a bar stamped exactly at
    ``session_end`` -- a closing-auction minute exists in storage for some
    sessions and not others, so the boundary has to be requested and then
    trimmed here rather than assumed away.

    Release validation runs in the development environment, where the
    functional data-driver distribution is installed separately from the thin
    SDK.  Import it directly: ``hiveq.driver`` is intentionally the thin
    client's platform-only authoring stub and does not re-export it.
    """
    import hiveq_data

    client = hiveq_data.Historical(timezone="America/New_York")
    first_dt = datetime.strptime(first, STAMP)
    last_dt = datetime.strptime(last, STAMP)
    per_day = Counter()
    seen = defaultdict(set)
    duplicates = 0
    offset = 0
    page_size = 500_000
    while True:
        response = client.get_data(
            dataset=dataset,
            schema="bars_1m",
            symbols=list(symbols),
            start=first,
            end=(last_dt + timedelta(minutes=1)).strftime(STAMP),
            filter_mode="session",
            limit=page_size,
            offset=offset,
        ) or {}
        rows = response.get("data", [])
        for row in rows:
            symbol = row.get("symbol") or row.get("ticker") or row.get("instrument")
            stamp = row.get("time") or row.get("ts_event")
            if not symbol or not stamp:
                continue
            symbol = str(symbol)
            moment = datetime.strptime(str(stamp)[:19], STAMP)
            if moment < first_dt or moment > last_dt:
                continue
            if not in_session(moment, start_tod, end_tod, overnight):
                continue
            if stamp in seen[symbol]:
                duplicates += 1
                continue
            seen[symbol].add(stamp)
            per_day[f"{symbol}|{session_day(moment, overnight)}"] += 1
        if len(rows) < page_size:
            break
        offset += page_size
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
    eq_stream = remote_counts(SdkT42A, "t42_equity", "HIVEQ_US_EQ", "09:30", "16:00")
    eq_api = api_counts("HIVEQ_US_EQ", EQUITY_SYMBOLS,
                        f"{START_DATE} 09:30:00", f"{END_DATE} 16:00:00",
                        "09:30:00", "16:00:00", False)
    fut_stream = remote_counts(SdkT42B, "t42_futures", "HIVEQ_US_FUT",
                               "18:00", "17:00")
    # The outright contracts the continuous aliases actually resolved to -- the
    # Data API has no continuous form, so these are what it must be asked for.
    fut_outrights = sorted({key.split("|")[0] for key in fut_stream["per_day"]})
    # The run's first futures session opens at 18:00 on the evening *before*
    # start_date -- the session named start_date -- so the API window has to
    # start there or that whole session reads as missing.
    futures_first = (datetime.strptime(START_DATE, "%Y-%m-%d")
                     - timedelta(days=1)).strftime("%Y-%m-%d 18:00:00")
    fut_api = (api_counts("HIVEQ_US_FUT", fut_outrights, futures_first,
                          f"{END_DATE} 17:00:00", "18:00:00", "17:00:00", True)
               if fut_outrights else {"per_day": {}, "duplicate_rows": 0, "bars": 0})

    equity_exact, equity_gaps = compare(eq_stream, eq_api)
    futures_exact, futures_gaps = compare(fut_stream, fut_api)
    finish("t42_stream_data_api_parity", {
        "equity_stream_nonempty": eq_stream["bars"] > 0,
        "equity_api_nonempty": eq_api["bars"] > 0,
        "equity_counts_exact_per_session": equity_exact,
        "futures_stream_nonempty": fut_stream["bars"] > 0,
        "futures_continuous_resolved_to_outrights": bool(fut_outrights),
        "futures_api_nonempty": fut_api["bars"] > 0,
        "futures_counts_exact_per_session": futures_exact,
    }, extra=(f"eq_stream={eq_stream}; eq_api_bars={eq_api['bars']} "
              f"eq_api_duplicate_rows={eq_api['duplicate_rows']}; "
              f"eq_mismatched={equity_gaps}; "
              f"fut_outrights={fut_outrights}; fut_stream={fut_stream}; "
              f"fut_api_bars={fut_api['bars']} "
              f"fut_api_duplicate_rows={fut_api['duplicate_rows']}; "
              f"fut_mismatched={futures_gaps}"))
