"""Bar timestamp / delivery contract (remote translation of Flow QA t13).

The contract (hiveq-flow bar.py docstrings):

* an intraday bar covers [start_time, end_time), end_time = start_time +
  interval, start_time a whole multiple of the interval; it is delivered at
  end_time: ts_event == bar.time == ctx.now() == end_time, every trade inside
  it has already been delivered, and no trade at or after its end has;
* a daily bar spans its asset's trading session (equities regular hours,
  09:30 -> 16:00 New York, early closes from the holiday calendar; futures the
  Globex day) and, by default (daily_bar_delivery='close'), is delivered at the
  session close: ts_event == ctx.now() == end_time == session close, after
  every trade of the session (the 'open' opt-in is covered separately);
* bars arrive in event-time order, shortest interval first on a tie;
* the clock never runs backwards across on_trade / on_bar / on_timer.

Bars are checked against an independent ground truth: OHLC rebuilt from the
trade stream (last-price-eligible prints plus odd lots, the prints a vendor
bar is built from). A
self-referential check (bar vs bar) cannot see a mislabelled or early bar --
that is how the 2026-07 regression (bars stamped and replayed at their start,
a full interval of look-ahead) passed t01/t60 unnoticed.

Only the intervals the data service serves natively are covered (1s, 1m,
equity 1d); futures daily bars are not served.
"""

from pathlib import Path
import sys

# Slice-assign, not sys.path.insert(...): the engine grafts only imports/defs/assignments
# from this script and strips bare calls, and qa_common has to import remotely too.
sys.path[:0] = [str(Path(__file__).resolve().parent)]
from collections import defaultdict                                   # noqa: E402

from qa_common import completed_checkpoint, emit_checkpoint, finish   # noqa: E402

import pandas as pd                                                   # noqa: E402

import hiveq.flow as hf                                               # noqa: E402
from hiveq.flow import StrategyConfig                                 # noqa: E402
from hiveq.flow.config import AssetType                               # noqa: E402

CHECKPOINT = "t80_bar_timestamp_contract"
NS = 1_000_000_000
INTRADAY = {"1s": 1, "1m": 60}
DAILY = "1d"
SYMBOLS = {"ES.c.0": AssetType.FUTURES, "AAPL": AssetType.EQUITY}
DAILY_SYMBOLS = ("AAPL",)
# Equity daily session (Sigma MarketCenterHours::dailySession), New York time.
EQUITY_SESSION_LOCAL = ("09:30", "16:00")
# Share of intraday bars whose OHLC must equal the trades rebuilt for their
# own window; below 1.0 only because a vendor bar may exclude a print the
# trade feed keeps. Bars matching a neighbouring window instead (a shifted
# label) may not exceed MAX_SHIFTED.
MIN_CONTENT_MATCH = 0.95
MAX_SHIFTED = 0.01
SAMPLES = 5
DAY = "2025-06-02"


def nanos(value):
    return int(pd.Timestamp(value).value)


def rth(ts):
    """Whether a UTC ns time falls in US equity regular hours (09:30-16:00 ET)."""
    local = pd.Timestamp(ts, tz="UTC").tz_convert("America/New_York")
    return "09:30" <= local.strftime("%H:%M") < "16:00"


def equity_session(day):
    open_local, close_local = EQUITY_SESSION_LOCAL
    return (nanos(pd.Timestamp(f"{day} {open_local}", tz="America/New_York")),
            nanos(pd.Timestamp(f"{day} {close_local}", tz="America/New_York")))


class BarContract:

    def setup(self):
        if hasattr(self, "state"):
            return
        self.trades = {s: {i: {} for i in INTRADAY} for s in SYMBOLS}
        self.delivered_end = {s: {i: 0 for i in INTRADAY} for s in SYMBOLS}
        self.max_trade_ts = defaultdict(int)
        self.session_trades = {s: {} for s in DAILY_SYMBOLS}
        self.daily_delivered = {s: set() for s in DAILY_SYMBOLS}
        self.bars = []
        self.daily_bars = []
        self.last_now = 0
        self.last_bar = (0, 0)
        self.state = defaultdict(list)
        self.state["counts"] = defaultdict(lambda: defaultdict(int))

    def subscribe(self, ctx):
        for symbol, asset_type in SYMBOLS.items():
            ctx.subscribe_trades([symbol], asset_type=asset_type)
            for interval in INTRADAY:
                ctx.subscribe_bars([symbol], asset_type=asset_type, interval=interval)
            if symbol in DAILY_SYMBOLS:
                ctx.subscribe_bars([symbol], asset_type=asset_type, interval=DAILY)
        ctx.set_timer("bar_clock", pd.Timedelta(seconds=7))

    @staticmethod
    def resolve(symbol):
        """The subscribed series for an event symbol (ESM5 -> ES.c.0)."""
        if symbol in SYMBOLS:
            return symbol
        for series in SYMBOLS:
            root = series.split(".")[0]
            if "." in series and symbol.startswith(root) and len(symbol) <= len(root) + 3:
                return series
        return None

    def violation(self, key, sample):
        self.state[key + "_n"] = self.state.get(key + "_n", 0) + 1
        if len(self.state[key]) < SAMPLES:
            self.state[key].append(sample)

    def clock(self, ctx, where):
        now = nanos(ctx.now())
        if now < self.last_now:
            self.violation("clock_backwards", [where, now, self.last_now])
        self.last_now = max(self.last_now, now)
        return now

    def on_trade(self, ctx, event):
        trade = event.data()
        symbol = self.resolve(trade.symbol)
        # The prints a vendor bar is built from: last-price-eligible prints
        # plus odd lots (98.5% of AAPL 1m bars, against 72% for every print).
        eligible = (trade.last_price or 0) > 0 or str(trade.condition).endswith("NonBoardLot")
        price = float(trade.price or 0)
        if symbol is None or not eligible or price <= 0 or not trade.size:
            return
        ts = int(event.ts_event)
        self.clock(ctx, "on_trade")
        self.max_trade_ts[symbol] = max(self.max_trade_ts[symbol], ts)
        size = float(trade.size)
        if symbol in self.session_trades:
            day = pd.Timestamp(ts, tz="UTC").strftime("%Y-%m-%d")
            open_ns, close_ns = equity_session(day)
            if open_ns <= ts < close_ns:
                if day in self.daily_delivered[symbol]:
                    # A session trade after that day's daily bar: look-ahead.
                    self.violation("trade_after_daily_bar", [symbol, day, ts])
                self.add(self.session_trades[symbol], day, price, size)
        for interval, secs in INTRADAY.items():
            start = ts // (secs * NS) * secs * NS
            # A trade inside a window already delivered as a bar: the bar was
            # handed to the strategy before its own trades -- look-ahead.
            if start + secs * NS <= self.delivered_end[symbol][interval]:
                self.violation("trade_after_its_bar", [symbol, interval, ts])
            self.add(self.trades[symbol][interval], start, price, size)

    @staticmethod
    def add(book, key, price, size):
        row = book.get(key)
        if row is None:
            book[key] = [price, price, price, price, size]
        else:
            row[1] = max(row[1], price)
            row[2] = min(row[2], price)
            row[3] = price
            row[4] += size

    def order(self, interval_secs, delivered):
        # Event-time order across every bar; shortest interval first on a tie.
        if (delivered, interval_secs) < self.last_bar:
            self.violation("bar_order", [interval_secs, delivered, list(self.last_bar)])
        self.last_bar = max(self.last_bar, (delivered, interval_secs))

    def on_bar(self, ctx, event):
        bar = event.data()
        symbol = self.resolve(bar.symbol)
        interval = bar.interval
        if symbol in DAILY_SYMBOLS and interval == DAILY:
            self.on_daily_bar(ctx, event, symbol, bar)
            return
        if symbol is None or interval not in INTRADAY:
            self.violation("unexpected_bar", [bar.symbol, interval])
            return
        secs = INTRADAY[interval]
        now = self.clock(ctx, "on_bar")
        start, end = nanos(bar.start_time), nanos(bar.end_time)
        self.state["counts"][symbol][interval] += 1

        if end - start != secs * NS:
            self.violation("end_not_start_plus_interval",
                           [symbol, interval, str(bar.start_time), str(bar.end_time)])
        if start % (secs * NS):
            self.violation("start_not_aligned", [symbol, interval, str(bar.start_time)])
        if int(event.ts_event) != end or nanos(bar.time) != end:
            self.violation("ts_event_not_end", [symbol, interval, int(event.ts_event), end])
        if now != end:
            self.violation("now_not_end_in_on_bar", [symbol, interval, now, end])
        if self.max_trade_ts[symbol] >= end:
            self.violation("bar_after_later_trades", [symbol, interval, end, self.max_trade_ts[symbol]])
        self.order(secs, end)
        self.delivered_end[symbol][interval] = max(self.delivered_end[symbol][interval], end)
        self.bars.append((symbol, interval, start,
                          [float(bar.open), float(bar.high), float(bar.low), float(bar.close)]))

    def on_daily_bar(self, ctx, event, symbol, bar):
        now = self.clock(ctx, "on_bar_daily")
        self.state["counts"][symbol][DAILY] += 1
        start, end = nanos(bar.start_time), nanos(bar.end_time)
        day = pd.Timestamp(start, tz="UTC").strftime("%Y-%m-%d")
        open_ns, close_ns = equity_session(day)
        if start != open_ns or end != close_ns:
            self.violation("daily_not_on_session", [symbol, str(bar.start_time), str(bar.end_time)])
        if int(event.ts_event) != end or nanos(bar.time) != end:
            self.violation("daily_ts_event_not_close", [symbol, day, int(event.ts_event), end])
        if now != end:
            self.violation("daily_now_not_close", [symbol, day, now, end])
        if day not in self.session_trades[symbol]:
            self.violation("daily_before_session_trades", [symbol, day, now])
        self.daily_delivered[symbol].add(day)
        self.order(86400, end)
        self.daily_bars.append((symbol, day, [float(bar.open), float(bar.high),
                                              float(bar.low), float(bar.close)]))

    def on_timer(self, ctx, event):
        self.clock(ctx, "on_timer")

    def summarize(self):
        """Content checks once every trade is in: each bar vs its own window."""
        own, shifted, total = defaultdict(int), defaultdict(int), defaultdict(int)
        for symbol, interval, start, ohlc in self.bars:
            if symbol in DAILY_SYMBOLS and not rth(start):
                # Equity content is compared in regular hours only: extended-
                # hours vendor bars use a different print rule. Timing and
                # ordering are still checked on every bar.
                continue
            key = f"{symbol}:{interval}"
            secs = INTRADAY[interval] * NS
            book = self.trades[symbol][interval]

            def same(window):
                row = book.get(window)
                return row is not None and all(abs(a - b) < 1e-6 for a, b in zip(ohlc, row[:4]))
            total[key] += 1
            hit = same(start)
            own[key] += hit
            shifted[key] += (not hit) and (same(start - secs) or same(start + secs))
        state = dict(self.state)
        state["counts"] = {s: dict(c) for s, c in self.state["counts"].items()}
        state["content_match"] = {k: round(own[k] / total[k], 4) for k in total}
        state["shifted_rate"] = {k: round(shifted[k] / total[k], 4) for k in total}
        # Daily content is reported, not asserted: the vendor daily bar is
        # split/dividend adjusted and built from official prints.
        state["daily_ohlc_minus_session_trades"] = {
            f"{s}:{d}": None if self.session_trades[s].get(d) is None else [
                round(a - b, 4) for a, b in zip(ohlc, self.session_trades[s][d][:4])]
            for s, d, ohlc in self.daily_bars}
        return state


CONTRACT = BarContract()


class SdkT80:
    def on_start(self, ctx, event):
        CONTRACT.setup()
        CONTRACT.subscribe(ctx)

    def on_trade(self, ctx, event):
        CONTRACT.on_trade(ctx, event)

    def on_bar(self, ctx, event):
        CONTRACT.on_bar(ctx, event)

    def on_timer(self, ctx, event):
        CONTRACT.on_timer(ctx, event)

    def on_stop(self, ctx, event):
        # on_stop fires once per calendar day; the last checkpoint covers the run.
        emit_checkpoint(ctx, CHECKPOINT, CONTRACT.summarize())


def contract_checks(state):
    checks = {}
    for symbol in SYMBOLS:
        counts = state["counts"].get(symbol, {})
        for interval in INTRADAY:
            key = f"{symbol}:{interval}"
            checks[f"{symbol}_{interval}_delivered"] = counts.get(interval, 0) > 0
            checks[f"{symbol}_{interval}_content"] = state["content_match"].get(key, 0.0) >= MIN_CONTENT_MATCH
            checks[f"{symbol}_{interval}_not_shifted"] = state["shifted_rate"].get(key, 1.0) <= MAX_SHIFTED
        if symbol in DAILY_SYMBOLS:
            checks[f"{symbol}_{DAILY}_delivered"] = counts.get(DAILY, 0) == 1
    for key in ("end_not_start_plus_interval", "start_not_aligned", "ts_event_not_end",
                "now_not_end_in_on_bar", "bar_after_later_trades", "trade_after_its_bar",
                "daily_not_on_session", "daily_ts_event_not_close", "daily_now_not_close",
                "daily_before_session_trades", "trade_after_daily_bar", "bar_order", "clock_backwards", "unexpected_bar"):
        checks[key] = not state.get(key + "_n")
    return checks


def detail(state):
    violations = {k[:-2]: (state[k], state.get(k[:-2], [])[:2])
                  for k in state if k.endswith("_n")}
    return (f"counts={state['counts']} content={state['content_match']} "
            f"shifted={state['shifted_rate']} "
            f"daily_diff={state['daily_ohlc_minus_session_trades']} violations={violations}")


if __name__ == "__main__":
    data_configs = [{"type": "hiveq_historical", "dataset": dataset, "schema": [schema]}
                    for dataset, trades in (("HIVEQ_US_FUT", "fut_trades"), ("HIVEQ_US_EQ", "eq_trades"))
                    for schema in ("bars_1s", "bars_1m", trades)]
    data_configs.append({"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ", "schema": ["bars_1d"]})
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name="SdkT80", type="SdkT80", symbols=list(SYMBOLS))],
        symbols=list(SYMBOLS), start_date=DAY, end_date=DAY, data_configs=data_configs)
    state = completed_checkpoint(run, CHECKPOINT)
    finish(CHECKPOINT, contract_checks(state), extra=detail(state))
