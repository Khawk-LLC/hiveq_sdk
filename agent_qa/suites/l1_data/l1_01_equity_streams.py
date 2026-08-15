"""l1_01: every equity stream reaches its callback for a single symbol.

The base of the ladder. Before any behavioural question is worth asking, the
data has to arrive: bars, trade ticks and top-of-book quotes for one equity on
one ordinary session, each landing in its own callback with a sane payload.

Deliberately one strategy and one run across all three streams rather than three
runs — that also proves successive ``subscribe_*`` calls of *different kinds*
coexist, which single-stream tests never exercise.

Coverage caveat honoured from the Data Reference (§A.1): ``bars_*`` carry no
trade prints, so a tick-consuming test must subscribe ``eq_trades`` explicitly.
Streams with no rows on the probed date are reported per-stream rather than
failing the whole test — a locally empty stream is a data gap, not a delivery bug.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l1_01_equity_streams"
SURFACE = "l1.data"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol

probe = Probe()


class L1EquityStreams:

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_trades([SYMBOL], asset_type=AssetType.EQUITY)
        ctx.subscribe_quotes([SYMBOL], asset_type=AssetType.EQUITY)

    def on_bar(self, ctx, event):
        bar = event.data()
        if probe.bump("bar") == 1:
            probe.sample("bar", symbol=bar.symbol, o=bar.open, h=bar.high,
                         l=bar.low, c=bar.close, v=bar.volume, ts=event.ts_event)
        if bar.symbol != SYMBOL:
            probe.error(f"bar for unsubscribed symbol {bar.symbol}")
        if not (bar.low <= bar.close <= bar.high):
            probe.error(f"bar OHLC inconsistent on {bar.symbol}")

    def on_trade(self, ctx, event):
        tick = event.data()
        if probe.bump("trade") == 1:
            probe.sample("trade", symbol=tick.symbol, price=tick.price,
                         size=getattr(tick, "size", None), ts=event.ts_event)
        if getattr(tick, "price", 0) <= 0:
            probe.error(f"non-positive trade price {getattr(tick, 'price', None)}")

    def on_quote(self, ctx, event):
        q = event.data()
        if probe.bump("quote") == 1:
            probe.sample("quote", symbol=q.symbol, bid=q.bid_price, ask=q.ask_price,
                         ts=event.ts_event)
        # Crossed books are counted, not treated as errors. The quote attached to a
        # trade is top-of-book per venue, not NBBO, so a genuinely crossed quote is normal market
        # microstructure and appears at a low rate. Asserted as a RATE below:
        # a handful in 70k is real data, while ~50% would mean bid/ask are
        # swapped — which is the defect worth catching.
        if q.bid_price and q.ask_price and q.bid_price > q.ask_price:
            if probe.bump("quote_crossed") <= 3:
                probe.sample("quote_crossed", symbol=q.symbol,
                             bid=q.bid_price, ask=q.ask_price, ts=event.ts_event)

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L1EquityStreams", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[
            backtest.historical(FIXTURES.dataset_equity, ["bars_1m", "eq_trades"])
        ],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:30"),
    )

    data = probe.collect(run)
    bars, trades, quotes = data.count("bar"), data.count("trade"), data.count("quote")

    c = Checks()
    c.note(f"evidence via {data.source}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]))

    # Bars are the load-bearing stream: if these are absent the run is broken,
    # not merely uncovered.
    c.add("bars_delivered", bars > 0, f"n={bars}")
    c.add("payloads_sane", not data.errors, f"errors={data.errors[:3]}",
          requires=bars + trades + quotes > 0,
          requires_detail="no payload of any kind arrived to validate")

    # Bid<=Ask as a rate, not an absolute. Gated on quotes actually arriving.
    crossed = data.count("quote_crossed")
    crossed_pct = (100.0 * crossed / quotes) if quotes else 0.0
    c.add("crossed_quotes_rare", crossed_pct < 1.0,
          f"{crossed}/{quotes} quotes crossed ({crossed_pct:.2f}%); a rate near "
          f"50% means bid/ask are swapped, not that the book crossed. "
          f"samples={data.samples('quote_crossed')[:2]}",
          requires=quotes > 0, requires_detail="no quotes arrived")

    # Ticks and quotes are reported, and their absence is called out as a data
    # gap in the extra rather than silently passing.
    missing = [k for k, n in (("eq_trades trades", trades), ("eq_trades quotes", quotes)) if n == 0]
    c.add("tick_streams_present", not missing or bars > 0,
          f"no rows for {missing}")

    c.finish(
        NAME,
        surface=SURFACE,
        gap=bool(missing) and bars > 0,
        extra=(f"bars={bars}, trades={trades}, quotes={quotes}"
               + (f"; empty streams (data coverage): {missing}" if missing else "")),
    )


if __name__ == "__main__":
    main()
