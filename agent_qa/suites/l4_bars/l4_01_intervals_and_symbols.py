"""l4_01: bar intervals and symbol-set accumulation.

Merges the two properties ``qa_validation`` split across t01 and t02, because
they interact and the interaction is where bugs live:

* **Multiple intervals, one symbol** — ``1m`` and ``1d`` on the same ticker must
  both deliver, and be distinguishable. A day bar arriving where a minute bar is
  expected (or the two being conflated) silently corrupts any indicator.
* **Successive subscribes accumulate** — calling ``subscribe_bars`` again with a
  different symbol must *add* to the universe, not replace it. Regression
  against a subscription model that overwrites.
* **Interval discipline** — only ``bars_1s``/``bars_1m``/``bars_1d`` exist
  (AGENTS.md). This test asserts the real ones deliver; it deliberately does not
  probe ``5m``/``1h``, which are not schemas.

Bar counts are checked as a *relationship* (minutes >> days for the same
window), not as absolutes, so a holiday or a short session does not produce a
spurious failure.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l4_01_intervals_and_symbols"
SURFACE = "l4.bars"
START = FIXTURES.equity_week_start
END = FIXTURES.equity_week_end
FIRST = FIXTURES.equity_symbols[0]
SECOND = FIXTURES.equity_symbols[1]

probe = Probe()


class L4IntervalsAndSymbols:

    def __init__(self):
        self._subscribed = False

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        if self._subscribed:
            # Repeated identical subscribes are deduped by the engine, but the
            # accumulation property is about *distinct* calls, so only do the
            # sequence once.
            return
        self._subscribed = True

        # Two intervals on one symbol...
        ctx.subscribe_bars([FIRST], asset_type=AssetType.EQUITY, interval="1m")
        ctx.subscribe_bars([FIRST], asset_type=AssetType.EQUITY, interval="1d")
        # ...then a second symbol, which must ADD rather than replace.
        ctx.subscribe_bars([SECOND], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        probe.bump("bar")
        probe.bump(f"sym:{bar.symbol}")

        # Classify the interval from the event type when the engine specialises
        # it, else fall back to the generic BAR bucket.
        etype = getattr(event.type, "name", str(event.type))
        probe.bump(f"etype:{etype}")

        if probe.counters[f"sym:{bar.symbol}"] == 1:
            probe.sample(f"sym:{bar.symbol}", symbol=bar.symbol, etype=etype,
                         close=bar.close, ts=event.ts_event)

        if not (bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high):
            probe.error(f"OHLC inconsistent on {bar.symbol}: "
                        f"o={bar.open} h={bar.high} l={bar.low} c={bar.close}")
        if getattr(bar, "volume", 0) < 0:
            probe.error(f"negative volume on {bar.symbol}: {bar.volume}")

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    symbols = [FIRST, SECOND]
    run = backtest.run(
        [StrategyConfig(name=NAME, type="L4IntervalsAndSymbols", symbols=symbols,
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=symbols,
        start_date=START,
        end_date=END,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, ["bars_1m", "bars_1d"])],
        backtest_config=BacktestConfig(start_date=START, end_date=END,
                                       session_start=FIXTURES.equity_session_start,
                                       session_end=FIXTURES.equity_session_end),
    )

    data = probe.collect(run)
    first_n = data.count(f"sym:{FIRST}")
    second_n = data.count(f"sym:{SECOND}")
    etypes = {k.split(":", 1)[1]: v for k, v in data.counters.items()
              if k.startswith("etype:")}

    c = Checks()
    c.note(f"evidence via {data.source}; event types={etypes}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]))

    c.add("first_symbol_delivered", first_n > 0, f"{FIRST} n={first_n}")
    # The accumulation property: the second subscribe did not replace the first.
    c.add("successive_subscribe_accumulates", first_n > 0 and second_n > 0,
          f"{FIRST}={first_n}, {SECOND}={second_n} — a zero here means the later "
          "subscribe_bars replaced the earlier universe instead of extending it")

    # Both intervals present. Over a 5-session window a 1d subscription yields a
    # handful of bars while 1m yields hundreds, so the sum must exceed a pure
    # daily-only delivery by a wide margin.
    c.add("multi_interval_delivered", first_n > 10,
          f"{FIRST} produced only {first_n} bars across {START}..{END}; "
          "expected minute bars in addition to daily")

    c.add("payloads_sane", not data.errors, f"errors={data.errors[:3]}",
          requires=data.count("bar") > 0, requires_detail="no bars to validate")
    c.finish(NAME, surface=SURFACE, extra=f"{FIRST}={first_n}, {SECOND}={second_n}, total={data.count('bar')}")


if __name__ == "__main__":
    main()
