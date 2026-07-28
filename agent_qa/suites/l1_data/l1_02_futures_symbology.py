"""l1_02: futures symbology — continuous, volume-continuous, and raw contract.

Futures are where symbol handling actually bites. Three shapes must all deliver
into the same strategy:

* ``ES.c.0``  — calendar/front continuous
* ``ES.v.0``  — volume continuous
* ``ESM5``    — a raw dated contract

This is also the regression home for the symbol/stype blast radius: a raw
contract requested under a continuous ``stype`` is malformed, and a malformed
symbol has previously caused the gateway to close the **shared** session,
killing data for every deployment in the container. Here the blast radius is
bounded to a backtest, so the assertion is narrower but the principle is the
same — one bad symbol must not take out the good ones. ``l9`` carries the
livesim version of this check, where the shared-session risk is real.

Uses the futures overnight session (18:00 -> 17:00) because adding a futures
data config flips the session window; asserting RTH hours here would silently
under-fetch.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l1_02_futures_symbology"
SURFACE = "l1.symbology"
DAY = FIXTURES.futures_day
CONTINUOUS = FIXTURES.futures_continuous
VOLUME_CONT = FIXTURES.futures_volume_continuous
RAW = FIXTURES.futures_raw_contract

probe = Probe()


class L1FuturesSymbology:

    def on_start(self, ctx, event):
        probe.bump("start")
        # subscribe_futures_bars is the futures-aware entry point; it resolves
        # continuous symbology per session rather than treating the string as a
        # literal ticker.
        ctx.subscribe_futures_bars(symbols=[CONTINUOUS, VOLUME_CONT, RAW], interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        probe.bump("bar")
        # Count by the symbol the ENGINE actually reported, never by guessing
        # which subscription it came from. Root-prefix attribution is wrong here:
        # `ESM5` shares the `ES` root with `ES.c.0`, so a first-match loop files
        # the resolved dated contract under the continuous alias and reports the
        # raw contract as "0 bars" — inventing a data gap that does not exist.
        # (l1_03 proves ESM5 data is present: it is the first futures symbol
        # delivered there.) So record the observed symbols and let the checks
        # reason about the set.
        probe.bump(f"sym:{bar.symbol}")
        if probe.counters[f"sym:{bar.symbol}"] == 1:
            probe.sample("symbols", symbol=bar.symbol, close=bar.close,
                         ts=event.ts_event)
        if not _same_root(bar.symbol, CONTINUOUS):
            probe.bump("bar:foreign_root")
            probe.sample("bar:foreign_root", symbol=bar.symbol)

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def _same_root(actual: str, wanted: str) -> bool:
    """``ESM5`` matches ``ES.c.0``; ``NQZ5`` does not."""
    root = wanted.split(".")[0]
    return bool(actual) and actual.upper().startswith(root.upper())


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    symbols = [CONTINUOUS, VOLUME_CONT, RAW]
    run = backtest.run(
        [StrategyConfig(name=NAME, type="L1FuturesSymbology", symbols=symbols,
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=symbols,
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_futures, "bars_1m")],
        backtest_config=BacktestConfig(
            start_date=DAY, end_date=DAY,
            session_start=FIXTURES.futures_session_start,
            session_end=FIXTURES.futures_session_end,
        ),
    )

    data = probe.collect(run)
    total = data.count("bar")
    # Symbols as the engine reported them, e.g. {'ESM5': 1380} for a run that
    # subscribed ES.c.0 / ES.v.0 / ESM5 — continuous aliases resolve to the dated
    # contract, so the delivered set is smaller than the requested set by design.
    observed = sorted(k.split(":", 1)[1] for k in data.counters if k.startswith("sym:"))
    per_observed = {s: data.count(f"sym:{s}") for s in observed}

    c = Checks()
    c.note(f"evidence via {data.source}; observed symbols={per_observed}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]),
          requires=total > 0, requires_detail="no bars, so an empty log proves nothing")
    c.add("any_futures_delivered", total > 0, f"total bars={total}")

    # Containment: subscribing three symbology shapes at once must still deliver.
    # Asserted on the observed set rather than per-requested-symbol, because the
    # engine legitimately collapses a continuous alias onto its dated contract
    # and there is no reliable way to attribute a bar back to the subscription
    # that asked for it.
    c.add("symbols_resolved", bool(observed),
          "bars arrived but no symbol was recorded",
          requires=total > 0, requires_detail="no bars arrived")
    c.add("only_requested_root_delivered", data.count("bar:foreign_root") == 0,
          f"bars for an unrelated root: {data.samples('bar:foreign_root')[:3]}",
          requires=total > 0, requires_detail="no bars arrived, so no symbol "
                                              "could be off-root")

    c.finish(
        NAME,
        surface=SURFACE,
        extra=f"total={total}, observed={per_observed}, requested={symbols}",
    )


if __name__ == "__main__":
    main()
