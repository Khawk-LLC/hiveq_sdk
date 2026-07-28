"""l8_01: market order lifecycle — submit, fill, position, flatten.

The core OMS contract, ported and widened from ``qa_validation/t05``. Two things
a strategy author depends on absolutely:

* A **market buy fills** and the fill is reported through ``on_order``. There is
  no ``on_order_filled`` (§0/R1) — fills arrive in ``on_order`` and are read via
  ``order.is_filled`` / ``order.status`` / ``order.last_fill``. A test that
  waits for a filled-specific callback would hang forever, so this pins the
  documented shape.
* ``close_position`` flattens: ``net_position`` returns to 0 and ``is_flat``
  becomes true.

Resting-limit and cancel behaviour deliberately live in ``l8_02``, NOT here. A
market order fills immediately, so cancelling in a frame full of filled orders is
a no-op and proves nothing about ``cancel_all_orders``. That property needs a
passive limit in isolation.

Also records every distinct ``OrderStatus`` observed, so the run reports which
states this path actually exercises rather than assuming.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l8_01_order_lifecycle"
SURFACE = "l8.oms"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol
QTY = 100.0

probe = Probe()


class L8OrderLifecycle:
    """Walks one deterministic order sequence, one step per bar."""

    def __init__(self):
        self.step = 0

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        bar = event.data()
        probe.bump("bar")
        self.step += 1

        if self.step == 2:
            ctx.buy_order(SYMBOL, quantity=QTY)
            probe.bump("market_buy_sent")

        elif self.step == 6:
            # Record the position established by the market buy.
            probe.sample("after_buy", net=ctx.net_position(SYMBOL),
                         is_flat=ctx.is_flat(SYMBOL))
            probe.counters["net_after_buy"] = int(ctx.net_position(SYMBOL) or 0)

        elif self.step == 14:
            ctx.close_position(SYMBOL)
            probe.bump("close_position_sent")

        elif self.step == 18:
            probe.counters["net_after_close"] = int(ctx.net_position(SYMBOL) or 0)
            probe.sample("after_close", net=ctx.net_position(SYMBOL),
                         is_flat=ctx.is_flat(SYMBOL))
            if ctx.is_flat(SYMBOL):
                probe.bump("flat_after_close")

    def on_order(self, ctx, event):
        order = event.data()
        probe.bump("order_event")

        status = getattr(order, "status", None)
        status_name = getattr(status, "name", str(status))
        probe.bump(f"status:{status_name}")

        if getattr(order, "is_filled", False):
            probe.bump("fill")
            if probe.counters["fill"] == 1:
                probe.sample("fill", symbol=order.symbol,
                             filled_qty=getattr(order, "filled_qty", None),
                             avg_px=getattr(order, "avg_px", None),
                             status=status_name)

        if status_name == "REJECTED":
            probe.bump("rejected")
            probe.sample("rejected", symbol=order.symbol,
                         reason=getattr(order, "reject_reason", None) or
                                getattr(order, "reason", None))

        if status_name == "CANCELED":
            probe.bump("canceled")
            probe.sample("canceled", symbol=order.symbol,
                         qty=getattr(order, "quantity", None))

    def on_position(self, ctx, event):
        pos = event.data()
        probe.bump("position_event")
        if probe.counters["position_event"] == 1:
            probe.sample("position", symbol=pos.symbol,
                         qty=getattr(pos, "quantity", None))

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)
    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L8OrderLifecycle", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:30"),
    )

    data = probe.collect(run)
    statuses = sorted(k.split(":", 1)[1] for k in data.counters if k.startswith("status:"))

    bars = data.count("bar")
    fills = data.count("fill")
    traded = fills > 0

    c = Checks()
    c.note(f"evidence via {data.source}; statuses seen={statuses}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]),
          requires=bars > 0, requires_detail="no callback ran, so an empty log "
                                             "proves nothing")

    # R12: a zero-trade run is not a valid basis for ANY order-management claim.
    # Everything below is gated on it, so a dead run reports one honest failure
    # instead of a page of vacuous greens.
    c.add("sequence_reached_end", data.count("close_position_sent") > 0,
          "the run ended before the order sequence completed; widen the session",
          requires=bars > 0, requires_detail="no bars, so the sequence never ran")
    c.add("market_buy_filled", traded,
          f"no fill reported via on_order; order events={data.count('order_event')}")
    c.add("position_opened", data.counters.get("net_after_buy", 0) != 0,
          f"net_position after the market buy was "
          f"{data.counters.get('net_after_buy')}, expected {int(QTY)}",
          requires=traded, requires_detail="nothing filled, so no position could open")

    # Exact, not a bound: one fill for the market buy, one for the close.
    # "fills <= 2" would also be satisfied by ZERO fills.
    c.add("exactly_two_fills", fills == 2,
          f"{fills} fills, expected exactly 2 (market buy + close)",
          requires=traded, requires_detail="nothing filled")
    c.add("flat_after_close", data.count("flat_after_close") > 0,
          f"net_position after close_position was "
          f"{data.counters.get('net_after_close')}, expected 0",
          requires=data.counters.get("net_after_buy", 0) != 0,
          requires_detail="no position was open, so flattening is meaningless")
    c.add("position_events_fired", data.count("position_event") > 0,
          "no on_position events for a run that opened and closed a position",
          requires=traded, requires_detail="nothing filled, so no position event "
                                           "could fire")

    c.finish(NAME, surface=SURFACE, extra=(
        f"orders={data.count('order_event')}, fills={data.count('fill')}, "
        f"rejects={data.count('rejected')}, "
        f"positions={data.count('position_event')}, first_fill={data.first('fill')}"
    ))


if __name__ == "__main__":
    main()
