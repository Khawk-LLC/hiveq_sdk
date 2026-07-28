"""l8_02: a passive limit order rests, and cancelling it works.

Cancel gets its own test, with **no market orders anywhere**. The earlier version
folded it into ``l8_01``'s market-buy sequence, which cannot establish the
property: a market order fills immediately, so "cancel" has nothing left to act
on, and `cancel_all_orders()` was being called with filled orders in the frame.
Cancelling a filled order is a no-op, so a missing CANCELED event proved nothing.

The clean form, isolated:

1. Place ONE buy limit far below the market so it cannot fill.
2. Verify it is actually resting — ``ctx.has_open_order(symbol)`` is True. This
   is the positive precondition; without it the cancel assertion is vacuous.
3. ``ctx.cancel_all_orders(symbol)``.
4. Verify a CANCELED order event arrives AND ``has_open_order`` goes False.

Two API details this pins, both of which the previous version got wrong:

* ``order_type`` takes an ``OrderType`` **enum**, not a string. The default is
  ``OrderType.MARKET``, so a stringly-typed ``"LIMIT"`` risks silently placing a
  market order that fills instantly — the test would then "prove" that a limit
  does not rest.
* ``cancel_all_orders`` only *requests* the cancel; the order stays
  ``has_open_order == True`` for the remainder of that bar (§5.2). So the
  post-cancel state is checked on a LATER bar, never on the same one.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import backtest
from agent_qa.core.probe import Probe
from agent_qa.core.profiles import FIXTURES
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l8_02_passive_limit_cancel"
SURFACE = "l8.oms"
DAY = FIXTURES.equity_day
SYMBOL = FIXTURES.equity_symbol
QTY = 100.0

#: Far enough below the market that it cannot trade on this session.
PASSIVE_FACTOR = 0.5

PLACE_BAR = 3
CANCEL_BAR = 10
VERIFY_BAR = 16

probe = Probe()


class L8PassiveLimitCancel:

    def __init__(self):
        self.step = 0

    def on_start(self, ctx, event):
        from hiveq.flow.config import AssetType

        probe.bump("start")
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.EQUITY, interval="1m")

    def on_bar(self, ctx, event):
        from hiveq.flow.trading_types import OrderType

        bar = event.data()
        probe.bump("bar")
        self.step += 1

        if self.step == PLACE_BAR:
            limit = round(float(bar.close) * PASSIVE_FACTOR, 2)
            # OrderType enum — NOT the string "LIMIT".
            order = ctx.buy_order(SYMBOL, quantity=QTY,
                                  order_type=OrderType.LIMIT, limit_price=limit)
            probe.bump("limit_sent")
            probe.sample("limit_sent", limit=limit, market=float(bar.close),
                         returned=order is not None)

        elif self.step == PLACE_BAR + 2:
            # Resting confirmed on a later bar, and nothing filled.
            resting = bool(ctx.has_open_order(SYMBOL))
            probe.counters["resting_before_cancel"] = int(resting)
            probe.counters["net_before_cancel"] = int(ctx.net_position(SYMBOL) or 0)
            probe.sample("before_cancel", has_open_order=resting,
                         net=ctx.net_position(SYMBOL),
                         open_qty=ctx.open_order_qty(SYMBOL))

        elif self.step == CANCEL_BAR:
            ctx.cancel_all_orders(SYMBOL)
            probe.bump("cancel_sent")

        elif self.step == VERIFY_BAR:
            # Later bar: cancel_all_orders only REQUESTS the cancel, so the order
            # remains has_open_order for the rest of the bar it was issued on.
            still_open = bool(ctx.has_open_order(SYMBOL))
            probe.counters["open_after_cancel"] = int(still_open)
            probe.sample("after_cancel", has_open_order=still_open,
                         net=ctx.net_position(SYMBOL))

    def on_order(self, ctx, event):
        order = event.data()
        probe.bump("order_event")
        status = getattr(order, "status", None)
        status_name = getattr(status, "name", str(status))
        probe.bump(f"status:{status_name}")

        if status_name == "CANCELED":
            probe.bump("canceled")
            probe.sample("canceled", symbol=order.symbol, status=status_name)
        if getattr(order, "is_filled", False):
            probe.bump("fill")
            probe.sample("fill", symbol=order.symbol,
                         qty=getattr(order, "filled_qty", None),
                         px=getattr(order, "avg_px", None))

    def on_stop(self, ctx, event):
        probe.bump("stop")
        probe.flush(ctx)


def main():
    install_crash_handler(NAME, SURFACE)

    from hiveq.flow import BacktestConfig, StrategyConfig

    run = backtest.run(
        [StrategyConfig(name=NAME, type="L8PassiveLimitCancel", symbols=[SYMBOL],
                        params={"initial_capital": FIXTURES.initial_capital})],
        symbols=[SYMBOL],
        start_date=DAY,
        end_date=DAY,
        data_configs=[backtest.historical(FIXTURES.dataset_equity, "bars_1m")],
        backtest_config=BacktestConfig(start_date=DAY, end_date=DAY,
                                       session_start="09:30", session_end="10:30"),
    )

    data = probe.collect(run)
    bars = data.count("bar")
    statuses = sorted(k.split(":", 1)[1] for k in data.counters if k.startswith("status:"))
    rested = data.counters.get("resting_before_cancel", 0) == 1

    c = Checks()
    c.note(f"evidence via {data.source}; statuses seen={statuses}")
    c.add("run_completed", backtest.completed_ok(run), f"status={backtest.status_of(run)}")
    c.add("no_callback_crash", not backtest.crash_lines(run),
          "; ".join(backtest.crash_lines(run)[:2]),
          requires=bars > 0, requires_detail="no callback ran")
    c.add("sequence_reached_end", bars >= VERIFY_BAR,
          f"only {bars} bars; the sequence needs {VERIFY_BAR}")

    c.add("limit_order_accepted", data.count("limit_sent") > 0,
          "buy_order(order_type=OrderType.LIMIT) was never issued",
          requires=bars >= PLACE_BAR, requires_detail="run ended before placing")

    # The precondition that makes cancel meaningful at all.
    c.add("limit_rested", rested,
          f"has_open_order was False two bars after placing a limit at "
          f"{PASSIVE_FACTOR:.0%} of market — it did not rest, so it either filled "
          f"or was never placed. before={data.samples('before_cancel')[:1]}",
          requires=data.count("limit_sent") > 0,
          requires_detail="no limit order was placed")

    c.add("passive_limit_did_not_fill", data.count("fill") == 0,
          f"{data.count('fill')} fills on a limit at {PASSIVE_FACTOR:.0%} of "
          f"market: {data.samples('fill')[:1]}",
          requires=data.count("limit_sent") > 0,
          requires_detail="no limit order was placed")

    c.add("cancel_emits_canceled_event", data.count("canceled") > 0,
          f"cancel_all_orders({SYMBOL!r}) produced no CANCELED order event; "
          f"statuses seen={statuses}",
          requires=rested and data.count("cancel_sent") > 0,
          requires_detail="no resting order existed to cancel")

    c.add("order_cleared_after_cancel", data.counters.get("open_after_cancel", 1) == 0,
          f"has_open_order still True {VERIFY_BAR - CANCEL_BAR} bars after cancel: "
          f"{data.samples('after_cancel')[:1]}",
          requires=rested and data.count("cancel_sent") > 0,
          requires_detail="no resting order existed to cancel")

    c.finish(NAME, surface=SURFACE, extra=(
        f"bars={bars}, orders={data.count('order_event')}, "
        f"fills={data.count('fill')}, cancels={data.count('canceled')}, "
        f"rested={rested}, open_after_cancel={data.counters.get('open_after_cancel')}"
    ))


if __name__ == "__main__":
    main()
