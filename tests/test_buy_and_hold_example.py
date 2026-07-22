from datetime import datetime, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import SimpleNamespace

from hiveq.flow import EventType, OrderStatus


_EXAMPLE = Path(__file__).parents[1] / "examples" / "deploy_buy_and_hold.py"
_SPEC = spec_from_file_location("deploy_buy_and_hold", _EXAMPLE)
assert _SPEC and _SPEC.loader
_MODULE = module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
BuyAndHold = _MODULE.BuyAndHold


class FakeContext:
    def __init__(self, now_utc):
        self._now_utc = now_utc
        self.orders = []
        self.logs = []

    def now_utc(self):
        return self._now_utc

    def is_flat(self, symbol):
        return True

    def buy_order(self, symbol, quantity):
        self.orders.append((symbol, quantity))
        return SimpleNamespace(order_id="order-1")

    def add_event_log(self, message, symbol):
        self.logs.append((message, symbol))


def bar_event(symbol="AAPL"):
    return SimpleNamespace(data=lambda: SimpleNamespace(symbol=symbol))


def order_event(event_type, status, *, is_filled=False):
    order = SimpleNamespace(
        symbol="AAPL",
        status=status,
        is_filled=is_filled,
        filled_qty=100,
        avg_px=200.0,
    )
    return SimpleNamespace(type=event_type, data=lambda: order)


def test_waits_until_regular_market_hours_and_retries_after_rejection():
    strategy = BuyAndHold()
    ctx = FakeContext(datetime(2026, 7, 22, 13, 29, tzinfo=timezone.utc))

    strategy.on_bar(ctx, bar_event())
    assert ctx.orders == []

    ctx._now_utc = datetime(2026, 7, 22, 13, 30, tzinfo=timezone.utc)
    strategy.on_bar(ctx, bar_event())
    assert ctx.orders == [("AAPL", 100)]
    assert strategy.order_pending is True
    assert strategy.bought is False

    strategy.on_order(
        ctx,
        order_event(EventType.ORDER_REJECTED, OrderStatus.REJECTED),
    )
    assert strategy.order_pending is False
    assert strategy.bought is False

    strategy.on_bar(ctx, bar_event())
    assert ctx.orders == [("AAPL", 100), ("AAPL", 100)]


def test_marks_bought_only_after_fill_and_does_not_resubmit():
    strategy = BuyAndHold()
    ctx = FakeContext(datetime(2026, 7, 22, 14, 0, tzinfo=timezone.utc))

    strategy.on_bar(ctx, bar_event())
    strategy.on_order(
        ctx,
        order_event(EventType.ORDER_FILLED, OrderStatus.FILLED, is_filled=True),
    )

    assert strategy.bought is True
    assert strategy.order_pending is False
    strategy.on_bar(ctx, bar_event())
    assert ctx.orders == [("AAPL", 100)]
