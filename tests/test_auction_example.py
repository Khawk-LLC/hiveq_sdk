"""Regression coverage for the multi-day auction example."""

from datetime import datetime
import importlib.util
from pathlib import Path


EXAMPLE = Path(__file__).parents[1] / "examples" / "auction_moo_moc.py"
SPEC = importlib.util.spec_from_file_location("auction_moo_moc", EXAMPLE)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class _Event:
    def __init__(self, timestamp, symbol="AAPL"):
        self._data = type("Tick", (), {"time": timestamp, "symbol": symbol})()

    def data(self):
        return self._data


class _Context:
    def __init__(self):
        self.orders = []

    def buy_order(self, symbol, **kwargs):
        self.orders.append(("buy", symbol, kwargs["order_type"]))

    def sell_order(self, symbol, **kwargs):
        self.orders.append(("sell", symbol, kwargs["order_type"]))

    def add_event_log(self, *args, **kwargs):
        pass


def test_places_moo_and_moc_on_every_trading_day_without_tick_duplicates():
    strategy = MODULE.AuctionMooMoc()
    context = _Context()

    for day in (19, 22, 23, 24, 25):
        for minute in (1, 2):
            strategy.on_trade(context, _Event(datetime(2025, 9, day, 8, minute)))
        for minute in (31, 32):
            strategy.on_trade(context, _Event(datetime(2025, 9, day, 15, minute)))

    assert len(context.orders) == 10
    assert [side for side, _, _ in context.orders].count("buy") == 5
    assert [side for side, _, _ in context.orders].count("sell") == 5
