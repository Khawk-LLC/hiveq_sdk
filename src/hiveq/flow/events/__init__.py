"""Event system for strategy callbacks.

This module provides the event types used throughout HiveQ Flow for strategy
callbacks. Events are dispatched by the framework and received by your strategy's
event handlers (on_hiveq_event, on_bar, on_order, etc.).

Event Types
-----------
The following event types are available:

Market Data Events
------------------
- **BarEvent**: OHLCV bar data received (price bars)
- **TradeEvent**: Trade tick data received (on_trade callback)
- **QuoteEvent**: Quote tick data received (on_quote callback)
- **SnapEvent**: Options snapshot data received (on_snap callback)
- **CustomDataEvent**: Custom user-defined data events

Lifecycle Events
------------------
- **StartEvent**: Strategy initialization (on_start callback)
- **StopEvent**: Strategy teardown (on_stop callback)
- **TimerEvent**: Periodic timer fired (on_timer callback)

Order and Position Events
------------------
- **OrderEvent**: Order state changes (submitted, accepted, filled, etc.) - use on_order callback
- **PositionEvent**: Position state changes (opened, changed, closed) - use on_position callback

Event Handling
--------------
Events are typically handled in two ways:

1. **Unified event handler** - Single callback receives all events:

>>> class MyStrategy:
...     def on_hiveq_event(self, ctx: hf.Context, event: hf.Event):
...         if event.type == EventType.BAR:
...             bar = event.data()
...             print(f"Bar: {bar.close}")
...         elif event.type == EventType.ORDER_FILLED:
...             order = event.data()
...             print(f"Order filled")
...         elif event.type == EventType.POSITION_OPENED:
...             position = event.data()
...             print(f"Position opened: {position.quantity}")

2. **Specific event callbacks** - Dedicated callback per event type:

Expects strategy implementors to have these call backs implemented. The callback method name has to match.

>>> class MyStrategy:
...     def on_bar(self, ctx: hf.Context, barEvent: hf.events.BarEvent):
...         print(f"Bar: {bar.close}")
...
...     def on_order(self, ctx: hf.Context, event: hf.events.OrderEvent):
...         if event.type == EventType.ORDER_FILLED:
...             print("Order filled")
...
...     def on_position(self, ctx: hf.Context, event: hf.events.PositionEvent):
...         if event.type == EventType.POSITION_OPENED:
...             print(f"Position opened: {event.position.quantity}")
...
...     def on_custom_data(self, ctx:hf.Context, event:hf.events.CustomDataEvent):
...         print('Receiverd custom')
...
...     def on_timer(self, ctx:hf.Context, event:hf.events.TimerEvent):
...         print('Receiverd custom')

Event Structure
---------------
All events inherit from the base Event class and provide:

- **type**: EventType enum indicating the event kind
- **ts_event**: Event timestamp in nanoseconds
- **data()**: Method to access event-specific data

Examples
--------
Handle bar events in unified callback:

>>> from hiveq.flow.config import EventType
>>>
>>> class BarStrategy:
...     def on_hiveq_event(self, ctx, event):
...         if event.type == EventType.BAR:
...             bar = event.data()
...             print(f"{bar.symbol}: O={bar.open} C={bar.close}")

Handle multiple event types:

>>> class MultiEventStrategy:
...     def on_hiveq_event(self, ctx, event):
...         if event.type == EventType.START:
...             ctx.subscribe_bars(['AAPL'], interval='1m')
...
...         elif event.type == EventType.BAR:
...             bar = event.data()
...             if bar.close > 150:
...                 ctx.buy_order(bar.symbol, quantity=100)
...
...         elif event.type == EventType.ORDER_FILLED:
...             order = event.data()
...             print(f"Order filled at {order.order.last_px}")
...
...         elif event.type == EventType.POSITION_OPENED:
...             position = event.data()
...             print(f"Position opened: {position.position.quantity}")
...
...         elif event.type == EventType.TIMER:
...             timer = event.data()
...             print(f"Timer {timer.timer_id} fired")

See Also
--------
hiveq.flow.config.EventType : Event type enumeration
hiveq.flow.Context : Context object passed to all callbacks
hiveq.flow.data.Bar : Bar data structure
hiveq.flow.trading_types.Position : Position wrapper object
"""

from .event_types import (
    Event,
    BarEvent,
    TradeEvent,
    QuoteEvent,
    SnapEvent,
    IndexPriceEvent,
    Rollover,
    RolloverEvent,
    CustomDataEvent,
    StartEvent,
    StopEvent,
    OrderEvent,
    PositionEvent,
    TimerEvent,
)

__all__ = [
    "Event",
    "BarEvent",
    "TradeEvent",
    "QuoteEvent",
    "SnapEvent",
    "IndexPriceEvent",
    "Rollover",
    "RolloverEvent",
    "CustomDataEvent",
    "StartEvent",
    "StopEvent",
    "OrderEvent",
    "PositionEvent",
    "TimerEvent",
]