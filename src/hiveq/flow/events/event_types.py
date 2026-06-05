"""Auto-generated stub (thin client SDK).

Name-only authoring surface for ``hiveq.flow.events.event_types``.
The real implementation is engine-backed and runs only on the HiveQ
platform executor; see the adjacent ``.pyi`` for the typed surface.
"""
from __future__ import annotations
import enum

class Event:
    ...

class CustomDataEvent(Event):
    ...

class BarEvent(Event):
    ...

class TradeEvent(Event):
    ...

class QuoteEvent(Event):
    ...

class SnapEvent(Event):
    ...

class IndexPriceEvent(Event):
    ...

class Rollover:
    ...

class RolloverEvent(Event):
    ...

class StartEvent(Event):
    ...

class StopEvent(Event):
    ...

class OrderEvent(Event):
    ...

class PositionEvent(Event):
    ...

class TimerEvent(Event):
    ...

class ExecutorEventData:
    ...

class ExecutorEvent(Event):
    ...

class SecurityEventData:
    ...

class SecurityEvent(Event):
    ...

__all__ = ['BarEvent', 'CustomDataEvent', 'Event', 'ExecutorEvent', 'ExecutorEventData', 'IndexPriceEvent', 'OrderEvent', 'PositionEvent', 'QuoteEvent', 'Rollover', 'RolloverEvent', 'SecurityEvent', 'SecurityEventData', 'SnapEvent', 'StartEvent', 'StopEvent', 'TimerEvent', 'TradeEvent']
