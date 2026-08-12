"""Stub for hiveq_data.instrument_reference."""
from __future__ import annotations
from typing import Any, Dict, List, Optional


from hiveq_data.instrument_reference.client import InstrumentReference


class FuturesReference:
    def __init__(self, **kwargs) -> None: ...


class OptionsReference:
    def __init__(self, **kwargs) -> None: ...


class EquityReference:
    def __init__(self, **kwargs) -> None: ...


class IndexReference:
    def __init__(self, **kwargs) -> None: ...


class InstrumentReferenceBase:
    ...


__all__ = [
    "InstrumentReference",
    "FuturesReference",
    "OptionsReference",
    "EquityReference",
    "IndexReference",
    "InstrumentReferenceBase",
]
