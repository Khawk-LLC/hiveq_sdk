"""Stub for hiveq_data.instrument_reference."""
from __future__ import annotations
from typing import Any, Dict, List, Optional


class InstrumentReference:
    """Instrument reference client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> None: ...


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
