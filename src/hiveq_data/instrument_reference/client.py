"""Stub for hiveq_data.instrument_reference.client."""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Union


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

    def get_futures(
        self,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        expiry_type: str = "volume",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    def get_options(
        self,
        symbols: Optional[List[str]] = None,
        chains: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        expiration_date: Optional[str] = None,
        strike: Optional[Union[float, str]] = None,
        option_type: Optional[str] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        root: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def get_equities(
        self,
        symbols: Optional[List[str]] = None,
        date: Optional[str] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    def get_indices(
        self,
        symbols: Optional[List[str]] = None,
        date: Optional[str] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dict[str, Any]: ...

    def get_instruments(
        self,
        symbols: Optional[List[str]] = None,
        asset_class: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exchange: Optional[str] = None,
        currency: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Dict[str, Any]: ...
