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
        root: Optional[Union[str, List[str]]] = None,
        symbols: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]: ...

    def get_options(
        self,
        chains: Optional[Union[str, List[str]]] = None,
        symbols: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]: ...

    def get_equities(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]: ...

    def get_indices(
        self,
        symbols: Optional[Union[str, List[str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]: ...

    def get_instruments(
        self,
        asset_class: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]: ...
