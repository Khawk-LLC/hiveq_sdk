"""Stub for hiveq_data.historical.client."""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date


class Historical:
    """Historical data client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
        timezone: Optional[Union[str, Any]] = None,
    ) -> None: ...

    def get_data(
        self,
        dataset: str = ...,
        schema: str = ...,
        symbols: Optional[Union[str, List[str]]] = None,
        root: Optional[Union[str, List[str]]] = None,
        chains: Optional[Union[str, List[str]]] = None,
        start: Optional[Union[str, date, datetime]] = None,
        end: Optional[Union[str, date, datetime]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        filter_mode: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]: ...
