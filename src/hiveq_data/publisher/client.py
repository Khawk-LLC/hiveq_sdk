"""Stub for hiveq_data.publisher.client."""
from __future__ import annotations
from typing import Any, Dict, List, Optional


class Publisher:
    """REST publisher client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
        async_mode: bool = False,
    ) -> None: ...

    def publish(
        self,
        schema: str = ...,
        data: Optional[List[Dict[str, Any]]] = None,
        key: Optional[str] = None,
        operation: str = 'add',
    ) -> Any: ...

    def flush(self) -> None: ...
    def close(self) -> None: ...
