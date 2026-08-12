"""Stub for hiveq_data.publisher.client."""
from __future__ import annotations
from typing import Any, Dict, List, Optional


class Publisher:
    """REST publisher client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        async_mode: bool = True,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> None: ...

    def publish(
        self,
        schema: str,
        data: List[Dict[str, Any]],
        key: str,
        operation: str = 'add',
    ) -> Dict[str, Any]: ...

    def flush(self) -> None: ...
    def close(self) -> None: ...
