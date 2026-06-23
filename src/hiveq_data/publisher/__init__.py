"""Stub for hiveq_data.publisher."""
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


class HiveQPublisher:
    """Kafka publisher for HiveQ Data API."""

    def __init__(
        self,
        config_file: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
        **kwargs,
    ) -> None: ...

    def publish(
        self,
        schema: str = ...,
        data: Optional[Dict[str, Any]] = None,
        key: Optional[str] = None,
        **kwargs,
    ) -> None: ...

    def publish_data(
        self,
        schema: str = ...,
        data: Optional[List[Dict[str, Any]]] = None,
        key: Optional[str] = None,
        **kwargs,
    ) -> None: ...

    def flush(self, timeout: Optional[float] = None) -> None: ...
    def close(self) -> None: ...
    def is_connected(self) -> bool: ...


__all__ = ["Publisher", "HiveQPublisher"]
