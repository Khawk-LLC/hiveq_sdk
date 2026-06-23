"""Stub for hiveq_data.live."""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Union


class LiveStream:
    """Live data streaming client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> None: ...

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def subscribe(
        self,
        topic: str,
        key: Optional[str] = None,
        callback: Optional[Callable] = None,
        **kwargs,
    ) -> None: ...
    async def unsubscribe(self, topic: str, key: Optional[str] = None) -> None: ...
    async def wait_until_disconnected(self) -> None: ...


__all__ = ["LiveStream"]
