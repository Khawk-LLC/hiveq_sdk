"""Stub for hiveq_data.live."""
from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Union
from datetime import datetime


class LiveStream:
    """Live data streaming client for HiveQ Data API."""

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 8765,
        auto_reconnect: bool = True,
        reconnect_interval: float = 1.0,
        status_log_interval: float = 30.0,
    ) -> None: ...

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def subscribe(
        self,
        topic: str,
        keys: Union[str, List[str]],
        callback: Callable[[Dict[str, Any]], None],
        *,
        replay: Optional[Union[str, bool]] = None,
        from_ts: Optional[Union[datetime, str, int, float]] = None,
        to_ts: Optional[Union[datetime, str, int, float]] = None,
        replay_tz: Optional[str] = None,
    ) -> None: ...
    async def unsubscribe(self, topic: str, key: str,
                          callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> None: ...
    async def wait_until_disconnected(self) -> None: ...
    def is_connected(self) -> bool: ...
    async def __aenter__(self) -> LiveStream: ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...


__all__ = ["LiveStream"]
