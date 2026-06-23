"""Stub for hiveq_data.metadata."""
from __future__ import annotations
from typing import Optional


class Metadata:
    """Metadata client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        user_id: Optional[str] = None,
        org_id: Optional[str] = None,
        user_name: Optional[str] = None,
    ) -> None: ...


__all__ = ["Metadata"]
