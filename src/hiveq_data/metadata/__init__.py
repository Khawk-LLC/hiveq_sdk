"""Stub for hiveq_data.metadata."""
from __future__ import annotations
from typing import List, Optional, Union


class Metadata:
    """Metadata client for HiveQ Data API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ) -> None: ...

    def get_datasets(self) -> dict: ...
    def get_schemas(self, dataset: Union[str, List[str]]) -> dict: ...
    def get_schema(self, schema: str, dataset: str) -> dict: ...
    def refresh_schema(self) -> dict: ...


__all__ = ["Metadata"]
