"""Stub for hiveq_data.config."""
from __future__ import annotations
from typing import Optional


def configure(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    user_id: Optional[str] = None,
    org_id: Optional[str] = None,
    user_name: Optional[str] = None,
) -> None:
    ...


def get_config():
    ...


def ensure_configured() -> None:
    ...
