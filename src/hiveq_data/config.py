"""Stub for hiveq_data.config."""
from __future__ import annotations
from typing import Optional


class Config:
    @property
    def api_key(self) -> Optional[str]: ...
    @property
    def base_url(self) -> str: ...
    @property
    def user_id(self) -> Optional[str]: ...
    @property
    def org_id(self) -> Optional[str]: ...
    @property
    def user_name(self) -> Optional[str]: ...
    def is_configured(self) -> bool: ...
    def reset(self) -> None: ...
    def configure_from_env(self) -> None: ...


def configure(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    user_id: Optional[str] = None,
    org_id: Optional[str] = None,
    user_name: Optional[str] = None,
) -> None:
    ...


def get_config() -> Config:
    ...


def ensure_configured() -> None:
    ...
