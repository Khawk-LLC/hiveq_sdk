"""Stub for hiveq_data.exceptions."""
from __future__ import annotations
from typing import Optional


class HiveQAPIError(Exception):
    """Exception raised when HiveQ Data API requests fail."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
        response_json: Optional[dict] = None,
        request_url: Optional[str] = None,
        request_method: Optional[str] = None,
        original_exception: Optional[Exception] = None,
    ):
        self.message = message
        self.status_code = status_code
        self.response_text = response_text
        self.response_json = response_json
        self.request_url = request_url
        self.request_method = request_method
        self.original_exception = original_exception
        super().__init__(message)
