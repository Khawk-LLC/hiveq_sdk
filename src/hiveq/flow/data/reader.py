"""HiveQDataReader — REST client for the platform's ``runs`` resource family.

Symmetric counterpart to :class:`HiveQDataPublisher` (which only writes). It reads
a run's result + intermediate data from the strategy-service ``runs`` endpoints
(``GET /api/strategy/<version>/runs/{run_id}/...``) via the same platform gateway
and credentials hiveq.flow already uses for publishing.

Users normally don't touch this directly — they use the :class:`~hiveq.flow.runs.Run`
handle returned by ``run_backtest`` / ``hf.get_run(run_id)``, which wraps a reader.
"""
import os
from typing import Any, Dict, List, Optional

import requests

from hiveq.flow.logger import logger

logger = logger()

API_VERSION = "v0"


def _page(limit: Optional[int], offset: Optional[int]) -> Optional[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if limit is not None:
        params["limit"] = limit
    if offset is not None:
        params["offset"] = offset
    return params or None


class HiveQDataReader:
    """Read a run's data from the platform ``runs`` REST API.

    Credentials/URL default to the same environment hiveq.flow uses elsewhere
    (``HIVEQ_DATA_URL`` gateway + ``HIVEQ_API_KEY`` / ``HIVEQ_ORG_ID``).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        org_id: Optional[str] = None,
        user_id: Optional[str] = None,
        user_name: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.api_key = api_key or os.environ.get("HIVEQ_API_KEY")
        # Follow the auth host (HIVEQ_AUTH_URL) when HIVEQ_DATA_URL isn't set, so a
        # single configured URL points the whole SDK at the same platform.
        from hiveq.flow.config import platform_origin

        origin = platform_origin()
        self.base_url = (
            base_url
            or os.environ.get("HIVEQ_DATA_URL")
            or (f"{origin}/" if origin else None)
            or "http://localhost:80/"
        ).rstrip("/")
        self.org_id = org_id or os.environ.get("HIVEQ_ORG_ID")
        self.user_id = user_id or os.environ.get("HIVEQ_USER_ID")
        self.user_name = user_name or os.environ.get("HIVEQ_USER_NAME")
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        if self.org_id:
            h["X-Org-ID"] = self.org_id
        if self.user_id:
            h["X-User-ID"] = self.user_id
        if self.user_name:
            h["X-User-Name"] = self.user_name
        return h

    # Statuses worth retrying: rate limiting and transient gateway errors. A run
    # with e.g. no orders must still return an empty result, not crash — so a 429
    # from frequent polling backs off and retries rather than raising.
    _RETRY_STATUS = frozenset({429, 502, 503, 504})

    def _get(
        self, run_id: str, path: str = "", params: Optional[Dict[str, Any]] = None
    ) -> Any:
        import time
        url = f"{self.base_url}/api/strategy/{API_VERSION}/runs/{run_id}{path}"
        resp = None
        for attempt in range(5):
            resp = requests.get(
                url, headers=self._headers(), params=params, timeout=self.timeout
            )
            if resp.status_code in self._RETRY_STATUS and attempt < 4:
                ra = resp.headers.get("Retry-After")
                delay = float(ra) if (ra and ra.replace(".", "", 1).isdigit()) else min(2 ** attempt, 8)
                logger.debug(f"runs {path or '/'} -> {resp.status_code}; retrying in {delay:.1f}s")
                time.sleep(delay)
                continue
            break
        resp.raise_for_status()
        body = resp.json()
        if isinstance(body, dict) and "data" in body:
            return body["data"]
        return body

    # --- resource methods (mirror the REST paths 1:1) -----------------------
    def overview(self, run_id: str) -> Dict[str, Any]:
        return self._get(run_id)

    def status(self, run_id: str) -> Dict[str, Any]:
        return self._get(run_id, "/status")

    def report(
        self, run_id: str, include: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        params = {"include": ",".join(include)} if include else None
        return self._get(run_id, "/report", params)

    def summary(self, run_id: str) -> Dict[str, Any]:
        return self._get(run_id, "/summary")

    def metrics(self, run_id: str) -> List[Dict[str, Any]]:
        return self._get(run_id, "/metrics")

    def daily_returns(
        self, run_id: str, limit: Optional[int] = 100_000, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._get(run_id, "/daily-returns", _page(limit, offset))

    def equity_curve(
        self, run_id: str, limit: Optional[int] = 100_000, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._get(run_id, "/equity-curve", _page(limit, offset))

    def positions(
        self, run_id: str, limit: Optional[int] = 100_000, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._get(run_id, "/positions", _page(limit, offset))

    def orders(
        self, run_id: str, limit: Optional[int] = 100_000, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._get(run_id, "/orders", _page(limit, offset))

    def trades(
        self, run_id: str, limit: Optional[int] = 100_000, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._get(run_id, "/trades", _page(limit, offset))

    def event_logs(
        self, run_id: str, limit: Optional[int] = 100_000, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return self._get(run_id, "/event-logs", _page(limit, offset))
