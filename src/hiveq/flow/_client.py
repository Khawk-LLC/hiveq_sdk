"""HiveQ platform REST client — deploy a captured task and observe it.

Talks directly to the HiveQ platform task API over HTTP (pure ``requests``):
submit a captured task, then pull its status / result / logs. The SDK ships no
platform-internal client package; this is the entire client surface.

Authentication is a single ``X-API-Key`` header — the platform derives the user
and organization from the key. No identity headers are ever sent.

Endpoint base resolves from ``HIVEQ_BASE_URL`` (falling back to the canonical
host). The API key resolves from ``HIVEQ_API_KEY`` unless passed explicitly.
"""
from __future__ import annotations

import base64
import json
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import cloudpickle
import requests

from hiveq.flow._payload import _TaskWrapper

# The wrapper is serialized BY VALUE — its class definition is embedded in the
# payload so the platform executor can reconstruct it without importing the SDK.
# Registration is scoped to the (single-class) _payload module, so nothing else
# is forced by-value.
import hiveq.flow._payload as _payload_module  # noqa: E402

cloudpickle.register_pickle_by_value(_payload_module)


_DEFAULT_BASE_URL = "https://staging.hiveq.ai/api/orchestrator"
_TERMINAL_STATES = ("completed", "failed", "terminated")
_DUPLICATE_ACTIONS = ("override", "terminate", "duplicate")


class TaskType(str, Enum):
    """Platform task types (sent as the ``X-Task-Type`` header)."""

    QUANT_SCRIPTS = "QUANT_SCRIPTS"
    HIVEQ_FLOW_BT = "HIVEQ_FLOW_BT"
    HIVEQ_FLOW_LIVE_SIM = "HIVEQ_FLOW_LIVE_SIM"
    HIVEQ_FLOW_PROD = "HIVEQ_FLOW_PROD"
    HIVEQ_ALPHA_AI = "HIVEQ_ALPHA_AI"
    HIVEQ_DEV_OPS_SCRIPTS = "HIVEQ_DEV_OPS_SCRIPTS"


class DuplicateTaskError(RuntimeError):
    """Raised when a task with the same name already exists (HTTP 409)."""

    def __init__(self, task_name, existing_task_id, existing_status, message=None):
        self.task_name = task_name
        self.existing_task_id = existing_task_id
        self.existing_status = existing_status
        super().__init__(
            message
            or f"Task '{task_name}' already exists "
            f"(id={existing_task_id}, status={existing_status})"
        )


class _Client:
    """Thin HTTP client for the HiveQ platform task API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: int = 30,
    ):
        # Order: explicit arg > HIVEQ_BASE_URL > the auth host (HIVEQ_AUTH_URL)
        # + /api/orchestrator > the hosted default. Following the auth host means
        # one configured URL points the whole SDK at the same platform.
        from hiveq.flow.config import platform_origin

        origin = platform_origin()
        self.base_url = (
            base_url
            or os.environ.get("HIVEQ_BASE_URL")
            or (f"{origin}/api/orchestrator" if origin else None)
            or _DEFAULT_BASE_URL
        ).rstrip("/")
        self.api_key = api_key or os.environ.get("HIVEQ_API_KEY")
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        # Only the API key is sent; identity is resolved server-side from it.
        return {"X-API-Key": self.api_key} if self.api_key else {}

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        headers = self._headers()
        headers.update(kwargs.pop("headers", {}))
        return requests.request(
            method, url, timeout=self.timeout, headers=headers, **kwargs
        )

    def submit(
        self,
        task_type: Union[str, TaskType],
        task_name: str,
        task: Any,
        entry_method: Optional[str] = "run",
        job_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        requirements: Optional[List[str]] = None,
        allow_duplicate: bool = False,
        duplicate_action: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Capture-and-send a task for execution. Returns the parsed JSON reply."""
        task_type_value = (
            task_type.value
            if isinstance(task_type, TaskType)
            else TaskType(task_type).value
        )

        wrapper = _TaskWrapper(target=task, entry_method=entry_method)
        payload_b64 = base64.b64encode(cloudpickle.dumps(wrapper)).decode()

        body = {
            "payload_b64": payload_b64,
            "job_type": job_type,
            "schedule": None,
            "metadata": metadata or {},
            "requirements": requirements,
        }
        headers = {
            "X-Task-Type": task_type_value,
            "X-Task-Name": task_name,
            "Content-Type": "application/json",
        }
        if allow_duplicate:
            headers["X-Allow-Duplicate"] = "true"
            if duplicate_action:
                if duplicate_action not in _DUPLICATE_ACTIONS:
                    raise ValueError(
                        f"duplicate_action must be one of {_DUPLICATE_ACTIONS}"
                    )
                headers["X-Duplicate-Action"] = duplicate_action

        resp = self._request("POST", "/submit", data=json.dumps(body), headers=headers)

        if resp.status_code == 409:
            data = resp.json()
            details = data.get("details", {})
            raise DuplicateTaskError(
                task_name,
                details.get("existing_task_id", "unknown"),
                details.get("existing_status", "unknown"),
                data.get("message"),
            )

        resp.raise_for_status()
        result = resp.json()
        if not result.get("success", True):
            raise ValueError(
                f"Task submission failed: {result.get('message', 'Unknown error')}"
            )
        return result

    def get_result(self, task_id: str) -> Dict[str, Any]:
        """Current result/status of a task."""
        resp = self._request("GET", f"/result/{task_id}")
        resp.raise_for_status()
        return resp.json()

    def get_status(self, task_id: str) -> Dict[str, Any]:
        """Full status of a task."""
        resp = self._request("GET", f"/status/{task_id}")
        resp.raise_for_status()
        return resp.json()

    def get_logs(
        self,
        task_id: Optional[str] = None,
        task_name: Optional[str] = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """Logs for a task (by id or name)."""
        if not task_id and not task_name:
            raise ValueError("At least one of task_id or task_name is required")
        params: Dict[str, Any] = {"limit": limit}
        if task_id:
            params["task_id"] = task_id
        if task_name:
            params["task_name"] = task_name
        resp = self._request("GET", "/logs", params=params)
        resp.raise_for_status()
        return resp.json()

    def get_logs_gz(
        self,
        task_id: Optional[str] = None,
        run_id: Optional[str] = None,
        task_name: Optional[str] = None,
        dest: Optional[str] = None,
    ) -> str:
        """Download the FULL log via ``GET /logs?format=gz`` (whole file, not a tail).

        Streamed in chunks so huge logs never load fully in memory. If ``dest`` is
        given, the gzip is written there (``.gz``) and the path is returned;
        otherwise the decompressed log text is returned as a ``str``.
        """
        import gzip

        if not (task_id or run_id or task_name):
            raise ValueError("one of task_id, run_id, or task_name is required")
        params: Dict[str, Any] = {"format": "gz"}
        if task_id:
            params["task_id"] = task_id
        if run_id:
            params["run_id"] = run_id
        if task_name:
            params["task_name"] = task_name

        resp = self._request("GET", "/logs", params=params, stream=True)
        resp.raise_for_status()
        if dest is not None:
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
            return dest
        return gzip.decompress(resp.content).decode("utf-8", errors="replace")

    def poll_result(
        self,
        task_id: str,
        timeout: Optional[int] = None,
        poll_interval: float = 1.0,
    ) -> Dict[str, Any]:
        """Block until the task reaches a terminal state (or ``timeout``)."""
        start = time.time()
        while True:
            result = self.get_result(task_id)
            if result.get("status") in _TERMINAL_STATES:
                return result
            if timeout is not None and time.time() - start >= timeout:
                raise TimeoutError(
                    f"Task {task_id} did not complete within {timeout} seconds"
                )
            time.sleep(poll_interval)


_default_client: Optional[_Client] = None


def configure(
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout: int = 30,
) -> _Client:
    """Create/replace the default client. Only the API key is required."""
    global _default_client
    _default_client = _Client(base_url=base_url, api_key=api_key, timeout=timeout)
    return _default_client


def get_client() -> _Client:
    """Return the default client, creating it from the environment if needed."""
    global _default_client
    if _default_client is None:
        _default_client = _Client()
    return _default_client


def submit(*args, **kwargs) -> Dict[str, Any]:
    return get_client().submit(*args, **kwargs)


def get_result(task_id: str) -> Dict[str, Any]:
    return get_client().get_result(task_id)


def get_status(task_id: str) -> Dict[str, Any]:
    return get_client().get_status(task_id)


def get_logs(
    task_id: Optional[str] = None,
    task_name: Optional[str] = None,
    limit: int = 1000,
) -> Dict[str, Any]:
    return get_client().get_logs(task_id, task_name, limit)


def get_logs_gz(
    task_id: Optional[str] = None,
    run_id: Optional[str] = None,
    task_name: Optional[str] = None,
    dest: Optional[str] = None,
) -> str:
    return get_client().get_logs_gz(task_id, run_id, task_name, dest)


def poll_result(
    task_id: str,
    timeout: Optional[int] = None,
    poll_interval: float = 1.0,
) -> Dict[str, Any]:
    return get_client().poll_result(task_id, timeout, poll_interval)
