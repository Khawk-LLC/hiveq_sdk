"""Typed wrapper over the orchestrator's ``/livesim/*`` and ``/fleet/*`` routes.

Deliberately built on ``hiveq.flow._client.get_client()._request`` rather than a
fresh ``requests`` session: that method already resolves the base URL
(``HIVEQ_BASE_URL`` -> auth host + ``/api/orchestrator`` -> hosted default),
attaches the API key header, and applies the timeout. Reimplementing it here
would mean re-implementing the auth precedence too, and drifting from it.

Route inventory verified against
``hiveq/apps/orchestrator-service/src/hiveq_orchestrator/server/server.py``.

Every method returns ``(body, status)`` — never raises on a non-2xx — because
several tests assert *specific* failure codes (e.g. the strict-creator ACL on
promotion must return 403 for a non-owner).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from agent_qa.core.profiles import apply_profile

Response = Tuple[Any, int]


class LivesimClient:
    def __init__(self, timeout: float = 120.0) -> None:
        apply_profile()
        # A dedicated client rather than the module singleton: ``_request``
        # supplies ``timeout=self.timeout`` itself, so the per-call budget is an
        # attribute, and mutating the shared instance would change the timeout
        # for everything else in the process.
        from hiveq.flow._client import _Client  # noqa: PLC0415 - SDK-only namespace

        self._client = _Client(timeout=timeout)
        self.timeout = timeout

    @property
    def base_url(self) -> str:
        return getattr(self._client, "base_url", "")

    # ----------------------------------------------------------------- plumbing

    def request(self, method: str, endpoint: str, body: Optional[dict] = None,
                params: Optional[dict] = None) -> Response:
        kwargs: Dict[str, Any] = {}
        if params:
            kwargs["params"] = params
        if body is not None:
            kwargs["data"] = json.dumps(body)
            kwargs["headers"] = {"Content-Type": "application/json"}
        try:
            resp = self._client._request(method, endpoint, **kwargs)
        except Exception as exc:  # noqa: BLE001 - transport failure is a result
            return {"error": True, "message": str(exc)}, 0
        try:
            payload = resp.json()
        except ValueError:
            payload = {"raw": resp.text}
        return payload, resp.status_code

    # ------------------------------------------------------------------- health

    def health(self) -> Response:
        return self.request("GET", "/health")

    # --------------------------------------------------------------- deployment

    def deploy_native(self, body: dict) -> Response:
        """``POST /livesim/deploy-native`` — deploy without a source backtest.

        Body keys the handler reads: ``instance_name``, ``asset``, ``manifest``,
        ``run_yaml``, ``container_id``, ``hot_load``, ``n_instances``,
        ``parameter_overrides``, ``dry_run``, ``live_signals``, ``signal_ids``,
        ``data_configs``, ``dataset``, ``schemas``.
        """
        return self.request("POST", "/livesim/deploy-native", body=body)

    def promote(self, source_run_id: str, **extra) -> Response:
        """``POST /livesim/deploy`` — promote a finished backtest to livesim.

        This is *the* promotion path. The handler enforces a strict-creator ACL:
        only the user who submitted ``source_run_id`` may promote it (403
        otherwise), and a cross-org caller is rejected before that.
        """
        body = {"source_run_id": source_run_id}
        body.update(extra)
        return self.request("POST", "/livesim/deploy", body=body)

    def list_deployments(self, status: Optional[str] = None) -> Response:
        params = {"status": status} if status else None
        return self.request("GET", "/livesim/deployments", params=params)

    def cancel_deployment(self, deployment_id: str) -> Response:
        return self.request("POST", f"/livesim/deployments/{deployment_id}/cancel")

    def update_strategy_params(self, container_id: str, deployment_id: str,
                               instance_name: str, param_changes: dict) -> Response:
        """``PATCH .../strategies/<instance>/params`` — the hot-update path.

        ``param_changes`` is required by the route; omitting it is a 400. A
        successful change should surface as ``on_param_change`` in the strategy
        and a ``PARAM_CHANGE`` event-log row.
        """
        endpoint = (
            f"/livesim/containers/{container_id}/deployments/{deployment_id}"
            f"/strategies/{instance_name}/params"
        )
        return self.request("PATCH", endpoint, body={"param_changes": param_changes})

    def update_run_yaml(self, container_id: str, deployment_id: str, run_yaml: str) -> Response:
        endpoint = f"/livesim/containers/{container_id}/deployments/{deployment_id}/run-yaml"
        return self.request("PUT", endpoint, body={"run_yaml": run_yaml})

    def strategy_action(self, container_id: str, instance_name: str, action: str) -> Response:
        endpoint = f"/livesim/containers/{container_id}/strategies/{instance_name}/{action}"
        return self.request("POST", endpoint)

    # ---------------------------------------------------------------- containers

    def list_containers(self) -> Response:
        return self.request("GET", "/livesim/containers")

    def create_container(self, body: dict) -> Response:
        return self.request("POST", "/livesim/containers", body=body)

    def start_container(self, container_id: str) -> Response:
        return self.request("POST", f"/livesim/containers/{container_id}/start")

    def stop_container(self, container_id: str) -> Response:
        return self.request("POST", f"/livesim/containers/{container_id}/stop")

    def restart_container(self, container_id: str) -> Response:
        return self.request("POST", f"/livesim/containers/{container_id}/restart")

    def remove_container(self, container_id: str) -> Response:
        return self.request("POST", f"/livesim/containers/{container_id}/remove")

    def container_logs(self, container_id: str, **params) -> Response:
        return self.request("GET", f"/livesim/containers/{container_id}/logs", params=params or None)

    def container_log_components(self, container_id: str) -> Response:
        return self.request("GET", f"/livesim/containers/{container_id}/log-components")

    def container_deployments(self, container_id: str) -> Response:
        return self.request("GET", f"/livesim/containers/{container_id}/deployments")

    def strategy_logs(self, deployment_id: str, instance_name: str, **params) -> Response:
        endpoint = f"/livesim/deployments/{deployment_id}/strategies/{instance_name}/logs"
        return self.request("GET", endpoint, params=params or None)

    # --------------------------------------------------------------------- fleet

    def get_fleet_yaml(self) -> Response:
        return self.request("GET", "/livesim/fleet-yaml")

    def put_fleet_yaml(self, yaml_text: str) -> Response:
        return self.request("PUT", "/livesim/fleet-yaml", body={"yaml": yaml_text})

    def list_fleet_containers(self) -> Response:
        return self.request("GET", "/fleet/containers")


def container_ids(body: Any) -> List[str]:
    """Pull container ids out of a list response, whatever its envelope.

    The orchestrator wraps some list payloads in ``{'containers': [...]}`` and
    returns others bare, so normalise once here instead of in every test.
    """
    items = body
    if isinstance(body, dict):
        for key in ("containers", "data", "items", "results"):
            if isinstance(body.get(key), list):
                items = body[key]
                break
    if not isinstance(items, list):
        return []
    out = []
    for item in items:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            for key in ("container_id", "containerId", "id", "name"):
                if item.get(key):
                    out.append(str(item[key]))
                    break
    return out
