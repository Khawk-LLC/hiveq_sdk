"""Deploy strategies straight to LiveSim.

``hf.deploy_livesim(...)`` is the live counterpart of ``hf.run_backtest(...)``:
it captures your strategy the same way, ships it to the platform, and hands
back a handle you track. No backtest in the path.

    import hiveq.flow as hf

    class MyStrategy:
        def on_bar(self, ctx, event):
            ...

    if __name__ == "__main__":
        deployment = hf.deploy_livesim(
            [StrategyConfig(name="MyStrategy", type="MyStrategy", symbols=["ES"])]
        )
        print(deployment.wait())

Keep the call under ``if __name__ == "__main__":``. Your module is shipped as
source and re-imported to rebuild the strategy, so a deploy at import time would
re-run itself during that restore.

The handle is ``deployment_id``. It is stable across container restarts and is
what every later call is addressed by. There is deliberately no ``run_id`` in
this API -- a LiveSim run id names one container engine lifetime, shared by
every deployment in that container, so it does not identify yours.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

# Terminal for polling: either steady state, or it stopped progressing. A
# running LiveSim deployment then stays up indefinitely, so it is an endpoint.
SETTLED = frozenset(
    {"running", "paused", "stopped", "terminated", "failed", "cancelled"}
)


class LivesimError(RuntimeError):
    """A LiveSim API call was rejected. ``code`` is the platform error code."""

    def __init__(self, status: int, body: Dict[str, Any], url: str = ""):
        error = body.get("error") or {}
        self.code = error.get("code", "UNKNOWN")
        self.status = status
        self.url = url
        self.findings = (error.get("security") or {}).get("findings") or []
        message = error.get("message") or body.get("message") or body
        if status == 404 and not self.code.startswith("LIVESIM_"):
            # A bare 404 means the route is not there, which almost always
            # means this platform does not have the LiveSim v1 API deployed --
            # not that something of yours is missing.
            message = (
                f"no LiveSim v1 API at {url or 'the configured platform'}. "
                f"Point HIVEQ_AUTH_URL at a platform that has it."
            )
        if self.findings:
            detail = "; ".join(
                f"{f.get('category')}: {f.get('message')}" for f in self.findings
            )
            message = f"{message} ({detail})"
        super().__init__(f"[{self.code}] {message}")


def _base_url() -> str:
    from hiveq.flow.config import platform_origin

    origin = (platform_origin() or "http://localhost").rstrip("/")
    return f"{origin}/api/strategy/v1/livesim"


def _call(method: str, path: str, **kwargs) -> Dict[str, Any]:
    import os

    api_key = os.environ.get("HIVEQ_API_KEY")
    if not api_key:
        raise LivesimError(401, {"error": {"message": "HIVEQ_API_KEY is not set"}})

    headers = {"X-API-Key": api_key}
    headers.update(kwargs.pop("headers", {}))
    url = f"{_base_url()}{path}"
    response = requests.request(
        method, url, headers=headers, timeout=300, **kwargs
    )
    if not response.ok:
        try:
            body = response.json()
        except ValueError:
            body = {"message": response.text[:500]}
        raise LivesimError(response.status_code, body, url)
    return response.json()["data"]


@dataclass
class Deployment:
    """A LiveSim deployment, addressed by ``deployment_id``."""

    deployment_id: Optional[str]
    operation_id: Optional[str] = None
    artifact_id: Optional[str] = None
    dry_run: bool = False
    container_id: Optional[str] = None
    asset: Optional[str] = None
    strategies: List[str] = field(default_factory=list)

    def status(self) -> Dict[str, Any]:
        """Current state of the deployment, straight from the platform."""
        if not self.deployment_id:
            return {"status": "preview", "dry_run": True}
        return _call("GET", f"/deployments/{self.deployment_id}")

    def wait(
        self, timeout: float = 180.0, poll_interval: float = 3.0
    ) -> Dict[str, Any]:
        """Block until the deployment settles, then return its state.

        Returns the last state seen if the timeout expires -- a slow start is
        not an error, so this reports rather than raises.
        """
        if not self.deployment_id:
            return self.status()

        deadline = time.time() + timeout
        state = self.status()
        while time.time() < deadline and state.get("status") not in SETTLED:
            time.sleep(poll_interval)
            state = self.status()
        return state

    def logs(
        self, strategy: Optional[str] = None, tail: int = 500
    ) -> str:
        """Recent log lines for this deployment's strategies.

        A snapshot, not a stream. ``strategy`` narrows it to one instance;
        with several, each is headed by its instance name.
        """
        if not self.deployment_id:
            return ""
        params: Dict[str, Any] = {"tail": tail}
        if strategy:
            params["instance_name"] = strategy
        data = _call(
            "GET", f"/deployments/{self.deployment_id}/logs", params=params
        )
        entries = data.get("strategies") or []
        if len(entries) == 1:
            return entries[0].get("logs") or ""
        return "\n".join(
            f"=== {entry.get('instance_name')} ===\n{entry.get('logs') or ''}"
            for entry in entries
        )

    # --- lifecycle ---------------------------------------------------------

    def _action(self, action: str, strategy: Optional[str] = None) -> Dict[str, Any]:
        body: Dict[str, Any] = {"action": action}
        if strategy:
            body["instance_name"] = strategy
        return _call(
            "POST", f"/deployments/{self.deployment_id}/actions", json=body
        )

    def start(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """Start this deployment's strategies."""
        return self._action("start", strategy)

    def stop(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """Stop them, leaving the deployment in place."""
        return self._action("stop", strategy)

    def pause(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """Pause them; resume with :meth:`start`."""
        return self._action("pause", strategy)

    def terminate(self, strategy: Optional[str] = None) -> Dict[str, Any]:
        """Tear the deployment down.

        Without ``strategy`` this removes the deployment entirely -- folder and
        bookkeeping -- and is not reversible. With one, it stops that single
        strategy and leaves the deployment.
        """
        return self._action("terminate", strategy)

    # --- parameters --------------------------------------------------------

    def params(
        self, strategy: Optional[str] = None, *, live: bool = True
    ) -> Dict[str, Any]:
        """This deployment's strategy parameters, keyed by instance name.

        Each entry carries ``params`` -- the deployment's stored values -- and,
        when the engine answers, ``live`` with the values it is running right
        now plus ``drift`` naming the keys where the two disagree. Pass
        ``live=False`` to read only the stored ones, which also works while the
        container is down.

        ``live`` is the authoritative one. The engine keeps its own param
        store, writes it on every change and reloads it at startup, so it --
        not the stored configuration -- is what a container comes up on. The
        stored values seed a strategy the first time it runs and are a record
        afterwards, which is why ``drift`` is worth reading: it means the
        engine has moved on from what was deployed.

            for name, entry in deployment.params().items():
                print(name, entry["params"], entry.get("drift"))
        """
        if not self.deployment_id:
            return {}
        query: Dict[str, Any] = {}
        if strategy:
            query["instance_name"] = strategy
        if not live:
            query["live"] = "false"
        data = _call(
            "GET", f"/deployments/{self.deployment_id}/params", params=query
        )
        return {
            entry["instance_name"]: entry
            for entry in (data.get("strategies") or [])
        }

    def set_params(
        self, changes: Dict[str, Any], strategy: Optional[str] = None
    ) -> Dict[str, Any]:
        """Change parameter values on a running deployment.

        The change is pushed into the running engine *and* written to the
        deployment's stored configuration, so it takes effect immediately and
        is still in force after a container restart. Your strategy's
        ``on_param_change(name, old, new)`` fires for each one.

        The push is the part that lasts: the engine records the new value in
        its own param store, which is what it reloads at startup. That also
        means this needs the engine to be running. On a stopped deployment the
        stored configuration is updated but the engine never hears about it,
        and the next start comes up on what the engine last had -- so set
        params on a running deployment, or check ``drift`` afterwards.

        Only parameters the strategy already declares can be set -- an unknown
        name is rejected rather than quietly added, because a typo would
        otherwise look like it worked and change nothing.

        With several strategies, ``strategy`` picks one; omit it to apply the
        same change to all of them. Returns the result per instance, including
        ``changed`` and ``old_values``.

            deployment.set_params({"max_position": 5, "threshold": 0.25})
        """
        if not self.deployment_id:
            return {}
        if not changes:
            raise ValueError("set_params needs at least one parameter")
        body: Dict[str, Any] = {"param_changes": changes}
        if strategy:
            body["instance_name"] = strategy
        data = _call(
            "PATCH", f"/deployments/{self.deployment_id}/params", json=body
        )
        return {
            entry["instance_name"]: entry
            for entry in (data.get("strategies") or [])
        }

    # --- data --------------------------------------------------------------

    def orders(self, **kwargs) -> Any:
        """Orders for this deployment."""
        return self._data("orders", **kwargs)

    def trades(self, **kwargs) -> Any:
        """Trades for this deployment."""
        return self._data("trades", **kwargs)

    def positions(self, **kwargs) -> Any:
        """Positions for this deployment."""
        return self._data("positions", **kwargs)

    def metrics(self, **kwargs) -> Any:
        """Per-strategy metrics for this deployment."""
        return self._data("metrics", **kwargs)

    def events(self, **kwargs) -> Any:
        """Strategy event logs for this deployment."""
        return self._data("event-logs", **kwargs)

    def _data(
        self,
        resource: str,
        *,
        strategy: Optional[str] = None,
        limit: int = 10_000,
        offset: int = 0,
        format: str = "json",
    ) -> Any:
        """Pull one data resource. Returns rows, or CSV text with format="csv".

        A pull, not a stream: each call returns a complete snapshot.
        """
        if not self.deployment_id:
            return [] if format == "json" else ""
        params: Dict[str, Any] = {
            "format": format,
            "limit": limit,
            "offset": offset,
        }
        if strategy:
            params["strategy_id"] = strategy

        import os

        api_key = os.environ.get("HIVEQ_API_KEY")
        response = requests.get(
            f"{_base_url()}/deployments/{self.deployment_id}/{resource}",
            headers={"X-API-Key": api_key} if api_key else {},
            params=params,
            timeout=300,
        )
        if not response.ok:
            try:
                body = response.json()
            except ValueError:
                body = {"message": response.text[:500]}
            raise LivesimError(response.status_code, body, response.url)
        if format == "csv":
            return response.text
        return response.json()["data"]

    def __repr__(self) -> str:
        if not self.deployment_id:
            return "<Deployment preview (dry run)>"
        return (
            f"<Deployment {self.deployment_id} "
            f"strategies={self.strategies} container={self.container_id}>"
        )


def upload_artifact(payload: bytes, filename: str = "strategy.pkl") -> str:
    """Store a strategy payload and return its immutable ``artifact_id``.

    Rarely called directly -- ``deploy_livesim`` does it for you. Useful to
    upload once and deploy the same bytes several times.
    """
    data = _call(
        "POST",
        "/artifacts",
        data=payload,
        headers={
            "Content-Type": "application/octet-stream",
            "X-Artifact-Filename": filename,
        },
    )
    return data["artifact_id"]


def deploy_livesim(
    strategy_configs: List[Any],
    *,
    data_configs: Optional[List[dict]] = None,
    instance_name: Optional[str] = None,
    instances: int = 1,
    container_id: Optional[str] = None,
    parameter_overrides: Optional[List[dict]] = None,
    signal_ids: Optional[List[str]] = None,
    dry_run: bool = False,
    wait: bool = False,
    **kwargs,
) -> Deployment:
    """Deploy strategies straight to LiveSim and return a :class:`Deployment`.

    Captures your strategy code exactly as ``run_backtest`` does, uploads it as
    an immutable artifact, and deploys that artifact. Returns as soon as the
    platform accepts it; poll with ``deployment.status()`` or block with
    ``deployment.wait()``.

    Parameters
    ----------
    strategy_configs : list[StrategyConfig]
        Strategies to deploy. LiveSim runs exactly one per deployment.
    data_configs : list[dict], optional
        Live data sources. Defaults to whatever the strategy declares.
    instance_name : str, optional
        Name for this instance, unique within its container. Becomes the
        ``strategy_id`` on every row the strategy publishes. Defaults to the
        strategy's own name.
    instances : int
        How many copies to run, 1..10.
    container_id : str, optional
        Pin placement. Needs the ``livesim:select_target`` permission; omit and
        the platform routes to an eligible container.
    dry_run : bool
        Validate and plan without materializing anything. The returned
        deployment has no id, because nothing was written under one.
    wait : bool
        Block until the deployment settles before returning.
    """
    import cloudpickle

    # Read the module attributes rather than binding them at import time:
    # _trader_id and _api_key are set by _ensure_initialized(), so a
    # `from hiveq.flow import _trader_id` here would capture None.
    import hiveq.flow as _flow
    from hiveq.flow.deploy_task import DeploymentHelper, HiveQFlowBackTestTask

    _flow._ensure_initialized()

    if not strategy_configs:
        raise ValueError("strategy_configs is required")
    if not 1 <= instances <= 10:
        raise ValueError("instances must be between 1 and 10")

    # Same capture path as run_backtest: the first-party import graph plus the
    # entry script, as source. No user code is cloudpickled.
    _names, source_bundle = DeploymentHelper.capture_calling_module(strategy_configs)
    if not source_bundle:
        raise RuntimeError(
            "Could not capture strategy source to deploy. Define your strategy "
            "in an importable .py file (or a normally-defined notebook cell)."
        )

    task = HiveQFlowBackTestTask(
        trader_id=_flow._trader_id,
        api_key=_flow._api_key,
        config_params=_flow._deploy_config_params(),
        strategy_configs=[s.to_dict() for s in strategy_configs],
        symbols=None,
        start_date=None,
        end_date=None,
        data_configs=data_configs,
        backtest_config=None,
        kwargs=kwargs,
        source_bundle=source_bundle,
    )

    name = instance_name or strategy_configs[0].name
    artifact_id = upload_artifact(cloudpickle.dumps(task), f"{name}.pkl")

    body: Dict[str, Any] = {
        "artifact_id": artifact_id,
        "instance_name": name,
        "instances": instances,
    }
    if container_id:
        body["target"] = {"container_id": container_id}
    if parameter_overrides:
        body["parameter_overrides"] = parameter_overrides
    if signal_ids:
        body["signal_ids"] = signal_ids
    if dry_run:
        body["dry_run"] = True

    accepted = _call("POST", "/deployments", json=body)
    deployment = Deployment(
        artifact_id=artifact_id,
        asset=accepted.get("asset"),
        container_id=accepted.get("container_id"),
        deployment_id=accepted.get("deployment_id"),
        dry_run=bool(accepted.get("dry_run")),
        operation_id=accepted.get("operation_id"),
        strategies=accepted.get("instance_names") or [],
    )
    if wait:
        deployment.wait()
    return deployment


def get_deployment(deployment_id: str) -> Deployment:
    """Re-attach to a deployment you already have the handle for."""
    state = _call("GET", f"/deployments/{deployment_id}")
    return Deployment(
        asset=state.get("asset"),
        container_id=state.get("container_id"),
        deployment_id=state.get("deployment_id"),
        strategies=state.get("instance_names") or [],
    )
