"""Deployment.params() / set_params(): the SDK surface for runtime parameters.

The engine is the source of truth for parameter values: it keeps its own param
store, writes it on every change and reloads it at startup. So set_params()
pushes into the running engine -- a change that never reached it is not in
force -- and writes the deployment's stored config too, which seeds a strategy
on its first run and is what params() reads back.
"""

from typing import Any, Dict, List

import pytest

from hiveq.flow import livesim


@pytest.fixture
def calls(monkeypatch) -> List[Dict[str, Any]]:
    """Capture REST calls and answer them with a two-strategy deployment."""
    recorded: List[Dict[str, Any]] = []

    def fake_call(method: str, path: str, **kwargs) -> Dict[str, Any]:
        recorded.append({"method": method, "path": path, **kwargs})
        if method == "GET":
            return {
                "strategies": [
                    {
                        "instance_name": "ESMaker",
                        "params": {"threshold": 0.1},
                        "live": {"threshold": "0.9"},
                        "live_available": True,
                        "drift": ["threshold"],
                    },
                    {
                        "instance_name": "ESTaker",
                        "params": {"threshold": 0.1},
                        "live": {"threshold": "0.1"},
                        "live_available": True,
                        "drift": [],
                    },
                ]
            }
        return {
            "strategies": [
                {
                    "instance_name": "ESMaker",
                    "changed": {"threshold": 0.4},
                    "old_values": {"threshold": 0.1},
                    "engine_notified": True,
                }
            ]
        }

    monkeypatch.setattr(livesim, "_call", fake_call)
    return recorded


@pytest.fixture
def deployment() -> livesim.Deployment:
    return livesim.Deployment(deployment_id="dep-1", strategies=["ESMaker", "ESTaker"])


def test_params_are_keyed_by_instance_name(calls, deployment):
    result = deployment.params()

    assert sorted(result) == ["ESMaker", "ESTaker"]
    assert result["ESMaker"]["params"] == {"threshold": 0.1}


def test_params_surface_the_drift_between_disk_and_engine(calls, deployment):
    # Non-empty drift is how a caller learns something changed the running
    # values without recording them -- the failure this API exists to expose.
    result = deployment.params()

    assert result["ESMaker"]["drift"] == ["threshold"]
    assert result["ESTaker"]["drift"] == []


def test_live_false_is_sent_as_a_query_param(calls, deployment):
    deployment.params(live=False)

    assert calls[0]["params"] == {"live": "false"}


def test_params_can_be_narrowed_to_one_strategy(calls, deployment):
    deployment.params(strategy="ESMaker")

    assert calls[0]["params"]["instance_name"] == "ESMaker"
    assert "live" not in calls[0]["params"]


def test_set_params_patches_the_deployment_handle(calls, deployment):
    result = deployment.set_params({"threshold": 0.4})

    assert calls[0]["method"] == "PATCH"
    assert calls[0]["path"] == "/deployments/dep-1/params"
    assert calls[0]["json"] == {"param_changes": {"threshold": 0.4}}
    assert result["ESMaker"]["old_values"] == {"threshold": 0.1}


def test_set_params_can_target_one_strategy(calls, deployment):
    deployment.set_params({"threshold": 0.4}, strategy="ESMaker")

    assert calls[0]["json"]["instance_name"] == "ESMaker"


def test_set_params_rejects_an_empty_change(calls, deployment):
    # An empty patch would be a silent no-op the caller reads as success.
    with pytest.raises(ValueError):
        deployment.set_params({})

    assert calls == []


def test_a_deployment_without_an_id_has_no_params_to_read(calls):
    orphan = livesim.Deployment(deployment_id=None)

    assert orphan.params() == {}
    assert orphan.set_params({"threshold": 0.4}) == {}
    assert calls == []
