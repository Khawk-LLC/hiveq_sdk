"""Deployment.clone() and deploy_livesim(on_behalf_of_user_id=...).

Clone is a shortcut for redeploying a running instance under a new name: the
platform reuses its stored strategy, data sources and current params, with the
caller's changes merged on top. These tests pin the request the SDK sends.
"""

from typing import Any, Dict, List

import pytest

from hiveq.flow import livesim


@pytest.fixture
def calls(monkeypatch) -> List[Dict[str, Any]]:
    recorded: List[Dict[str, Any]] = []

    def fake_call(method: str, path: str, **kwargs) -> Dict[str, Any]:
        recorded.append({"method": method, "path": path, **kwargs})
        return {
            "asset": "FUTURES",
            "container_id": "livesim-futures-1",
            "deployment_id": "dep-clone",
            "instance_names": ["ES_MM_2"],
            "operation_id": "op-2",
            "status": "scheduled",
        }

    monkeypatch.setattr(livesim, "_call", fake_call)
    return recorded


@pytest.fixture
def deployment() -> livesim.Deployment:
    return livesim.Deployment(deployment_id="dep-1", strategies=["ES_MM"])


def test_clone_posts_the_new_name_and_returns_the_new_deployment(calls, deployment):
    # when the deployment is cloned with a param change
    clone = deployment.clone("ES_MM_2", params={"spread": 2.5})

    # then one POST names the clone and the change, and the new handle comes back
    assert calls == [{
        "method": "POST",
        "path": "/deployments/dep-1/clone",
        "json": {"instance_name": "ES_MM_2", "params": {"spread": 2.5}},
    }]
    assert clone.deployment_id == "dep-clone"
    assert clone.strategies == ["ES_MM_2"]
    assert clone.operation_id == "op-2"


def test_clone_sends_the_optional_selectors(calls, deployment):
    # when every option is given
    deployment.clone(
        " ES_MM_3 ",
        strategy="ES_MM",
        container_id="livesim-futures-2",
        on_behalf_of_user_id="trader-1",
    )

    # then they map onto the v1 body
    assert calls[-1]["json"] == {
        "instance_name": "ES_MM_3",
        "instance_to_clone": "ES_MM",
        "target": {"container_id": "livesim-futures-2"},
        "on_behalf_of_user_id": "trader-1",
    }


@pytest.mark.parametrize("name", ["", "   "])
def test_clone_needs_a_name(calls, deployment, name):
    # when no name is given
    with pytest.raises(ValueError, match="new instance_name"):
        deployment.clone(name)

    # then nothing is sent
    assert calls == []


def test_clone_of_a_preview_handle_is_refused(calls):
    # given a dry-run handle, which has no deployment_id
    with pytest.raises(livesim.LivesimError):
        livesim.Deployment(deployment_id=None).clone("X")
    assert calls == []


def test_deploy_livesim_sends_on_behalf_of_user_id(monkeypatch, calls):
    # given a strategy whose capture and upload are stubbed
    import hiveq.flow as flow
    from hiveq.flow import deploy_task

    monkeypatch.setattr(flow, "_ensure_initialized", lambda: None)
    monkeypatch.setattr(
        deploy_task.DeploymentHelper, "capture_calling_module",
        staticmethod(lambda _configs: (["S"], {"main.py": "x"})),
    )
    monkeypatch.setattr(deploy_task, "HiveQFlowBackTestTask", lambda **_kw: object())
    monkeypatch.setattr(livesim, "upload_artifact", lambda _payload, _name: "a" * 32)
    import cloudpickle
    monkeypatch.setattr(cloudpickle, "dumps", lambda _obj: b"payload")

    class Config:
        name = "ES_MM"

        def to_dict(self):
            return {"name": "ES_MM"}

    # when ops deploys it for a trader
    livesim.deploy_livesim([Config()], on_behalf_of_user_id="trader-1")

    # then the deploy names the trader
    assert calls[-1]["path"] == "/deployments"
    assert calls[-1]["json"]["on_behalf_of_user_id"] == "trader-1"
