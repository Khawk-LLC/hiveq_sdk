"""containers(): the fleet read, including the route fallback.

There is no v1 route for containers, so this talks to the v0 fleet routes: the
org-readable one first, the admin-only one when that is refused. Both carry the
same rows under a different key, which is the part worth pinning.
"""

from typing import Any, Dict, List

import pytest

from hiveq.flow import livesim

FLEET = [
    {"id": "livesim-futures-1", "type": "livesim", "asset": "FUTURES",
     "runtime": "hiveq_flow", "state": {"status": "stopped",
                                        "reason": "day_of_week:SAT_not_in_CUSTOM"}},
    {"id": "livesim-options-1", "type": "livesim", "asset": "OPTIONS",
     "runtime": "hiveq_flow", "state": {"status": "running"}},
    {"id": "native-futures-1", "type": "livesim", "asset": "FUTURES",
     "runtime": "sigma_cpp_plugin", "state": {"status": "missing"}},
    {"id": "prod-futures-1", "type": "prod", "asset": "FUTURES",
     "runtime": "sigma_python_prod", "state": {"status": "running"}},
]


class FakeResponse:
    def __init__(self, status: int, payload: Dict[str, Any]):
        self.status_code = status
        self.ok = status < 400
        self._payload = payload
        self.text = str(payload)

    def json(self) -> Dict[str, Any]:
        return self._payload


@pytest.fixture
def routes(monkeypatch) -> List[str]:
    """Record which fleet routes were called; answer both shapes."""
    called: List[str] = []

    def fake_get(url: str, **kwargs):
        called.append(url)
        if url.endswith("/fleet/containers"):
            return FakeResponse(200, {"containers": FLEET})
        if url.endswith("/livesim/containers"):
            return FakeResponse(200, {"data": FLEET})
        return FakeResponse(404, {"message": "no such route"})

    monkeypatch.setenv("HIVEQ_API_KEY", "test-key")
    monkeypatch.setattr(livesim.requests, "get", fake_get)
    return called


def test_it_reads_the_org_readable_route_and_drops_production(routes):
    rows = livesim.containers()

    assert [c["id"] for c in rows] == [
        "livesim-futures-1", "livesim-options-1", "native-futures-1",
    ]  # sorted by id, prod-futures-1 is not a livesim target
    assert routes == ["https://staging.hiveq.ai/api/strategy/v0/run/fleet/containers"]


def test_asset_and_runtime_narrow_it_to_deployable_targets(routes):
    rows = livesim.containers(asset="futures", runtime="hiveq_flow")

    assert [c["id"] for c in rows] == ["livesim-futures-1"]
    # The reason a container is down travels with it -- that is the point of
    # reading the fleet before deploying.
    assert rows[0]["state"]["reason"] == "day_of_week:SAT_not_in_CUSTOM"


def test_livesim_only_false_keeps_production_containers(routes):
    rows = livesim.containers(livesim_only=False)

    assert "prod-futures-1" in [c["id"] for c in rows]


def test_a_refused_fleet_route_falls_back_to_the_admin_one(monkeypatch):
    called: List[str] = []

    def fake_get(url: str, **kwargs):
        called.append(url)
        if url.endswith("/fleet/containers"):
            return FakeResponse(403, {"error": {"message": "forbidden"}})
        return FakeResponse(200, {"data": FLEET})

    monkeypatch.setenv("HIVEQ_API_KEY", "test-key")
    monkeypatch.setattr(livesim.requests, "get", fake_get)

    rows = livesim.containers(asset="OPTIONS")

    assert [c["id"] for c in rows] == ["livesim-options-1"]
    assert [u.rsplit("/run", 1)[1] for u in called] == [
        "/fleet/containers", "/livesim/containers",
    ]


def test_an_error_that_is_not_a_permission_problem_is_raised(monkeypatch):
    monkeypatch.setenv("HIVEQ_API_KEY", "test-key")
    monkeypatch.setattr(
        livesim.requests, "get",
        lambda url, **kw: FakeResponse(500, {"message": "boom"}),
    )

    with pytest.raises(livesim.LivesimError) as excinfo:
        livesim.containers()

    assert excinfo.value.status == 500


def test_no_api_key_is_a_401_before_any_request(monkeypatch):
    monkeypatch.delenv("HIVEQ_API_KEY", raising=False)

    with pytest.raises(livesim.LivesimError) as excinfo:
        livesim.containers()

    assert excinfo.value.status == 401
