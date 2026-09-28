"""hf.livesim_history(): a day's LiveSim data without a deployment_id.

A deployment handle reaches only its current container session and is gone once
terminated, so prior days are addressed by date, strategy and container. These
tests pin the request each call sends; the server-side scoping is tested in the
strategy-service.
"""

import datetime
from typing import Any, Dict, List

import pytest

import hiveq.flow as hf
from hiveq.flow import livesim


class _Response:
    ok = True
    status_code = 200
    url = "http://platform.test"
    text = "run_id,strategy_id\nrun-1,ESMaker\n"

    def json(self) -> Dict[str, Any]:
        return {"data": [{"run_id": "run-1", "strategy_id": "ESMaker"}]}


@pytest.fixture
def gets(monkeypatch) -> List[Dict[str, Any]]:
    """Capture every GET the SDK makes."""
    recorded: List[Dict[str, Any]] = []

    def fake_get(url: str, **kwargs) -> _Response:
        recorded.append({"url": url, **kwargs})
        return _Response()

    monkeypatch.setattr(livesim.requests, "get", fake_get)
    monkeypatch.setattr(livesim, "_base_url", lambda: "http://platform.test/v1")
    monkeypatch.setenv("HIVEQ_API_KEY", "hiveq-test")
    return recorded


def test_date_only_reads_every_container_that_day(gets):
    rows = hf.livesim_history("2026-09-25").orders()

    assert rows == [{"run_id": "run-1", "strategy_id": "ESMaker"}]
    [call] = gets
    assert call["url"] == "http://platform.test/v1/history/orders"
    assert call["params"] == {
        "date": "2026-09-25",
        "format": "json",
        "limit": 10_000,
        "offset": 0,
    }
    assert call["headers"] == {"X-API-Key": "hiveq-test"}


def test_strategy_and_container_narrow_the_day(gets):
    day = hf.livesim_history(
        datetime.date(2026, 9, 25),
        strategy="ESMaker",
        container="livesim-futures-1",
    )
    day.positions(limit=50, offset=100)

    [call] = gets
    assert call["url"].endswith("/history/positions")
    assert call["params"] == {
        "container": "livesim-futures-1",
        "date": "2026-09-25",
        "format": "json",
        "limit": 50,
        "offset": 100,
        "strategy_id": "ESMaker",
    }


@pytest.mark.parametrize(
    "method, resource",
    [
        ("runs", "runs"),
        ("orders", "orders"),
        ("trades", "trades"),
        ("positions", "positions"),
        ("metrics", "metrics"),
        ("events", "event-logs"),
    ],
)
def test_each_accessor_reads_its_resource(gets, method, resource):
    getattr(hf.livesim_history("2026-09-25"), method)()
    assert gets[0]["url"].endswith(f"/history/{resource}")


def test_csv_comes_back_as_text(gets):
    text = hf.livesim_history("2026-09-25").trades(format="csv")
    assert text.startswith("run_id,strategy_id")


@pytest.mark.parametrize(
    "date",
    ["2026-02-30", "09/25/2026", datetime.datetime(2026, 9, 25, 10, 0), 20260925],
)
def test_rejects_anything_but_a_calendar_day(date):
    with pytest.raises((TypeError, ValueError)):
        hf.livesim_history(date)


def test_rejects_a_full_container_identity():
    with pytest.raises(ValueError, match="bare container name"):
        hf.livesim_history(
            "2026-09-25", container="livesim-futures-1:liveSim:10.0.0.1:50051"
        )


def test_rejected_requests_raise_livesim_error(monkeypatch):
    class _Rejected(_Response):
        ok = False
        status_code = 400

        def json(self) -> Dict[str, Any]:
            return {
                "error": {
                    "code": "LIVESIM_INVALID_CONFIGURATION",
                    "message": "Request validation failed",
                }
            }

    monkeypatch.setattr(livesim.requests, "get", lambda url, **kw: _Rejected())
    with pytest.raises(livesim.LivesimError) as excinfo:
        hf.livesim_history("2026-09-25").orders()
    assert excinfo.value.code == "LIVESIM_INVALID_CONFIGURATION"


def test_deployment_reads_still_go_through_the_deployment_route(gets):
    # The shared pull helper must not change the per-deployment path.
    livesim.Deployment(deployment_id="dep-1").orders(strategy="ESMaker")

    [call] = gets
    assert call["url"] == "http://platform.test/v1/deployments/dep-1/orders"
    assert call["params"]["strategy_id"] == "ESMaker"
