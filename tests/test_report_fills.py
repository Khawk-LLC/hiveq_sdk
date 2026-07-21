"""Regression tests for the ``fills`` accessor surface.

`fills` is a documented ``PerformanceReport`` attribute (llms.txt §10.1) but was
never wired up by ``from_rest`` (remote runs always saw ``None``), and ``Run``
had no ``.fills()`` accessor at all while exposing positions/orders/trades. These
tests lock in both fixes.
"""
import pandas as pd

from hiveq.flow.metrics.report import PerformanceReport
from hiveq.flow.runs import Run


def test_from_rest_populates_fills():
    payload = {
        "summary": {"Sharpe": 1.2},
        "fills": [
            {"symbol": "AAPL", "qty": 100, "price": 190.1},
            {"symbol": "AAPL", "qty": -100, "price": 191.0},
        ],
    }
    report = PerformanceReport.from_rest(payload)
    assert report.fills is not None
    assert isinstance(report.fills, pd.DataFrame)
    assert len(report.fills) == 2
    assert set(report.fills.columns) == {"symbol", "qty", "price"}


def test_from_rest_fills_absent_is_none():
    # A report payload with no fills key must not crash and must stay None,
    # matching every other optional DataFrame attribute.
    report = PerformanceReport.from_rest({"summary": {"Sharpe": 1.0}})
    assert report.fills is None


def test_local_run_fills_returns_dataframe():
    fills_df = pd.DataFrame([{"symbol": "ES", "qty": 1, "price": 5000.0}])
    report = PerformanceReport(fills=fills_df)
    run = Run(run_id="local-1", report=report)
    out = run.fills()
    assert isinstance(out, pd.DataFrame)
    assert len(out) == 1
    assert out.iloc[0]["symbol"] == "ES"


def test_local_run_fills_none_yields_empty_frame():
    # No fills -> empty DataFrame, never None, so callers can .empty-check
    # uniformly with the other run.X() accessors.
    run = Run(run_id="local-2", report=PerformanceReport())
    out = run.fills()
    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test_remote_run_fills_derives_from_report(monkeypatch):
    # Remote runs have no /fills REST endpoint: run.fills() must read off
    # report().fills. Stub report() so the test stays offline.
    fills_df = pd.DataFrame([{"symbol": "NQ", "qty": 2, "price": 18000.0}])
    run = Run(run_id="remote-1")
    monkeypatch.setattr(run, "report", lambda: PerformanceReport(fills=fills_df))
    out = run.fills()
    assert isinstance(out, pd.DataFrame)
    assert len(out) == 1
    assert out.iloc[0]["symbol"] == "NQ"
