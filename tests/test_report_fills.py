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


def _platform_orders():
    # Mirrors the SigmaOrder REST shape (docs §7.3): a mix of executed and
    # non-executed orders, like a real /report orders payload.
    return [
        {"order_id": "1", "symbol": "AAPL", "side": "BUY", "status": "FILLED",
         "filled_qty": 100, "avg_px": 190.0, "commission": 0.11},
        {"order_id": "2", "symbol": "AAPL", "side": "SELL", "status": "PARTIALLY_FILLED",
         "filled_qty": 50, "avg_px": 191.0, "commission": 0.06},
        {"order_id": "3", "symbol": "AAPL", "side": "BUY", "status": "CANCELED",
         "filled_qty": 0, "avg_px": None, "commission": 0.0},
        {"order_id": "4", "symbol": "AAPL", "side": "BUY", "status": "REJECTED",
         "filled_qty": 0, "avg_px": None, "commission": 0.0},
    ]


def test_from_rest_derives_fills_from_orders_when_no_fills_key():
    # The real platform case: orders come through, no `fills` key -> derive.
    report = PerformanceReport.from_rest({"orders": _platform_orders()})
    assert report.orders is not None and len(report.orders) == 4
    assert report.fills is not None
    # Only FILLED + PARTIALLY_FILLED executed.
    assert len(report.fills) == 2
    assert set(report.fills["status"]) == {"FILLED", "PARTIALLY_FILLED"}


def test_explicit_fills_payload_takes_precedence_over_orders():
    payload = {
        "orders": _platform_orders(),
        "fills": [{"execution_id": "e1", "symbol": "AAPL", "last_qty": 100, "last_px": 190.0}],
    }
    report = PerformanceReport.from_rest(payload)
    assert "execution_id" in report.fills.columns
    assert len(report.fills) == 1


def test_run_fills_derives_when_report_fills_empty():
    # Local report with orders but no fills attr set -> run.fills() derives.
    orders = pd.DataFrame(_platform_orders())
    report = PerformanceReport(orders=orders)  # fills defaults to None
    run = Run(run_id="local-derive", report=report)
    out = run.fills()
    assert len(out) == 2
    assert set(out["status"]) == {"FILLED", "PARTIALLY_FILLED"}


def test_fills_from_orders_by_qty_when_no_status_column():
    # Fallback path: no status/is_filled column, filter on filled_qty > 0.
    orders = pd.DataFrame([
        {"order_id": "1", "filled_qty": 10},
        {"order_id": "2", "filled_qty": 0},
    ])
    report = PerformanceReport(orders=orders)
    run = Run(run_id="local-qty", report=report)
    out = run.fills()
    assert len(out) == 1
    assert out.iloc[0]["order_id"] == "1"


def _rest_orders_real_schema():
    # The actual GET /runs/{id}/orders payload schema (lowercase snake_case,
    # status == "FILLED"), captured from a live platform run.
    return [
        {"order_id": str(i), "symbol": "ES.n.0", "side": "BUY" if i % 2 else "SELL",
         "quantity": 1, "filled_qty": 1, "leaves_qty": 0, "avg_px": 5000.0 + i,
         "status": "FILLED", "commissions": 0.5}
        for i in range(284)
    ]


class _RemoteReaderStub:
    """Mirrors the platform split: /report has NO orders; /orders does."""
    def report(self, run_id, include=None):
        return {"summary": {"Sharpe": 1.1}}          # NO 'orders' key — the real bug
    def orders(self, run_id, **kw):
        return _rest_orders_real_schema()
    def status(self, run_id):
        return {"status": "completed", "is_final": True}


def test_remote_report_backfills_orders_and_fills():
    # Reproduces the .164 bug: /report omits orders, so report.orders/fills were
    # empty even though /orders had 284 rows. report() must backfill + derive.
    run = Run(run_id="7c45968c")
    run._reader = _RemoteReaderStub()
    report = run.report()
    assert len(run._as_df(report.orders)) == 284
    assert report.fills is not None and len(report.fills) == 284


def test_run_fills_and_report_fills_are_identical_remote():
    # The explicit requirement: run.fills() == run.report().fills.
    run = Run(run_id="7c45968c")
    run._reader = _RemoteReaderStub()
    a = run.fills()
    b = run._as_df(run.report().fills)
    assert len(a) == 284 and len(b) == 284
    pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))


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
