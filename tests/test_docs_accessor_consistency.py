"""Guard against code<->docs drift for the Run / PerformanceReport surface.

The canonical AI-facing doc (docs/llms.txt) advertises `run.X()` accessors and
`report.X` attributes. If the doc names an accessor that does not exist, an agent
will emit code that AttributeErrors. This test fails when the doc references a
`run.X()` / `report.X` that the code does not provide.

It intentionally does NOT require the reverse (every public member documented):
some members (`is_local`, `check_credentials`, `save_tearsheet_*`) are internal
plumbing the doc deliberately hides behind higher-level entry points.
"""
import dataclasses
import inspect
import re
from pathlib import Path

from hiveq.flow.metrics.report import PerformanceReport
from hiveq.flow.runs import Run

_DOC = Path(__file__).resolve().parents[1] / "docs" / "llms.txt"

# `report.` matches that are really filenames (my_report.html) or quantstats
# calls, not PerformanceReport attributes.
_REPORT_FALSE_POSITIVES = {"html"}


def _run_public_surface():
    methods = {
        n for n, _ in inspect.getmembers(Run, predicate=inspect.isfunction)
        if not n.startswith("_")
    }
    props = {n for n, v in vars(Run).items() if isinstance(v, property)}
    return methods | props


def _report_public_surface():
    fields = {f.name for f in dataclasses.fields(PerformanceReport)}
    methods = {
        n for n, _ in inspect.getmembers(PerformanceReport, predicate=inspect.isfunction)
        if not n.startswith("_")
    }
    # Properties too (e.g. `stats`) — mirrors _run_public_surface. Without this
    # the doc may legitimately reference a property and still fail the guard.
    props = {n for n, v in vars(PerformanceReport).items() if isinstance(v, property)}
    return fields | methods | props


def test_documented_run_accessors_exist():
    doc = _DOC.read_text()
    referenced = set(re.findall(r"\brun\.([a-z_]+)\s*\(", doc))
    surface = _run_public_surface()
    missing = sorted(referenced - surface)
    assert not missing, f"docs/llms.txt references run.X() not on Run: {missing}"


def test_documented_report_attrs_exist():
    doc = _DOC.read_text()
    referenced = set(re.findall(r"\breport\.([a-z_]+)", doc)) - _REPORT_FALSE_POSITIVES
    surface = _report_public_surface()
    missing = sorted(referenced - surface)
    assert not missing, f"docs/llms.txt references report.X not on PerformanceReport: {missing}"


def test_fills_is_symmetric_with_other_tabular_accessors():
    # The whole point of the fix: fills sits alongside positions/orders/trades
    # on BOTH surfaces.
    for name in ("positions", "orders", "trades", "fills"):
        assert hasattr(Run, name), f"Run.{name}() missing"
    report_fields = {f.name for f in dataclasses.fields(PerformanceReport)}
    for name in ("positions", "orders", "trades", "fills"):
        assert name in report_fields, f"PerformanceReport.{name} missing"
