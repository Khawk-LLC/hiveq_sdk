"""The RESULT protocol — one machine-readable verdict line per test process.

Ported from ``hiveq-flow/qa_validation/qa_common.py`` and extended with a JSON
sidecar so ``run_all.py`` can build a structured report without re-parsing prose.

Every test process prints exactly one line as its last act::

    RESULT: PASS|FAIL|GAP <test-name> — <check>=ok; <check>=FAIL; <extra>

``GAP`` marks a product/data limitation that is documented rather than asserted.
The run itself must still complete cleanly for a GAP to be reported — a crash is
an ``ERROR``, which only ``run_all.py`` can emit (the test process is gone).
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, Mapping, Optional

PASS = "PASS"
FAIL = "FAIL"
#: A product or data limitation, documented rather than asserted. The run
#: completed cleanly; the thing being tested does not exist yet.
GAP = "GAP"
#: The test does not apply to this engine/profile combination at all. Distinct
#: from GAP: nothing is missing, this run simply is not the right place to ask.
SKIP = "SKIP"

#: run_all.py sets this to a path; a test writes its structured verdict there.
JSON_OUT_ENV = "AGENT_QA_JSON_OUT"

_STARTED = time.time()


class Checks:
    """Accumulator for named boolean checks, with lazy failure detail.

    Preferred over building a raw dict when a test wants to attach a reason to
    each failure::

        c = Checks()
        c.add("bars_delivered", n_bars > 0, f"n_bars={n_bars}")
        c.add("ohlc_sane", not bad, f"bad={bad[:3]}")
        c.finish("l4_01_bars_1m")
    """

    def __init__(self) -> None:
        self._checks: Dict[str, bool] = {}
        self._details: Dict[str, str] = {}
        self._inconclusive: Dict[str, str] = {}
        self._notes: list[str] = []

    def add(self, name: str, ok: Any, detail: str = "",
            requires: Optional[Any] = None, requires_detail: str = "") -> "Checks":
        """Record a check.

        ``requires`` is the precondition that makes the check *meaningful*. Pass
        it for every assertion that is satisfied by absence — "nothing crashed",
        "no bad symbol arrived", "the far limit did not fill", "no event arrived
        after cancel". Those all pass trivially on a run where nothing happened,
        and a green check that proves nothing is worse than no check: it hides
        the very failure it was written to catch.

        When ``requires`` is falsey the check is recorded as **INCONCLUSIVE** —
        it neither passed nor failed, and it blocks a PASS verdict, because this
        run did not establish the property.
        """
        if requires is not None and not requires:
            self._inconclusive[name] = requires_detail or "precondition absent"
            return self
        self._checks[name] = bool(ok)
        if detail:
            self._details[name] = detail
        return self

    def note(self, text: str) -> "Checks":
        """Attach free-form context that is reported but never fails the test."""
        self._notes.append(text)
        return self

    @property
    def ok(self) -> bool:
        return all(self._checks.values()) and not self._inconclusive

    def finish(self, name: str, gap: bool = False, extra: str = "",
               surface: Optional[str] = None) -> None:
        parts = list(self._notes)
        if extra:
            parts.append(extra)
        for k, v in self._checks.items():
            if not v and k in self._details:
                parts.append(f"{k}: {self._details[k]}")
        for k, why in self._inconclusive.items():
            parts.append(f"{k} INCONCLUSIVE: {why}")
        finish(name, self._checks, extra="; ".join(parts), gap=gap, surface=surface,
               inconclusive=self._inconclusive)


def finish(
    name: str,
    checks: Mapping[str, Any],
    extra: str = "",
    gap: bool = False,
    meta: Optional[Dict[str, Any]] = None,
    surface: Optional[str] = None,
    inconclusive: Optional[Dict[str, str]] = None,
) -> None:
    """Print the RESULT line, write the JSON sidecar, and exit the process.

    ``checks``: {check_name: truthy}. All true -> PASS (or GAP when ``gap``).
    Any false -> FAIL, regardless of ``gap`` — a gap is a documented absence,
    not an excuse for a broken assertion.

    ``surface`` is the ``agent/surface_map.yaml`` id this test covers (e.g.
    ``l2.callbacks``). Declaring it is what lets the coverage ledger and the
    agent's work-list speak the same language — without it, coverage is only
    known at level granularity and every touched surface reads as uncovered.
    """
    normalized = {k: bool(v) for k, v in checks.items()}
    failed = [k for k, ok in normalized.items() if not ok]
    vacuous = sorted(inconclusive or {})

    # An inconclusive check blocks PASS. The run completed but did not establish
    # the property, and reporting that as success is how a vacuously-green suite
    # comes to certify a broken product.
    if failed:
        status = FAIL
    elif vacuous:
        status = GAP if gap else FAIL
    else:
        status = GAP if gap else PASS

    detail = "; ".join(f"{k}={'ok' if ok else 'FAIL'}" for k, ok in normalized.items())
    for k in vacuous:
        detail = f"{detail}; {k}=n/a" if detail else f"{k}=n/a"
    if extra:
        detail = f"{detail}; {extra}" if detail else extra

    payload = {
        "test": name,
        "status": status,
        "checks": normalized,
        "failed": failed,
        "inconclusive": dict(inconclusive or {}),
        "extra": extra,
        "duration_s": round(time.time() - _STARTED, 2),
        "engine": os.environ.get("AGENT_QA_ENGINE", "unknown"),
        "profile": os.environ.get("AGENT_QA_PROFILE", "unknown"),
        "meta": meta or {},
    }
    if surface:
        payload["surface"] = surface
    _write_json(payload)

    print(f"RESULT: {status} {name} — {detail}", flush=True)
    sys.exit(0 if status in (PASS, GAP) else 1)


def install_crash_handler(name: str, surface: Optional[str] = None) -> None:
    """Make an uncaught exception produce a verdict instead of a bare traceback.

    Call once at the top of ``main()``. Without it, anything that raises before
    ``finish()`` — a platform 401 at submit, a renamed attribute, a network drop
    — exits with no ``RESULT:`` line, and the runner can only report a
    contentless ``ERROR (rc=1)``. With it, the scorecard carries the exception
    type and message, which is usually the whole diagnosis.

    The traceback still goes to stderr and is captured in the run report, so
    nothing is hidden; this only guarantees a machine-readable verdict.
    """
    import traceback

    def _hook(exc_type, exc, tb):
        traceback.print_exception(exc_type, exc, tb)
        detail = f"{exc_type.__name__}: {exc}".replace("\n", " ")[:400]
        _write_json(
            {
                "test": name,
                "status": "ERROR",
                "checks": {},
                "failed": [],
                "extra": detail,
                "duration_s": round(time.time() - _STARTED, 2),
                "engine": os.environ.get("AGENT_QA_ENGINE", "unknown"),
                "profile": os.environ.get("AGENT_QA_PROFILE", "unknown"),
                "surface": surface or "",
                "meta": {"uncaught": True},
            }
        )
        print(f"RESULT: ERROR {name} — uncaught {detail}", flush=True)
        sys.exit(1)

    sys.excepthook = _hook


def gap(name: str, reason: str) -> None:
    """Report a documented absence: the product/data cannot satisfy this yet.

    Use for "this dataset has no rows on any probed date", "on_imbalance is
    wired but nothing publishes to it", "the market is closed so there is no
    live tick to assert on" — conditions that make the assertion meaningless
    rather than false. A GAP belongs in ``ledger/gaps.json`` with a reason.
    """
    finish(name, {}, extra=f"gap: {reason}", gap=True)


def not_applicable(name: str, reason: str) -> None:
    """Report SKIP: this engine/profile is not where the question can be asked.

    E.g. a livesim REST test under ``--engine=inproc`` (the fat engine package
    has no platform client at all), or a remote-only observability check. Never
    gates the run.
    """
    _write_json(
        {
            "test": name,
            "status": SKIP,
            "checks": {},
            "failed": [],
            "extra": reason,
            "duration_s": round(time.time() - _STARTED, 2),
            "engine": os.environ.get("AGENT_QA_ENGINE", "unknown"),
            "profile": os.environ.get("AGENT_QA_PROFILE", "unknown"),
            "meta": {},
        }
    )
    print(f"RESULT: {SKIP} {name} — not applicable: {reason}", flush=True)
    sys.exit(0)


#: Backwards-compatible alias; prefer :func:`gap` or :func:`not_applicable`.
skip = gap


def _write_json(payload: Dict[str, Any]) -> None:
    path = os.environ.get(JSON_OUT_ENV)
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
    except OSError:
        # The sidecar is a convenience; never let it mask the real verdict.
        pass
