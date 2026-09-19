"""Profile-scoped data fixtures for the quant-data validations.

``vm`` and ``staging`` are backed by different quant databases — ``qa_quant_001``
and ``staging_quant_001`` — so a fixture that is populated on one profile can be
sparse or entirely absent on the other.  Measured 2026-09-18 through
``POST /api/read/v0/data`` on both profiles:

``HIVEQ_QUANT_SIGNALS`` / ``signals``
    vm       ``Prillach_MC_ES`` — 6 rows, all on 2024-08-27, the only symbol in
             the schema.  ``Spy-ClusterDecay-5m-v2`` returns nothing here.
    staging  ``Prillach_MC_ES`` — the same 6 rows on 2024-08-27, plus 127
             further days through 2025-12-16.  Also carries
             ``Spy-ClusterDecay-5m-v2`` (2018-01-02) and ``TEST_WS``.

``HIVEQ_QUANT_FEATURES_V3`` / ``quant_features_v3``
    vm       the schema does not exist — reads fail ``SCHEMA_NOT_FOUND``.
    staging  ``0800_Prillach_MC_ES_PF`` — 2000+ rows on 2026-01-02 with every
             projected column populated (``signal1..3``, ``weight1..3``,
             ``stop_px``, ``target_px``).  The older ``0930_NBV_Mom_Long_ES_V0``
             has 5 rows on 2026-06-30 and leaves ``signal2/3``, ``weight2/3``,
             ``stop_px`` and ``target_px`` null, so it cannot exercise the
             ``filters.fields`` projection.

A validation must fail on SDK behaviour, never on which platform it was pointed
at.  Each case resolves its signal name and date window here.  Where a profile
carries no data at all the fixture is ``None`` and the case reports ``GAP`` —
amber in the report, and ignored by ``run_all.baseline_passed`` — instead of a
red ``FAIL`` that says nothing about the SDK.

Both ``date`` values are inclusive backtest dates, which is what
``run_backtest(start_date=…, end_date=…)`` takes.  They are *not* the Data API's
``start``/``end`` filter, whose ``end`` is exclusive.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from typing import NoReturn
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Fixture:
    """One profile's coordinates for a quant dataset."""

    symbol: str
    start_date: str
    end_date: str
    note: str = ""


# Keyed by fixture kind, then profile. ``None`` means "this profile has no data
# for that dataset" and is a deliberate entry, not a missing one.
FIXTURES: dict[str, dict[str, Fixture | None]] = {
    "quant_signals": {
        "vm": Fixture(
            "Prillach_MC_ES", "2024-08-27", "2024-08-27",
            note="the only symbol in qa_quant_001.signals; 6 rows",
        ),
        "staging": Fixture(
            "Prillach_MC_ES", "2024-08-27", "2024-08-27",
            note="same 6 rows as vm, so both profiles assert identical delivery",
        ),
    },
    "quant_features_v3": {
        "vm": None,  # schema absent from qa_quant_001 entirely
        "staging": Fixture(
            "0800_Prillach_MC_ES_PF", "2026-01-02", "2026-01-02",
            note="2000+ rows with every signal/weight/stop/target column filled",
        ),
    },
}


def profile() -> str:
    """The environment profile this validation is pointed at.

    ``hiveq_env.py`` exports ``HIVEQ_ENV_PROFILE``, but a case run directly
    (``python release_validation/baseline_t45_signal_data.py``) has only the
    URLs, so fall back to the host in ``HIVEQ_BASE_URL``/``HIVEQ_AUTH_URL``.
    """
    named = os.environ.get("HIVEQ_ENV_PROFILE", "").strip()
    if named:
        return named
    for variable in ("HIVEQ_BASE_URL", "HIVEQ_AUTH_URL", "HIVEQ_DATA_URL"):
        host = urlsplit(os.environ.get(variable, "")).hostname or ""
        if host.startswith("vm."):
            return "vm"
        if host.startswith("staging."):
            return "staging"
        if host in {"localhost", "127.0.0.1"}:
            return "local"
    return "unknown"


def resolve(kind: str) -> Fixture | None:
    """The fixture for ``kind`` on the active profile, or ``None`` if there is none.

    An unregistered profile returns ``None`` rather than borrowing another
    profile's coordinates: a silently wrong symbol reads as an SDK failure.
    """
    return FIXTURES[kind].get(profile())


def gap(name: str, reason: str) -> NoReturn:
    """Report a validation as GAP and exit cleanly.

    ``run_all`` scores the last ``RESULT:`` line, and ``GAP`` is the suite's
    existing word for "this environment cannot exercise the contract" (see
    ``baseline_t32``).  It is amber in the report and does not close the
    long-running gate.
    """
    print(f"RESULT: GAP {name} — {reason}")
    raise SystemExit(0)


def require(kind: str, name: str) -> Fixture:
    """Resolve ``kind`` or report the GAP and exit."""
    fixture = resolve(kind)
    if fixture is None:
        gap(name, f"profile={profile()} carries no {kind} fixture; "
                  "nothing to exercise, so the case is not scored")
    return fixture
