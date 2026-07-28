"""Preconditions a test declares up front, before doing any work.

Each guard either returns (precondition met) or ends the process with a SKIP or
GAP verdict. Putting these at the top of a test keeps the body free of
``if engine == ...`` branching and makes the reason for a non-result explicit in
the scorecard rather than hidden in a passing-but-vacuous assertion.

The distinction the guards enforce:

* **SKIP** — wrong place to ask. A livesim REST test under ``--engine=inproc``
  is not a failure and not a gap; the fat engine package simply has no platform
  client.
* **GAP** — right place, but the product or the data cannot answer yet. Market
  closed, dataset unentitled, table empty on every probed date.
"""

from __future__ import annotations

from typing import Optional, Sequence

from agent_qa.core import observe
from agent_qa.core.profiles import (
    ENGINE_INPROC,
    ENGINE_REMOTE,
    PROFILE_STAGING,
    detect_engine,
    profile_name,
)
from agent_qa.core.result import gap, not_applicable


def require_engine(name: str, engine: str) -> None:
    """Only run under ``engine`` (``inproc`` or ``remote``)."""
    current = detect_engine()
    if current != engine:
        not_applicable(name, f"requires --engine={engine}, running {current}")


def require_remote(name: str) -> None:
    """Platform-facing test: needs the thin SDK's REST client.

    The fat ``hiveq-flow`` package provides only the ``hiveq.flow`` namespace —
    no ``_client``, no ``hiveq.datasets`` — so anything talking to the
    orchestrator or metadata API can only run here.
    """
    require_engine(name, ENGINE_REMOTE)


def require_inproc(name: str) -> None:
    """Engine-internal test: needs the real in-process engine objects."""
    require_engine(name, ENGINE_INPROC)


def require_profile(name: str, profile: str) -> None:
    current = profile_name()
    if current != profile:
        not_applicable(name, f"requires --profile={profile}, running {current}")


def require_staging(name: str) -> None:
    """Livesim fidelity guard.

    Local Kafka is PLAINTEXT while the deployed stack is SASL_SSL, so livesim
    transport behaviour does not reproduce locally. A livesim test that passes
    against the local stack would be actively misleading.
    """
    require_profile(name, PROFILE_STAGING)


def require_market_open(name: str, futures: bool = False) -> None:
    """Live-data guard — report GAP out of hours rather than failing."""
    is_open = observe.futures_session_is_open() if futures else observe.market_is_open()
    if not is_open:
        session = "futures session" if futures else "equity RTH"
        gap(name, f"{session} is closed; no live data to assert on")


def require_dataset(name: str, dataset: str, schema: Optional[str] = None) -> None:
    """Entitlement guard — an unavailable dataset is a GAP, never a FAIL."""
    from agent_qa.core import catalog

    try:
        available = catalog.has(dataset, schema)
    except catalog.CatalogUnavailable as exc:
        gap(name, f"dataset catalog unavailable ({exc})")
        return
    if not available:
        target = f"{dataset}/{schema}" if schema else dataset
        gap(name, f"{target} is not in this account's catalog")


def require_rows(name: str, count: int, what: str, minimum: int = 1) -> None:
    """Post-run guard: the data source produced nothing on the probed window.

    Distinguishes "the plumbing is broken" from "this table is empty on this
    date" only when the caller already knows the plumbing works — so use it
    after a clean run, not instead of asserting delivery.
    """
    if count < minimum:
        gap(name, f"no {what} on the probed window (got {count}, need {minimum})")


def any_of(engines: Sequence[str]) -> bool:
    """Non-terminating check, for tests that adapt rather than skip."""
    return detect_engine() in engines


__all__ = [
    "ENGINE_INPROC",
    "ENGINE_REMOTE",
    "any_of",
    "require_dataset",
    "require_engine",
    "require_inproc",
    "require_market_open",
    "require_profile",
    "require_remote",
    "require_rows",
    "require_staging",
]
