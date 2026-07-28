"""Environment profiles, engine detection, and date/symbol fixtures.

Two orthogonal axes:

* **engine** — which package currently provides the ``hiveq.flow`` namespace.
  ``hiveq-flow`` (fat, in-process engine) and ``hiveq-sdk`` (thin, remote
  submit) both claim that namespace and MUST NOT be co-installed, so the runner
  dispatches each test into one of two venvs. A test never chooses; it asks.
* **profile** — which platform the run talks to (``local`` or ``staging``).
  Only the URL/credential surface differs, and it is expressed entirely through
  the environment variables the SDK already reads.
"""

from __future__ import annotations

import importlib.util
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

ENGINE_INPROC = "inproc"
ENGINE_REMOTE = "remote"

#: The default. Sets NO urls — whatever ``~/.hiveq/.env`` (or the shell) already
#: configures is authoritative, which is how every other SDK caller behaves.
#: Backtests auto-deploy to that host using the stored API key; the suite should
#: not second-guess it.
PROFILE_ENV = "env"

#: Explicit overrides, for pointing the suite somewhere other than your default
#: without editing ``~/.hiveq/.env``.
PROFILE_LOCAL = "local"
PROFILE_STAGING = "staging"
PROFILE_VM = "vm"

ENGINE_VAR = "AGENT_QA_ENGINE"
PROFILE_VAR = "AGENT_QA_PROFILE"
FIXTURES_VAR = "AGENT_QA_FIXTURES"

#: Only the fat engine package ships a callback dispatcher — the cheapest
#: unambiguous marker that distinguishes the two ``hiveq.flow`` providers
#: without importing (and therefore initialising) either.
_FAT_MARKER = "hiveq.flow.callback_dispatcher"

#: A profile is purely a URL/credential preset. ``local`` matches what the SDK
#: sign-in flow already writes to ``~/.hiveq/.env`` when pointed at the local
#: stack (nginx on :80 fronts every service), so the two agree by construction.
#: Note the API key is environment-specific: a local key 401s against staging
#: and vice versa. L0 preflight asserts key-vs-profile agreement rather than
#: letting every downstream test fail with an opaque auth error.
_PROFILE_URLS: Dict[str, Dict[str, str]] = {
    # Empty on purpose: the `env` profile defers entirely to ~/.hiveq/.env.
    PROFILE_ENV: {},
    # NOTE: plain http. vm.hiveq.ai has no TLS listener — https connects to
    # nothing (curl exit 000), which fails as an opaque timeout rather than a
    # clear error. Verified 2026-07-28: http health 200, authed 200.
    PROFILE_VM: {
        "HIVEQ_AUTH_URL": "http://vm.hiveq.ai",
        "HIVEQ_DATA_URL": "http://vm.hiveq.ai",
        "HIVEQ_BASE_URL": "http://vm.hiveq.ai/api/orchestrator",
    },
    PROFILE_LOCAL: {
        "HIVEQ_AUTH_URL": "http://localhost:80",
        "HIVEQ_DATA_URL": "http://localhost:80",
        "HIVEQ_BASE_URL": "http://localhost:80/api/orchestrator",
    },
    PROFILE_STAGING: {
        "HIVEQ_AUTH_URL": "https://staging.hiveq.ai",
        "HIVEQ_DATA_URL": "https://staging.hiveq.ai",
        "HIVEQ_BASE_URL": "https://staging.hiveq.ai/api/orchestrator",
    },
}


def detect_engine() -> str:
    """Which ``hiveq.flow`` is installed in *this* interpreter.

    The runner also exports ``AGENT_QA_ENGINE``; detection is the fallback so a
    test run by hand still knows what it is.
    """
    declared = os.environ.get(ENGINE_VAR)
    if declared in (ENGINE_INPROC, ENGINE_REMOTE):
        return declared
    try:
        found = importlib.util.find_spec(_FAT_MARKER) is not None
    except (ImportError, ValueError):
        found = False
    return ENGINE_INPROC if found else ENGINE_REMOTE


def is_inproc() -> bool:
    return detect_engine() == ENGINE_INPROC


def profile_name() -> str:
    return os.environ.get(PROFILE_VAR) or PROFILE_ENV


def apply_profile(name: Optional[str] = None) -> str:
    """Export the URL environment for ``name`` without clobbering the user's.

    Under the default ``env`` profile this is a no-op: ``~/.hiveq/.env`` and the
    shell decide where backtests deploy, exactly as they do for any other SDK
    caller. The named profiles fill in only what is *absent*, so an explicitly
    exported ``HIVEQ_BASE_URL`` still wins.
    """
    name = name or profile_name()
    # Named profiles must win over ~/.hiveq/.env, so they are set BEFORE the
    # file is loaded (the loader only fills absent variables). The `env` profile
    # sets nothing, leaving the file fully authoritative.
    for key, value in _PROFILE_URLS.get(name, {}).items():
        os.environ.setdefault(key, value)
    load_hiveq_env()
    os.environ[PROFILE_VAR] = name
    return name


def load_hiveq_env() -> None:
    """Load ``~/.hiveq/.env`` into the process, without overriding what is set.

    The SDK does this lazily — on the first request, not at import — so anything
    that reads ``HIVEQ_*`` from ``os.environ`` early sees nothing and silently
    falls back to the hosted default. Call this before resolving the host.
    Mirrors the SDK's own precedence: an already-exported variable wins.
    """
    path = os.path.expanduser("~/.hiveq/.env")
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if key.startswith("HIVEQ_") and key not in os.environ:
                    os.environ[key] = value.strip().strip('"').strip("'")
    except OSError:
        return


def resolved_host() -> str:
    """Where a backtest will actually deploy — asked, not guessed.

    Delegates to ``_Client``, which owns the precedence chain (explicit
    ``base_url`` > ``HIVEQ_BASE_URL`` > auth host + ``/api/orchestrator`` >
    hosted default). Reimplementing that chain here is how this function got it
    wrong once already: it read ``os.environ`` before the SDK had loaded
    ``~/.hiveq/.env`` and reported the hosted default while the client was
    correctly using the configured host.
    """
    load_hiveq_env()
    try:
        from hiveq.flow._client import _Client  # noqa: PLC0415 - SDK-only

        return str(_Client().base_url)
    except ImportError:
        # Fat engine package: no platform client, so fall back to the variables.
        base = os.environ.get("HIVEQ_BASE_URL")
        if base:
            return base.rstrip("/")
        auth = (os.environ.get("HIVEQ_AUTH_URL") or "").rstrip("/")
        return f"{auth}/api/orchestrator" if auth else ""


def api_key() -> Optional[str]:
    """The API key, from the environment or ``~/.hiveq/.env``.

    Never triggers the interactive browser sign-in — a QA process that blocks
    for five minutes waiting on a human is a hung CI job. L0 preflight asserts
    the key is present and tells the operator to run a normal SDK command once
    if it is not.
    """
    key = os.environ.get("HIVEQ_API_KEY")
    if key:
        return key.strip()
    path = os.path.expanduser("~/.hiveq/.env")
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line.startswith("HIVEQ_API_KEY=") and not line.startswith("#"):
                    return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return None


@dataclass
class Fixtures:
    """Known-good dates and symbols for the data the suite asserts against.

    These are deliberately *data* rather than code: the platform's coverage
    moves, and when a date goes stale the fix should be a one-line JSON edit
    (or ``AGENT_QA_FIXTURES=/path/to/overrides.json``), not a sweep through
    every test file.

    Dates are chosen to be ordinary, liquid sessions — not holidays, not
    half-days, and away from a futures roll unless the fixture says otherwise.
    """

    # --- equities
    equity_symbol: str = "AAPL"
    equity_symbols: List[str] = field(default_factory=lambda: ["AAPL", "MSFT"])
    equity_day: str = "2025-06-02"
    equity_week_start: str = "2025-06-02"
    equity_week_end: str = "2025-06-06"
    equity_month_start: str = "2025-05-28"
    equity_month_end: str = "2025-06-03"
    equity_session_start: str = "09:30"
    equity_session_end: str = "16:00"

    # --- futures
    futures_continuous: str = "ES.c.0"
    futures_volume_continuous: str = "ES.v.0"
    futures_raw_contract: str = "ESM5"
    futures_symbols: List[str] = field(default_factory=lambda: ["ES.c.0", "NQ.c.0"])
    futures_day: str = "2025-06-02"
    # ESM5 expired Fri 2025-06-20; ES.c.0 rolls on the first session AFTER
    # expiry (Mon 06-23), not on the earlier volume/rank handoff date.
    futures_roll_start: str = "2025-06-18"
    futures_roll_end: str = "2025-06-24"
    futures_session_start: str = "18:00"
    futures_session_end: str = "17:00"

    # --- index
    index_symbol: str = "VIX"
    index_symbols: List[str] = field(default_factory=lambda: ["VIX", "SPX"])

    # --- options
    option_underlying: str = "SPXW"

    # --- non-trading day, for the holiday/empty-range cases
    holiday_day: str = "2025-07-04"

    # --- datasets (verified live by core.catalog; these are the expected names)
    dataset_equity: str = "HIVEQ_US_EQ"
    dataset_futures: str = "HIVEQ_US_FUT"
    dataset_signals: str = "HIVEQ_QUANT_SIGNALS"

    initial_capital: float = 1_000_000.0

    def as_dict(self) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def load_fixtures() -> Fixtures:
    """Defaults, overlaid with ``fixtures.json`` and then ``AGENT_QA_FIXTURES``."""
    fx = Fixtures()
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for path in (os.path.join(here, "fixtures.json"), os.environ.get(FIXTURES_VAR)):
        if not path or not os.path.exists(path):
            continue
        try:
            with open(path) as fh:
                overrides = json.load(fh)
        except (OSError, ValueError):
            continue
        for key, value in (overrides or {}).items():
            if hasattr(fx, key):
                setattr(fx, key, value)
    return fx


#: Import-time convenience so tests read ``from ...profiles import FIXTURES``.
FIXTURES = load_fixtures()
