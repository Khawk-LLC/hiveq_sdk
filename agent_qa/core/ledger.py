"""The ledgers — the QA agent's memory between runs.

Three JSON files under ``agent_qa/ledger/``:

``coverage.json``
    ``{surface_id: {case_id: {...}}}`` — what the permanent suite already
    asserts. The agent diffs touched surfaces against this to decide what is
    genuinely uncovered, so a busy week of commits in an already-covered area
    produces no redundant tests.

``commits.json``
    ``{repo: last_seen_sha}`` — the watermark per watched repo. Advanced only
    after a clean agent run, so a crashed run re-processes the same commits
    instead of silently skipping them.

``gaps.json``
    ``{gap_id: {reason, first_seen, commit, test}}`` — documented product/data
    limitations. This is the part that makes the agent *stop* re-proposing the
    same impossible test every night; a gap is knowledge, not a backlog item.

All writes are atomic (temp file + rename) because a cron'd agent and a manual
run can overlap, and a half-written ledger would be worse than a stale one.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from typing import Any, Dict, Iterable, List, Optional

LEDGER_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ledger")

COVERAGE = "coverage.json"
COMMITS = "commits.json"
GAPS = "gaps.json"


def _path(name: str) -> str:
    return os.path.join(LEDGER_DIR, name)


def read(name: str, default: Optional[Any] = None) -> Any:
    try:
        with open(_path(name)) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {} if default is None else default


def write(name: str, payload: Any) -> None:
    os.makedirs(LEDGER_DIR, exist_ok=True)
    target = _path(name)
    fd, tmp = tempfile.mkstemp(dir=LEDGER_DIR, prefix=f".{name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True, default=str)
        os.replace(tmp, target)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------- coverage


def coverage() -> Dict[str, Dict[str, Any]]:
    return read(COVERAGE, {})


def record_coverage(surface: str, case: str, **meta) -> None:
    data = coverage()
    data.setdefault(surface, {})[case] = {"updated_at": _now(), **meta}
    write(COVERAGE, data)


def record_run(results: Iterable[Dict[str, Any]]) -> None:
    """Fold one suite run's results into the coverage ledger.

    Called by ``run_all.py``. Coverage is recorded from what actually executed,
    not from what exists on disk — a test that never runs covers nothing.
    """
    data = coverage()
    for res in results:
        # A SKIPped test asserted nothing, so it covers nothing. Recording it
        # would let a livesim test "cover" l9 from an inproc run where it never
        # executed, and the agent would then stop proposing work for l9.
        if res.get("status") == "SKIP":
            continue
        surface = res.get("surface") or _surface_of(res.get("test", ""))
        case = res.get("test")
        if not surface or not case:
            continue
        entry = data.setdefault(surface, {}).setdefault(case, {})
        entry.update(
            {
                "updated_at": _now(),
                "status": res.get("status"),
                "engine": res.get("engine"),
                "profile": res.get("profile"),
                "duration_s": res.get("duration_s"),
            }
        )
    write(COVERAGE, data)


def covered_surfaces() -> List[str]:
    return sorted(coverage().keys())


def cases_for(surface: str) -> List[str]:
    """Tests covering ``surface``, counting level-granularity records too.

    A test that declares ``surface='l2.callbacks'`` is recorded under that exact
    id. A test that declares nothing is recorded under its level (``l2``), which
    is coarser but still real coverage. Asking for ``l2.callbacks`` must see
    both, or the agent re-proposes tests for surfaces the suite already
    exercises.
    """
    data = coverage()
    cases = set(data.get(surface, {}).keys())
    level = surface.split(".")[0]
    if level != surface:
        cases |= set(data.get(level, {}).keys())
    return sorted(cases)


def is_covered(surface: str) -> bool:
    return bool(cases_for(surface))


# ----------------------------------------------------------------------- commits


def commits() -> Dict[str, str]:
    return read(COMMITS, {})


def last_seen(repo: str) -> Optional[str]:
    return commits().get(repo)


def set_last_seen(repo: str, sha: str) -> None:
    data = commits()
    data[repo] = sha
    write(COMMITS, data)


def set_last_seen_many(mapping: Dict[str, str]) -> None:
    """Advance several watermarks at once — the end of a clean agent run."""
    data = commits()
    data.update(mapping)
    write(COMMITS, data)


# -------------------------------------------------------------------------- gaps


def gaps() -> Dict[str, Dict[str, Any]]:
    return read(GAPS, {})


def record_gap(gap_id: str, reason: str, test: str = "", commit: str = "") -> None:
    data = gaps()
    existing = data.get(gap_id, {})
    data[gap_id] = {
        "reason": reason,
        "test": test or existing.get("test", ""),
        "commit": commit or existing.get("commit", ""),
        "first_seen": existing.get("first_seen") or _now(),
        "last_seen": _now(),
    }
    write(GAPS, data)


def is_known_gap(gap_id: str) -> bool:
    return gap_id in gaps()


def clear_gap(gap_id: str) -> bool:
    """Drop a gap — call when the product fills it, so coverage can resume."""
    data = gaps()
    if gap_id not in data:
        return False
    del data[gap_id]
    write(GAPS, data)
    return True


# ------------------------------------------------------------------------ helpers


def _surface_of(test_name: str) -> str:
    """``l4_02_bars_multi_symbol`` -> ``l4`` — the default surface grouping."""
    head = test_name.split("_", 1)[0]
    return head if head.startswith("l") and head[1:].isdigit() else ""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")
