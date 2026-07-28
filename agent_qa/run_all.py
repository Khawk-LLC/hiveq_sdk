#!/usr/bin/env python3
"""Tiered test runner and scorecard.

Generalises ``hiveq-flow/qa_validation/run_all.py``: one subprocess per test,
the last ``RESULT:`` line wins, a timeout becomes ERROR, and the process exits
non-zero if anything FAILed or ERRORed. On top of that it adds engine dispatch,
profile selection, level/tier filtering, a JSON report, and ledger updates.

Engine dispatch
---------------
``hiveq-flow`` (fat, in-process) and ``hiveq-sdk`` (thin, remote) both provide
the ``hiveq.flow`` namespace and cannot be co-installed. Rather than demand two
virtualenvs, the runner puts ``hiveq-flow/src`` at the *front* of ``PYTHONPATH``
for ``--engine=inproc`` — the same shadowing trick the existing qa_validation
suite uses (``PYTHONPATH=src python qa_validation/...``). ``--engine=remote``
leaves the installed SDK in place. Pass ``--python`` to use a different
interpreter for either.

Examples
--------
    python agent_qa/run_all.py --tier 1                       # fast, both engines
    python agent_qa/run_all.py --tier 1 --engine remote
    python agent_qa/run_all.py --level l1,l4 --engine inproc
    python agent_qa/run_all.py --tier 2 --profile staging     # livesim
    python agent_qa/run_all.py --refresh-catalog
    python agent_qa/run_all.py --include-proposed --only 'proposed/*'
"""

from __future__ import annotations

import argparse
import concurrent.futures
import fnmatch
import glob
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
SDK_ROOT = os.path.dirname(HERE)
REPORTS_DIR = os.path.join(HERE, "reports")

sys.path.insert(0, SDK_ROOT)

from agent_qa.core import ledger  # noqa: E402
from agent_qa.core.profiles import (  # noqa: E402
    ENGINE_INPROC,
    ENGINE_REMOTE,
    PROFILE_ENV,
    PROFILE_LOCAL,
    PROFILE_STAGING,
    PROFILE_VM,
    _PROFILE_URLS,
)

#: Where the fat engine package lives, for PYTHONPATH shadowing.
DEFAULT_FLOW_SRC = os.path.join(os.path.dirname(SDK_ROOT), "hiveq-flow", "src")

#: Levels that only make sense against a live platform with a fleet.
TIER2_LEVELS = {"l9"}

#: Per-level subprocess budget in seconds. Livesim deploys are slow.
LEVEL_TIMEOUT = {"l0": 120, "l9": 1800}
DEFAULT_TIMEOUT = 900

STATUS_ORDER = ["PASS", "GAP", "FAIL", "ERROR", "SKIP"]


# ------------------------------------------------------------------- discovery


def level_of(path: str) -> str:
    """``suites/l4_bars/l4_02_foo.py`` -> ``l4``; '' when not level-scoped."""
    name = os.path.basename(path)
    head = name.split("_", 1)[0]
    if head.startswith("l") and head[1:].isdigit():
        return head
    parent = os.path.basename(os.path.dirname(path))
    head = parent.split("_", 1)[0]
    return head if head.startswith("l") and head[1:].isdigit() else ""


def tier_of(path: str) -> int:
    return 2 if level_of(path) in TIER2_LEVELS else 1


def discover(include_proposed: bool, include_quarantine: bool) -> List[str]:
    roots = [os.path.join(HERE, "suites")]
    if include_proposed:
        roots.append(os.path.join(HERE, "proposed"))
    if include_quarantine:
        roots.append(os.path.join(HERE, "quarantine"))

    found: List[str] = []
    for root in roots:
        found.extend(glob.glob(os.path.join(root, "**", "l[0-9]*_*.py"), recursive=True))
    return sorted(p for p in found if not os.path.basename(p).startswith("_"))


def select(paths: List[str], tier: str, levels: Optional[List[str]],
           only: Optional[str]) -> List[str]:
    out = []
    for path in paths:
        if tier != "all" and tier_of(path) != int(tier):
            continue
        if levels and level_of(path) not in levels:
            continue
        if only and not (fnmatch.fnmatch(os.path.relpath(path, HERE), only)
                         or fnmatch.fnmatch(os.path.basename(path), only)):
            continue
        out.append(path)
    return out


# --------------------------------------------------------------------- execution


def child_env(engine: str, profile: str, flow_src: str, json_out: str) -> Dict[str, str]:
    """Build the child environment.

    The profile URLs are exported here rather than left to ``apply_profile``:
    ``~/.hiveq/.env`` only fills variables that are absent, so whoever sets them
    first wins, and the runner must be that first setter to make ``--profile``
    authoritative.
    """
    env = dict(os.environ)
    env["AGENT_QA_ENGINE"] = engine
    env["AGENT_QA_PROFILE"] = profile
    env["AGENT_QA_JSON_OUT"] = json_out
    env.update(_PROFILE_URLS.get(profile, {}))

    # Test the SDK checkout this QA suite belongs to, not an older wheel from
    # site-packages. The package uses a src/ layout, so SDK_ROOT alone only
    # exposes agent_qa and does not make ``hiveq.flow`` importable.
    parts = [os.path.join(SDK_ROOT, "src"), SDK_ROOT]
    if engine == ENGINE_INPROC:
        # Front of the path so the fat engine shadows the installed thin SDK.
        parts.insert(0, flow_src)
    existing = env.get("PYTHONPATH")
    if existing:
        parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def run_one(path: str, engine: str, profile: str, flow_src: str,
            python: str, timeout: Optional[int]) -> Dict[str, Any]:
    name = os.path.splitext(os.path.basename(path))[0]
    level = level_of(path)
    budget = timeout or LEVEL_TIMEOUT.get(level, DEFAULT_TIMEOUT)
    json_out = os.path.join(REPORTS_DIR, ".json", f"{name}.{engine}.json")
    os.makedirs(os.path.dirname(json_out), exist_ok=True)
    if os.path.exists(json_out):
        os.unlink(json_out)

    started = time.time()
    try:
        proc = subprocess.run(
            [python, path],
            env=child_env(engine, profile, flow_src, json_out),
            cwd=SDK_ROOT,
            capture_output=True,
            text=True,
            timeout=budget,
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        lines = [ln for ln in output.splitlines() if ln.startswith("RESULT:")]
        if lines:
            line = lines[-1]
            status = line.split()[1]
        else:
            # No verdict at all. install_crash_handler() normally prevents this,
            # so include the last stderr line — otherwise the scorecard says
            # only "rc=1", which diagnoses nothing.
            status = "ERROR"
            hint = next((ln for ln in reversed(output.strip().splitlines())
                         if ln.strip() and not ln.startswith(" ")), "")
            line = (f"RESULT: ERROR {name} — no RESULT line (rc={proc.returncode})"
                    + (f": {hint[:220]}" if hint else ""))
        tail = "\n".join(output.strip().splitlines()[-25:])
    except subprocess.TimeoutExpired:
        status, line, tail = "ERROR", f"RESULT: ERROR {name} — timeout after {budget}s", ""

    record: Dict[str, Any] = {
        "test": name,
        "path": os.path.relpath(path, HERE),
        "surface": level,
        "engine": engine,
        "profile": profile,
        "status": status,
        "line": line,
        "duration_s": round(time.time() - started, 1),
    }
    # The sidecar carries structured checks; merge it over the scraped basics.
    try:
        with open(json_out) as fh:
            record.update({k: v for k, v in json.load(fh).items() if k != "test"})
    except (OSError, ValueError):
        pass
    if status in ("FAIL", "ERROR"):
        record["tail"] = tail
    return record


# ----------------------------------------------------------------------- report


def scorecard(results: List[Dict[str, Any]]) -> Tuple[str, Dict[str, int]]:
    counts: Dict[str, int] = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    width = 78
    out = ["", "=" * width, "SCORECARD", "=" * width]
    for r in sorted(results, key=lambda x: (STATUS_ORDER.index(x["status"])
                                            if x["status"] in STATUS_ORDER else 9, x["test"])):
        out.append(f"[{r['engine'][:6]:6s}] {r['line']}")
    out.append("-" * width)
    out.append(", ".join(f"{k}: {counts[k]}" for k in STATUS_ORDER if k in counts))
    return "\n".join(out), counts


def write_report(results: List[Dict[str, Any]], counts: Dict[str, int], args) -> str:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    path = os.path.join(REPORTS_DIR, f"run-{stamp}.json")
    with open(path, "w") as fh:
        json.dump(
            {
                "started_at": stamp,
                "tier": args.tier,
                "profile": args.profile,
                "engines": args.engine,
                "levels": args.level,
                "counts": counts,
                "results": results,
            },
            fh,
            indent=2,
            default=str,
        )
    return path


# ------------------------------------------------------------------------- main


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", default="1", choices=["1", "2", "all"],
                    help="1=historical/backtest (default), 2=livesim, all=both")
    ap.add_argument("--profile", default=PROFILE_ENV,
                    choices=[PROFILE_ENV, PROFILE_LOCAL, PROFILE_STAGING, PROFILE_VM],
                    help="'env' (default) defers to ~/.hiveq/.env; the others "
                         "override the host for this run only")
    ap.add_argument("--engine", default=ENGINE_REMOTE,
                    choices=[ENGINE_INPROC, ENGINE_REMOTE, "both"],
                    help="'remote' (default) deploys every backtest through the "
                         "hiveq-sdk to the configured host — the real user path. "
                         "'inproc' runs the hiveq-flow engine locally, for "
                         "engine-internal debugging only")
    ap.add_argument("--level", default="", help="comma-separated, e.g. l1,l4")
    ap.add_argument("--only", default="", help="glob against the test path or filename")
    ap.add_argument("--include-proposed", action="store_true",
                    help="also run agent-generated tests awaiting review")
    ap.add_argument("--include-quarantine", action="store_true",
                    help="also run quarantined tests (never affects exit code)")
    ap.add_argument("--jobs", type=int, default=1,
                    help="parallel tests; keep at 1 for tier 2 (shared fleet)")
    ap.add_argument("--timeout", type=int, default=0, help="override per-test seconds")
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument("--flow-src", default=os.environ.get("AGENT_QA_FLOW_SRC", DEFAULT_FLOW_SRC))
    ap.add_argument("--refresh-catalog", action="store_true",
                    help="re-fetch the dataset catalog and exit")
    ap.add_argument("--list", action="store_true", help="list selected tests and exit")
    ap.add_argument("--no-ledger", action="store_true", help="do not update coverage.json")
    args = ap.parse_args()

    if args.refresh_catalog:
        return refresh_catalog(args.profile)

    engines = [ENGINE_INPROC, ENGINE_REMOTE] if args.engine == "both" else [args.engine]
    if args.tier == "2" and ENGINE_INPROC in engines:
        # Livesim has no in-process form; it is a platform deployment. Only drop
        # the engine when tier 2 is ALL that was selected — under --tier all the
        # tier-1 tests still need it, and the per-job filter below excludes the
        # tier-2/inproc combination on its own.
        engines = [e for e in engines if e != ENGINE_INPROC] or [ENGINE_REMOTE]

    levels = [x.strip() for x in args.level.split(",") if x.strip()] or None
    paths = select(discover(args.include_proposed, args.include_quarantine),
                   args.tier, levels, args.only or None)

    if not paths:
        print("no tests matched the selection", file=sys.stderr)
        return 0

    if args.list:
        for p in paths:
            print(f"tier{tier_of(p)} {level_of(p):3s} {os.path.relpath(p, HERE)}")
        return 0

    if ENGINE_INPROC in engines and not os.path.isdir(args.flow_src):
        print(f"warning: --flow-src not found ({args.flow_src}); skipping inproc engine",
              file=sys.stderr)
        engines = [e for e in engines if e != ENGINE_INPROC]
        if not engines:
            return 1

    jobs: List[Tuple[str, str]] = [(p, e) for e in engines for p in paths
                                   if not (tier_of(p) == 2 and e == ENGINE_INPROC)]
    print(f"running {len(jobs)} test-runs "
          f"({len(paths)} tests × {len(engines)} engine(s)), profile={args.profile}")

    results: List[Dict[str, Any]] = []
    # Tier 2 shares one livesim fleet; parallel deploys would race for capacity.
    parallel = 1 if args.tier in ("2", "all") else max(1, args.jobs)

    def work(job):
        path, engine = job
        return run_one(path, engine, args.profile, args.flow_src,
                       args.python, args.timeout or None)

    if parallel == 1:
        for job in jobs:
            res = work(job)
            results.append(res)
            print(f"[{res['status']:5s}] {res['engine']:6s} {res['test']} ({res['duration_s']}s)")
            sys.stdout.flush()
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as pool:
            for res in pool.map(work, jobs):
                results.append(res)
                print(f"[{res['status']:5s}] {res['engine']:6s} {res['test']} "
                      f"({res['duration_s']}s)")
                sys.stdout.flush()

    text, counts = scorecard(results)
    print(text)
    report = write_report(results, counts, args)
    print(f"\nreport: {os.path.relpath(report, SDK_ROOT)}")

    if not args.no_ledger:
        ledger.record_run([r for r in results if "quarantine" not in r["path"]])

    # Quarantined tests are informational and never gate the run.
    blocking = [r for r in results
                if r["status"] in ("FAIL", "ERROR") and "quarantine" not in r["path"]]
    return 1 if blocking else 0


def refresh_catalog(profile: str) -> int:
    os.environ["AGENT_QA_PROFILE"] = profile
    os.environ.update(_PROFILE_URLS.get(profile, {}))
    from agent_qa.core import catalog

    try:
        entries = catalog.refresh(profile)
    except Exception as exc:  # noqa: BLE001
        print(f"catalog refresh failed: {exc}", file=sys.stderr)
        return 1
    print(f"cached {len(entries)} datasets -> {os.path.relpath(catalog.cache_path(profile), SDK_ROOT)}")
    for e in entries:
        print(f"  {e['dataset']:28s} {','.join(e.get('schemas') or [])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
