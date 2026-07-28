#!/usr/bin/env python3
"""What changed since last time, and which QA surfaces does it touch?

This is the deterministic half of the QA agent. It does no reasoning: it reads
the commit watermark, diffs each watched repo, classifies changed paths through
``surface_map.yaml``, and emits a JSON work-list. The model-driven half (the
``qa-agent`` skill) consumes that list and writes tests.

Splitting it this way is deliberate. Path classification and watermark
management are exactly the kind of thing that must be reproducible and cheap —
if the agent re-derived them from prose every night it would drift, double-
propose, and occasionally skip a commit. Here the same commits always produce
the same work-list, and the skill's judgement is spent only on the part that
needs judgement: what the test should actually assert.

Usage::

    python agent_qa/agent/watch.py                  # work-list as JSON
    python agent_qa/agent/watch.py --summary        # human-readable
    python agent_qa/agent/watch.py --since 20       # ignore the watermark
    python agent_qa/agent/watch.py --advance        # commit the new watermark

``--advance`` is intentionally separate from reading: the watermark must only
move after a *successful* agent run, otherwise a crash silently skips commits.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_QA = os.path.dirname(HERE)
SDK_ROOT = os.path.dirname(AGENT_QA)
sys.path.insert(0, SDK_ROOT)

from agent_qa.core import ledger  # noqa: E402

SURFACE_MAP = os.path.join(HERE, "surface_map.yaml")

#: Commits touching more files than this are almost always merges, vendored
#: dependency bumps, or bulk reformats. They are recorded but not mined for
#: surfaces, because their path lists are noise.
BULK_COMMIT_FILE_LIMIT = 200


def load_map() -> Dict[str, Any]:
    try:
        import yaml
    except ImportError:
        sys.exit("pyyaml is required: pip install pyyaml")
    with open(SURFACE_MAP) as fh:
        return yaml.safe_load(fh) or {}


def git(repo: str, *args: str) -> str:
    try:
        out = subprocess.run(["git", "-C", repo, *args], capture_output=True,
                             text=True, timeout=60)
    except (OSError, subprocess.SubprocessError) as exc:
        return f"__error__ {exc}"
    if out.returncode != 0:
        return f"__error__ {(out.stderr or '').strip()}"
    return out.stdout


def head_sha(repo: str) -> Optional[str]:
    out = git(repo, "rev-parse", "HEAD").strip()
    return None if out.startswith("__error__") or not out else out


def commits_since(repo: str, since: Optional[str], count: Optional[int]) -> List[Dict[str, Any]]:
    """Commits newer than ``since`` (or the last ``count``), with changed paths."""
    rev = f"{since}..HEAD" if since else f"-n{count or 20}"
    fmt = "%H%x1f%an%x1f%aI%x1f%s"
    raw = git(repo, "log", rev, f"--format={fmt}", "--name-only")
    if raw.startswith("__error__"):
        # A watermark from a rebased/force-pushed branch no longer exists; fall
        # back to a bounded window rather than reporting nothing.
        if since:
            return commits_since(repo, None, count or 20)
        return []

    out: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    for line in raw.splitlines():
        if "\x1f" in line:
            if current:
                out.append(current)
            sha, author, when, subject = line.split("\x1f", 3)
            current = {"sha": sha, "author": author, "date": when,
                       "subject": subject, "files": []}
        elif line.strip() and current is not None:
            current["files"].append(line.strip())
    if current:
        out.append(current)
    return out


def classify(path: str, surfaces: Dict[str, List[str]], ignore: List[str]) -> List[str]:
    """Every surface whose globs match ``path``. May be empty or several."""
    if any(fnmatch.fnmatch(path, pat) for pat in ignore):
        return []
    hits = []
    for surface, patterns in surfaces.items():
        for pat in patterns:
            # A bare `**/x` glob does not match a top-level `x` in fnmatch, so
            # try the de-anchored form too.
            if fnmatch.fnmatch(path, pat) or fnmatch.fnmatch("/" + path, "*" + pat):
                hits.append(surface)
                break
    return hits


def build(since_map: Dict[str, Optional[str]], count: Optional[int]) -> Dict[str, Any]:
    cfg = load_map()
    surfaces = cfg.get("surfaces") or {}
    ignore = cfg.get("ignore") or []
    spec_sections = cfg.get("spec_sections") or {}
    repos_cfg = cfg.get("repos") or {}

    known_gaps = ledger.gaps()

    per_repo: Dict[str, Any] = {}
    touched: Dict[str, Dict[str, Any]] = {}
    new_watermarks: Dict[str, str] = {}

    for repo_name, meta in repos_cfg.items():
        repo_path = os.path.expanduser(str(meta.get("path") or ""))
        if not os.path.isdir(os.path.join(repo_path, ".git")):
            per_repo[repo_name] = {"status": "missing", "path": repo_path,
                                   "optional": bool(meta.get("optional"))}
            continue

        head = head_sha(repo_path)
        entries = commits_since(repo_path, since_map.get(repo_name), count)
        per_repo[repo_name] = {
            "status": "ok",
            "path": repo_path,
            "head": head,
            "since": since_map.get(repo_name),
            "n_commits": len(entries),
            "commits": [{k: v for k, v in e.items() if k != "files"} for e in entries],
        }
        if head:
            new_watermarks[repo_name] = head

        for entry in entries:
            files = entry["files"]
            if len(files) > BULK_COMMIT_FILE_LIMIT:
                per_repo[repo_name].setdefault("bulk_commits", []).append(entry["sha"][:9])
                continue
            for path in files:
                for surface in classify(path, surfaces, ignore):
                    rec = touched.setdefault(surface, {
                        "surface": surface,
                        "level": surface.split(".")[0],
                        "spec_sections": spec_sections.get(surface, []),
                        "files": [],
                        "commits": [],
                    })
                    if path not in rec["files"]:
                        rec["files"].append(path)
                    tag = f"{repo_name}@{entry['sha'][:9]}: {entry['subject']}"
                    if tag not in rec["commits"]:
                        rec["commits"].append(tag)

    # Rank the work-list: uncovered surfaces first, then by evidence volume.
    work = []
    for surface, rec in touched.items():
        # cases_for() also counts level-granularity records, so a test that did
        # not declare a precise surface still registers as coverage.
        covered_cases = ledger.cases_for(surface)
        rec["covered_by"] = covered_cases
        rec["is_covered"] = bool(covered_cases)
        rec["known_gaps"] = [g for g in known_gaps if g.startswith(surface)]
        rec["priority"] = (
            0 if not covered_cases else 1
        )
        work.append(rec)
    work.sort(key=lambda r: (r["priority"], -len(r["commits"]), r["surface"]))

    return {
        "repos": per_repo,
        "work": work,
        "new_watermarks": new_watermarks,
        "totals": {
            "surfaces_touched": len(work),
            "uncovered_surfaces": sum(1 for r in work if not r["is_covered"]),
            "commits": sum(r.get("n_commits", 0) for r in per_repo.values()
                           if isinstance(r, dict)),
        },
    }


def summarize(result: Dict[str, Any]) -> str:
    lines = ["repos:"]
    for name, meta in result["repos"].items():
        if meta.get("status") != "ok":
            flag = "optional" if meta.get("optional") else "MISSING"
            lines.append(f"  {name:12s} [{flag}] {meta.get('path')}")
            continue
        lines.append(f"  {name:12s} {meta['n_commits']:3d} new commit(s) "
                     f"since {str(meta.get('since'))[:9] or 'origin'} "
                     f"-> head {str(meta.get('head'))[:9]}")
        for com in meta["commits"][:5]:
            lines.append(f"      {com['sha'][:9]} {com['subject'][:68]}")
        if meta["n_commits"] > 5:
            lines.append(f"      ... {meta['n_commits'] - 5} more")

    t = result["totals"]
    lines.append("")
    lines.append(f"work-list: {t['surfaces_touched']} surface(s) touched, "
                 f"{t['uncovered_surfaces']} with no existing coverage")
    for rec in result["work"]:
        mark = "NEW " if not rec["is_covered"] else "grow"
        lines.append(f"  [{mark}] {rec['surface']:16s} -> {rec['level']}  "
                     f"spec §{','.join(rec['spec_sections']) or '-'}")
        for path in rec["files"][:4]:
            lines.append(f"           {path}")
        if len(rec["files"]) > 4:
            lines.append(f"           ... {len(rec['files']) - 4} more file(s)")
        if rec["covered_by"]:
            lines.append(f"           covered by: {', '.join(rec['covered_by'][:3])}")
        if rec["known_gaps"]:
            lines.append(f"           known gaps (do not re-propose): "
                         f"{', '.join(rec['known_gaps'][:3])}")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--summary", action="store_true", help="human-readable output")
    ap.add_argument("--since", type=int, default=0,
                    help="ignore the watermark; use the last N commits per repo")
    ap.add_argument("--advance", action="store_true",
                    help="write the new watermarks (only after a successful run)")
    ap.add_argument("--out", default="", help="also write the JSON here")
    args = ap.parse_args()

    since_map: Dict[str, Optional[str]] = {}
    if not args.since:
        since_map = {k: v for k, v in ledger.commits().items()}

    result = build(since_map, args.since or None)

    if args.advance:
        ledger.set_last_seen_many(result["new_watermarks"])
        result["watermarks_advanced"] = True

    text = json.dumps(result, indent=2, default=str)
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as fh:
            fh.write(text)

    print(summarize(result) if args.summary else text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
