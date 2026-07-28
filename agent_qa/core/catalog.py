"""Live dataset/schema discovery, with a disk cache.

Two reasons this is not a hardcoded table:

1. ``hiveq-flow/AGENTS.md`` is explicit — *"Never invent dataset or schema
   names. Discover them live."* Entitlements also differ per account, so a
   baked list would fail for some operators and silently over-claim for others.
2. The L1 data matrix is *generated* from the catalog. New datasets therefore
   grow coverage automatically instead of waiting for someone to notice.

Caching exists because ``hiveq.datasets`` lives in the ``hiveq`` namespace
shipped by **hiveq-sdk only** — the fat ``hiveq-flow`` package provides
``hiveq.flow`` and nothing else. So under ``--engine=inproc`` there is no live
catalog client at all, and tests read the cache written by the last remote run
(or by ``run_all.py --refresh-catalog``).
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from agent_qa.core.profiles import apply_profile, profile_name

#: Cache is per-profile: a local stack and staging expose different catalogs.
_CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ledger")
_CACHE_MAX_AGE_S = float(os.environ.get("AGENT_QA_CATALOG_MAX_AGE", str(24 * 3600)))


class CatalogUnavailable(RuntimeError):
    """No live client and no usable cache — tests should report GAP, not FAIL."""


def cache_path(profile: Optional[str] = None) -> str:
    return os.path.join(_CACHE_DIR, f"catalog.{profile or profile_name()}.json")


def fetch_live() -> List[Dict[str, Any]]:
    """Query the metadata API. Only works where ``hiveq.datasets`` is importable."""
    apply_profile()
    try:
        from hiveq.datasets import fetch_catalog  # noqa: PLC0415 - engine-dependent
    except ImportError as exc:
        raise CatalogUnavailable(
            "hiveq.datasets is not importable (expected under --engine=inproc, "
            "where hiveq-flow provides only the hiveq.flow namespace)"
        ) from exc
    return fetch_catalog()


def refresh(profile: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetch live and write the cache. Returns the catalog."""
    entries = fetch_live()
    path = cache_path(profile)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump({"fetched_at": time.time(), "profile": profile or profile_name(),
                   "entries": entries}, fh, indent=2, default=str)
    return entries


def load(allow_stale: bool = True) -> List[Dict[str, Any]]:
    """Catalog from cache when fresh, else live, else stale cache.

    Falls back to a stale cache rather than failing: a day-old dataset list is
    a far better basis for a coverage matrix than no matrix at all, and every
    generated case verifies its own data at run time anyway.
    """
    path = cache_path()
    cached, age = _read_cache(path)

    if cached is not None and age is not None and age <= _CACHE_MAX_AGE_S:
        return cached
    try:
        return refresh()
    except (CatalogUnavailable, Exception) as exc:  # noqa: BLE001
        if cached is not None and allow_stale:
            return cached
        raise CatalogUnavailable(f"no live catalog and no cache at {path}: {exc}") from exc


def _read_cache(path: str):
    try:
        with open(path) as fh:
            blob = json.load(fh)
    except (OSError, ValueError):
        return None, None
    entries = blob.get("entries")
    if not isinstance(entries, list):
        return None, None
    fetched = blob.get("fetched_at")
    age = (time.time() - float(fetched)) if fetched else None
    return entries, age


# --------------------------------------------------------------------- queries


def datasets(catalog: Optional[Sequence[Dict[str, Any]]] = None) -> List[str]:
    return sorted(e["dataset"] for e in (catalog or load()) if e.get("dataset"))


def schemas_for(dataset: str, catalog: Optional[Sequence[Dict[str, Any]]] = None) -> List[str]:
    for entry in catalog or load():
        if entry.get("dataset") == dataset:
            return list(entry.get("schemas") or [])
    return []


def has(dataset: str, schema: Optional[str] = None,
        catalog: Optional[Sequence[Dict[str, Any]]] = None) -> bool:
    """Is this dataset (and optionally schema) entitled for this account?

    The guard every generated L1 case opens with — an unentitled pair is a GAP,
    never a FAIL.
    """
    found = schemas_for(dataset, catalog)
    if schema is None:
        return bool(found) or dataset in datasets(catalog)
    return schema in found


def pairs(catalog: Optional[Sequence[Dict[str, Any]]] = None) -> List[tuple]:
    """Every (dataset, schema) pair, the raw input to the L1 matrix."""
    out = []
    for entry in catalog or load():
        ds = entry.get("dataset")
        for sc in entry.get("schemas") or []:
            out.append((ds, sc))
    return sorted(out)


def fields(dataset: str, schema: str) -> List[str]:
    """Column names for a dataset/schema, live. [] when unavailable."""
    apply_profile()
    try:
        from hiveq.datasets import fetch_schema_details  # noqa: PLC0415
    except ImportError:
        return []
    try:
        details = fetch_schema_details(dataset=dataset, schema=schema)
    except Exception:  # noqa: BLE001
        return []
    return _field_names(details)


def _field_names(details: Any) -> List[str]:
    """Pull column names out of whatever shape the metadata API returned."""
    if isinstance(details, dict):
        for key in ("fields", "columns", "schema"):
            got = details.get(key)
            if isinstance(got, list):
                return [
                    str(f.get("name") or f.get("column") or f)
                    if isinstance(f, dict) else str(f)
                    for f in got
                ]
        return sorted(details.keys())
    if isinstance(details, list):
        return [str(f.get("name") if isinstance(f, dict) else f) for f in details]
    return []
