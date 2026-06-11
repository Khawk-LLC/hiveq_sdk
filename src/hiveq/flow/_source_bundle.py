"""Build a source archive of everything a deployed task references.

Why this exists
---------------
The legacy capture path cloudpickles user classes/functions — i.e. it ships
**code objects (bytecode)**. CPython bytecode is interpreter-version-specific, so
a client on Python 3.13 or 3.11 talking to a 3.12 engine breaks (unpickle error
or, worse, a segfault on malformed-but-loaded bytecode). The fix is to ship
**source** and let the engine recompile it under its own interpreter; the only
remaining contract becomes "the source parses on the engine's Python", which is
far weaker than "match my exact interpreter".

What this module does
---------------------
Given the caller's frame and its strategy configs, it collects:

1. **The first-party import graph** — every already-imported module whose file
   lives under the detected project root (and isn't a venv / site-packages / the
   ``hiveq`` SDK itself). Using ``sys.modules`` captures the *executed* import
   graph, which is more reliable than statically re-resolving imports.
2. **The entry script** itself (so a ``__main__``-defined strategy recompiles).
3. **Explicit includes** — files/globs the user names (configs, data referenced
   by relative path, etc.). Static analysis can't see ``open("params.yaml")``,
   so configurations are declared, not guessed.

Everything is packed into an in-memory zip (with a ``__hiveq_manifest__.json``
describing the layout) and returned base64-encoded, ready to embed in the submit
payload alongside the existing pickle (dual-ship: a source-aware engine
recompiles; an older one ignores the archive and falls back to the pickle).
"""
from __future__ import annotations

import base64
import glob
import inspect
import io
import json
import os
import sys
import sysconfig
import zipfile
from typing import Any, Dict, List, Optional, Sequence, Tuple

from hiveq.flow.logger import logger

logger = logger(show_logo=False)

# Directory names that are never first-party even when nested under the root.
_SKIP_DIR_NAMES = {
    "__pycache__", ".git", ".hg", ".svn", "node_modules",
    "site-packages", "dist-packages", ".tox", ".mypy_cache", ".pytest_cache",
    ".venv", "venv", "env", ".env", ".eggs", "build", "dist",
}
# Markers that identify a project root when walking up from the entry script.
_ROOT_MARKERS = ("pyproject.toml", "setup.py", "setup.cfg", ".git")

# Hard size bound. The archive rides inside the submit payload, so a bundle is
# meant to hold *source + small configs*, not data sets. We refuse to build a
# bundle past these caps — both to keep payloads sane and so a stray/malicious
# multi-GB file can't be packed and shipped. Override with HIVEQ_SOURCE_BUNDLE_MAX_MB.
_DEFAULT_MAX_MB = 25
# A single file this big almost always means data got swept in by mistake — fail
# loudly and point at it rather than silently shipping it.
_MAX_FILE_BYTES = 25 * 1024 * 1024
# Warn well before the hard cap so large-but-legal bundles are visible.
_SIZE_WARN_BYTES = 10 * 1024 * 1024


def _max_total_bytes() -> int:
    """Hard cap on the bundle's uncompressed size (env-overridable, in MB)."""
    raw = os.environ.get("HIVEQ_SOURCE_BUNDLE_MAX_MB")
    try:
        mb = int(raw) if raw else _DEFAULT_MAX_MB
    except ValueError:
        mb = _DEFAULT_MAX_MB
    return max(1, mb) * 1024 * 1024


class SourceBundleTooLarge(RuntimeError):
    """Raised when the source bundle would exceed the configured size cap."""


def _interpreter_lib_dirs() -> Tuple[str, ...]:
    """Absolute dirs that hold the stdlib / installed packages — never bundled."""
    dirs = set()
    for key in ("stdlib", "platstdlib", "purelib", "platlib"):
        try:
            p = sysconfig.get_paths().get(key)
        except Exception:
            p = None
        if p:
            dirs.add(os.path.realpath(p))
    for p in (sys.prefix, sys.base_prefix, sys.exec_prefix):
        if p:
            dirs.add(os.path.realpath(p))
    return tuple(dirs)


def _find_project_root(start_dir: str) -> str:
    """Walk up from ``start_dir`` to the nearest dir holding a project marker.

    Falls back to ``start_dir`` itself when no marker is found (or at the
    filesystem root). The root anchors every archived file's relative path, so
    the engine can rebuild the package layout exactly.
    """
    cur = os.path.realpath(start_dir)
    last = None
    while cur and cur != last:
        for marker in _ROOT_MARKERS:
            if os.path.exists(os.path.join(cur, marker)):
                return cur
        last, cur = cur, os.path.dirname(cur)
    return os.path.realpath(start_dir)


def _is_under(path: str, parent: str) -> bool:
    try:
        return os.path.commonpath([path, parent]) == parent
    except ValueError:  # different drives on Windows, etc.
        return False


def _has_skip_component(path: str, root: str) -> bool:
    """True if any path component *below the root* is a skip dir (venv, .git…)."""
    rel = os.path.relpath(path, root)
    return any(part in _SKIP_DIR_NAMES for part in rel.split(os.sep))


def _first_party_files(root: str, lib_dirs: Sequence[str]) -> Dict[str, str]:
    """Map ``abs_path -> rel_path`` for every imported first-party source file.

    First-party = a loaded module whose ``__file__`` is a real ``.py`` under the
    project root, not inside a venv / site-packages / the stdlib, and not part of
    the ``hiveq`` SDK (the engine already has hiveq-flow installed).
    """
    files: Dict[str, str] = {}
    # Snapshot — importing nothing here, but be defensive against mutation.
    for name, mod in list(sys.modules.items()):
        if name == "hiveq" or name.startswith("hiveq."):
            continue  # shipped by the engine; never bundle the SDK
        f = getattr(mod, "__file__", None)
        if not f or not f.endswith(".py"):
            continue
        ap = os.path.realpath(f)
        if not os.path.isfile(ap):
            continue
        if not _is_under(ap, root):
            continue
        if any(_is_under(ap, d) for d in lib_dirs):
            continue
        if _has_skip_component(ap, root):
            continue
        files[ap] = os.path.relpath(ap, root).replace(os.sep, "/")
    return files


# Config file extensions auto-bundled when found beside a bundled module.
_CONFIG_EXTS = (".yaml", ".yml", ".json", ".toml", ".ini", ".cfg")
# Project-meta files we never want to sweep in as "config".
_CONFIG_NAME_DENY = {"pyproject.toml", "setup.cfg", "tox.ini", "mypy.ini"}


def _adjacent_config_files(
    members: Dict[str, str], root: str
) -> Dict[str, str]:
    """Find config files sitting in the same directories as bundled modules.

    Returns ``abs_path -> rel_path`` for each. Non-recursive per directory (so a
    sibling ``data/`` full of CSVs is never swept in), known config extensions
    only, and project-meta files (``pyproject.toml`` …) excluded.
    """
    out: Dict[str, str] = {}
    member_dirs = {os.path.dirname(ap) for ap in members}
    for d in member_dirs:
        try:
            entries = os.listdir(d)
        except OSError:
            continue
        for fn in entries:
            if not fn.lower().endswith(_CONFIG_EXTS) or fn in _CONFIG_NAME_DENY:
                continue
            ap = os.path.realpath(os.path.join(d, fn))
            if not os.path.isfile(ap) or ap in members or ap in out:
                continue
            if not _is_under(ap, root) or _has_skip_component(ap, root):
                continue
            out[ap] = os.path.relpath(ap, root).replace(os.sep, "/")
    return out


def _resolve_includes(
    includes: Sequence[str], root: str
) -> Dict[str, str]:
    """Expand user-named files/globs to ``abs_path -> rel_path``.

    Globs and relative paths resolve against the current working directory.
    A file under the project root keeps its root-relative path; one outside the
    root is stored under ``_includes/<basename>`` so it still lands on the engine
    without escaping the archive.
    """
    out: Dict[str, str] = {}
    for pattern in includes:
        matches = glob.glob(pattern, recursive=True)
        if not matches:
            logger.warning(f"source bundle: include pattern matched nothing: {pattern!r}")
            continue
        for m in matches:
            ap = os.path.realpath(m)
            if not os.path.isfile(ap):
                continue  # skip directories themselves; glob '**/*' yields files
            if _is_under(ap, root):
                rel = os.path.relpath(ap, root).replace(os.sep, "/")
            else:
                rel = f"_includes/{os.path.basename(ap)}"
            out[ap] = rel
    return out


def build_source_bundle(
    strategy_configs: Sequence[Any],
    frame,
    includes: Optional[Sequence[str]] = None,
    main_objects: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Build the source archive for a deploy.

    Parameters
    ----------
    strategy_configs : sequence
        The deploy's strategy configs (their ``type`` names are recorded in the
        manifest so the engine knows which classes to resolve from the source).
    frame : frame
        The caller frame already located by ``capture_calling_module`` — used to
        find the entry script and anchor the project root.
    includes : sequence of str, optional
        Extra files/globs to bundle (rarely needed: config files sitting beside
        bundled modules are picked up automatically).
    main_objects : dict, optional
        ``name -> object`` for the user-defined classes/functions found in the
        caller's ``__main__`` frame. Used only in the notebook/REPL case (no entry
        ``.py`` file): their source is recovered via ``inspect.getsource`` into a
        synthetic ``__hiveq_main__.py`` so they still ship as source.

    Returns
    -------
    dict or None
        ``{"bundle_b64", "manifest"}`` on success, or ``None`` if there was
        nothing to bundle (so the caller can simply omit the field).
    """
    includes = list(includes or [])
    main_objects = main_objects or {}
    script_path = frame.f_globals.get("__file__") if frame else None

    # Anchor the root on the entry script when there is one (the common script
    # case); otherwise on the cwd (notebook / REPL — imported locals get bundled
    # and __main__ definitions are recovered as source below).
    if script_path and os.path.exists(script_path):
        root = _find_project_root(os.path.dirname(os.path.abspath(script_path)))
    else:
        root = _find_project_root(os.getcwd())

    lib_dirs = _interpreter_lib_dirs()
    members = _first_party_files(root, lib_dirs)        # abs_path -> rel (on disk)
    virtual: Dict[str, str] = {}                        # rel -> generated content

    # Always include the entry script and the source files that define the
    # captured strategy classes, even if module introspection missed them.
    entry_rel: Optional[str] = None
    if script_path and os.path.isfile(script_path):
        ap = os.path.realpath(script_path)
        if _is_under(ap, root) and not _has_skip_component(ap, root):
            entry_rel = os.path.relpath(ap, root).replace(os.sep, "/")
            members.setdefault(ap, entry_rel)

    # Notebook / REPL: no entry .py — recover the __main__ definitions as source
    # into a synthetic entry module so the engine can recompile them.
    if entry_rel is None and main_objects:
        parts = []
        for name, obj in main_objects.items():
            try:
                parts.append(inspect.getsource(obj))
            except (OSError, TypeError):
                logger.warning(
                    f"source bundle: could not recover source for '{name}' "
                    f"(defined interactively?); it will not be deployable."
                )
        if parts:
            entry_rel = "__hiveq_main__.py"
            virtual[entry_rel] = "\n\n".join(parts) + "\n"

    strategy_types: List[str] = []
    for cfg in strategy_configs:
        t = getattr(cfg, "type", None)
        if t:
            strategy_types.append(t)

    # Auto-include config files sitting beside the bundled modules (no explicit
    # include= needed). Conservative: only known config extensions, only in dirs
    # that already contain bundled source, never recursing into data dirs.
    members.update(_adjacent_config_files(members, root))

    # Explicit includes (internal/testing escape hatch).
    members.update(_resolve_includes(includes, root))

    if not members and not virtual:
        logger.debug("source bundle: nothing to bundle (no first-party files)")
        return None

    # Size guard BEFORE we read anything into the archive: a single oversized
    # file (data swept in, or a malicious drop) or an oversized total both raise
    # rather than build a multi-GB payload.
    max_total = _max_total_bytes()
    total = sum(len(c.encode("utf-8")) for c in virtual.values())
    for ap, rel in members.items():
        try:
            sz = os.path.getsize(ap)
        except OSError:
            continue
        if sz > _MAX_FILE_BYTES:
            raise SourceBundleTooLarge(
                f"source bundle: file '{rel}' is {sz / 1024 / 1024:.1f} MB, over the "
                f"{_MAX_FILE_BYTES / 1024 / 1024:.0f} MB per-file limit. The bundle is "
                f"for source + small configs; upload data with hf.upload_files() and "
                f"reference it by relative path."
            )
        total += sz
        if total > max_total:
            raise SourceBundleTooLarge(
                f"source bundle exceeds the {max_total / 1024 / 1024:.0f} MB limit "
                f"(reached at '{rel}'). Raise the cap with HIVEQ_SOURCE_BUNDLE_MAX_MB, "
                f"or move large data to hf.upload_files()."
            )

    all_rels = list(members.values()) + list(virtual.keys())
    py_files = sorted(rel for rel in all_rels if rel.endswith(".py"))
    other_files = sorted(rel for rel in all_rels if not rel.endswith(".py"))

    manifest = {
        "schema": "hiveq.source_bundle/v1",
        "root": os.path.basename(root),
        "entry": entry_rel,
        "files": py_files,
        "includes": other_files,
        "strategy_types": strategy_types,
        "python_version": "%d.%d.%d" % sys.version_info[:3],
        "platform": sys.platform,
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("__hiveq_manifest__.json", json.dumps(manifest, indent=2))
        for ap, rel in sorted(members.items(), key=lambda kv: kv[1]):
            try:
                zf.write(ap, rel)
            except OSError as e:
                logger.warning(f"source bundle: could not add {rel}: {e}")
        for rel, content in sorted(virtual.items()):
            zf.writestr(rel, content)

    raw = buf.getvalue()
    if len(raw) > _SIZE_WARN_BYTES:
        logger.warning(
            f"source bundle is large ({len(raw) / 1024 / 1024:.1f} MB); it rides "
            f"inside the submit payload. Consider moving large data to "
            f"hf.upload_files() and referencing it by relative path."
        )

    logger.debug(
        f"source bundle: {len(py_files)} py + {len(other_files)} other file(s), "
        f"{len(raw)} bytes, root={root}"
    )
    return {
        "bundle_b64": base64.b64encode(raw).decode("ascii"),
        "manifest": manifest,
    }
