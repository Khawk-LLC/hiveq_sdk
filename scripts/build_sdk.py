#!/usr/bin/env python3
"""Assemble the thin ``hiveq-sdk`` client package from this full repo.

The thin SDK ships only what runs on a *client*: the deploy + observe code and
**type stubs** for the authoring surface. The proprietary engine (``oms/sigma/
{adapter,engine,...}``, ``app.py``/``backtest_app.py`` run loops, ``market_data/``,
the C++ ``PySigma`` binding) never leaves the platform — the executor installs the
*full* ``hiveq-flow`` and ships only the user's strategy code at deploy time.

Three kinds of module land in the SDK:
  1. VERBATIM  — engine-free deploy/observe + config code, copied as-is.
  2. STUB      — authoring DTO/type modules (events, data_types, sigma types):
                 the real wrappers are replaced by auto-generated name-only
                 runtime stubs (empty classes, sibling inheritance preserved)
                 plus a ``.pyi`` type surface (via stubgen). Users only reference
                 these types in annotations / inside callbacks that run on the
                 executor — never instantiated on the client.
  3. PROTECTED — hand-authored thin files (the deploy glue, the SigmaContext
                 placeholder, package inits). Never overwritten by this script.

Usage:
    python scripts/build_sdk.py [--sdk-root /path/to/hiveq-sdk]
"""
from __future__ import annotations

import argparse
import ast
import os
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
# This build script lives in the hiveq-sdk project and GENERATES the thin SDK
# from the engine source in the sibling hiveq-flow checkout (../../hiveq-flow).
FULL_SRC = os.path.normpath(os.path.join(HERE, "..", "..", "hiveq-flow", "src", "hiveq", "flow"))
# Target = this project's own root (where scripts/ lives).
DEFAULT_SDK_ROOT = os.path.normpath(os.path.join(HERE, ".."))

# 1. Copied verbatim (engine-free; lazy/in-method engine imports never fire here).
VERBATIM_FILES = [
    "jobs.py",          # orchestrator deploy/observe wrapper (re-exports)
    "runs.py",          # Run handle / observe (REST) + live progress bar
    "config.py",        # StrategyConfig/BacktestConfig/EngineConfig + enums (DTOs)
    "context.py",       # Context (authoring placeholder, raises on construct)
    "trading_types.py", # OrderSide/OrderType/OrderStatus enums
    "data/__init__.py", # re-exports the data_types names
    "data/reader.py",   # observe: REST results client
    "metrics/report.py",# observe: PerformanceReport.from_rest (real parsing)
    # utils: only the files actually used on the client. timezone_utils (tz
    # helpers), date_calendar (TradingCalendar — public authoring util, exported
    # by utils/__init__), and __init__ (is_notebook + name helpers). NOT
    # symbol_parser.py — unused on the client and not even exported.
    "utils/__init__.py",
    "utils/timezone_utils.py",
    "utils/date_calendar.py",
]
VERBATIM_DIRS = [
    "logger",           # logging
    # NOTE: events/__init__.py and oms/sigma/types/__init__.py are re-export
    # shims — copied verbatim below; their content modules are STUBBED.
]
VERBATIM_INITS = [      # package __init__ re-export shims kept as-is
    "events/__init__.py",
    "oms/sigma/types/__init__.py",
]

# 2. Authoring modules whose bodies are replaced with auto-generated stubs.
STUB_FILES = [
    "events/event_types.py",
    "data/data_types.py",
    "oms/sigma/types/order.py",
    "oms/sigma/types/position.py",
    "oms/sigma/types/portfolio.py",
    "oms/sigma/types/fill.py",
    "oms/sigma/types/bar.py",
    "oms/sigma/types/trade_tick.py",
    "oms/sigma/types/quote_tick.py",
    "oms/sigma/types/snap.py",
    "oms/sigma/types/custom_data.py",
    "oms/sigma/types/trade_stats.py",
    "oms/sigma/types/executor.py",
]

# 3. Hand-authored thin files — never overwritten.
PROTECTED = {
    "__init__.py",                  # thin deploy/observe API + authoring re-exports
    "uploads.py",                   # hiveq-data CLI / upload_files — SDK-only (the
                                    # full hiveq-flow package no longer ships it)
    "deploy_task.py",               # thin capture+submit (run() stubbed)
    "metrics/__init__.py",          # minimal: exports only PerformanceReport
    "oms/__init__.py",
    "oms/sigma/__init__.py",
    "oms/sigma/sigma_context.py",   # SigmaContext placeholder (+ curated .pyi)
}

_ENUM_BASES = {"Enum", "IntEnum", "StrEnum", "Flag", "IntFlag"}

import re as _re

def _clean_pyi(text: str) -> str:
    """Make stubgen output mypy-clean for use as the AI's authoring contract.

    1. ``x: T = None`` -> ``x: T | None = None`` (stubgen reproduces implicit
       Optional, which trips ``no_implicit_optional``).
    2. Drop ``@dataclass`` decorators. The event/DTO stubs are pure type surfaces
       — the AI reads fields (``event.bar``) but never constructs them (the engine
       does). As dataclasses, mypy enforces field-ordering across the Event
       hierarchy ("non-default cannot follow default"), which the flattened stub
       violates. Plain annotated classes expose the same attributes, no ordering
       rule, no synthesized ``__init__``.
    """
    def repl(m: "_re.Match") -> str:
        typ = m.group(1).strip()
        if typ.endswith("None") or "Optional" in typ or ("|" in typ and "None" in typ):
            return m.group(0)
        return f": {typ} | None = None"
    text = _re.sub(r": ([^,()\[\]=]+?) = None", repl, text)
    # Strip @dataclass / @dataclass(...) decorator lines.
    text = _re.sub(r"^[ \t]*@dataclass(\([^)]*\))?[ \t]*\n", "", text, flags=_re.MULTILINE)
    # Resolve sibling Sigma* type cross-references stubgen left unimported
    # (e.g. order.pyi returns SigmaFill). All Sigma* types are re-exported from
    # the oms.sigma.types package, so one import line covers the gap.
    used = set(_re.findall(r"\bSigma[A-Za-z0-9_]+\b", text))
    defined = set(_re.findall(r"^class (\w+)", text, flags=_re.MULTILINE))
    import_lines = "\n".join(l for l in text.splitlines() if l.startswith(("from ", "import ")))
    imported = set(_re.findall(r"\bSigma[A-Za-z0-9_]+\b", import_lines))
    missing = sorted(used - defined - imported)
    if missing:
        text = ("from hiveq.flow.oms.sigma.types import " + ", ".join(missing) + "\n") + text
    return text


# Backwards-compatible alias used by _gen_pyi.
_fix_implicit_optional = _clean_pyi


def _copy_file(rel: str, dest_root: str) -> None:
    src = os.path.join(FULL_SRC, rel)
    dst = os.path.join(dest_root, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
    print(f"  copy  {rel}")


def _copy_dir(rel: str, dest_root: str) -> None:
    src = os.path.join(FULL_SRC, rel)
    dst = os.path.join(dest_root, rel)
    if os.path.isdir(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    print(f"  copy  {rel}/  (tree)")


def _emit_stub(src_path: str, rel: str) -> str:
    """Build a name-only runtime stub from a module's AST (no import, no engine).

    Emits each top-level class as an empty stub (``class X(...): ...``), keeping
    only base classes that are siblings defined in the same module (so intra-module
    hierarchies like the Event tree still satisfy ``isinstance``). Enums keep their
    members. Public module-level functions become ``def f(*a, **k): ...``. Anything
    a user only *reads inside a callback* needs no body here — that runs on the
    executor against the real types.
    """
    with open(src_path) as f:
        tree = ast.parse(f.read())

    local_classes = {n.name for n in tree.body if isinstance(n, ast.ClassDef)}
    out = [
        '"""Auto-generated stub (thin client SDK).',
        "",
        f"Name-only authoring surface for ``hiveq.flow.{rel[:-3].replace('/', '.')}``.",
        "The real implementation is engine-backed and runs only on the HiveQ",
        "platform executor; see the adjacent ``.pyi`` for the typed surface.",
        '"""',
        "from __future__ import annotations",
        "import enum",
        "",
    ]
    exported = []

    def is_enum(node: ast.ClassDef) -> bool:
        return any(isinstance(b, ast.Name) and b.id in _ENUM_BASES for b in node.bases)

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            exported.append(node.name)
            if is_enum(node):
                base = next(b.id for b in node.bases if isinstance(b, ast.Name) and b.id in _ENUM_BASES)
                members = [
                    f"    {t.id} = {ast.unparse(s.value)}"
                    for s in node.body if isinstance(s, ast.Assign)
                    for t in s.targets if isinstance(t, ast.Name)
                ]
                out.append(f"class {node.name}(enum.{base if base in _ENUM_BASES else 'Enum'}):")
                out.extend(members or ["    ..."])
                out.append("")
                continue
            kept = [b.id for b in node.bases if isinstance(b, ast.Name) and b.id in local_classes]
            head = f"class {node.name}({', '.join(kept)})" if kept else f"class {node.name}"
            out.append(f"{head}:")
            out.append("    ...")
            out.append("")
        elif isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
            exported.append(node.name)
            out.append(f"def {node.name}(*args, **kwargs): ...")
            out.append("")

    out.append(f"__all__ = {sorted(exported)!r}")
    out.append("")
    return "\n".join(out)


def _stub_file(rel: str, dest_root: str) -> None:
    src = os.path.join(FULL_SRC, rel)
    dst = os.path.join(dest_root, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w") as f:
        f.write(_emit_stub(src, rel))
    print(f"  stub  {rel}")


def _gen_pyi(rel: str, dest_root: str) -> None:
    """Emit a .pyi type surface for a stubbed module via stubgen (AST parse-only)."""
    src = os.path.join(FULL_SRC, rel)
    out_dir = os.path.join(dest_root, os.path.dirname(rel))
    try:
        subprocess.run(
            ["stubgen", src, "--parse-only", "-o", "/tmp/_sdk_pyi", "-q"],
            check=False, capture_output=True,
        )
        # stubgen mirrors the full package path under -o: <out>/hiveq/flow/<rel>.pyi
        produced = os.path.join("/tmp/_sdk_pyi", "hiveq", "flow", rel[:-3] + ".pyi")
        if os.path.exists(produced):
            os.makedirs(out_dir, exist_ok=True)
            with open(produced) as f:
                text = _fix_implicit_optional(f.read())
            with open(os.path.join(out_dir, os.path.basename(rel)[:-3] + ".pyi"), "w") as f:
                f.write(text)
            print(f"  pyi   {rel[:-3]}.pyi")
    except FileNotFoundError:
        pass  # stubgen not installed; runtime stub still works, just less typing


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sdk-root", default=DEFAULT_SDK_ROOT)
    ap.add_argument("--no-pyi", action="store_true", help="skip .pyi generation")
    args = ap.parse_args()

    dest_root = os.path.join(args.sdk_root, "src", "hiveq", "flow")
    if not os.path.isdir(FULL_SRC):
        print(f"full source not found: {FULL_SRC}", file=sys.stderr)
        return 1
    os.makedirs(dest_root, exist_ok=True)

    print(f"Assembling thin SDK -> {dest_root}")
    # Top-level package marker + PEP 561 typing marker (mirrors the full repo).
    pkg_root = os.path.dirname(dest_root)  # .../src/hiveq
    open(os.path.join(pkg_root, "__init__.py"), "a").close()
    open(os.path.join(pkg_root, "py.typed"), "a").close()
    for rel in VERBATIM_FILES:
        _copy_file(rel, dest_root)
    for rel in VERBATIM_DIRS:
        _copy_dir(rel, dest_root)
    for rel in VERBATIM_INITS:
        _copy_file(rel, dest_root)
    for rel in STUB_FILES:
        _stub_file(rel, dest_root)
        if not args.no_pyi:
            _gen_pyi(rel, dest_root)

    missing = [p for p in PROTECTED if not os.path.exists(os.path.join(dest_root, p))]
    if missing:
        print("\nWARNING: hand-authored thin files missing (author them once):", file=sys.stderr)
        for p in missing:
            print(f"  - {p}", file=sys.stderr)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
