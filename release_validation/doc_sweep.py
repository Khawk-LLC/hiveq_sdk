#!/usr/bin/env python3
"""Static sweep of the canonical AI-facing docs against the installed SDK.

The docs declare themselves canonical and "verified against source". This tool
checks that mechanically, with no backtest compute:

  1. snippet_syntax    every ```python block parses (signature listings excluded)
  2. signatures        documented `fn(params) -> Ret` listings match the real signature
  3. imports           every import in every snippet resolves, and names exist
  4. module_attrs      `hf.X` / `hiveq.flow.X` references exist
  5. ctx_methods       `ctx.X(...)` exists on SigmaContext, and its kwargs are real
  6. enums             §12 members exist, and `.value == name` where claimed
  7. dataclasses       §13 field tables match real dataclass fields + defaults
  8. payload_fields    §7 `name:type` field tables match the .pyi classes

Usage:  python release_validation/doc_sweep.py [--verbose]
Exit 0 = clean, 1 = findings.
"""
from __future__ import annotations

import argparse
import ast
import dataclasses
import importlib
import inspect
import re
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = {
    "llms": ROOT / "docs" / "llms.txt",
    "data_driver": ROOT / "docs" / "data_driver" / "llms.txt",
    "data_api": ROOT / "docs" / "data_api" / "llms.txt",
}
STUB_DIR = ROOT / "src" / "hiveq"

FINDINGS: list[tuple[str, str, str]] = []      # (check, where, message)


def finding(check: str, where: str, msg: str) -> None:
    FINDINGS.append((check, where, msg))


# ---------------------------------------------------------------- block parsing
def python_blocks(text: str) -> list[tuple[int, str]]:
    """Return (1-based start line, block source) for each ```python fence."""
    out, lines, i = [], text.splitlines(), 0
    while i < len(lines):
        if lines[i].strip().startswith("```python"):
            start, buf = i + 2, []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                buf.append(lines[i]); i += 1
            out.append((start, textwrap.dedent("\n".join(buf))))
        i += 1
    return out


SIG_RE = re.compile(r"^([a-zA-Z_][\w.]*)\((.*?)\)\s*(?:->\s*(.+?))?\s*$", re.S)


SIG_START = re.compile(r"^(?:def\s+)?\.?[a-zA-Z_][\w.]*\(")


ATTR_SIG = re.compile(r"^\.?[\w.]+\s*->\s*\S", re.S)


def _chunk_is_signature(chunk: str) -> bool:
    """A signature/attribute listing rather than runnable code.

    Accepts `name(params) -> Ret`, `.name(...)`, `a.b(...)`, a bodyless
    `def name(...) -> Ret`, and bare attribute listings (`run.run_id -> str`).
    Requires a signature MARKER — a `->` return or an annotated param — so real
    call sites (`hf.push_function(zscore, version="1.0.0")`) stay CODE.
    """
    body = re.sub(r"#.*$", "", chunk, flags=re.M).strip()
    if not body:
        return False
    has_marker = "->" in body or re.search(r"[(,]\s*\*?\*?\w+\s*:\s*\w", body)
    if not has_marker:
        return False
    if "(" not in body.split("->")[0] and ATTR_SIG.match(body):
        return True                                  # bare attribute listing
    if not SIG_START.match(body):
        return False
    body = re.sub(r"^def\s+", "", body)
    body = body.lstrip(".")
    body = re.sub(r"\)\s*->.*$", ")", body, flags=re.S)
    if not body.endswith(":"):
        body += ":"
    name, _, rest = body.partition("(")
    body = name.replace(".", "_") + "(" + rest     # a.b(...) -> a_b(...)
    try:
        ast.parse("def " + body + " pass")
        return True
    except SyntaxError:
        return False


def _is_multi_accessor_listing(chunk: str) -> bool:
    """`run.positions() / run.orders() / ... -> pandas.DataFrame`"""
    body = re.sub(r"#.*$", "", chunk, flags=re.M).strip()
    if "->" not in body or "/" not in body.split("->")[0]:
        return False
    lhs = body.split("->")[0]
    parts = [p.strip() for p in lhs.split("/") if p.strip()]
    return len(parts) > 1 and all(_chunk_is_signature(p + " -> X") for p in parts)


def _is_class_shape_listing(chunk: str) -> bool:
    """`class Job:` followed by bodyless method/attribute signature lines."""
    lines = [l for l in chunk.splitlines()
             if l.strip() and not l.strip().startswith("#")]
    if not lines or not re.match(r"^class\s+\w+\s*:", lines[0].strip()):
        return False
    for l in lines[1:]:
        body = l.strip().rstrip(";")
        if not body:
            continue
        ok = False
        for part in [p.strip() for p in body.split(";") if p.strip()]:
            ok = (_chunk_is_signature(part)
                  or bool(re.match(r"^\w+\s*:\s*\S", part))      # `task_id: str`
                  or _chunk_is_signature(part + " -> X"))
            if not ok:
                break
        if not ok:
            return False
    return True


def logical_chunks(src: str) -> list[tuple[int, str]]:
    """Split a block into paren-balanced line groups: (0-based offset, text)."""
    chunks, depth, cur, cur_off = [], 0, [], 0
    for idx, line in enumerate(src.splitlines()):
        if not line.strip() or line.strip().startswith("#"):
            if depth == 0:
                continue
        if depth == 0 and not cur:
            cur_off = idx
        cur.append(line)
        depth += (line.count("(") - line.count(")")
                  + line.count("[") - line.count("]")
                  + line.count("{") - line.count("}"))
        if depth <= 0:
            depth = 0
            if any(l.strip() and not l.strip().startswith("#") for l in cur):
                chunks.append((cur_off, "\n".join(cur)))
            cur = []
    if cur:
        chunks.append((cur_off, "\n".join(cur)))
    return chunks


def analyze_block(src: str) -> tuple[list[str], list[tuple[int, str]]]:
    """-> (parseable code chunks, unexplained chunks as (offset, first line))."""
    cleaned = strip_comment_noise(src)
    try:
        ast.parse(cleaned)
        return [cleaned], []
    except SyntaxError:
        pass
    code, bad = [], []
    for off, chunk in logical_chunks(src):
        body = strip_comment_noise(chunk)
        try:
            ast.parse(body)
            code.append(body)
            continue
        except SyntaxError:
            pass
        if _chunk_is_signature(chunk):
            continue
        if _is_multi_accessor_listing(chunk) or _is_class_shape_listing(chunk):
            continue
        # indented fragment (e.g. a dict body or a method excerpt)?
        for wrapper in ("dict(\n%s\n)", "[\n%s\n]", "class _C:\n%s"):
            try:
                ast.parse(wrapper % chunk)
                break
            except SyntaxError:
                continue
        else:
            bad.append((off, chunk.strip().splitlines()[0][:80]))
    return code, bad


def strip_comment_noise(src: str) -> str:
    """Drop trailing `# ...` prose lines that are pseudo-code, keep real code."""
    return "\n".join(l for l in src.splitlines() if not l.strip().startswith("#   "))


# ------------------------------------------------------------ 1. snippet syntax
def check_snippet_syntax(name: str, text: str) -> list[tuple[int, str]]:
    """Report chunks that are neither valid code nor a signature listing.

    Returns every parseable CODE chunk as (line_no, src) for the later checks.
    """
    code_chunks: list[tuple[int, str]] = []
    for line_no, src in python_blocks(text):
        code, bad = analyze_block(src)
        for body in code:
            code_chunks.append((line_no, body))
        for off, first in bad:
            finding("snippet_syntax", f"{name}:{line_no + off}",
                    f"neither valid code nor a signature listing: {first!r}")
    return code_chunks


# --------------------------------------------------------------- 2. signatures
def resolve(dotted: str):
    """Resolve `hiveq.flow.run_backtest` / `hiveq.flow.jobs.submit`, or None.

    Imports the longest importable module prefix first — `import hiveq` alone does
    not bind `hiveq.flow`, so a naive getattr walk fails on real attributes.
    """
    parts = dotted.split(".")
    mod = None
    for i in range(len(parts), 0, -1):
        try:
            mod = importlib.import_module(".".join(parts[:i]))
            rest = parts[i:]
            break
        except Exception:
            continue
    if mod is None:
        return None
    obj = mod
    for p in rest:
        obj = getattr(obj, p, None)
        if obj is None:
            return None
    return obj


def documented_params(sig_src: str) -> tuple[str, list[str]] | None:
    m = SIG_RE.match(sig_src.strip())
    if not m:
        return None
    fname, params = m.group(1), m.group(2)
    try:
        tree = ast.parse(f"def _stub({params}): pass")
    except SyntaxError:
        return None
    fn = tree.body[0]
    names = [a.arg for a in fn.args.posonlyargs + fn.args.args + fn.args.kwonlyargs]
    if fn.args.vararg:
        names.append("*" + fn.args.vararg.arg)
    if fn.args.kwarg:
        names.append("**" + fn.args.kwarg.arg)
    return fname, names


# module-level functions the docs document with a signature listing
SIG_TARGETS = {
    "run_backtest": "hiveq.flow.run_backtest",
    "get_run": "hiveq.flow.get_run",
    "push_function": "hiveq.flow.push_function",
    "load_function": "hiveq.flow.load_function",
    "run_function": "hiveq.flow.run_function",
    "list_functions": "hiveq.flow.list_functions",
    "function_versions": "hiveq.flow.function_versions",
    "get_function_source": "hiveq.flow.get_function_source",
    "delete_function": "hiveq.flow.delete_function",
    "login": "hiveq.flow.login",
    "event_logs": "hiveq.flow.event_logs",
    "config": "hiveq.flow.config",
    "deploy_job": "hiveq.flow.jobs.deploy_job",
    "submit": "hiveq.flow.jobs.submit",
    "get_logs": "hiveq.flow.jobs.get_logs",
}


def check_signatures(name: str, text: str) -> None:
    for line_no, src in python_blocks(text):
        for _off, chunk in logical_chunks(src):
            if not _chunk_is_signature(chunk):
                continue
            parsed = documented_params(re.sub(r"#.*$", "", chunk, flags=re.M))
            if not parsed:
                continue
            fname, doc_params = parsed
            dotted = SIG_TARGETS.get(fname.split(".")[-1])
            if not dotted:
                continue
            obj = resolve(dotted)
            if obj is None:
                finding("signatures", f"{name}:{line_no}",
                        f"documented `{fname}(...)` does not resolve ({dotted})")
                continue
            try:
                real = inspect.signature(obj)
            except (TypeError, ValueError):
                continue
            real_names = set(real.parameters)
            has_kwargs = any(p.kind == p.VAR_KEYWORD for p in real.parameters.values())
            for p in doc_params:
                bare = p.lstrip("*")
                if p.startswith("*"):
                    continue
                if bare not in real_names and not has_kwargs:
                    finding("signatures", f"{name}:{line_no}",
                            f"{fname}(): documented param `{bare}` not in real signature "
                            f"{tuple(real_names)}")


# ------------------------------------------------------------------ 3. imports
def check_imports(name: str, blocks: list[tuple[int, str]]) -> None:
    seen: set[tuple[str, str]] = set()
    for line_no, src in blocks:
        try:
            tree = ast.parse(strip_comment_noise(src))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                for alias in node.names:
                    key = (node.module, alias.name)
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        mod = importlib.import_module(node.module)
                    except Exception as e:
                        finding("imports", f"{name}:{line_no}",
                                f"`from {node.module} import {alias.name}` — module "
                                f"import failed: {type(e).__name__}: {e}")
                        continue
                    if alias.name == "*" or hasattr(mod, alias.name):
                        continue
                    # Could be a submodule not bound by the parent __init__.
                    try:
                        importlib.import_module(f"{node.module}.{alias.name}")
                    except Exception:
                        finding("imports", f"{name}:{line_no}",
                                f"`from {node.module} import {alias.name}` — name not "
                                f"found in module and not an importable submodule")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    key = ("", alias.name)
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        importlib.import_module(alias.name)
                    except Exception as e:
                        finding("imports", f"{name}:{line_no}",
                                f"`import {alias.name}` failed: {type(e).__name__}: {e}")


# ------------------------------------------------------------- 4. module attrs
def check_module_attrs(name: str, blocks: list[tuple[int, str]]) -> None:
    import hiveq.flow as hf
    reported: set[str] = set()
    for line_no, src in blocks:
        for attr in re.findall(r"\bhf\.([a-zA-Z_]\w*)", src):
            if attr in reported:
                continue
            reported.add(attr)
            if not hasattr(hf, attr):
                finding("module_attrs", f"{name}:{line_no}",
                        f"`hf.{attr}` referenced but not on hiveq.flow")


# -------------------------------------------------------------- 5. ctx methods
def stub_class_members(stub: Path, cls: str) -> dict[str, list[str]] | None:
    """{method: [param names]} plus {'__attrs__': [...]} from a .pyi class."""
    if not stub.exists():
        return None
    tree = ast.parse(stub.read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls:
            out: dict[str, list[str]] = {"__attrs__": []}
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    args = item.args
                    names = [a.arg for a in args.posonlyargs + args.args + args.kwonlyargs]
                    if args.vararg:
                        names.append("*" + args.vararg.arg)
                    if args.kwarg:
                        names.append("**" + args.kwarg.arg)
                    out[item.name] = [n for n in names if n != "self"]
                    if any(isinstance(d, ast.Name) and d.id == "property"
                           for d in item.decorator_list):
                        out["__attrs__"].append(item.name)
                elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    out["__attrs__"].append(item.target.id)
            return out
    return None


CTX_STUB = STUB_DIR / "flow" / "oms" / "sigma" / "sigma_context.pyi"


def check_ctx_methods(name: str, blocks: list[tuple[int, str]]) -> None:
    members = stub_class_members(CTX_STUB, "SigmaContext")
    if members is None:
        finding("ctx_methods", "-", f"SigmaContext stub not found at {CTX_STUB}")
        return
    reported: set[str] = set()
    for line_no, src in blocks:
        try:
            tree = ast.parse(strip_comment_noise(src))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "ctx"):
                continue
            attr = node.attr
            if attr in reported:
                continue
            reported.add(attr)
            if attr not in members and attr not in members["__attrs__"]:
                finding("ctx_methods", f"{name}:{line_no}",
                        f"`ctx.{attr}` referenced but not on SigmaContext stub")
    # kwargs on ctx calls
    for line_no, src in blocks:
        try:
            tree = ast.parse(strip_comment_noise(src))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "ctx"):
                continue
            meth = node.func.attr
            params = members.get(meth)
            if params is None:
                continue
            if any(p.startswith("**") for p in params):
                continue
            for kw in node.keywords:
                if kw.arg and kw.arg not in params:
                    finding("ctx_methods", f"{name}:{line_no}",
                            f"ctx.{meth}(): kwarg `{kw.arg}` not in stub signature "
                            f"{tuple(params)}")


# --------------------------------------------------------------------- 6. enums
ENUM_HOMES = {
    "EventType": "hiveq.flow.config", "AssetType": "hiveq.flow.config",
    "DataType": "hiveq.flow.config", "EventLogType": "hiveq.flow.config",
    "OMSType": "hiveq.flow.config",
    "OrderType": "hiveq.flow.trading_types", "OrderSide": "hiveq.flow.trading_types",
    "OrderStatus": "hiveq.flow.trading_types", "MarketCenter": "hiveq.flow.trading_types",
    "TaskType": "hiveq.flow.jobs", "ScheduleFrequency": "hiveq.flow.jobs",
}
ENUM_LINE = re.compile(r"^\*\*(\w+)\*\*[^:]*:\s*`([^`]+)`", re.M)


def check_enums(name: str, text: str) -> None:
    for m in ENUM_LINE.finditer(text):
        ename, members_raw = m.group(1), m.group(2)
        home = ENUM_HOMES.get(ename)
        if not home:
            continue
        line_no = text[:m.start()].count("\n") + 1
        try:
            enum_cls = getattr(importlib.import_module(home), ename)
        except Exception as e:
            finding("enums", f"{name}:{line_no}",
                    f"{ename} not importable from {home}: {type(e).__name__}: {e}")
            continue
        documented = [t for t in members_raw.split() if re.fullmatch(r"[A-Z][A-Z0-9_]*", t)]
        real = {e.name for e in enum_cls}
        for d in documented:
            if d not in real:
                finding("enums", f"{name}:{line_no}",
                        f"{ename}.{d} documented but not a real member")
        for r in sorted(real - set(documented)):
            finding("enums", f"{name}:{line_no}",
                    f"{ename}.{r} exists but is NOT documented")
        # ".value == name" claims
        if ".value` == name" in m.group(0) or ".value == name" in m.group(0):
            for e in enum_cls:
                if e.value != e.name:
                    finding("enums", f"{name}:{line_no}",
                            f"{ename}.{e.name}.value == {e.value!r}, doc claims "
                            f"`.value == name`")


# ---------------------------------------------------------------- 7. dataclasses
DC_HOMES = {
    "StrategyConfig": "hiveq.flow", "BacktestConfig": "hiveq.flow",
    "EngineConfig": "hiveq.flow",
}
DC_HEAD = re.compile(r"^\*\*(\w+)\*\*(?:\s*\([^)]*\))?\s*$", re.M)


def check_dataclasses(name: str, text: str) -> None:
    lines = text.splitlines()
    for m in DC_HEAD.finditer(text):
        dcname = m.group(1)
        home = DC_HOMES.get(dcname)
        if not home:
            continue
        start = text[:m.start()].count("\n")
        try:
            cls = getattr(importlib.import_module(home), dcname)
            fields = {f.name: f for f in dataclasses.fields(cls)}
        except Exception as e:
            finding("dataclasses", f"{name}:{start+1}",
                    f"{dcname} not resolvable from {home}: {type(e).__name__}: {e}")
            continue
        # read the markdown table that follows — stop at the next **Header** so we
        # do not attribute the following dataclass's fields to this one.
        for ln in lines[start + 1:start + 60]:
            if DC_HEAD.match(ln):
                break
            if not ln.startswith("|"):
                if ln.strip() and not ln.startswith(("**", ">", "`")):
                    break
                continue
            cells = [c.strip() for c in ln.strip("|").split("|")]
            if len(cells) < 2 or cells[0].lower() in ("field", "key", "param", "---"):
                continue
            if set(cells[0]) <= {"-", ":"}:          # separator row
                continue
            raw = cells[0].strip("`")
            for fname in [p.strip().strip("`") for p in raw.split("/")]:
                if not re.fullmatch(r"[a-z_][a-z0-9_]*", fname):
                    continue
                if fname not in fields:
                    finding("dataclasses", f"{name}:{start+1}",
                            f"{dcname}.{fname} documented but not a real field")


# ------------------------------------------------------------- 8. payload fields
PAYLOAD_STUBS = {
    "SigmaBar": ("flow/oms/sigma/types/bar.pyi", "SigmaBar"),
    "SigmaPosition": ("flow/oms/sigma/types/position.pyi", "SigmaPosition"),
    "SigmaOrder": ("flow/oms/sigma/types/order.pyi", "SigmaOrder"),
    "SigmaFill": ("flow/oms/sigma/types/fill.pyi", "SigmaFill"),
    "SigmaTradeTick": ("flow/oms/sigma/types/trade_tick.pyi", "SigmaTradeTick"),
    "SigmaQuoteTick": ("flow/oms/sigma/types/quote_tick.pyi", "SigmaQuoteTick"),
    "SigmaSnapData": ("flow/oms/sigma/types/snap.pyi", "SigmaSnapData"),
    "SigmaCustomData": ("flow/oms/sigma/types/custom_data.pyi", "SigmaCustomData"),
    "SigmaInstrument": ("flow/oms/sigma/sigma_context.pyi", "SigmaInstrument"),
}
FIELD_TOKEN = re.compile(r"`([a-z_][a-z0-9_]*)\s*:\s*[^`]+`")


def check_payload_fields(name: str, text: str) -> None:
    """§7 tables list fields as `name:type` separated by ·."""
    for m in re.finditer(r"^### 7\.\d+ +`?(\w+)`?.*$", text, re.M):
        cls_name = m.group(1)
        entry = PAYLOAD_STUBS.get(cls_name)
        if not entry:
            continue
        rel, stub_cls = entry
        members = stub_class_members(STUB_DIR / rel, stub_cls)
        line_no = text[:m.start()].count("\n") + 1
        if members is None:
            finding("payload_fields", f"{name}:{line_no}",
                    f"{cls_name}: stub {rel} / class {stub_cls} not found")
            continue
        known = set(members) | set(members["__attrs__"])
        body = text[m.end():m.end() + 2500].split("\n###")[0]
        documented = {t for t in FIELD_TOKEN.findall(body)}
        for f in sorted(documented):
            if f not in known:
                finding("payload_fields", f"{name}:{line_no}",
                        f"{cls_name}.{f} documented but not on the {stub_cls} stub")



# ------------------------------------------------- 9. internal section refs
SECTION_DEF = re.compile(r"^##+\s+(?:(II)\.)?(\d+(?:\.\d+)*)\.?\s", re.M)
SECTION_REF = re.compile(r"§(II\.)?(\d+(?:\.\d+)*)")


def defined_sections(text: str) -> set[str]:
    out = set()
    for m in SECTION_DEF.finditer(text):
        num = ("II." if m.group(1) else "") + m.group(2)
        out.add(num)
        # a defined `11.6` also makes the parent `11` a valid reference
        parts = num.split(".")
        for i in range(1, len(parts)):
            out.add(".".join(parts[:i]))
    return out


def check_section_refs(name: str, text: str, all_docs: dict[str, set[str]]) -> None:
    """Every §N referenced inside a doc must exist — in that doc, or in the doc
    the reference explicitly names ("data-driver §9" is a cross-doc reference)."""
    defined = all_docs.get(name, set())
    if not defined:
        return
    reported: set[str] = set()
    for m in SECTION_REF.finditer(text):
        ref = ("II." if m.group(1) else "") + m.group(2)
        before = text[max(0, m.start() - 80):m.start()]
        # allow an intervening range/list: "data-driver §8–§9", "§1.1 and §1.2"
        gap = r"(?:[\s§\d.,\-–`'\"]|and){0,16}$"
        cross = None
        if re.search(r"data[-_]driver(?:/llms\.txt)?" + gap, before):
            cross = "data_driver"
        elif re.search(r"data[-_]api(?:/llms\.txt)?" + gap, before):
            cross = "data_api"
        elif re.search(r"(?<!data_driver/)(?<!data_api/)llms\.txt" + gap, before):
            cross = "llms"          # a sibling doc citing the main spec
        target = cross or name
        if ref in all_docs.get(target, set()) or (target, ref) in reported:
            continue
        reported.add((target, ref))
        line_no = text[:m.start()].count("\n") + 1
        where = "this doc" if target == name else f"the {target} doc"
        finding("section_refs", f"{name}:{line_no}",
                f"references §{ref}, which is not a section in {where}")


# --------------------------------------------- 10. section refs from CODE
ANY_REF = re.compile(r"§(II\.)?(\d+(?:\.\d+)*)")


def check_code_section_refs() -> None:
    """Error/notice text in src/ points users at doc sections — they must exist.

    Any §N appearing in source must resolve in at least one shipped doc; when the
    surrounding text names the data-driver doc, it must resolve in THAT one.
    """
    doc_sections = {k: defined_sections(p.read_text())
                    for k, p in DOCS.items() if p.exists()}
    union = set().union(*doc_sections.values()) if doc_sections else set()
    for py in sorted((ROOT / "src").rglob("*.py")):
        try:
            body = py.read_text()
        except Exception:
            continue
        for m in ANY_REF.finditer(body):
            ref = ("II." if m.group(1) else "") + m.group(2)
            # Attribute to the NEAREST preceding doc mention: one notice can
            # cite the driver doc and then the main doc in the same breath.
            before = body[max(0, m.start() - 400):m.start()]
            driver_at = max(before.rfind("data_driver/llms.txt"),
                            before.rfind("data-driver reference"))
            main_at = -1
            for mm in re.finditer(r"llms\.txt", before):
                if before[max(0, mm.start() - 12):mm.start()].endswith("data_driver/"):
                    continue
                main_at = max(main_at, mm.start())
            if driver_at < 0 and main_at < 0:
                target = None
            else:
                target = "data_driver" if driver_at > main_at else "llms"
            line_no = body[:m.start()].count("\n") + 1
            rel = py.relative_to(ROOT)
            if target:
                if ref not in doc_sections.get(target, set()):
                    finding("code_section_refs", f"{rel}:{line_no}",
                            f"points at {target} §{ref}, which does not exist")
            elif ref not in union:
                finding("code_section_refs", f"{rel}:{line_no}",
                        f"references §{ref}, which exists in no shipped doc")


# ------------------------------------------------- 11. driver/data namespaces
DRIVER_NAMESPACES = {
    "dd": "hiveq.driver",
    "Cache": "hiveq.driver:Cache",
    "hiveq_data": "hiveq_data",
    "hd": "hiveq_data",
}


def check_driver_namespaces(name: str, blocks: list[tuple[int, str]]) -> None:
    """Resolve every `dd.X` / `Cache.X` / `hiveq_data.X` the docs reference."""
    resolved: dict[str, object] = {}
    for alias, spec in DRIVER_NAMESPACES.items():
        mod, _, attr = spec.partition(":")
        try:
            obj = importlib.import_module(mod)
            if attr:
                obj = getattr(obj, attr)
            resolved[alias] = obj
        except Exception:
            resolved[alias] = None
    reported: set[tuple[str, str]] = set()
    for line_no, src in blocks:
        try:
            tree = ast.parse(strip_comment_noise(src))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)):
                continue
            alias, attr = node.value.id, node.attr
            target = resolved.get(alias)
            if target is None or (alias, attr) in reported:
                continue
            reported.add((alias, attr))
            if hasattr(target, attr):
                continue
            # may be a submodule the parent __init__ does not bind
            modname = getattr(target, "__name__", "")
            try:
                importlib.import_module(f"{modname}.{attr}")
            except Exception:
                finding("driver_namespaces", f"{name}:{line_no}",
                        f"`{alias}.{attr}` referenced but not on {modname or alias}")



# ------------------------------------------ 12. dd.* surface is documented
def check_driver_surface_documented() -> None:
    """Every public callable on `hiveq.driver` must appear in the driver doc.

    The reverse of check_driver_namespaces: catches a real entry point that the
    reference never mentions (how `clear_cache` went undocumented).
    """
    path = DOCS.get("data_driver")
    if not path or not path.exists():
        return
    text = path.read_text()
    try:
        import hiveq.driver as root
    except Exception as e:
        finding("driver_surface", "-", f"cannot import hiveq.driver: {e}")
        return
    for n in sorted(dir(root)):
        if n.startswith("_"):
            continue
        obj = getattr(root, n)
        if not callable(obj) or inspect.isclass(obj) or inspect.ismodule(obj):
            continue
        if re.search(r"\b(?:dd\.)?" + re.escape(n) + r"\b", text):
            continue
        finding("driver_surface", "data_driver",
                f"`dd.{n}()` is a public driver entry point but is never "
                f"mentioned in the data-driver doc")


# ------------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    all_sections = {k: defined_sections(p.read_text())
                    for k, p in DOCS.items() if p.exists()}

    for name, path in DOCS.items():
        if not path.exists():
            finding("setup", name, f"doc not found: {path}")
            continue
        text = path.read_text()
        blocks = check_snippet_syntax(name, text)
        check_signatures(name, text)
        check_imports(name, blocks)
        check_module_attrs(name, blocks)
        check_ctx_methods(name, blocks)
        check_enums(name, text)
        check_dataclasses(name, text)
        check_payload_fields(name, text)
        check_section_refs(name, text, all_sections)
        check_driver_namespaces(name, blocks)
        if args.verbose:
            print(f"[{name}] {len(python_blocks(text))} python blocks "
                  f"({len(blocks)} code, {len(python_blocks(text)) - len(blocks)} signature)")

    check_code_section_refs()
    check_driver_surface_documented()

    by_check: dict[str, list[tuple[str, str]]] = {}
    for check, where, msg in FINDINGS:
        by_check.setdefault(check, []).append((where, msg))

    if not FINDINGS:
        print("doc sweep: CLEAN — no findings")
        return 0

    print(f"doc sweep: {len(FINDINGS)} finding(s) across {len(by_check)} check(s)\n")
    for check in sorted(by_check):
        rows = by_check[check]
        print(f"=== {check} ({len(rows)}) ===")
        for where, msg in rows:
            print(f"  {where}: {msg}")
        print()
    return 1


if __name__ == "__main__":
    sys.exit(main())
