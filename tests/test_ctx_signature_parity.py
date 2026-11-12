"""Every ctx.<method>(...) call in examples/ must fit the authoring stub's signature.

The .pyi stub is the authoring contract (type checkers, autocomplete, the spec).
When the runtime gains a parameter and the examples use it, the stub must gain it
too, or the SDK's own examples fail type checking. The client-side prefetch
context runs the same callbacks before submit, so it must accept the same calls.
"""
import ast
import inspect
from pathlib import Path

from hiveq.flow.context import PrefetchContext

_ROOT = Path(__file__).resolve().parents[1]
_STUB = _ROOT / "src/hiveq/flow/oms/sigma/sigma_context.pyi"


def _stub_signatures():
    tree = ast.parse(_STUB.read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "SigmaContext")
    sigs = {}
    for fn in cls.body:
        if isinstance(fn, ast.FunctionDef):
            a = fn.args
            pos = [p.arg for p in a.posonlyargs + a.args][1:]          # drop self
            n_required = len(pos) - len(a.defaults)
            sigs[fn.name] = (pos, n_required, {k.arg for k in a.kwonlyargs},
                             a.vararg is not None, a.kwarg is not None)
    return sigs


def _ctx_calls():
    for path in sorted((_ROOT / "examples").glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "ctx"):
                yield path.name, node.lineno, node


def test_example_ctx_calls_fit_the_stub():
    sigs = _stub_signatures()
    problems = []
    for fname, line, call in _ctx_calls():
        name = call.func.attr
        if name not in sigs:
            continue          # attribute access / non-method; covered by other tests
        pos, n_required, kwonly, varargs, varkw = sigs[name]
        if any(isinstance(a, ast.Starred) for a in call.args) or any(k.arg is None for k in call.keywords):
            continue
        n_pos = len(call.args)
        kws = {k.arg for k in call.keywords}
        if n_pos > len(pos) and not varargs:
            problems.append(f"{fname}:{line} ctx.{name}: {n_pos} positional args, stub takes {len(pos)}")
        unknown = kws - set(pos[n_pos:]) - kwonly
        if unknown and not varkw:
            problems.append(f"{fname}:{line} ctx.{name}: unknown keyword(s) {sorted(unknown)}")
        supplied = n_pos + len(kws & set(pos[n_pos:n_required]))
        if supplied < n_required:
            problems.append(f"{fname}:{line} ctx.{name}: missing required args")
    assert not problems, "\n".join(problems)


def test_prefetch_context_accepts_example_calls():
    """The client-side prefetch stand-in must accept every call shape the examples use."""
    problems = []
    for fname, line, call in _ctx_calls():
        impl = getattr(PrefetchContext, call.func.attr, None)
        if not callable(impl):
            continue
        if any(isinstance(a, ast.Starred) for a in call.args) or any(k.arg is None for k in call.keywords):
            continue
        try:
            inspect.signature(impl).bind(None, *([0] * len(call.args)),
                                         **{k.arg: 0 for k in call.keywords})
        except TypeError as e:
            problems.append(f"{fname}:{line} PrefetchContext.{call.func.attr}: {e}")
    assert not problems, "\n".join(problems)
