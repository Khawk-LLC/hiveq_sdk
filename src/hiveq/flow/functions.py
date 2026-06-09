"""Push reusable Python functions to the HiveQ function registry (REST).

Capture a Python callable, ship it to the platform's function registry, and
reference it by name/version from your strategies. Like the rest of the SDK this
is REST-only and self-contained: the function is cloudpickled client-side and
sent over HTTP — no platform-internal package is imported.

    import hiveq.flow as hf

    def zscore(series, window=20):
        '''Rolling z-score.'''
        ...

    hf.push_function(zscore, version="1.0.0", requirements=["pandas>=2.0"])
    hf.list_functions()
    hf.function_versions("zscore")
    hf.get_function_source("zscore")
    hf.delete_function("zscore", version="1.0.0")

Functions go to YOUR namespace by default (resolved from the API key); pass
``namespace="default"`` to publish to the shared/public namespace.

Configuration (env / ``~/.hiveq/.env``):
- ``HIVEQ_API_KEY``               — required.
- ``HIVEQ_FUNCTION_REGISTRY_URL`` — registry base URL override. By default the
  registry follows the platform host (``HIVEQ_AUTH_URL``) at ``/api/functions``.
"""
import base64
import inspect
import os
import time
from typing import Any, Callable, Dict, List, Optional, Union

import cloudpickle
import requests

from hiveq.flow.logger import logger

logger = logger(show_logo=False)

# (connect timeout, read timeout). Payloads are small (a pickled function).
_TIMEOUT = (10, 120)
_MAX_ATTEMPTS = 3
_RETRY_WAIT = 2.0

# The caller's own namespace, resolved once from the registry and cached.
_own_namespace: Optional[str] = None


# --- config -----------------------------------------------------------------
def _registry_base_url() -> str:
    """Registry base URL.

    ``HIVEQ_FUNCTION_REGISTRY_URL`` wins; otherwise follow the platform host
    (from ``HIVEQ_AUTH_URL``) at ``/api/functions`` (the gateway rewrites that to
    the service's ``/api/v1``); else fall back to the local service.
    """
    explicit = os.environ.get("HIVEQ_FUNCTION_REGISTRY_URL")
    if explicit:
        return explicit.rstrip("/")
    from hiveq.flow.config import platform_origin

    origin = platform_origin()
    if origin:
        return f"{origin}/api/functions"
    return "http://localhost:5001/api/v1"


def _endpoint(path: str) -> str:
    return f"{_registry_base_url()}/{path.lstrip('/')}"


def _auth_headers() -> dict:
    key = os.environ.get("HIVEQ_API_KEY")
    if not key:
        from hiveq.flow.config import HIVEQ_CREDS_FILE

        raise RuntimeError(
            f"HIVEQ_API_KEY not found. Set it in your environment or in "
            f"{HIVEQ_CREDS_FILE}. [Error: API_KEY_MISSING]"
        )
    headers = {"X-API-Key": key}
    org = os.environ.get("HIVEQ_ORG_ID")
    if org:
        headers["X-Org-ID"] = org
    user = os.environ.get("HIVEQ_USER_NAME")
    if user:
        headers["X-User-Name"] = user
    return headers


def _raise_for_status(resp: requests.Response, what: str) -> None:
    if resp.status_code < 400:
        return
    detail = ""
    try:
        body = resp.json()
        detail = body.get("error", {}).get("message") or body.get("message") or ""
    except Exception:
        detail = (resp.text or "")[:300]
    raise RuntimeError(
        f"{what} failed [{resp.status_code}] at {resp.url}: {detail}".rstrip()
    )


def _request(method: str, url: str, what: str, **kwargs) -> requests.Response:
    """Issue a request, retrying only transient transport errors."""
    last_exc = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = requests.request(method, url, timeout=_TIMEOUT, **kwargs)
            _raise_for_status(resp, what)
            return resp
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            if attempt < _MAX_ATTEMPTS:
                logger.warning(
                    f"{what}: network problem ({type(exc).__name__}); "
                    f"retry {attempt}/{_MAX_ATTEMPTS - 1} in {_RETRY_WAIT * attempt:.0f}s"
                )
                time.sleep(_RETRY_WAIT * attempt)
    raise RuntimeError(
        f"{what}: the HiveQ function registry is unreachable after "
        f"{_MAX_ATTEMPTS} attempts ({type(last_exc).__name__}). Try again later."
    ) from last_exc


def _resolve_namespace(namespace: Optional[str]) -> str:
    """Explicit namespace, else the caller's own namespace (cached)."""
    if namespace:
        return namespace
    global _own_namespace
    if _own_namespace:
        return _own_namespace
    resp = _request(
        "GET", _endpoint("namespaces"), "Resolve namespace", headers=_auth_headers()
    )
    data = resp.json() or {}
    ns = data.get("own_namespace") or data.get("ownNamespace")
    if not ns:
        raise RuntimeError(
            "Could not resolve your function namespace from the registry; "
            "pass namespace=... explicitly."
        )
    _own_namespace = ns
    return ns


def _normalize_requirements(
    requirements: Optional[Union[Dict[str, Any], List[str]]]
) -> Dict[str, Any]:
    """Accept a list of package specs (convenience) or a full dict."""
    if requirements is None:
        return {"packages": []}
    if isinstance(requirements, dict):
        return requirements
    return {"packages": list(requirements)}


# --- public API -------------------------------------------------------------
def push_function(
    func: Callable,
    *,
    version: str,
    name: Optional[str] = None,
    requirements: Optional[Union[Dict[str, Any], List[str]]] = None,
    docstring: Optional[str] = None,
    namespace: Optional[str] = None,
    override: bool = False,
    include_source: bool = True,
) -> dict:
    """Push a Python callable to the function registry.

    Parameters
    ----------
    func : callable
        The function to register (captured with cloudpickle).
    version : str
        Semantic version, e.g. ``"1.0.0"`` (required).
    name : str, optional
        Registry name; defaults to ``func.__name__``.
    requirements : list[str] | dict, optional
        Package specs the function needs, e.g. ``["pandas>=2.0"]`` — or a full
        dict ``{"packages": [...], "python_version": "3.11"}``.
    docstring : str, optional
        Defaults to the function's own docstring.
    namespace : str, optional
        Target namespace; defaults to your own. Use ``"default"`` to publish to
        the shared/public namespace.
    override : bool
        Overwrite an existing ``name@version`` (default ``False``).
    include_source : bool
        Also store the function's source (best-effort via ``inspect``).

    Returns ``{"function_id", "namespace", "name", "version", ...}``.
    """
    if not callable(func):
        raise TypeError("push_function(func, ...): func must be callable")
    fn_name = name or getattr(func, "__name__", None)
    if not fn_name or fn_name == "<lambda>":
        raise ValueError("Could not determine a function name; pass name=...")
    doc = docstring if docstring is not None else (inspect.getdoc(func) or "")
    ns = _resolve_namespace(namespace)

    payload_b64 = base64.b64encode(cloudpickle.dumps(func)).decode("ascii")
    body: Dict[str, Any] = {
        "name": fn_name,
        "version": version,
        "requirements": _normalize_requirements(requirements),
        "docstring": doc,
        "payload_b64": payload_b64,
        "override": bool(override),
    }
    if include_source:
        try:
            body["source_code"] = inspect.getsource(func)
        except (OSError, TypeError):
            pass  # lambdas / REPL-defined funcs — the pickled payload still works

    resp = _request(
        "POST",
        _endpoint(f"namespaces/{ns}/functions"),
        f"Push function '{fn_name}'",
        headers={**_auth_headers(), "Content-Type": "application/json"},
        json=body,
    )
    result = resp.json() or {}
    logger.info(f"Pushed function {ns}/{fn_name} v{version}")
    return result


def list_functions(
    namespace: Optional[str] = None, name: Optional[str] = None
) -> List[dict]:
    """List functions in a namespace (yours by default). ``name`` is a regex filter."""
    ns = _resolve_namespace(namespace)
    params = {"name": name} if name else None
    resp = _request(
        "GET",
        _endpoint(f"namespaces/{ns}/functions"),
        "List functions",
        headers=_auth_headers(),
        params=params,
    )
    return (resp.json() or {}).get("functions", [])


def load_function(
    name: str, version: Optional[str] = None, namespace: Optional[str] = None
) -> Callable:
    """Fetch a registered function as a callable (cloudpickle-loaded).

    Useful for running a registered function on the platform without the
    sandbox needing the registry client:

        fn = hf.load_function("zscore")
        hf.run_function(fn, prices, requirements=["numpy"])
    """
    ns = _resolve_namespace(namespace)
    path = (
        f"namespaces/{ns}/functions/{name}"
        if not version
        else f"namespaces/{ns}/functions/{name}/{version}"
    )
    resp = _request(
        "GET", _endpoint(path), f"Load function '{name}'", headers=_auth_headers()
    )
    data = resp.json() or {}
    b64 = data.get("payload_b64")
    if not b64:
        raise RuntimeError(f"Registry returned no payload for function '{name}'.")
    return cloudpickle.loads(base64.b64decode(b64))


def function_versions(name: str, namespace: Optional[str] = None) -> dict:
    """All versions of a function: ``{"name", "versions": [...], "latest"}``."""
    ns = _resolve_namespace(namespace)
    resp = _request(
        "GET",
        _endpoint(f"namespaces/{ns}/functions/{name}/versions"),
        f"List versions of '{name}'",
        headers=_auth_headers(),
    )
    return resp.json() or {}


def get_function_source(
    name: str, version: Optional[str] = None, namespace: Optional[str] = None
) -> dict:
    """Stored source for a function (latest, or a specific ``version``)."""
    ns = _resolve_namespace(namespace)
    path = (
        f"namespaces/{ns}/functions/{name}/source"
        if not version
        else f"namespaces/{ns}/functions/{name}/{version}/source"
    )
    resp = _request(
        "GET", _endpoint(path), f"Get source of '{name}'", headers=_auth_headers()
    )
    return resp.json() or {}


_TERMINAL_STATUSES = {
    "completed", "failed", "done", "error", "terminated", "stopped", "cancelled",
}


def run_function(
    func: Callable,
    *args,
    task_name: Optional[str] = None,
    requirements: Optional[List[str]] = None,
    job_type: Optional[str] = None,
    wait: bool = True,
    timeout: Optional[float] = None,
    poll_interval: float = 2.0,
    **kwargs,
) -> Any:
    """Run a Python function on the HiveQ platform as a QUANT_SCRIPTS task.

    The function is cloudpickled and executed on the platform as
    ``func(*args, **kwargs)``; this submits only as the ``QUANT_SCRIPTS`` task
    type. By default it blocks until the run finishes and returns the function's
    **return value**; with ``wait=False`` it returns the submission reply
    (``{"task_id", ...}``) immediately.

    Parameters
    ----------
    func : callable
        The function to run remotely. ``*args`` / ``**kwargs`` are passed to it.
    task_name : str, optional
        Name for the task; defaults to ``fn-<name>-<short id>``.
    requirements : list[str], optional
        pip specs to install in the sandbox, e.g. ``["pandas>=2.0"]``.
    wait : bool
        Block for the result (default) or return the submission reply.
    timeout, poll_interval : float
        Polling controls when ``wait=True``.
    """
    if not callable(func):
        raise TypeError("run_function(func, ...): func must be callable")

    import uuid

    from hiveq.flow import _ensure_initialized
    from hiveq.flow._client import TaskType, _Client

    _ensure_initialized()  # resolves the API key (triggers sign-in if missing)

    name = task_name or f"fn-{getattr(func, '__name__', 'task')}-{uuid.uuid4().hex[:8]}"
    client = _Client()
    submitted = client.submit(
        task_type=TaskType.QUANT_SCRIPTS,
        task_name=name,
        task=func,
        entry_method=None,  # plain callable -> executor calls func(*args, **kwargs)
        args=tuple(args),
        kwargs=dict(kwargs),
        job_type=job_type,
        requirements=list(requirements) if requirements else None,
        allow_duplicate=True,
        duplicate_action="override",
    )
    task_id = submitted.get("task_id")
    if not wait or not task_id:
        return submitted

    logger.info(f"Running '{name}' on the platform (task {task_id})…")
    deadline = (time.monotonic() + timeout) if timeout else None
    while True:
        res = client.get_result(task_id) or {}
        status = (res.get("status") or "").lower()
        if status in _TERMINAL_STATUSES:
            if status in ("failed", "error", "terminated"):
                raise RuntimeError(
                    f"Function task {task_id} {status}: "
                    f"{res.get('error_message') or res.get('message') or ''}".rstrip()
                )
            return res.get("result")
        if deadline and time.monotonic() >= deadline:
            raise TimeoutError(
                f"Function task {task_id} not finished within {timeout}s "
                f"(status: {status or 'pending'})"
            )
        time.sleep(poll_interval)


def delete_function(
    name: str, version: Optional[str] = None, namespace: Optional[str] = None
) -> dict:
    """Delete a function (all versions, or a specific ``version``)."""
    ns = _resolve_namespace(namespace)
    path = (
        f"namespaces/{ns}/functions/{name}"
        if not version
        else f"namespaces/{ns}/functions/{name}/{version}"
    )
    resp = _request(
        "DELETE", _endpoint(path), f"Delete function '{name}'", headers=_auth_headers()
    )
    return resp.json() or {}
