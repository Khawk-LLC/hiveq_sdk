"""HiveQ platform REST client — deploy a captured task and observe it.

Talks directly to the HiveQ platform task API over HTTP (pure ``requests``):
submit a captured task, then pull its status / result / logs. The SDK ships no
platform-internal client package; this is the entire client surface.

Authentication is a single ``X-API-Key`` header — the platform derives the user
and organization from the key. No identity headers are ever sent.

Endpoint base resolves from ``HIVEQ_BASE_URL`` (falling back to the canonical
host). The API key resolves from ``HIVEQ_API_KEY`` unless passed explicitly.
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin

import cloudpickle
import requests

from hiveq.flow._payload import _TaskWrapper

# The wrapper is serialized BY VALUE — its class definition is embedded in the
# payload so the platform executor can reconstruct it without importing the SDK.
# Registration is scoped to the (single-class) _payload module, so nothing else
# is forced by-value.
import hiveq.flow._payload as _payload_module  # noqa: E402

cloudpickle.register_pickle_by_value(_payload_module)

# A submitted callable can legitimately close over public SDK values re-exported
# from this module (for example ``ScheduleFrequency``).  Executors intentionally
# do not install the thin-client SDK, so such references must travel by value as
# well.  This registration is lazy in practice: cloudpickle only embeds this
# module when the submitted object graph actually references one of its values,
# leaving ordinary Flow strategy payloads unchanged.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


_DEFAULT_BASE_URL = "https://staging.hiveq.ai/api/orchestrator"
_TERMINAL_STATES = ("completed", "failed", "terminated")
_DUPLICATE_ACTIONS = ("override", "terminate", "duplicate")


class TaskType(str, Enum):
    """Platform task types (sent as the ``X-Task-Type`` header)."""

    QUANT_SCRIPTS = "QUANT_SCRIPTS"
    HIVEQ_FLOW_BT = "HIVEQ_FLOW_BT"
    HIVEQ_FLOW_LIVE_SIM = "HIVEQ_FLOW_LIVE_SIM"
    HIVEQ_FLOW_PROD = "HIVEQ_FLOW_PROD"
    HIVEQ_ALPHA_AI = "HIVEQ_ALPHA_AI"
    HIVEQ_DEV_OPS_SCRIPTS = "HIVEQ_DEV_OPS_SCRIPTS"


class ScheduleFrequency(str, Enum):
    """Recurring-execution frequency for a scheduled task."""

    ONCE = "ONCE"
    DAILY = "DAILY"
    WEEKDAYS = "WEEKDAYS"
    WEEKENDS = "WEEKENDS"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    INTERVAL = "INTERVAL"


# ---------------------------------------------------------------------------
# Schedule timezone resolution
#
# A schedule's ``start_time`` is a wall-clock time, so it only means something
# once a zone is attached to it.  HiveQ is a US-market platform and everything
# else the user sees (session windows, run output, in-strategy time) is Eastern,
# so a schedule is Eastern too unless the caller says otherwise -- a UTC default
# silently fired "16:05" four or five hours off the close.
#
# Resolution order for ``Schedule.timezone``:
#   1. what the caller passed,
#   2. the ``HIVEQ_SCHEDULE_TIMEZONE`` env var (may itself be ``"local"``),
#   3. ``America/New_York``.
#
# ``"local"`` resolves to the client machine's own zone (falling back to ET when
# detection fails).  Abbreviations are aliased onto their IANA zone on purpose:
# ``ZoneInfo("EST")`` is a *fixed* -05:00 with no DST, so a schedule written as
# "EST" would drift an hour every summer.  "EST" here means US Eastern, DST and
# all -- which is what anyone asking for an EST schedule means.
# ---------------------------------------------------------------------------

_DEFAULT_SCHEDULE_TIMEZONE = "America/New_York"
_LOCAL_TIMEZONE_ALIASES = ("local", "system", "machine", "auto")
_TIMEZONE_ALIASES = {
    "est": "America/New_York",
    "edt": "America/New_York",
    "et": "America/New_York",
    "eastern": "America/New_York",
    "us/eastern": "America/New_York",
    "cst": "America/Chicago",
    "cdt": "America/Chicago",
    "ct": "America/Chicago",
    "central": "America/Chicago",
    "us/central": "America/Chicago",
    "mst": "America/Denver",
    "mdt": "America/Denver",
    "mt": "America/Denver",
    "us/mountain": "America/Denver",
    "pst": "America/Los_Angeles",
    "pdt": "America/Los_Angeles",
    "pt": "America/Los_Angeles",
    "pacific": "America/Los_Angeles",
    "us/pacific": "America/Los_Angeles",
}


def _detect_local_timezone() -> str:
    """The client machine's IANA zone name, or ET when it can't be detected."""
    try:
        from hiveq.flow.utils.timezone_utils import get_local_timezone

        name = get_local_timezone()
    except Exception:
        name = None
    if not name:
        return _DEFAULT_SCHEDULE_TIMEZONE
    # A detected zone can still be an abbreviation (e.g. TZ=EST) or unusable
    # (e.g. 'localtime'); alias/validate it like any caller-supplied value.
    try:
        return resolve_schedule_timezone(name, _allow_local=False)
    except ValueError:
        return _DEFAULT_SCHEDULE_TIMEZONE


def _timezone_validators() -> List[Any]:
    """Zone-name lookups available in this interpreter (zoneinfo and/or pytz)."""
    validators: List[Any] = []
    try:
        from zoneinfo import ZoneInfo

        validators.append(ZoneInfo)
    except ImportError:  # pragma: no cover — Python < 3.9
        pass
    try:
        import pytz

        validators.append(pytz.timezone)
    except ImportError:
        pass  # pytz is optional client-side; the platform always has it
    return validators


def resolve_schedule_timezone(
    timezone: Optional[str] = None, _allow_local: bool = True
) -> str:
    """Resolve a schedule timezone to a validated IANA zone name.

    ``None`` (or ``""``) picks up ``HIVEQ_SCHEDULE_TIMEZONE`` if it is set, and
    otherwise ``"America/New_York"``. ``"local"`` resolves to this machine's own
    zone. Common abbreviations (``"EST"``, ``"ET"``, ``"PT"``, ...) are aliased
    onto the matching DST-aware IANA zone.

    Raises ``ValueError`` on a name no timezone database knows.
    """
    name = (timezone or "").strip()
    if not name:
        name = (os.environ.get("HIVEQ_SCHEDULE_TIMEZONE") or "").strip()
    if not name:
        return _DEFAULT_SCHEDULE_TIMEZONE

    key = name.lower()
    if key in _LOCAL_TIMEZONE_ALIASES:
        return _detect_local_timezone() if _allow_local else _DEFAULT_SCHEDULE_TIMEZONE
    name = _TIMEZONE_ALIASES.get(key, name)
    if name.upper() == "UTC":
        return "UTC"

    # Validate against every tz database we can reach. The platform scheduler
    # resolves the name with pytz and falls back to UTC when it doesn't know it
    # (and 400s an unknown zone on a ONCE schedule), so a name that only
    # zoneinfo knows would fire at the wrong hour with nothing to show for it —
    # better to fail here, at the call site that wrote it.
    checked = False
    for _load in _timezone_validators():
        try:
            _load(name)
            checked = True
        except Exception as e:
            raise ValueError(
                f"Unknown schedule timezone {timezone!r}: {e}. Pass an IANA "
                f"name (e.g. 'America/New_York'), an abbreviation like "
                f"'EST'/'ET', or 'local' for this machine's timezone."
            )
    # No tz database at all (neither zoneinfo nor pytz): pass the name through
    # unvalidated rather than refusing to schedule.
    return name


# ---------------------------------------------------------------------------
# The job clock
#
# A scheduled script almost always does its own time checks ("only publish
# after the close", "skip if it's not a trading day"), and those have to agree
# with the schedule that woke it up.
#
# On the platform they already do: the sandbox container is pinned to
# TZ=America/New_York, so a bare ``datetime.now()`` inside a deployed job is
# Eastern.  The two places that drift are (a) the *same function* run on the
# author's own laptop while they develop it, where ``datetime.now()`` is
# whatever the laptop is set to, and (b) a schedule deliberately written in
# another zone, which the container's fixed ET clock knows nothing about.
#
# ``job_now()`` closes both: it returns an aware timestamp in the job's own
# timezone -- the schedule's, when ``deploy_job`` propagated one, else the same
# default a Schedule gets -- and it reads identically locally and on the
# executor.  It is defined here, in a module cloudpickle serializes BY VALUE, so
# a submitted function can call it on an executor that has no SDK installed.
# ---------------------------------------------------------------------------

#: Set inside the job process (by ``_TaskWrapper``) to the schedule's timezone.
_JOB_TIMEZONE_ENV = "HIVEQ_SCHEDULE_TIMEZONE"


def job_timezone() -> str:
    """The timezone this job's wall-clock checks should use.

    The schedule's timezone when running as a deployed job, otherwise the
    same default a :class:`Schedule` gets (``HIVEQ_SCHEDULE_TIMEZONE`` if set,
    else ``"America/New_York"``).
    """
    return resolve_schedule_timezone(None)


def job_now(timezone: Optional[str] = None) -> "datetime.datetime":
    """``now`` as a timezone-aware datetime on the job clock (US Eastern by default).

    Use this instead of ``datetime.now()`` for any time check inside a script
    you deploy, so the check reads the same on your machine as it does on the
    executor. Import it at module level, like any other name the deployed
    function closes over — the executor has no SDK to import it from, so it has
    to travel inside the payload::

        from hiveq.flow.jobs import job_now

        def publish_after_the_close():
            if job_now().hour < 16:      # 16:00 Eastern, wherever this runs
                return {"skipped": "before the close"}

    ``timezone`` overrides the job clock for one call (any name
    ``Schedule(timezone=...)`` accepts).
    """
    import datetime as _datetime

    name = resolve_schedule_timezone(timezone)
    try:
        from zoneinfo import ZoneInfo

        return _datetime.datetime.now(ZoneInfo(name))
    except ImportError:  # pragma: no cover — Python < 3.9
        import pytz

        return _datetime.datetime.now(pytz.timezone(name))


def job_today(timezone: Optional[str] = None) -> "datetime.date":
    """Today's date on the job clock — the date the schedule is reasoning about."""
    return job_now(timezone).date()


@dataclass
class Schedule:
    """Recurring-schedule config for a task, mirroring the platform's job schedule.

    Attributes
    ----------
    frequency : ScheduleFrequency
        How often the task runs.
    start_time : str
        Time of day to run, ``"HH:MM"`` or ``"HH:MM:SS"``.
    timezone : str, optional
        Timezone ``start_time``/``end_time`` are written in. Defaults to US
        Eastern (``"America/New_York"``) — the zone everything else on the
        platform is expressed in — unless ``HIVEQ_SCHEDULE_TIMEZONE`` says
        otherwise. Accepts an IANA name, an abbreviation (``"EST"``, ``"ET"``,
        ``"PT"``, ... — aliased onto the DST-aware zone, so ``"EST"`` keeps
        firing at the same wall-clock time through the summer), or ``"local"``
        for this machine's own timezone. Resolved and validated on construction,
        so ``Schedule(...).timezone`` is always the concrete zone that was sent.
    days_of_week : list[int], optional
        For ``WEEKLY``/``INTERVAL`` — days to run, ``0``=Monday..``6``=Sunday.
    day_of_month : int, optional
        For ``MONTHLY`` — day of month (1-31).
    end_time : str, optional
        For ``INTERVAL`` — stop firing after this time of day (``"HH:MM"``).
    interval_minutes : int, optional
        For ``INTERVAL`` — run every N minutes within the window.
    end_date : str, optional
        ``"YYYY-MM-DD"`` after which the schedule stops firing entirely.
    enabled : bool
        Whether the schedule is active (default ``True``).
    """

    frequency: "ScheduleFrequency"
    start_time: str
    timezone: Optional[str] = None
    days_of_week: Optional[List[int]] = None
    day_of_month: Optional[int] = None
    end_time: Optional[str] = None
    interval_minutes: Optional[int] = None
    end_date: Optional[str] = None
    enabled: bool = True

    def __post_init__(self) -> None:
        # Resolve here rather than in to_dict() so a bad zone fails at the call
        # site that wrote it, and so the attribute always reads back concrete.
        self.timezone = resolve_schedule_timezone(self.timezone)

    def to_dict(self) -> Dict[str, Any]:
        freq = self.frequency
        return {
            "frequency": freq.value if isinstance(freq, ScheduleFrequency) else freq,
            "start_time": self.start_time,
            "timezone": self.timezone,
            "days_of_week": self.days_of_week,
            "day_of_month": self.day_of_month,
            "end_time": self.end_time,
            "interval_minutes": self.interval_minutes,
            "end_date": self.end_date,
            "enabled": self.enabled,
        }


def _normalize_schedule(
    schedule: Optional[Union["Schedule", Dict[str, Any]]]
) -> Optional[Dict[str, Any]]:
    """Schedule (or equivalent dict) -> the wire dict, with the timezone resolved.

    A plain dict is accepted in place of :class:`Schedule`, so it gets the same
    default and the same alias/validation treatment — otherwise a dict schedule
    would land on the platform with no timezone at all.
    """
    if schedule is None:
        return None
    if isinstance(schedule, Schedule):
        return schedule.to_dict()
    if isinstance(schedule, dict):
        out = dict(schedule)
        out["timezone"] = resolve_schedule_timezone(out.get("timezone"))
        freq = out.get("frequency")
        if isinstance(freq, ScheduleFrequency):
            out["frequency"] = freq.value
        return out
    raise TypeError(
        f"schedule must be a Schedule or a dict, got {type(schedule).__name__}"
    )


class DuplicateTaskError(RuntimeError):
    """Raised when a task with the same name already exists (HTTP 409)."""

    def __init__(self, task_name, existing_task_id, existing_status, message=None):
        self.task_name = task_name
        self.existing_task_id = existing_task_id
        self.existing_status = existing_status
        super().__init__(
            message
            or f"Task '{task_name}' already exists "
            f"(id={existing_task_id}, status={existing_status})"
        )


class _Client:
    """Thin HTTP client for the HiveQ platform task API."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: int = 30,
    ):
        # Order: explicit arg > HIVEQ_BASE_URL > the auth host (HIVEQ_AUTH_URL)
        # + /api/orchestrator > the hosted default. Following the auth host means
        # one configured URL points the whole SDK at the same platform.
        from hiveq.flow.config import platform_origin

        origin = platform_origin()
        self.base_url = (
            base_url
            or os.environ.get("HIVEQ_BASE_URL")
            or (f"{origin}/api/orchestrator" if origin else None)
            or _DEFAULT_BASE_URL
        ).rstrip("/")
        self.api_key = api_key or os.environ.get("HIVEQ_API_KEY")
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        # Only the API key is sent; identity is resolved server-side from it.
        return {"X-API-Key": self.api_key} if self.api_key else {}

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        headers = self._headers()
        headers.update(kwargs.pop("headers", {}))
        return requests.request(
            method, url, timeout=self.timeout, headers=headers, **kwargs
        )

    def submit(
        self,
        task_type: Union[str, TaskType],
        task_name: str,
        task: Any,
        entry_method: Optional[str] = "run",
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        job_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        requirements: Optional[List[str]] = None,
        schedule: Optional[Union[Schedule, Dict[str, Any]]] = None,
        allow_duplicate: bool = False,
        duplicate_action: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Capture-and-send a task for execution. Returns the parsed JSON reply.

        ``entry_method`` defaults to ``"run"`` (the backtest task shape). For a
        plain callable pass ``entry_method=None`` with ``args``/``kwargs`` — the
        executor then calls ``task(*args, **kwargs)``.

        ``schedule`` (a :class:`Schedule` or an equivalent dict) registers the
        task for recurring execution instead of a single one-off run — the
        platform validates/stores it and Celery Beat drives future firings.

        ``requirements`` travels in JSON outside the pickle so the updated
        executor can install dependencies before deserializing user code.
        Package availability and compatible binary wheels remain required.
        """
        task_type_value = (
            task_type.value
            if isinstance(task_type, TaskType)
            else TaskType(task_type).value
        )

        schedule_dict = _normalize_schedule(schedule)

        # Let the task see the clock it was scheduled on: job_now() inside the
        # script then reads the schedule's timezone rather than guessing.
        task_env = (
            {_JOB_TIMEZONE_ENV: schedule_dict["timezone"]}
            if schedule_dict and schedule_dict.get("timezone")
            else None
        )
        wrapper = _TaskWrapper(
            target=task,
            entry_method=entry_method,
            args=args,
            kwargs=kwargs,
            env=task_env,
        )
        if requirements is not None and not isinstance(requirements, list):
            raise ValueError("requirements must be a list of package specs")
        reqs = list(requirements) if requirements is not None else None
        payload_b64 = base64.b64encode(cloudpickle.dumps(wrapper)).decode()

        body = {
            "payload_b64": payload_b64,
            "job_type": job_type,
            "schedule": schedule_dict,
            "metadata": metadata or {},
            "requirements": reqs,
        }
        headers = {
            "X-Task-Type": task_type_value,
            "X-Task-Name": task_name,
            "Content-Type": "application/json",
        }
        if allow_duplicate:
            headers["X-Allow-Duplicate"] = "true"
            if duplicate_action:
                if duplicate_action not in _DUPLICATE_ACTIONS:
                    raise ValueError(
                        f"duplicate_action must be one of {_DUPLICATE_ACTIONS}"
                    )
                headers["X-Duplicate-Action"] = duplicate_action

        resp = self._request("POST", "/submit", data=json.dumps(body), headers=headers)

        if resp.status_code == 409:
            data = resp.json()
            details = data.get("details", {})
            raise DuplicateTaskError(
                task_name,
                details.get("existing_task_id", "unknown"),
                details.get("existing_status", "unknown"),
                data.get("message"),
            )

        resp.raise_for_status()
        result = resp.json()
        if not result.get("success", True):
            raise ValueError(
                f"Task submission failed: {result.get('message', 'Unknown error')}"
            )
        return result

    def get_result(self, task_id: str) -> Dict[str, Any]:
        """Current result/status of a task."""
        resp = self._request("GET", f"/result/{task_id}")
        resp.raise_for_status()
        return resp.json()

    def get_status(self, task_id: str) -> Dict[str, Any]:
        """Full status of a task."""
        resp = self._request("GET", f"/status/{task_id}")
        resp.raise_for_status()
        return resp.json()

    def get_logs(
        self,
        task_id: Optional[str] = None,
        task_name: Optional[str] = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """Logs for a task (by id or name)."""
        if not task_id and not task_name:
            raise ValueError("At least one of task_id or task_name is required")
        params: Dict[str, Any] = {"limit": limit}
        if task_id:
            params["task_id"] = task_id
        if task_name:
            params["task_name"] = task_name
        resp = self._request("GET", "/logs", params=params)
        resp.raise_for_status()
        return resp.json()

    def get_logs_gz(
        self,
        task_id: Optional[str] = None,
        run_id: Optional[str] = None,
        task_name: Optional[str] = None,
        dest: Optional[str] = None,
    ) -> str:
        """Download the FULL log via ``GET /logs?format=gz`` (whole file, not a tail).

        Streamed in chunks so huge logs never load fully in memory. If ``dest`` is
        given, the gzip is written there (``.gz``) and the path is returned;
        otherwise the decompressed log text is returned as a ``str``.
        """
        import gzip

        if not (task_id or run_id or task_name):
            raise ValueError("one of task_id, run_id, or task_name is required")
        params: Dict[str, Any] = {"format": "gz"}
        if task_id:
            params["task_id"] = task_id
        if run_id:
            params["run_id"] = run_id
        if task_name:
            params["task_name"] = task_name

        resp = self._request("GET", "/logs", params=params, stream=True)
        resp.raise_for_status()
        if dest is not None:
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
            return dest
        return gzip.decompress(resp.content).decode("utf-8", errors="replace")

    def poll_result(
        self,
        task_id: str,
        timeout: Optional[int] = None,
        poll_interval: float = 1.0,
    ) -> Dict[str, Any]:
        """Block until the task reaches a terminal state (or ``timeout``)."""
        start = time.time()
        while True:
            result = self.get_result(task_id)
            if result.get("status") in _TERMINAL_STATES:
                return result
            if timeout is not None and time.time() - start >= timeout:
                raise TimeoutError(
                    f"Task {task_id} did not complete within {timeout} seconds"
                )
            time.sleep(poll_interval)

    def terminate(self, task_name: str) -> Dict[str, Any]:
        """Terminate a task (and cancel its schedule, if any) by task name."""
        resp = self._request(
            "POST", "/terminate", data=json.dumps({"task_name": task_name}),
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()


_default_client: Optional[_Client] = None


def configure(
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    timeout: int = 30,
) -> _Client:
    """Create/replace the default client. Only the API key is required."""
    global _default_client
    _default_client = _Client(base_url=base_url, api_key=api_key, timeout=timeout)
    return _default_client


def get_client() -> _Client:
    """Return the default client, creating it from the environment if needed."""
    global _default_client
    if _default_client is None:
        _default_client = _Client()
    return _default_client


def submit(*args, **kwargs) -> Dict[str, Any]:
    return get_client().submit(*args, **kwargs)


def get_result(task_id: str) -> Dict[str, Any]:
    return get_client().get_result(task_id)


def get_status(task_id: str) -> Dict[str, Any]:
    return get_client().get_status(task_id)


def get_logs(
    task_id: Optional[str] = None,
    task_name: Optional[str] = None,
    limit: int = 1000,
) -> Dict[str, Any]:
    return get_client().get_logs(task_id, task_name, limit)


def get_logs_gz(
    task_id: Optional[str] = None,
    run_id: Optional[str] = None,
    task_name: Optional[str] = None,
    dest: Optional[str] = None,
) -> str:
    return get_client().get_logs_gz(task_id, run_id, task_name, dest)


def poll_result(
    task_id: str,
    timeout: Optional[int] = None,
    poll_interval: float = 1.0,
) -> Dict[str, Any]:
    return get_client().poll_result(task_id, timeout, poll_interval)


def terminate(task_name: str) -> Dict[str, Any]:
    return get_client().terminate(task_name)
