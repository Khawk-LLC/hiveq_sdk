"""Deploy an arbitrary Python callable to the HiveQ platform as a job.

This is the ``QUANT_SCRIPTS`` counterpart to ``run_backtest``/``deploy_backtest``:
same capture mechanism (cloudpickle, REST-only, no platform-internal package),
but for a plain fetch/compute/publish script rather than a ``hiveq.flow``
strategy — and, unlike ``run_function`` (which blocks and returns the value),
this returns a handle immediately by default so you can query status/logs/result
later, and optionally register a recurring ``schedule`` instead of a one-off run.

    import hiveq.flow as hf
    from hiveq.flow import Schedule, ScheduleFrequency

    def fetch_and_publish():
        from hiveq import dd
        df = dd.load(dataset="HIVEQ_US_EQ", schema="bars_1d", symbols=["AAPL"])
        dd.save(df, schema="quant_features", key="my_signal")

    job = hf.deploy_job(
        fetch_and_publish,
        task_name="my-daily-signal-job",
        requirements=["pandas"],
        schedule=Schedule(frequency=ScheduleFrequency.DAILY, start_time="16:05",
                          timezone="US/Eastern"),
    )
    job.status()
    job.logs(limit=200)
    job.terminate()
"""
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Union

from hiveq.flow._client import Schedule
from hiveq.flow.logger import logger

logger = logger(show_logo=False)

_TERMINAL_STATUSES = {
    "completed", "failed", "done", "error", "terminated", "stopped", "cancelled",
}


class Job:
    """A handle to a single deployed QUANT_SCRIPTS task.

    Deliberately lighter than ``Run`` (§10.0) — a generic script has no
    backtest-shaped metrics/positions/trades, just status/result/logs.
    """

    def __init__(self, task_id: str, task_name: str):
        if not task_id:
            raise ValueError("task_id is required")
        self.task_id = task_id
        self.task_name = task_name

    def __repr__(self) -> str:
        return f"Job(task_id={self.task_id!r}, task_name={self.task_name!r})"

    def _client(self):
        from hiveq.flow._client import _Client

        return _Client()

    def status(self) -> Dict[str, Any]:
        """Full status document.

        Prefers ``GET /status/{task_id}`` (backed by the ClickHouse task-history
        pipeline); falls back to ``GET /result/{task_id}`` (the Celery/Mongo
        result backend, always populated) when that pipeline has no row yet —
        e.g. a local/dev stack without ClickHouse task-status wired up, or a
        ``schedule``d job that hasn't fired yet.
        """
        client = self._client()
        try:
            return client.get_status(self.task_id)
        except Exception as e:
            logger.debug(f"get_status failed, falling back to get_result: {e}")
            return client.get_result(self.task_id)

    def result(self) -> Dict[str, Any]:
        """Current result/status document (``{"status", "result", ...}``)."""
        return self._client().get_result(self.task_id)

    def logs(self, limit: int = 1000) -> Dict[str, Any]:
        """Tail of the executor log (paginate with ``limit``)."""
        return self._client().get_logs(task_id=self.task_id, limit=limit)

    def download_logs(self, dest: Optional[str] = None) -> str:
        """Full (gzip) executor log — decompressed text, or a path if ``dest`` is given."""
        return self._client().get_logs_gz(task_id=self.task_id, dest=dest)

    def wait(self, timeout: Optional[float] = None, poll_interval: float = 2.0) -> Dict[str, Any]:
        """Block until the job reaches a terminal state; returns the final result doc.

        Not meaningful for a recurring ``schedule`` job (it never reaches a
        terminal state on its own) — use ``status()``/``logs()`` for those instead.
        """
        client = self._client()
        deadline = (time.monotonic() + timeout) if timeout else None
        while True:
            res = client.get_result(self.task_id) or {}
            status = (res.get("status") or "").lower()
            if status in _TERMINAL_STATUSES:
                return res
            if deadline and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Job {self.task_id} not finished within {timeout}s "
                    f"(status: {status or 'pending'})"
                )
            time.sleep(poll_interval)

    def terminate(self) -> Dict[str, Any]:
        """Terminate the task (and cancel its schedule, if any)."""
        return self._client().terminate(self.task_name)


def deploy_job(
    func: Callable,
    *,
    task_name: Optional[str] = None,
    args: Optional[tuple] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    requirements: Optional[List[str]] = None,
    schedule: Optional[Union[Schedule, Dict[str, Any]]] = None,
    job_type: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    wait: bool = False,
    allow_duplicate: bool = True,
    duplicate_action: Optional[str] = "override",
) -> Job:
    """Deploy a Python callable to the platform as a ``QUANT_SCRIPTS`` job.

    Like ``deploy_backtest``, this is a stub: it submits and returns a handle
    immediately (``wait=False``, the default) rather than blocking for a
    result — the natural shape for a ``schedule``d recurring job, which never
    reaches a terminal state on its own. For a one-off run you want to block
    on, either pass ``wait=True`` here or use ``run_function`` instead.

    Parameters
    ----------
    func : callable
        The function to run remotely, cloudpickled client-side (no platform
        package import required). Called as ``func(*args, **kwargs)``.
    task_name : str, optional
        Name for the task (also its dedup key); defaults to
        ``job-<func name>-<short id>``.
    requirements : list[str], optional
        pip specs to install in the sandbox, e.g. ``["pandas>=2.0"]``. Wired
        through in the shape the executor expects; treat actual installation
        as best-effort until the platform confirms it runs for every task type.
    schedule : Schedule, optional
        Recurring-execution config (see ``Schedule``/``ScheduleFrequency``,
        §11.6) — registers the job for repeated firing via the platform's
        scheduler instead of a single run.
    wait : bool
        Block for a terminal result before returning (default ``False``).

    Returns
    -------
    Job
        Handle exposing ``.status()``, ``.result()``, ``.logs()``,
        ``.download_logs()``, ``.wait()``, ``.terminate()``.
    """
    if not callable(func):
        raise TypeError("deploy_job(func, ...): func must be callable")

    from hiveq.flow import _ensure_initialized
    from hiveq.flow._client import TaskType, _Client

    _ensure_initialized()  # resolves the API key (triggers sign-in if missing)

    name = task_name or f"job-{getattr(func, '__name__', 'task')}-{uuid.uuid4().hex[:8]}"
    client = _Client()
    submitted = client.submit(
        task_type=TaskType.QUANT_SCRIPTS,
        task_name=name,
        task=func,
        entry_method=None,  # plain callable -> executor calls func(*args, **kwargs)
        args=tuple(args) if args else (),
        kwargs=dict(kwargs) if kwargs else {},
        job_type=job_type,
        metadata=metadata,
        requirements=list(requirements) if requirements else None,
        schedule=schedule,
        allow_duplicate=allow_duplicate,
        duplicate_action=duplicate_action,
    )
    task_id = submitted.get("task_id")
    if not task_id:
        raise RuntimeError(f"Job submission returned no task_id: {submitted}")

    job = Job(task_id=task_id, task_name=name)
    logger.info(f"Deployed '{name}' to the platform (task {task_id})")
    if wait:
        job.wait()
    return job
