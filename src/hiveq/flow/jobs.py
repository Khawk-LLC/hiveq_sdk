"""
hiveq.flow.jobs — deploy jobs to the HiveQ platform and pull their status,
results, and logs through one standard surface.

Deploy
------
Submit a task (any callable, or an object with a ``run()`` method)::

    from hiveq.flow.jobs import submit, TaskType
    res = submit(
        task_type=TaskType.QUANT_SCRIPTS,
        task_name="my-quant-script",
        task=my_callable,
        requirements=["pandas"], metadata={"owner": "rm"},
    )

Observe (any job type, same calls)
----------------------------------
    from hiveq.flow.jobs import poll_result, get_status, get_result, get_logs
    poll_result(res["task_id"])              # block until terminal state
    get_status(res["task_id"])               # current status
    get_result(res["task_id"])               # result/output
    get_logs(task_id=res["task_id"], limit=200)   # tail / paginate logs

Schedule a recurring job
------------------------
    from hiveq.flow.jobs import deploy_job, Schedule, ScheduleFrequency
    deploy_job(my_callable, task_name="daily-signal",
               schedule=Schedule(ScheduleFrequency.DAILY, start_time="16:05"))

``start_time`` is US Eastern (``America/New_York``) by default — the zone the
rest of the platform speaks. Pass ``timezone="local"`` for the client machine's
own zone, or any IANA name / abbreviation (``"EST"``, ``"PT"``, ``"UTC"``, ...).

Time checks inside the script
-----------------------------
The sandbox the job runs in is pinned to ``TZ=America/New_York``, so a bare
``datetime.now()`` inside a deployed script is already Eastern — but the same
function run on your own machine is not. For a check that reads the same in
both places (and follows the schedule's timezone when it isn't ET), use the job
clock::

    from hiveq.flow.jobs import job_now, job_today   # module level, see below

    def publish_after_the_close():
        if job_now().hour < 16:      # 16:00 Eastern, wherever this runs
            return {"skipped": "before the close"}

``job_now()`` travels inside the payload, so it works on the executor even
though no SDK is installed there — which is why it is imported at module level
and closed over, rather than imported inside the deployed function.
``datetime.now(timezone.utc)`` is still UTC, of course — the job clock is only
for wall-clock checks.

Configuration (env)
-------------------
``HIVEQ_BASE_URL`` (platform endpoint) and ``HIVEQ_API_KEY`` (auth). Only the
API key is sent on the wire — the platform resolves identity from it.
``HIVEQ_SCHEDULE_TIMEZONE`` overrides the default schedule timezone (set it to
``local`` to follow the machine).
"""

from hiveq.flow._client import (  # noqa: F401 — re-exported public surface
    Schedule,
    ScheduleFrequency,
    job_now,
    job_timezone,
    job_today,
    TaskType,
    get_client,
    get_logs,
    get_logs_gz,
    get_result,
    get_status,
    poll_result,
    submit,
    terminate,
)
from hiveq.flow.job_deploy import Job, deploy_job  # noqa: F401 — re-exported public surface

__all__ = [
    "TaskType",
    "Schedule",
    "ScheduleFrequency",
    # the job clock (US Eastern by default — see the module docstring)
    "job_now",
    "job_today",
    "job_timezone",
    # deploy
    "submit",
    "deploy_job",
    "Job",
    # observe / pull
    "poll_result",
    "get_status",
    "get_result",
    "get_logs",
    "get_logs_gz",
    "get_client",
    "terminate",
]
