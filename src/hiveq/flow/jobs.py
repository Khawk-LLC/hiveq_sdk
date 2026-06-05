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

Configuration (env)
-------------------
``HIVEQ_BASE_URL`` (platform endpoint) and ``HIVEQ_API_KEY`` (auth). Only the
API key is sent on the wire — the platform resolves identity from it.
"""

from hiveq.flow._client import (  # noqa: F401 — re-exported public surface
    TaskType,
    get_client,
    get_logs,
    get_logs_gz,
    get_result,
    get_status,
    poll_result,
    submit,
)

__all__ = [
    "TaskType",
    # deploy
    "submit",
    # observe / pull
    "poll_result",
    "get_status",
    "get_result",
    "get_logs",
    "get_logs_gz",
    "get_client",
]
