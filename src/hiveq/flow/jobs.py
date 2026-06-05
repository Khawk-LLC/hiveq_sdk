"""
hiveq.flow.jobs — deploy jobs to the HiveQ orchestrator and pull their logs,
status, and results, with one standard surface for every job type.

This is a thin wrapper over the ``hiveq_orchestrator`` client so hiveq-flow
users get deploy + observability without installing or importing a second
package directly.

Deploy
------
Quant script (arbitrary callable / object with ``run()``)::

    from hiveq.flow.jobs import submit, TaskType
    res = submit(
        task_type=TaskType.QUANT_SCRIPTS,
        task_name="my-quant-script",
        task=my_callable, args=(...), kwargs={...},
        requirements=["pandas"], metadata={"owner": "rm"},
    )

Backtest::

    from hiveq.flow.jobs import deploy, FlowBacktestTask, TaskType
    res = deploy(FlowBacktestTask(strategy_code="...", config={...}),
                 TaskType.HIVEQ_FLOW_BT)

Alpha AI (GPU training)::

    from hiveq.flow.jobs import submit_gpu, GPUTask
    res = submit_gpu(GPUTask(training_module=run_training,
                             training_config={...}, gpu_type="gpu_1x_a10",
                             code_folder="./my_code"),
                     task_name="my-training")

Observe (any job type, same calls)
----------------------------------
    from hiveq.flow.jobs import poll_result, get_status, get_result, get_logs
    poll_result(res["task_id"])              # block until terminal state
    get_status(res["task_id"])               # current status
    get_result(res["task_id"])               # result/output
    get_logs(task_id=res["task_id"], limit=200)   # tail / paginate logs

Configuration (env)
-------------------
HIVEQ_BASE_URL, HIVEQ_API_KEY, HIVEQ_USER_ID, HIVEQ_ORG_ID, HIVEQ_USER_NAME
(read by the underlying client; same vars hiveq-flow already uses).
"""

from hiveq_orchestrator import (  # noqa: F401 — re-exported public surface
    FlowBacktestTask,
    GPUTask,
    Schedule,
    ScheduleFrequency,
    TaskType,
    deploy,
    get_artifact_metadata,
    get_client,
    get_logs,
    get_result,
    get_status,
    poll_result,
    submit,
    submit_gpu,
)

__all__ = [
    # task types / config
    "TaskType",
    "Schedule",
    "ScheduleFrequency",
    # payload wrappers
    "FlowBacktestTask",
    "GPUTask",
    # deploy
    "submit",
    "submit_gpu",
    "deploy",
    # observe / pull
    "poll_result",
    "get_status",
    "get_result",
    "get_logs",
    "get_artifact_metadata",
    "get_client",
]
