"""Deploy: capture the user's strategy code and submit it to the HiveQ platform.

This is the *client* half of deployment. It captures the user's strategy code as
**source** — the first-party import graph, the entry script (or, in a notebook,
the ``__main__`` definitions recovered via ``inspect.getsource``), and the config
files beside them — packs that into a source archive (see
:mod:`hiveq.flow._source_bundle`), attaches it to a ``HiveQFlowBackTestTask``, and
submits that to the HiveQ platform's task REST API (see :mod:`hiveq.flow._client`).

There is **no cloudpickle of user code**: the executor recompiles the source under
its own interpreter, so a remote machine on any Python minor version can deploy to
the engine without the bytecode-version coupling that made pickled code "almost
unusable" in a hand-off. (The task object itself is still serialized for transport,
but it carries only data — strings, dicts, the base64 archive — not user bytecode.)

The task is serialized **by reference** (``hiveq.flow.deploy_task.
HiveQFlowBackTestTask``), so the executor — which has the full ``hiveq-flow`` and
engine installed — resolves and runs its own ``run()``. The client therefore ships
only the constructors (the attributes the executor's ``run()`` reads) and the
capture/submit helpers; ``run()`` here is a deliberate stub that never executes on
the client.
"""
import inspect
from abc import ABC
from typing import Optional, Dict, Any, List

from hiveq.flow import _client
from hiveq.flow.logger import logger

logger = logger(show_logo=False)


def _runs_on_executor(self) -> Dict[str, Any]:
    raise RuntimeError(
        "HiveQ*Task.run() executes on the HiveQ platform executor, not on the "
        "client. The thin client SDK only captures + submits the task; the "
        "executor (full hiveq-flow) provides the real run()."
    )


class HiveQDeployTask(ABC):
    """Client-side constructor for a deployable task (capture/submit only)."""

    def __init__(
            self,
            trader_id: str,
            api_key: str,
            config_params: Optional[Dict[str, Any]],
            strategy_configs: List[Dict[str, Any]],
            symbols: Optional[List[str]],
            data_configs: Optional[List[Dict[str, Any]]],
            kwargs: Dict[str, Any],
            source_bundle: Dict[str, Any]
    ):
        self.trader_id = trader_id
        self.api_key = api_key
        self.config_params = config_params
        self.strategy_configs = strategy_configs
        self.symbols = symbols
        self.data_configs = data_configs
        self.kwargs = kwargs
        # The sole carrier of user code: a base64 zip of the strategy source +
        # first-party import graph + config files, with a manifest. The executor
        # recompiles it under its own interpreter (see hiveq.flow._source_bundle).
        # No cloudpickle of user code is sent.
        self.source_bundle = source_bundle

    # run() resolves to the executor's full implementation via by-reference
    # cloudpickle; the client never calls it.
    run = _runs_on_executor


class HiveQFlowBackTestTask(HiveQDeployTask):
    """Self-contained backtest task. Constructed on the client, run on executor.

    Holds everything the executor's ``run()`` needs: credentials, backtest
    parameters, and the captured user code (strategy classes + local modules).
    """

    def __init__(
            self,
            trader_id: str,
            api_key: str,
            config_params: Optional[Dict[str, Any]],
            strategy_configs: List[Dict[str, Any]],
            symbols: Optional[List[str]],
            start_date: str,
            end_date: str,
            data_configs: Optional[List[Dict[str, Any]]],
            backtest_config: Optional[Dict[str, Any]],
            kwargs: Dict[str, Any],
            source_bundle: Dict[str, Any]
    ):
        super().__init__(
            trader_id=trader_id,
            api_key=api_key,
            config_params=config_params,
            strategy_configs=strategy_configs,
            symbols=symbols,
            data_configs=data_configs,
            kwargs=kwargs,
            source_bundle=source_bundle
        )
        self.start_date = start_date
        self.end_date = end_date
        self.backtest_config = backtest_config


class DeploymentHelper:
    """Capture user code (as source) and submit tasks to the HiveQ platform."""

    @staticmethod
    def capture_calling_module(strategy_configs) -> tuple:
        """Capture the caller's strategy code as a source archive.

        Walks the call stack to the user frame, collects the names of the
        user-defined classes/functions defined there (used only to validate the
        deploy — e.g. to confirm a module-level ``on_hiveq_event`` exists for the
        global-dispatch form), and builds the source archive: the first-party
        import graph + the entry script + adjacent config files, or — in a
        notebook/REPL with no entry file — the ``__main__`` definitions recovered
        via ``inspect.getsource`` (see :mod:`hiveq.flow._source_bundle`).

        No user code is cloudpickled; the source archive is the only carrier.

        Returns ``(user_object_names, source_bundle)``.
        """
        from hiveq.flow._source_bundle import build_source_bundle

        user_object_names = set()
        user_objects = {}
        source_bundle = None
        capture_frame = None

        frame = inspect.currentframe()
        try:
            while frame:
                mod = frame.f_globals.get("__name__")
                if (mod == "__main__" or
                        mod.startswith("__main__") or
                        "ipykernel" in str(mod) or
                        "jupyter" in str(mod) or
                        "marimo" in str(mod) or
                        frame.f_globals.get("get_ipython") is not None):

                    capture_frame = frame
                    for name, obj in frame.f_globals.items():
                        if name.startswith('__'):
                            continue
                        obj_module = getattr(obj, '__module__', '') or ''
                        is_user_defined = (
                            obj_module == "__main__" or
                            obj_module.startswith("__main__") or
                            "ipykernel" in obj_module or
                            "marimo" in obj_module
                        )
                        if is_user_defined and (inspect.isclass(obj) or inspect.isfunction(obj)):
                            user_object_names.add(name)
                            user_objects[name] = obj
                    break
                frame = frame.f_back

            if capture_frame is not None:
                source_bundle = build_source_bundle(
                    strategy_configs, capture_frame, main_objects=user_objects
                )
        finally:
            del frame  # Avoid reference cycles
            capture_frame = None

        return user_object_names, source_bundle

    @staticmethod
    def submit(
            task,
            task_type,
            task_name: str,
            metadata: Dict[str, Any],
            requirements: List[str],
            job_type: Optional[str] = None,
            allow_duplicate: bool = True,
            duplicate_action: str = 'override',
            run_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Submit a captured task to the HiveQ platform task REST API.

        Returns a dict with status / message / task_id / payload_id.
        """
        try:
            # The thin client sends ONLY the API key. The REST server authorizes
            # the key and derives identity (user_id / org_id / user_name) from it,
            # so we deliberately do NOT forward identity headers — that keeps a
            # dev's local env from overriding the server-resolved identity.
            from hiveq.flow import config as flow_config
            cfg = flow_config()
            _client.configure(api_key=cfg.hiveq_api_key)

            result = _client.submit(
                task_type=task_type,
                task_name=task_name,
                task=task,
                entry_method='run',
                job_type=job_type,
                metadata=metadata,
                requirements=requirements,
                allow_duplicate=allow_duplicate,
                duplicate_action=duplicate_action
            )

            task_id = result.get('task_id')
            payload_id = result.get('payload_id')

            logger.info(f"Task deployed successfully: task_id={task_id}, payload_id={payload_id}, name={task_name}")

            if run_config is not None and payload_id:
                run_config['payload_id'] = payload_id
                logger.debug(f"Stored payload_id in run_config: {payload_id}")

            return {
                'status': 'success',
                'message': None,
                'task_id': task_id,
                'payload_id': payload_id
            }

        except Exception as e:
            logger.error(f"Failed to deploy task: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'task_id': None,
                'payload_id': None
            }
