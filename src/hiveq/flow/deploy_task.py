"""Deploy: capture the user's strategy code and push it to the orchestrator.

This is the *client* half of deployment. It captures the user-defined strategy
classes/functions (cloudpickle) plus any local module sources, packs them into a
``HiveQFlowBackTestTask``, and submits that to the HiveQ orchestrator's REST API.

The task is cloudpickled **by reference** (``hiveq.flow.deploy_task.
HiveQFlowBackTestTask``), so the executor — which has the full ``hiveq-flow`` and
engine installed — resolves and runs its own ``run()``. The client therefore
ships only the constructors (the attributes the executor's ``run()`` reads) and
the capture/submit helpers; ``run()`` here is a deliberate stub that never
executes on the client.

The constructors and ``DeploymentHelper`` are lifted verbatim from the full
package's ``deploy_task.py``. The engine-side ``run()`` bodies and the live-sim
task are intentionally omitted — they live only on the executor.
"""
import ast
import inspect
import os
import types
from abc import ABC
from typing import Optional, Dict, Any, List

import cloudpickle

from hiveq.flow.logger import logger
logger = logger(show_logo=False)

# Optional import for orchestrator client
try:
    import hiveq_orchestrator as orchestrator
    from hiveq_orchestrator import TaskType
    ORCHESTRATOR_AVAILABLE = True
except ImportError:
    ORCHESTRATOR_AVAILABLE = False
    orchestrator = None
    TaskType = None


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
            local_modules: Dict[str, str],
            pickled_objects: Dict[str, bytes]
    ):
        self.trader_id = trader_id
        self.api_key = api_key
        self.config_params = config_params
        self.strategy_configs = strategy_configs
        self.symbols = symbols
        self.data_configs = data_configs
        self.kwargs = kwargs
        self.local_modules = local_modules
        self.pickled_objects = pickled_objects

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
            local_modules: Dict[str, str],
            pickled_objects: Dict[str, bytes]
    ):
        super().__init__(
            trader_id=trader_id,
            api_key=api_key,
            config_params=config_params,
            strategy_configs=strategy_configs,
            symbols=symbols,
            data_configs=data_configs,
            kwargs=kwargs,
            local_modules=local_modules,
            pickled_objects=pickled_objects
        )
        self.start_date = start_date
        self.end_date = end_date
        self.backtest_config = backtest_config


class DeploymentHelper:
    """Capture user code and submit tasks to the orchestrator (REST)."""

    @staticmethod
    def _sanitize_for_pickle(obj):
        """Strip notebook-runtime monkey-patches (e.g. marimo's ``print_override``)
        from user functions/classes before cloudpickling.

        cloudpickle resolves a method's free variables against the function's
        ``__globals__``. When the user's class is defined in a marimo cell,
        ``print`` is bound to ``marimo._messaging.print_override``; cloudpickle
        pickles that by reference and the executor (no marimo) fails on unpickle.
        Dropping any global whose ``__module__`` is under ``marimo`` lets
        cloudpickle fall back to the real builtin.
        """
        def _strip_globals(fn):
            if not inspect.isfunction(fn) or not hasattr(fn, "__globals__"):
                return fn
            new_globals = {
                k: v for k, v in fn.__globals__.items()
                if not (getattr(v, "__module__", "") or "").startswith("marimo")
            }
            if len(new_globals) == len(fn.__globals__):
                return fn
            return types.FunctionType(
                fn.__code__, new_globals, fn.__name__,
                fn.__defaults__, fn.__closure__,
            )

        if inspect.isfunction(obj):
            return _strip_globals(obj)
        if inspect.isclass(obj):
            replaced = {
                name: _strip_globals(val)
                for name, val in obj.__dict__.items()
                if inspect.isfunction(val)
            }
            if not replaced:
                return obj
            for name, fn in replaced.items():
                if fn is not obj.__dict__[name]:
                    try:
                        setattr(obj, name, fn)
                    except (AttributeError, TypeError):
                        pass
            return obj
        return obj

    @staticmethod
    def capture_calling_module(strategy_configs) -> tuple:
        """Capture user-defined classes/functions and local module sources from
        the caller's frame.

        Returns ``(local_modules, pickled_objects)``.
        """
        from hiveq.flow.config import StrategyConfig
        strategy_types = {cfg.type for cfg in strategy_configs}
        local_modules = {}
        pickled_objects = {}
        script_path = None

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

                    script_path = frame.f_globals.get('__file__')

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

                        if not is_user_defined:
                            continue

                        if inspect.isclass(obj) or inspect.isfunction(obj):
                            try:
                                sanitized = DeploymentHelper._sanitize_for_pickle(obj)
                                pickled_objects[name] = cloudpickle.dumps(sanitized)
                                logger.debug(f"Captured user object: {name}")
                            except Exception as e:
                                logger.warning(f"Failed to cloudpickle {name}: {e}")

                    break
                frame = frame.f_back
        finally:
            del frame  # Avoid reference cycles

        if script_path:
            local_modules = DeploymentHelper.capture_local_imports(script_path)

        return local_modules, pickled_objects

    @staticmethod
    def capture_local_imports(script_path: str) -> Dict[str, str]:
        """Find and capture local modules (sibling ``.py`` files) imported by the
        user's script. System packages are not captured.
        """
        local_modules = {}

        if not script_path or not os.path.exists(script_path):
            return local_modules

        script_dir = os.path.dirname(os.path.abspath(script_path))

        try:
            with open(script_path, 'r') as f:
                source = f.read()
            tree = ast.parse(source)
        except Exception as e:
            logger.warning(f"Failed to parse script for imports: {e}")
            return local_modules

        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])

        for import_name in imports:
            local_file = os.path.join(script_dir, f"{import_name}.py")
            if os.path.exists(local_file):
                try:
                    with open(local_file, 'r') as f:
                        local_modules[import_name] = f.read()
                    logger.debug(f"Captured local module: {import_name}")
                    sub_imports = DeploymentHelper.capture_local_imports(local_file)
                    local_modules.update(sub_imports)
                except Exception as e:
                    logger.warning(f"Failed to capture local module {import_name}: {e}")

        return local_modules

    @staticmethod
    def submit_to_orchestrator(
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
        """Submit a captured task to the orchestrator REST API.

        Returns a dict with status / message / task_id / payload_id.
        """
        if not ORCHESTRATOR_AVAILABLE:
            return {
                'status': 'error',
                'message': 'hiveq_orchestrator package is not installed. Install it with: pip install hiveq-orchestrator',
                'task_id': None,
                'payload_id': None
            }

        try:
            # The thin client sends ONLY the API key. The REST server authorizes
            # the key and derives identity (user_id / org_id / user_name) from it,
            # so we deliberately do NOT forward identity headers — that keeps a
            # dev's local env from overriding the server-resolved identity.
            from hiveq.flow import config as flow_config
            cfg = flow_config()
            orchestrator.configure(api_key=cfg.hiveq_api_key)

            result = orchestrator.submit(
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
