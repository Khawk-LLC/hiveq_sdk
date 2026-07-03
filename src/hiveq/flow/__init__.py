"""HiveQ Flow — thin client SDK (deploy + observe).

This is the lightweight client package. You author a strategy against the type
surface here, and ``run_backtest`` captures your strategy code and submits it to
the HiveQ platform, which runs the engine and returns results. The engine itself
(the proprietary ``oms/sigma`` core, PySigma, the run loop) ships only with the
full ``hiveq-flow`` installed on the platform executor — never to the client.

Same import path as the full package, so strategy code is identical either way::

    import hiveq.flow as hf
    from hiveq.flow import StrategyConfig, BacktestConfig

    run = hf.run_backtest([StrategyConfig(name='S', type='MyStrategy')],
                          symbols=['AAPL'], start_date='2025-01-01',
                          end_date='2025-01-31')
    run.report(); run.positions(); run.daily_returns()

Credentials are read from the environment: ``HIVEQ_API_KEY``, ``HIVEQ_USER_NAME``,
``HIVEQ_USER_ID``, ``HIVEQ_ORG_ID``.
"""
import os
from typing import Optional, List

import pandas as pd

# Config DTOs + enums (real — serialized into the deploy payload).
from .config import (
    BacktestConfig,
    StrategyConfig,
    EngineConfig,
    AssetType,
    DataType,
    ExecutionMode,
    EventType,
    EventLogType,
)
# Authoring types (stubs / lightweight DTOs — used only inside strategy
# callbacks, which run on the executor).
from .trading_types import OrderSide, OrderType, OrderStatus, MarketCenter
from hiveq.flow.oms.sigma.types import (
    SigmaFill as Fill,
    SigmaOrder as Order,
    SigmaPosition as Position,
    SigmaPortfolio as Portfolio,
)
from .context import Context
from .events import Event
from hiveq.flow.logger import logger

# Show the HiveQ banner + version on import — this MUST be the first logger()
# call, before importing modules that set up the logger with show_logo=False
# (uploads, deploy_task), which would otherwise create the handlers without the
# logo and make this a no-op.
logger()

from hiveq.flow.metrics.report import PerformanceReport
from hiveq.flow.uploads import upload_files, list_files, delete_files
from hiveq.flow.auth import login
from hiveq.flow.functions import (
    push_function,
    run_function,
    load_function,
    list_functions,
    function_versions,
    get_function_source,
    delete_function,
)
from hiveq.flow._client import Schedule, ScheduleFrequency, terminate
from hiveq.flow.job_deploy import Job, deploy_job

# --- credentials (lazy, from env) -------------------------------------------
_trader_id: Optional[str] = None
_api_key: Optional[str] = None
_user_id: Optional[str] = None
_org_id: Optional[str] = None
_config_params: Optional[dict] = None
_timezone: Optional[str] = None
_initialized: bool = False


def _init_from_env() -> None:
    """Initialize the client. The ONLY required credential is ``HIVEQ_API_KEY``,
    read from the environment or ``~/.hiveq/.env`` (resolved by config.py at
    import). The platform validates the key and resolves identity (org / user)
    from it at submit time, so user_id / org_id / user_name are NOT required.
    """
    import os
    global _trader_id, _api_key, _user_id, _org_id, _timezone

    api_key = os.environ.get('HIVEQ_API_KEY')
    if not api_key:
        # No key yet — sign in through the browser (loopback flow). login() opens
        # the sign-in page (host from HIVEQ_AUTH_URL), waits for the redirect that
        # carries the freshly minted key, writes it to ~/.hiveq/.env, and returns
        # it so we carry on.
        from hiveq.flow import auth
        api_key = auth.login()

    # Optional (resolved from the key by the platform if absent).
    _trader_id = os.environ.get('HIVEQ_USER_NAME')
    _user_id = os.environ.get('HIVEQ_USER_ID')
    _org_id = os.environ.get('HIVEQ_ORG_ID')
    _api_key = api_key

    if _timezone is None:
        from hiveq.flow.utils.timezone_utils import get_local_timezone, reset_timezone_cache
        _timezone = get_local_timezone()
        reset_timezone_cache()

    logger().info("HiveQ flow (client) initialized (API key only; identity resolved by platform).")


def _ensure_initialized() -> None:
    global _initialized
    if not _initialized:
        _init_from_env()
        _initialized = True


# Connection + identity are resolved by the platform at execution time: the
# executor is handed connection URLs from its own (local-or-remote) environment,
# and the REST server derives org/user from the validated API key. The thin
# client must therefore NOT freeze its local values into a deployed payload —
# doing so is what let a dev's `HIVEQ_DATA_URL=http://localhost:80` ride into the
# sandbox and break publishing. We strip these keys from what we ship; only the
# API key (carried separately) goes with the task.
_CLIENT_ONLY_ENV_KEYS = frozenset({
    'hiveq_api_key', 'hiveq_data_url', 'hiveq_base_url',
    'hiveq_user_id', 'hiveq_org_id', 'hiveq_user_name',
})


def _engine_config() -> EngineConfig:
    params = (_config_params or {}).copy()
    return EngineConfig(timezone=_timezone, params=params)


def _deploy_config_params() -> dict:
    """config_params safe to serialize into a deployed payload: strategy/engine
    behavior only, with connection/identity env values removed (the platform
    supplies those at execution time)."""
    return {
        k: v for k, v in _engine_config().params.items()
        if k not in _CLIENT_ONLY_ENV_KEYS
    }


def _sanitize_kwargs_for_deploy(kwargs: dict) -> dict:
    """Strip connection/identity from any EngineConfig passed via kwargs (e.g.
    `run_backtest(engine_config=EngineConfig(...))`). Constructing an EngineConfig
    on the client freezes the local defaults — hiveq_data_url, api_key, user/org —
    into its `params`. If serialized, the executor's in-run publisher would use the
    client's `https://staging.hiveq.ai/` instead of the platform's data URL. We
    deep-copy and strip so the executor rebuilds these from its own environment
    (App -> EngineConfig.__post_init__ fills hiveq_data_url from HIVEQ_DATA_URL)."""
    import copy
    out = {}
    for k, v in (kwargs or {}).items():
        params = getattr(v, 'params', None)
        if isinstance(params, dict) and any(ck in params for ck in _CLIENT_ONLY_ENV_KEYS):
            v = copy.deepcopy(v)
            for ck in _CLIENT_ONLY_ENV_KEYS:
                v.params.pop(ck, None)
        out[k] = v
    return out


# --- config accessors -------------------------------------------------------
def config() -> EngineConfig:
    """Return the engine configuration (credentials/headers from env)."""
    _ensure_initialized()
    return _engine_config()


# --- observe ----------------------------------------------------------------
_last_run = None  # set by run_backtest, used by event_logs()


def get_run(run_id: str, task_id: Optional[str] = None):
    """Attach to an existing run and return a :class:`~hiveq.flow.runs.Run`.

    Mirrors the platform ``runs`` REST endpoints, e.g.
    ``hf.get_run(run_id).report()`` / ``.positions()`` / ``.wait()``.
    """
    from hiveq.flow.runs import Run
    return Run(run_id=run_id, task_id=task_id)


def event_logs() -> pd.DataFrame:
    """Event logs for the most recent ``run_backtest`` (fetched over REST).

    The thin client runs nothing in-process, so this returns the deployed run's
    event-log rows from the platform. For an arbitrary run use
    ``hf.get_run(run_id).event_logs()``.
    """
    if _last_run is None:
        raise RuntimeError(
            "No run yet. Call hf.run_backtest(...) first, or use "
            "hf.get_run(run_id).event_logs() for a specific run."
        )
    return _last_run.event_logs()


# --- deploy -----------------------------------------------------------------
def run_backtest(
    strategy_configs: List[StrategyConfig],
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    data_configs: Optional[List[dict]] = None,
    backtest_config: Optional[BacktestConfig] = None,
    *,
    requirements: Optional[List[str]] = None,
    silent: bool = False,
    task_name: Optional[str] = None,
    allow_duplicate: bool = True,
    duplicate_action: str = 'override',
    timeout: Optional[float] = None,
    poll_interval: float = 1.0,
    **kwargs,
):
    """Submit a backtest to the HiveQ platform and return a :class:`Run`.

    Captures your strategy code, ships it to the HiveQ platform, and (unless
    ``silent=True``) blocks with a live progress line until the run finishes,
    then returns the completed ``Run``. ``requirements`` is accepted for API
    compatibility; package installation is handled by the remote orchestrator
    once supported. Results via ``run.report()`` etc.
    """
    result = _deploy(
        strategy_configs,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        data_configs=data_configs,
        backtest_config=backtest_config,
        task_name=task_name,
        allow_duplicate=allow_duplicate,
        duplicate_action=duplicate_action,
        **kwargs,
    )
    if result.get('status') != 'success':
        raise RuntimeError(f"run_backtest deploy failed: {result.get('message')}")

    from hiveq.flow.runs import Run
    run = Run(
        run_id=result.get('payload_id'),
        task_id=result.get('task_id'),
        start_date=start_date,
        end_date=end_date,
        task_name=task_name or result.get('task_name'),
    )
    run.check_credentials()
    global _last_run
    _last_run = run
    if silent:
        return run
    return run.wait(timeout=timeout, poll_interval=poll_interval, progress=True)


def _deploy(
    strategy_configs,
    symbols=None,
    start_date=None,
    end_date=None,
    data_configs=None,
    backtest_config=None,
    task_name=None,
    allow_duplicate=True,
    duplicate_action='override',
    publish=True,
    **kwargs,
) -> dict:
    """Capture the caller's strategy code and submit it to the HiveQ platform.

    Mirrors the full package's ``BacktestApp.deploy_backtest`` but uses module
    credentials (from env) and the existing ``DeploymentHelper`` — no engine.
    """
    from hiveq.flow.deploy_task import HiveQFlowBackTestTask, DeploymentHelper
    from hiveq.flow._client import TaskType

    _ensure_initialized()

    # NOTE: empty strategy_configs is allowed for the GLOBAL-DISPATCH form — a
    # module-level on_hiveq_event(ctx, event) the engine auto-discovers from the
    # captured frame. We validate that below (after capture). For the normal
    # per-strategy form, strategy_configs must name the strategy classes.
    strategy_configs = strategy_configs or []

    if backtest_config is None:
        backtest_config = BacktestConfig()
    if symbols is not None:
        backtest_config.symbols = symbols
    if start_date is not None:
        backtest_config.start_date = start_date
    if end_date is not None:
        backtest_config.end_date = end_date
    backtest_config.deploy = bool(publish)

    if not backtest_config.start_date:
        return {'status': 'error', 'message': 'start_date is required', 'task_id': None}
    if not backtest_config.end_date:
        return {'status': 'error', 'message': 'end_date is required', 'task_id': None}

    if task_name is None:
        if strategy_configs:
            names = '-'.join([s.name for s in strategy_configs[:3]])
        else:  # global on_hiveq_event form — name off the symbol universe
            names = 'on_hiveq_event-' + ('-'.join((backtest_config.symbols or [])[:3]) or 'global')
        task_name = f"bt-{names}-{backtest_config.start_date}-{backtest_config.end_date}"
        task_name = task_name.replace(' ', '_').replace('/', '-')[:100]

    kwargs.pop('test_mode', None)
    # Strip connection/identity from any EngineConfig in kwargs so the deployed
    # payload carries no client-frozen data URL — the executor supplies it.
    kwargs = _sanitize_kwargs_for_deploy(kwargs)

    # Capture the caller's strategy code AS SOURCE: the first-party import graph
    # + the entry script (or notebook __main__ definitions) + adjacent config
    # files, packed into a source archive. The executor recompiles it under its
    # own interpreter — no user code is cloudpickled.
    user_object_names, source_bundle = DeploymentHelper.capture_calling_module(strategy_configs)

    # Empty strategy_configs is valid ONLY for the global-dispatch form: a
    # module-level on_hiveq_event the engine auto-discovers. Otherwise there's
    # nothing to run.
    if not strategy_configs and 'on_hiveq_event' not in user_object_names:
        return {'status': 'error',
                'message': "strategy_configs is required, or define a module-level "
                           "on_hiveq_event(ctx, event) for global dispatch",
                'task_id': None}

    # Source is the only carrier of user code now — if we couldn't capture it,
    # the deploy cannot run on the engine, so fail loudly here.
    if not source_bundle:
        return {'status': 'error',
                'message': "Could not capture strategy source to deploy. Define your "
                           "strategy in an importable .py file (or a normally-defined "
                           "notebook cell whose source inspect can read).",
                'task_id': None}

    task = HiveQFlowBackTestTask(
        trader_id=_trader_id,
        api_key=_api_key,
        config_params=_deploy_config_params(),
        strategy_configs=[s.to_dict() for s in strategy_configs],
        symbols=backtest_config.symbols,
        start_date=backtest_config.start_date,
        end_date=backtest_config.end_date,
        data_configs=data_configs,
        backtest_config=backtest_config.to_dict() if hasattr(backtest_config, 'to_dict') else None,
        kwargs=kwargs,
        source_bundle=source_bundle,
    )

    run_config = {
        'trader_id': _trader_id,
        'strategies': [s.name for s in strategy_configs],
        'start_date': backtest_config.start_date,
        'end_date': backtest_config.end_date,
        'symbols': backtest_config.symbols,
        'task_name': task_name,
    }

    return DeploymentHelper.submit(
        task=task,
        task_type=TaskType.HIVEQ_FLOW_BT,
        task_name=task_name,
        metadata={
            'trader_id': _trader_id,
            'strategies': [s.name for s in strategy_configs],
            'start_date': backtest_config.start_date,
            'end_date': backtest_config.end_date,
            'symbols': backtest_config.symbols,
        },
        requirements=['hiveq-flow'],
        allow_duplicate=allow_duplicate,
        duplicate_action=duplicate_action,
        run_config=run_config,
    )


__all__ = [
    # auth
    "login",
    # function registry
    "push_function",
    "run_function",
    "load_function",
    "list_functions",
    "function_versions",
    "get_function_source",
    "delete_function",
    # deploy + observe
    "run_backtest",
    "deploy_job",
    "Job",
    "Schedule",
    "ScheduleFrequency",
    "terminate",
    "upload_files",
    "list_files",
    "delete_files",
    "get_run",
    "event_logs",
    "config",
    # config DTOs + enums
    "BacktestConfig",
    "StrategyConfig",
    "EngineConfig",
    "AssetType",
    "DataType",
    "ExecutionMode",
    "EventType",
    "EventLogType",
    # authoring types (stubs)
    "Context",
    "Event",
    "Order",
    "Fill",
    "Position",
    "Portfolio",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "MarketCenter",
    # logging
    "logger",
]

__version__ = "0.3.6"
