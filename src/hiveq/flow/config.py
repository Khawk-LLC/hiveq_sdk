"""Configuration classes and enums for hiveq.flow API."""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

# Client config: ONE well-known place — ~/.hiveq/.env — like ~/.aws/credentials.
# Put HIVEQ_API_KEY (and, for non-default endpoints, HIVEQ_BASE_URL / HIVEQ_DATA_URL)
# there once and every run finds it, from any working directory.
#
#   HIVEQ_API_KEY=...
#   HIVEQ_BASE_URL=http://localhost:5010     # optional (platform)
#   HIVEQ_DATA_URL=http://localhost:80        # optional (runs gateway)
#
# We load HIVEQ_* keys from that file WITHOUT overriding the environment (env
# always wins), and NEVER load identity (org/user) — the platform resolves that
# from the key at submit time. (The old code auto-loaded a repo .env with
# override=True and walked up the tree, pulling a different environment's
# staging org into local runs → 403. That is intentionally gone.)
HIVEQ_CREDS_FILE = os.path.join(os.path.expanduser("~"), ".hiveq", ".env")
_WELL_KNOWN_SKIP = {"HIVEQ_ORG_ID", "HIVEQ_USER_ID", "HIVEQ_USER_NAME"}


def _load_well_known_env() -> None:
    if not os.path.isfile(HIVEQ_CREDS_FILE):
        return
    try:
        with open(HIVEQ_CREDS_FILE) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                if (_k.startswith("HIVEQ_") and _k not in _WELL_KNOWN_SKIP
                        and _k not in os.environ):
                    os.environ[_k] = _v.strip().strip('"').strip("'")
    except OSError:
        pass


_load_well_known_env()


_DEFAULT_AUTH_URL = "https://staging.hiveq.ai"


def platform_origin() -> str:
    """The HiveQ platform's ``scheme://host``, derived from ``HIVEQ_AUTH_URL``.

    ``HIVEQ_AUTH_URL`` is the single host users configure (the sign-in host).
    When the orchestrator / data URLs aren't set explicitly, they follow this
    host so one setting points the whole SDK at the right platform (local, LAN,
    or hosted). Defaults to ``https://staging.hiveq.ai`` when unset.
    """
    url = os.environ.get("HIVEQ_AUTH_URL")
    if not url:
        return _DEFAULT_AUTH_URL
    from urllib.parse import urlparse
    parsed = urlparse(url if "://" in url else "http://" + url)
    if not parsed.netloc:
        return _DEFAULT_AUTH_URL
    return f"{parsed.scheme}://{parsed.netloc}"


def _fetch_user_info_from_api_key():
    """
    Fetch user info from auth service using API key.

    If only HIVEQ_API_KEY is set, this function queries the auth service
    to get user_id, org_id, and user_name, and sets them as environment variables.
    """
    api_key = os.environ.get('HIVEQ_API_KEY')
    if not api_key:
        return

    # Skip if user info is already set
    if (os.environ.get('HIVEQ_USER_ID') and
        os.environ.get('HIVEQ_ORG_ID') and
        os.environ.get('HIVEQ_USER_NAME')):
        return

    try:
        import requests

        # Try multiple auth URLs: custom env var, then local, then staging
        auth_urls = [
            os.environ.get('HIVEQ_AUTH_URL'),  # Custom override
            'http://localhost:3001',            # Local dev
            'http://localhost:8080',            # Local via nginx
            'https://staging.hiveq.ai',         # Staging
        ]
        # Remove None values
        auth_urls = [u for u in auth_urls if u]

        for auth_url in auth_urls:
            url = f'{auth_url}/api/auth/verify-api-key'

            try:
                resp = requests.post(url, json={'apiKey': api_key}, timeout=5)

                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('success') and data.get('data', {}).get('valid'):
                        user = data['data'].get('user')
                        if user:
                            # Set environment variables from user info
                            if user.get('hiveqUserId') and not os.environ.get('HIVEQ_USER_ID'):
                                os.environ['HIVEQ_USER_ID'] = user['hiveqUserId']
                            if user.get('orgId') and not os.environ.get('HIVEQ_ORG_ID'):
                                os.environ['HIVEQ_ORG_ID'] = user['orgId']
                            if user.get('name') and not os.environ.get('HIVEQ_USER_NAME'):
                                os.environ['HIVEQ_USER_NAME'] = user['name']
                            return  # Success - exit
            except requests.exceptions.RequestException:
                continue
    except Exception:
        pass


# Identity (org / user) is NOT auto-resolved at import. On deploy, the platform
# resolves it from the API key at submit time. For an in-process engine run, the
# engine resolves it from the key on demand (see
# SigmaConfigBuilder.validate_hiveq_api_access). Auto-resolving at import would
# risk picking up the key's identity from the wrong environment.


class AssetType(Enum):
    """Supported asset types for trading."""
    EQUITY = 'EQUITY'
    OPTIONS = 'OPTIONS'
    FUTURES = 'FUTURES'
    CRYPTO = 'CRYPTO'
    INDEX = 'INDEX'


class DataType(Enum):
    """Types of market data for subscriptions."""
    BAR = "BAR"
    BAR_1_MIN = "BAR_1_MIN"
    BAR_5_MIN = "BAR_5_MIN"
    BAR_15_MIN = "BAR_15_MIN"
    BAR_30_MIN = "BAR_30_MIN"
    BAR_1_HOUR = "BAR_1_HOUR"
    BAR_1_DAY = "BAR_1_DAY"
    TICK = "TICK"
    QUOTE = "QUOTE"


class EventType(Enum):
    """Event types that can trigger strategy callbacks."""
    BAR = "BAR"
    BAR_1_MIN = "BAR_1_MIN"
    BAR_5_MIN = "BAR_5_MIN"
    BAR_15_MIN = "BAR_15_MIN"
    BAR_30_MIN = "BAR_30_MIN"
    BAR_1_HOUR = "BAR_1_HOUR"
    BAR_1_DAY = "BAR_1_DAY"
    TICK = "TICK"
    TRADE = "TRADE"
    QUOTE = "QUOTE"
    SNAP = "SNAP"
    START = "START"
    STOP = "STOP"
    # Order events
    ORDER = "ORDER"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_CANCELED = "ORDER_CANCELED"
    ORDER_DENIED = "ORDER_DENIED"
    ORDER_EMULATED = "ORDER_EMULATED"
    ORDER_EXPIRED = "ORDER_EXPIRED"
    ORDER_INITIALIZED = "ORDER_INITIALIZED"
    ORDER_PENDING_CANCEL = "ORDER_PENDING_CANCEL"
    ORDER_PENDING_UPDATE = "ORDER_PENDING_UPDATE"
    ORDER_UPDATED = "ORDER_UPDATED"
    ORDER_TRIGGERED = "ORDER_TRIGGERED"
    ORDER_RELEASED = "ORDER_RELEASED"
    ORDER_CANCEL_REJECTED = "ORDER_CANCEL_REJECTED"
    ORDER_MODIFY_REJECTED = "ORDER_MODIFY_REJECTED"
    # Position events
    POSITION = "POSITION"
    POSITION_OPENED = "POSITION_OPENED"
    POSITION_CHANGED = "POSITION_CHANGED"
    POSITION_CLOSED = "POSITION_CLOSED"
    # Custom data events
    CUSTOM_DATA = "CUSTOM_DATA"
    TIMER = "TIMER"
    # Index events
    INDEX_PRICE = "INDEX_PRICE"
    # Rollover events
    ROLLOVER = "ROLLOVER"
    # Executor events (Sigma-specific)
    EXECUTOR_EVENT = "EXECUTOR_EVENT"
    # Security events (Sigma-specific: rollover, open, close, etc.)
    SECURITY_EVENT = "SECURITY_EVENT"


class EventLogType(Enum):
    """Event log types for logging strategy events."""
    POSITION = "POSITION"
    ORDER = "ORDER"
    FILL = "FILL"
    CUSTOM_DATA = "CUSTOM_DATA"
    USER_LOG = "USER_LOG"
    ENTRY_TRADE = "ENTRY_TRADE"
    EXIT_TRADE = "EXIT_TRADE"
    PARAM_CHANGE = "PARAM_CHANGE"


class ExecutionMode(Enum):
    """Execution modes for running strategies."""
    BACKTEST = "backtest"
    LIVE = "live"


class OMSType(Enum):
    """Supported OMS types."""
    SIGMA = "SIGMA"


class RunState(Enum):
    STARTING = 'STARTING'
    STARTED = 'STARTED'
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    ERROR = 'ERROR'
    TERMINATED = 'TERMINATED'
    DONE = 'DONE'


# =============================================================================
# Market Session Defaults (all times in configured timezone, default EST)
# =============================================================================
# These are internal defaults used when building OMS configs.
# Users set session_start/session_end in BacktestConfig for data filtering.
# These defaults define actual market trading hours for order simulation.

@dataclass
class MarketSessionDefaults:
    """Default market session times for different asset types.

    All times are in the configured timezone (default: America/New_York / EST).
    These are used internally by OMS adapters for proper market simulation.
    """
    # Equity order acceptance window (pre-market to after-hours)
    # Starts at pre-market open so orders placed during pre-market are held in book
    equity_session_start: str = "04:00"
    equity_session_end: str = "20:00"

    # Equity extended hours
    equity_pre_market_start: str = "04:00"
    equity_after_hours_end: str = "20:00"

    # Equity auction cutoffs
    equity_open_auction_cutoff: str = "09:28"    # MOO orders must be in by this time
    equity_close_auction_cutoff: str = "15:55"   # MOC orders must be in by this time

    # Futures session (CME Globex - overnight session)
    futures_session_start: str = "18:00"   # 6 PM ET (previous day)
    futures_session_end: str = "17:00"     # 5 PM ET (trading day)

    # Futures daily settlement
    futures_settlement_time: str = "16:00"  # 4 PM ET

    # Index calculation hours (for reference data like SPX, VIX)
    index_calc_start: str = "09:30"
    index_calc_end: str = "16:00"


# Global default instance - used by OMS adapters
DEFAULT_MARKET_SESSIONS = MarketSessionDefaults()


@dataclass
class EngineConfig:
    """Configuration for the trading engine.

    Parameters
    ----------
    oms : str
        Order Management System type. Default: "SIGMA"
    timezone : str, optional
        Timezone for local time display and session time interpretation.
        If None (default), auto-detected from system wallclock.
        Examples: 'America/New_York', 'Europe/London', 'Asia/Tokyo'
    params : dict
        Additional configuration parameters
    """

    oms: str = "SIGMA"
    timezone: Optional[str] = None  # Auto-detected if None
    params: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Set default values if not provided in params"""
        # Validate timezone if provided
        if self.timezone is not None:
            from hiveq.flow.utils.timezone_utils import validate_timezone
            if not validate_timezone(self.timezone):
                raise ValueError(f"Invalid timezone '{self.timezone}'. Use IANA timezone names like 'America/New_York'.")

        # Read from environment variables with fallback to defaults
        defaults = {
            'oms_console_log': False,  # Default: disable OMS console logging
            'base_data_path': '',
            'hiveq_api_key': os.environ.get('HIVEQ_API_KEY', 'test_api_key_abcdef123456'),
            'hiveq_data_url': os.environ.get('HIVEQ_DATA_URL', 'https://staging.hiveq.ai/'),
            'hiveq_data_publish': True,
            # User/org identifiers for SDK client headers (Historical, Instrument provider, etc.)
            # Note: For published data records, user_id/org_id/user_name are injected by the publisher
            'hiveq_user_id': os.environ.get('HIVEQ_USER_ID'),
            'hiveq_org_id': os.environ.get('HIVEQ_ORG_ID'),
            'hiveq_user_name': os.environ.get('HIVEQ_USER_NAME'),
            # Configurable dataset lists for asset type detection
            # These datasets will be treated as futures (enabling contract resolution, rollover events)
            'futures_datasets': ['HIVEQ_US_FUT'],
            # These datasets will use config['symbols'] instead of run_config.symbols
            'signals_datasets': ['HIVEQ_QUANT_SIGNALS'],
            # Max records per API request for pagination (e.g., SPXW options can have ~4M records/day)
            'hiveq_data_page_size': 100_000,
        }
        # Merge defaults with provided params (params take precedence)
        for key, value in defaults.items():
            if key not in self.params:
                self.params[key] = value

    def __getattr__(self, name):
        """Allow accessing params as attributes"""
        if 'params' in self.__dict__ and name in self.__dict__['params']:
            return self.__dict__['params'][name]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        """Allow setting params as attributes"""
        if name in ('oms', 'timezone', 'params'):
            super().__setattr__(name, value)
        else:
            # Store in params if not a class attribute
            if 'params' not in self.__dict__:
                super().__setattr__('params', {})
            self.params[name] = value

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert EngineConfig to dictionary format.

        Returns:
            dict: Configuration as dictionary
        """
        return {
            'oms': self.oms,
            'timezone': self.timezone,
            'params': self.params.copy()
        }


@dataclass
class BacktestConfig:
    """Configuration for backtesting parameters.

    This class defines all parameters needed to run a backtest, including
    trading universe, time period, capital, costs, and risk metrics.

    Parameters
    ----------
    id : str, optional
        Unique identifier for this backtest run. Auto-generated if not provided.
    symbols : list[str], optional
        List of symbols to trade in the backtest. Can be overridden by
        individual strategy configs.
    start_date : str, optional
        Backtest start date in 'YYYY-MM-DD' format (e.g., '2023-01-01').
    end_date : str, optional
        Backtest end date in 'YYYY-MM-DD' format (e.g., '2023-12-31').
    initial_capital : float, optional
        Starting capital for the backtest in USD. Default: 1,000,000.
    commission : float, optional
        Commission rate as a decimal (e.g., 0.001 = 0.1% or 10 bps). Default: 0.001.
    slippage : float, optional
        Slippage rate as a decimal. Default: 0.0 (no slippage).
    data_feed : str, optional
        Data feed type identifier. Default: "historical".
    venue : str, optional
        Trading venue name. Default: "SIM" (simulated exchange).
    deploy : bool, optional
        Whether to deploy this backtest remotely. Default: False.
    benchmark : str, optional
        Benchmark symbol for performance comparison (e.g., 'SPY'). Default: None.
    rebalance_frequency : str, optional
        Portfolio rebalancing frequency. Default: None.
    risk_free_rate : float, optional
        Risk-free rate for Sharpe ratio calculation as annual rate. Default: 0.02 (2%).
    extra_config : dict, optional
        Additional custom configuration parameters.
    equity_fee : float, optional
        Fee per share for equity trades. Default: 0.0011 (0.11 bps).
    futures_fee : float, optional
        Fee per contract for futures trades. Default: 0.5.
    crypto_fee : float, optional
        Fee per unit for crypto trades. Default: 0.00005.

    Attributes
    ----------
    id : str or None
        Backtest run identifier
    symbols : list[str] or None
        Trading symbols
    start_date : str or None
        Start date
    end_date : str or None
        End date
    initial_capital : float
        Starting capital
    commission : float
        Commission rate
    slippage : float
        Slippage rate
    data_feed : str
        Data feed type
    venue : str
        Trading venue
    deploy : bool
        Deploy flag
    benchmark : str or None
        Benchmark symbol
    rebalance_frequency : str or None
        Rebalancing frequency
    risk_free_rate : float
        Risk-free rate
    extra_config : dict
        Extra configuration
    equity_fee : float
        Equity fee per share
    futures_fee : float
        Futures fee per contract
    crypto_fee : float
        Crypto fee per unit

    Examples
    --------
    >>> from hiveq.flow import BacktestConfig
    >>>
    >>> # Simple backtest config
    >>> config = BacktestConfig(
    ...     symbols=['AAPL', 'GOOGL'],
    ...     start_date='2023-01-01',
    ...     end_date='2023-12-31',
    ...     initial_capital=100000.0
    ... )
    >>>
    >>> # Detailed backtest config with costs
    >>> config = BacktestConfig(
    ...     symbols=['AAPL', 'MSFT', 'GOOGL'],
    ...     start_date='2023-01-01',
    ...     end_date='2023-12-31',
    ...     initial_capital=500000.0,
    ...     commission=0.002,  # 0.2% commission
    ...     slippage=0.0005,   # 0.05% slippage
    ...     benchmark='SPY',
    ...     risk_free_rate=0.045  # 4.5% annual rate
    ... )

    Notes
    -----
    - All rates (commission, slippage, risk_free_rate) are expressed as decimals
    - Commission is applied per trade
    - Risk-free rate is annual, used for Sharpe ratio calculation
    """
    id: Optional[str] = None
    symbols: list = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    initial_capital: float = 1000000.0
    commission: float = 0.001
    slippage: float = 0.0
    data_feed: str = "historical"
    venue: str = "SIM"
    deploy: bool = False
    benchmark: Optional[str] = None
    rebalance_frequency: Optional[str] = None
    risk_free_rate: float = 0.02
    extra_config: Dict[str, Any] = field(default_factory=dict)

    # Fee model parameters (used for MixedFeeModel)
    equity_fee: float = 0.0011  # 0.11 bps per share
    futures_fee: float = 0.5    # per contract
    crypto_fee: float = 0.00005  # per unit

    # Publisher configuration (new)
    batch_publish_interval_minutes: float = 1.0  # Periodic publish for backtest (wall-clock)
    metrics_publish_interval_minutes: float = 1440.0  # Metrics publish for backtest (1 day)
    metrics_publish_interval_seconds: int = 5  # Metrics publish for live/livesim
    async_queue_max_size: int = 10000  # Queue size for async publisher
    enable_realtime_metrics: bool = True  # Enable realtime metrics calculation
    publish_only_changed_metrics: bool = True  # Only publish metrics if they changed

    # Session configuration for day-by-day execution
    # If None, uses calendar day boundaries (midnight to midnight)
    # Times are in US/Eastern (EST/EDT). Converted to UTC internally.
    # For futures: session_start='18:00', session_end='17:00' (CME Globex 6PM-5PM ET)
    session_start: Optional[str] = None  # Session start time in HH:MM format ET (e.g., '18:00')
    session_end: Optional[str] = None    # Session end time in HH:MM format ET (e.g., '17:00')

    # Export orders CSV to ~/.tmp/<run_id>_orders.csv after backtest
    export_orders_csv: bool = False

    # Futures auto-rollover: auto-roll positions when continuous contracts roll
    # When True, injects filter_mode='continuous' into futures data_configs
    # and sets enableFuturesRollover=True in strategy params automatically
    enable_auto_rollover: bool = False

    # Transaction Cost Analysis
    enable_tca: bool = False  # Run TCA analysis on fills/orders after backtest

    # Payload ID for tracking code versions (set by platform executor)
    payload_id: Optional[str] = None

    # Run ID for correlating all published data (Python + C++) under one key
    run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert BacktestConfig to dictionary format.

        Returns:
            dict: Configuration as dictionary with all fields
        """
        return {
            'id': self.id,
            'symbols': self.symbols if self.symbols else [],
            'start_date': self.start_date,
            'end_date': self.end_date,
            'initial_capital': self.initial_capital,
            'commission': self.commission,
            'slippage': self.slippage,
            'data_feed': self.data_feed,
            'venue': self.venue,
            'deploy': self.deploy,
            'benchmark': self.benchmark,
            'rebalance_frequency': self.rebalance_frequency,
            'risk_free_rate': self.risk_free_rate,
            'equity_fee': self.equity_fee,
            'futures_fee': self.futures_fee,
            'crypto_fee': self.crypto_fee,
            'batch_publish_interval_minutes': self.batch_publish_interval_minutes,
            'metrics_publish_interval_minutes': self.metrics_publish_interval_minutes,
            'metrics_publish_interval_seconds': self.metrics_publish_interval_seconds,
            'async_queue_max_size': self.async_queue_max_size,
            'enable_realtime_metrics': self.enable_realtime_metrics,
            'publish_only_changed_metrics': self.publish_only_changed_metrics,
            'session_start': self.session_start,
            'session_end': self.session_end,
            'enable_auto_rollover': self.enable_auto_rollover,
            'payload_id': self.payload_id,
            'extra_config': self.extra_config.copy() if self.extra_config else {}
        }


@dataclass
class LiveConfig:
    """Configuration for live trading.

    Parameters
    ----------
    venue : str
        Trading venue (e.g., 'XCME', 'NASDAQ')
    broker : str
        Execution broker/client type (e.g., 'sandbox', 'interactive_brokers')
    account_id : str
        Trading account identifier
    trader_id : str, optional
        Trader identifier. If not provided, will use App's trader_id
    starting_balances : list[str], optional
        Starting balances (e.g., ['1000000 USD'])
    base_currency : str, optional
        Base currency for the account. Default: 'USD'
    api_key : str, optional
        API key for broker authentication
    api_secret : str, optional
        API secret for broker authentication
    paper_trading : bool, optional
        Whether to use paper trading mode. Default: True
    risk_limits : dict, optional
        Risk management limits
    order_timeout : int, optional
        Order timeout in seconds. Default: 30
    heartbeat_interval : int, optional
        Heartbeat interval in seconds. Default: 60
    timeout_connection : float, optional
        Connection timeout in seconds. Default: 30.0
    timeout_reconciliation : float, optional
        Reconciliation timeout in seconds. Default: 5.0
    timeout_portfolio : float, optional
        Portfolio timeout in seconds. Default: 5.0
    timeout_disconnection : float, optional
        Disconnection timeout in seconds. Default: 10.0
    timeout_post_stop : float, optional
        Post-stop timeout in seconds. Default: 2.0
    reconciliation : bool, optional
        Enable reconciliation. Default: False
    equity_fee : float, optional
        Equity commission per share. Default: 0.0011 (0.11 bps)
    futures_fee : float, optional
        Futures commission per contract. Default: 0.5
    crypto_fee : float, optional
        Crypto commission per unit. Default: 0.00005
    extra_config : dict, optional
        Additional configuration parameters
    instance_id : str, optional
        Unique-within-cluster identifier for this live/livesim container.
        Named to match Sigma's C++ ``instanceID`` (same concept as the old
        sigma ``instanceID=RM`` tag). Becomes the Kafka partition key for
        run_meta envelopes and Sigma's instanceID on the C++ side, so the
        same value correlates logs, dashboards, and telemetry for a given
        container across restarts. If omitted, the sigma config builder
        auto-derives ``f'{trader_id[:20]}-LIVESIM'``.
    """

    venue: str
    broker: str
    account_id: str
    trader_id: Optional[str] = None
    instance_id: Optional[str] = None
    initial_capital: float = 1000000.0
    starting_balances: List[str] = field(default_factory=lambda: ["1000000 USD"])
    base_currency: str = "USD"
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    paper_trading: bool = True
    risk_limits: Dict[str, float] = field(
        default_factory=lambda: {
            "max_position_size": 0.1,
            "max_daily_loss": 0.02,
            "max_leverage": 1.0,
        }
    )
    order_timeout: int = 30
    heartbeat_interval: int = 60
    timeout_connection: float = 30.0
    timeout_reconciliation: float = 5.0
    timeout_portfolio: float = 5.0
    timeout_disconnection: float = 10.0
    timeout_post_stop: float = 2.0
    reconciliation: bool = False
    equity_fee: float = 0.0011  # 0.11 bps per share
    futures_fee: float = 0.5    # per contract
    crypto_fee: float = 0.00005  # per unit
    extra_config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert LiveConfig to dictionary format.

        Returns:
            dict: Configuration as dictionary with all fields
        """
        return {
            'venue': self.venue,
            'broker': self.broker,
            'account_id': self.account_id,
            'trader_id': self.trader_id,
            'instance_id': self.instance_id,
            'starting_balances': self.starting_balances.copy() if self.starting_balances else [],
            'base_currency': self.base_currency,
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'paper_trading': self.paper_trading,
            'risk_limits': self.risk_limits.copy() if self.risk_limits else {},
            'order_timeout': self.order_timeout,
            'heartbeat_interval': self.heartbeat_interval,
            'timeout_connection': self.timeout_connection,
            'timeout_reconciliation': self.timeout_reconciliation,
            'timeout_portfolio': self.timeout_portfolio,
            'timeout_disconnection': self.timeout_disconnection,
            'timeout_post_stop': self.timeout_post_stop,
            'reconciliation': self.reconciliation,
            'equity_fee': self.equity_fee,
            'futures_fee': self.futures_fee,
            'crypto_fee': self.crypto_fee,
            'extra_config': self.extra_config.copy() if self.extra_config else {}
        }


class ObservableDict(dict):
    def __init__(self, *args, on_change: Optional[Callable] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_change = on_change

    def _trigger(self, *args, **kwargs):
        if self._on_change:
            self._on_change(*args, **kwargs)

    def __setitem__(self, key, value):
        old_value = self.get(key)
        super().__setitem__(key, value)
        self._trigger(event="set", key=key, old=old_value, new=value)

    def __delitem__(self, key):
        old_value = self[key]
        super().__delitem__(key)
        self._trigger(event="delete", key=key, old=old_value, new=None)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self.__setitem__(k, v)


@dataclass
class StrategyConfig:
    """
    Configuration for strategy parameters.

    This dataclass defines all configuration fields needed to instantiate
    and run a trading strategy, with reactive parameter updates.
    """
    name: str
    type: str
    symbols: Optional[List[str]] = None
    params: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        # Internal callbacks list
        object.__setattr__(self, "_callbacks", [])

        # Wrap params in an ObservableDict that triggers _notify
        observable_params = ObservableDict(self.params, on_change=self._notify)
        object.__setattr__(self, "params", observable_params)

    def __getattr__(self, item):
        if item in self.params:
            return self.params[item]
        raise AttributeError(f"{item} not found in StrategyConfig or params")

    def __setattr__(self, key, value):
        if key not in {"name", "type", "symbols", "params", "_callbacks"}:
            self.params[key] = value  # triggers callback automatically
        else:
            super().__setattr__(key, value)

    def register_callback(self, fn: Callable):
        self._callbacks.append(fn)

    def _notify(self, **event):
        for cb in self._callbacks:
            cb(event)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "symbols": self.symbols or [],
            "params": dict(self.params)
        }


def can_publish(execution_mode: ExecutionMode, backtest_config: Optional[BacktestConfig] = None) -> bool:
    """
    Check if publishing to message bus / data API is enabled.

    Publishing behavior:
    - BACKTEST mode: Only publish if deploy=True in backtest_config
    - LIVE mode: Always publish (paper_trading flag in LiveConfig controls paper vs real)

    Args:
        execution_mode: Current execution mode
        backtest_config: Backtest configuration (required for BACKTEST mode)

    Returns:
        bool: True if publishing is enabled, False otherwise
    """
    if execution_mode == ExecutionMode.BACKTEST:
        return backtest_config is not None and backtest_config.deploy
    # LIVE mode always publishes
    return True


# ============================================================
# Instrument Registry - Global Singleton
# ============================================================

_instrument_registry = None


def initialize_instrument_registry(engine):
    """
    Initialize the global instrument registry with the trading engine.

    This should be called once during engine initialization in app.py.

    Parameters
    ----------
    engine : object
        The trading engine (BacktestEngine or TradingNode)
    """
    global _instrument_registry
    from hiveq.flow.instrument_registry import InstrumentRegistry

    if _instrument_registry is None:
        _instrument_registry = InstrumentRegistry(engine)
    else:
        _instrument_registry.initialize(engine)


def get_instrument_registry():
    """
    Get the global instrument registry instance.

    Returns
    -------
    InstrumentRegistry
        The global instrument registry

    Raises
    ------
    RuntimeError
        If registry has not been initialized
    """
    global _instrument_registry

    if _instrument_registry is None:
        from hiveq.flow.instrument_registry import InstrumentRegistry
        _instrument_registry = InstrumentRegistry()

    return _instrument_registry
