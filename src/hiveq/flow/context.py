"""
Context class for strategy execution.
"""
from typing import Optional, List
from enum import Enum
from datetime import datetime

from .config import AssetType


class OrderState(Enum):
    """Order state enumeration for tracking order lifecycle."""
    INITIALIZED = "INITIALIZED"
    SUBMITTED = "SUBMITTED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    DENIED = "DENIED"
    EMULATED = "EMULATED"
    EXPIRED = "EXPIRED"
    PENDING_CANCEL = "PENDING_CANCEL"
    PENDING_UPDATE = "PENDING_UPDATE"
    UPDATED = "UPDATED"
    TRIGGERED = "TRIGGERED"
    RELEASED = "RELEASED"
    CANCEL_REJECTED = "CANCEL_REJECTED"
    MODIFY_REJECTED = "MODIFY_REJECTED"


class Context:
    """Strategy execution context — public type alias.

    The actual context object passed to strategy callbacks is the Sigma
    context (``hiveq.flow.oms.sigma.sigma_context.SigmaContext``). This
    class exists as a public name for type hints in user strategies; do
    not instantiate it directly.

    Examples
    --------
    >>> def on_bar(ctx: Context, bar):
    ...     if ctx.is_flat("AAPL"):
    ...         ctx.buy_order("AAPL", quantity=100)
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "Context is a type alias for the runtime SigmaContext. "
            "Strategies receive a context instance from the engine; "
            "do not construct Context directly."
        )


class PrefetchContext:
    """Lightweight context for prefetch phase - captures subscriptions without executing.

    This context is used during the prefetch phase of day-by-day backtesting to
    capture what symbols a strategy will subscribe to, without requiring a full
    engine or data to be loaded. The captured subscriptions are then used to
    load only the necessary data for each day.

    Parameters
    ----------
    current_date : datetime
        The date being prefetched for
    strategy_config : StrategyConfig, optional
        The strategy configuration (needed by strategies that access config during on_start)

    Examples
    --------
    Used internally by the framework:

    >>> ctx = PrefetchContext(current_date=pd.Timestamp('2024-01-15'))
    >>> strategy.on_start(ctx, start_event)
    >>> symbols = ctx.get_subscribed_symbols()
    """

    def __init__(self, current_date: datetime, strategy_config=None, trading_day: str = None):
        self._current_date = current_date
        self._strategy_config = strategy_config
        self._trading_day = trading_day  # Trading day as YYYY-MM-DD string
        self._subscribed_symbols: List[str] = []
        self._subscribed_bars: List[tuple] = []  # (symbols, asset_type, interval)
        self._subscribed_option_snaps: List[dict] = []  # option snap subscriptions with filters
        self._subscribed_index: List[str] = []  # index symbols
        self._subscribed_data: List[dict] = []  # custom data subscriptions with signal filters

    def subscribe_bars(self, symbols: List[str], asset_type=None, interval: str = '1m', **kwargs):
        """Capture bar subscription request without executing."""
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)
        self._subscribed_bars.append((symbols, asset_type, interval))

    def subscribe_trades(self, symbols: List[str], asset_type=None, **kwargs):
        """Capture trade subscription request without executing."""
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)

    def subscribe_quotes(self, symbols: List[str], asset_type=None, **kwargs):
        """Capture quote subscription request without executing."""
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)

    def subscribe_tbbo(self, symbols: List[str], asset_type=None, **kwargs):
        """Capture TBBO subscription request without executing."""
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)

    def subscribe_data(self, data_id: str = None, signals: List[str] = None, **kwargs):
        """Capture custom data subscription with signal filter.

        Parameters
        ----------
        data_id : str
            Identifier for the custom data source.
        signals : List[str], optional
            List of signal symbols to fetch from the API.
        """
        self._subscribed_data.append({
            'data_id': data_id,
            'signals': signals or []
        })

    def subscribe_option_snaps(self,
                               symbol: str,
                               option_type: Optional[str] = None,
                               strike: Optional[float] = None,
                               expiration_type: Optional[str] = None,
                               underlying: Optional[str] = None,
                               interval: str = "1s"):
        """Capture option snap subscription request with filters.

        Parameters
        ----------
        symbol : str
            Root option symbol to subscribe to (e.g., "SPXW", "SPY")
        option_type : str, optional
            Filter by option type: 'C', 'P', 'CALL', or 'PUT'
        strike : float, optional
            Specific strike price to filter
        expiration_type : str, optional
            Filter by expiration: '0dte' for same-day, or 'YYYY-MM-DD'
        underlying : str, optional
            Underlying symbol to filter by
        interval : str, optional
            Snapshot interval. Default: "1s"
        """
        self._subscribed_symbols.append(symbol)
        # Store the full subscription details for passing to data provider
        self._subscribed_option_snaps.append({
            'symbol': symbol,
            'option_type': option_type,
            'strike': strike,
            'expiration_type': expiration_type,
            'underlying': underlying,
            'interval': interval
        })

    def subscribe_index(self, symbols: List[str], **kwargs):
        """Capture index subscription request without executing."""
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)  # Add to general symbols list for prefetch
        self._subscribed_index.extend(symbols)

    def subscribe_index_bars(self, symbols: List[str], interval: str = '1d', **kwargs):
        """Capture index bar subscription request without executing.

        Subscribes to daily bar data (OHLCV) for indices like VIX.
        Unlike subscribe_index() which returns IndexPrice objects,
        this returns full bar data (open, high, low, close, volume).

        NOTE: Indices only support daily bars, not intraday bars.

        Parameters
        ----------
        symbols : list[str]
            List of index symbols to subscribe to (e.g., ["VIX", "SPX"])
        interval : str, optional
            Bar interval. Default: "1d" (daily). Indices only support daily bars.
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)
        self._subscribed_bars.append((symbols, AssetType.INDEX, interval))

    def subscribe_futures_bars(self,
                               symbols: Optional[List[str]] = None,
                               root: Optional[str] = None,
                               contract: Optional[str] = None,
                               continuous: Optional[str] = None,
                               interval: str = "1m"):
        """Capture futures bar subscription request without executing.

        Parameters
        ----------
        symbols : list[str], optional
            List of specific futures symbols to subscribe to.
        root : str, optional
            Futures root symbol (e.g., "ES", "NQ", "CL").
        contract : str, optional
            Specific contract month code (e.g., "H25" for March 2025).
        continuous : str, optional
            Continuous contract notation (e.g., "ES.c.0", "ES.v.0").
        interval : str, optional
            Bar time interval. Default: "1m"
        """
        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            self._subscribed_symbols.extend(symbols)
            self._subscribed_bars.append((symbols, AssetType.FUTURES, interval))
        elif continuous:
            self._subscribed_symbols.append(continuous)
            self._subscribed_bars.append(([continuous], AssetType.FUTURES, interval))
        elif root and contract:
            symbol = f"{root}.{contract}"
            self._subscribed_symbols.append(symbol)
            self._subscribed_bars.append(([symbol], AssetType.FUTURES, interval))
        elif root:
            self._subscribed_symbols.append(root)
            self._subscribed_bars.append(([root], AssetType.FUTURES, interval))

    def subscribe_futures_trade_ticks(self,
                                      symbols: Optional[List[str]] = None,
                                      root: Optional[str] = None,
                                      contract: Optional[str] = None,
                                      continuous: Optional[str] = None):
        """Capture futures trade tick subscription request without executing.

        Parameters
        ----------
        symbols : list[str], optional
            List of specific futures symbols to subscribe to.
        root : str, optional
            Futures root symbol (e.g., "ES", "NQ", "CL").
        contract : str, optional
            Specific contract month code (e.g., "H25" for March 2025).
        continuous : str, optional
            Continuous contract notation (e.g., "ES.c.0", "ES.v.0").
        """
        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            self._subscribed_symbols.extend(symbols)
        elif continuous:
            self._subscribed_symbols.append(continuous)
        elif root and contract:
            self._subscribed_symbols.append(f"{root}.{contract}")
        elif root:
            self._subscribed_symbols.append(root)

    def subscribe_futures_tbbo(self,
                               symbols: Optional[List[str]] = None,
                               root: Optional[str] = None,
                               contract: Optional[str] = None,
                               continuous: Optional[str] = None):
        """Capture futures TBBO subscription request without executing.

        Parameters
        ----------
        symbols : list[str], optional
            List of specific futures symbols to subscribe to.
        root : str, optional
            Futures root symbol (e.g., "ES", "NQ", "CL").
        contract : str, optional
            Specific contract month code (e.g., "H25" for March 2025).
        continuous : str, optional
            Continuous contract notation (e.g., "ES.c.0", "ES.v.0").
        """
        if symbols:
            if isinstance(symbols, str):
                symbols = [symbols]
            self._subscribed_symbols.extend(symbols)
        elif continuous:
            self._subscribed_symbols.append(continuous)
        elif root and contract:
            self._subscribed_symbols.append(f"{root}.{contract}")
        elif root:
            self._subscribed_symbols.append(root)

    def subscribe_trade_ticks(self, symbols: List[str], asset_type=None, **kwargs):
        """Capture trade tick subscription request without executing.

        Parameters
        ----------
        symbols : list[str]
            List of trading symbols to subscribe to.
        asset_type : AssetType, optional
            Type of asset (EQUITY, FUTURES, OPTIONS).
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        self._subscribed_symbols.extend(symbols)

    def get_subscribed_symbols(self) -> List[str]:
        """Return deduplicated list of all subscribed symbols."""
        return list(set(self._subscribed_symbols))

    def get_subscribed_bars(self) -> List[tuple]:
        """Return list of bar subscription details."""
        return self._subscribed_bars

    def get_subscribed_option_snaps(self) -> List[dict]:
        """Return list of option snap subscription details with filters."""
        return self._subscribed_option_snaps

    def get_subscribed_index(self) -> List[str]:
        """Return list of index symbols."""
        return list(set(self._subscribed_index))

    def get_subscribed_data(self) -> List[dict]:
        """Return list of custom data subscriptions with signal filters."""
        return self._subscribed_data

    def now(self) -> datetime:
        """Return the prefetch date."""
        return self._current_date

    @property
    def trading_day(self) -> str:
        """Return the trading day as YYYY-MM-DD string.

        Use this to get the correct trading day instead of deriving it
        from event.ts_event, which may give incorrect results due to
        timezone conversions (e.g., midnight UTC converts to previous day in ET).
        """
        return self._trading_day

    def add_event_log(self, **kwargs):
        """No-op during prefetch."""
        pass

    def set_timer(self, timer_id: str, timer_interval, **kwargs):
        """No-op during prefetch - timers don't affect symbol collection."""
        pass

    def cancel_timer(self, timer_id: str):
        """No-op during prefetch."""
        pass

    def instrument(self, symbol: str):
        """Return a stub instrument for prefetch - just needs to be non-None."""
        # Return a simple object that can be stored but won't be used during prefetch
        class PrefetchInstrument:
            def __init__(self, sym):
                self.symbol = sym
        return PrefetchInstrument(symbol)

    def buy_order(self, symbol, quantity=None, **kwargs):
        """No-op during prefetch - orders are not executed during symbol collection."""
        pass

    def sell_order(self, symbol, quantity=None, **kwargs):
        """No-op during prefetch - orders are not executed during symbol collection."""
        pass

    def cancel_order(self, order_id: str) -> bool:
        """No-op during prefetch - orders are not executed during symbol collection."""
        return False

    # Property stubs for compatibility with strategies that access these
    @property
    def strategy_config(self):
        """Return the strategy config if provided during prefetch."""
        return self._strategy_config
