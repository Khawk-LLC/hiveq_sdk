"""Trading calendar utilities for date and session calculations.

This module provides utilities for calculating trading days and session boundaries
across different market locations. Currently only US markets are fully supported,
with other locations raising NotImplementedError for future implementation.

Example usage:
    >>> from hiveq.flow.utils.date_calendar import TradingCalendar, MarketLocation
    >>>
    >>> # Get trading days for US markets (default)
    >>> trading_days = TradingCalendar.get_trading_days('2025-01-01', '2025-01-31')
    >>>
    >>> # Get session boundaries for a trading day
    >>> start_str, end_str, start_ts = TradingCalendar.get_session_boundaries(
    ...     trading_day=pd.Timestamp('2025-01-15'),
    ...     session_start='09:30',  # Optional custom session
    ...     session_end='16:00'
    ... )
"""

import pandas as pd
from enum import Enum
from typing import Optional


class MarketLocation(Enum):
    """Supported market locations/countries.

    Used to specify which market's calendar and session times to use.
    Currently only US is fully implemented; other locations will raise
    NotImplementedError.

    Attributes:
        US: United States - NYSE/NASDAQ
        UK: United Kingdom - LSE
        EU: European Union - Euronext
        JP: Japan - TSE
        HK: Hong Kong - HKEX
        AU: Australia - ASX
    """
    US = "US"       # United States - NYSE/NASDAQ
    UK = "UK"       # United Kingdom - LSE
    EU = "EU"       # European Union - Euronext
    JP = "JP"       # Japan - TSE
    HK = "HK"       # Hong Kong - HKEX
    AU = "AU"       # Australia - ASX


class TradingCalendar:
    """Trading calendar utilities supporting multiple market locations.

    Provides methods for calculating trading days (excluding weekends and
    market-specific holidays) and session boundaries for different markets.

    Currently only US markets are fully implemented. Other locations will
    raise NotImplementedError with guidance on future support.

    Example:
        >>> # Get US trading days
        >>> days = TradingCalendar.get_trading_days('2025-01-01', '2025-01-31')
        >>> len(days)
        21

        >>> # Get session boundaries with custom session times (e.g., futures)
        >>> start, end, ts = TradingCalendar.get_session_boundaries(
        ...     pd.Timestamp('2025-01-15'),
        ...     session_start='23:00',  # Futures session start
        ...     session_end='22:00'     # Next day
        ... )
    """

    # Default session times by location (UTC)
    # These represent typical regular trading hours for each market
    _DEFAULT_SESSIONS = {
        MarketLocation.US: {"start": "14:30", "end": "21:00"},   # 9:30 AM - 4:00 PM ET
        MarketLocation.UK: {"start": "08:00", "end": "16:30"},   # LSE hours
        MarketLocation.EU: {"start": "08:00", "end": "16:30"},   # Euronext hours
        MarketLocation.JP: {"start": "00:00", "end": "06:00"},   # TSE hours (UTC)
        MarketLocation.HK: {"start": "01:30", "end": "08:00"},   # HKEX hours (UTC)
        MarketLocation.AU: {"start": "00:00", "end": "06:00"},   # ASX hours (UTC)
    }

    @staticmethod
    def get_trading_days(
        start_date: str,
        end_date: str,
        location: MarketLocation = MarketLocation.US,
        use_calendar_days: bool = False
    ) -> list[pd.Timestamp]:
        """Get list of trading days between start and end dates.

        Calculates all valid trading days for the specified market,
        excluding weekends and market-specific holidays.

        Args:
            start_date: Start date string in 'YYYY-MM-DD' format
            end_date: End date string in 'YYYY-MM-DD' format
            location: Market location for holiday calendar (default: US)
            use_calendar_days: If True, return all calendar days (for futures
                which trade Sunday evening through Friday). Default: False.

        Returns:
            List of trading day timestamps

        Raises:
            ValueError: If start_date or end_date is not provided
            NotImplementedError: If location is not US (other markets not yet supported)

        Example:
            >>> days = TradingCalendar.get_trading_days('2025-01-01', '2025-01-10')
            >>> [d.strftime('%Y-%m-%d') for d in days]
            ['2025-01-02', '2025-01-03', '2025-01-06', '2025-01-07', '2025-01-08', '2025-01-09', '2025-01-10']
        """
        if not start_date:
            raise ValueError(
                "start_date is required for backtest. "
                "Please provide start_date in 'YYYY-MM-DD' format."
            )
        if not end_date:
            raise ValueError(
                "end_date is required for backtest. "
                "Please provide end_date in 'YYYY-MM-DD' format."
            )

        if location != MarketLocation.US:
            raise NotImplementedError(
                f"Trading calendar for {location.value} is not yet implemented. "
                f"Currently only MarketLocation.US is supported. "
                f"Please use location=MarketLocation.US or contribute support for {location.value}."
            )

        # Extract date-only parts for trading day calculation
        # Handles both 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM:SS' formats
        start_date_only = start_date.split(' ')[0] if ' ' in start_date else start_date
        end_date_only = end_date.split(' ')[0] if ' ' in end_date else end_date

        if use_calendar_days:
            # All calendar days — futures trade Sun 18:00 through Fri 17:00
            # Saturdays will have no data; the adapter handles empty days gracefully
            return pd.date_range(start=start_date_only, end=end_date_only, freq='D').tolist()

        # US market calendar using pandas holiday calendar
        from pandas.tseries.holiday import USFederalHolidayCalendar
        from pandas.tseries.offsets import CustomBusinessDay

        us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())
        return pd.date_range(start=start_date_only, end=end_date_only, freq=us_bd).tolist()

    @staticmethod
    def get_session_boundaries(
        trading_day: pd.Timestamp,
        session_start: Optional[str] = None,
        session_end: Optional[str] = None,
        location: MarketLocation = MarketLocation.US,
        timezone: Optional[str] = None
    ) -> tuple[str, str, pd.Timestamp]:
        """Get session start and end timestamps for a trading day.

        Calculates the exact session boundaries for a given trading day.
        Supports both calendar day sessions (midnight to midnight) and
        custom session times (e.g., futures sessions that span overnight).

        For calendar day sessions (default when session_start/end not provided):
            Returns midnight to midnight UTC boundaries.

        For custom sessions (e.g., futures 23:00 to 22:00):
            If end time <= start time (overnight session), session starts
            on the PREVIOUS calendar day and ends on trading_day.
            Example: Monday trading day with 23:00-22:00 -> Sunday 23:00 to Monday 22:00.
            If end time > start time (same-day session), both are on trading_day.

        Args:
            trading_day: The trading day (date only, used as session identifier)
            session_start: Optional custom session start time 'HH:MM' or 'HH:MM:SS'.
                          Interpreted in the specified timezone (or UTC if None).
                          If None, uses midnight UTC.
            session_end: Optional custom session end time 'HH:MM' or 'HH:MM:SS'.
                        Interpreted in the specified timezone (or UTC if None).
                        If None, uses midnight next day UTC.
            location: Market location (for future default session times support)
            timezone: Timezone for interpreting session_start/session_end times.
                     E.g., 'America/New_York' means session_start='09:30' is 9:30 AM ET.
                     If None, times are interpreted as UTC.

        Returns:
            Tuple of (session_start_str, session_end_str, session_start_ts):
            - session_start_str: Session start for engine run ('YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD') in UTC
            - session_end_str: Session end for engine run in UTC
            - session_start_ts: Timestamp for prefetch and StartEvent in UTC

        Example:
            >>> import pandas as pd
            >>> # Calendar day session
            >>> start, end, ts = TradingCalendar.get_session_boundaries(
            ...     pd.Timestamp('2025-01-15')
            ... )
            >>> start, end
            ('2025-01-15', '2025-01-16')

            >>> # Custom futures session in ET (overnight - starts previous day)
            >>> start, end, ts = TradingCalendar.get_session_boundaries(
            ...     pd.Timestamp('2025-01-15'),
            ...     session_start='18:00',  # 6 PM ET
            ...     session_end='17:00',    # 5 PM ET next day
            ...     timezone='America/New_York'
            ... )
            >>> # Returns UTC times for engine
        """
        if session_start is None or session_end is None:
            # Calendar day boundaries with explicit times (start of day to end of day)
            # This ensures data fetching gets exactly one day's worth of data
            day_str = trading_day.strftime('%Y-%m-%d')
            session_start_str = f"{day_str} 00:00:00"
            session_end_str = f"{day_str} 23:59:59"

            # Session timestamp for prefetch is midnight UTC of the trading day
            session_start_ts = pd.Timestamp(day_str, tz='UTC')

            return session_start_str, session_end_str, session_start_ts

        # Custom session times - interpreted in the specified timezone (or UTC if None)
        # Parse session times - supports both HH:MM and HH:MM:SS formats
        start_parts = session_start.split(':')
        start_hour, start_minute = int(start_parts[0]), int(start_parts[1])
        start_second = int(start_parts[2]) if len(start_parts) > 2 else 0

        end_parts = session_end.split(':')
        end_hour, end_minute = int(end_parts[0]), int(end_parts[1])
        end_second = int(end_parts[2]) if len(end_parts) > 2 else 0

        # Determine if session spans overnight (e.g., futures 18:00 to 17:00)
        # If end time <= start time, session starts previous calendar day and ends on trading_day
        if (end_hour, end_minute, end_second) <= (start_hour, start_minute, start_second):
            # Overnight session (e.g., futures): starts PREVIOUS DAY evening, ends on trading_day
            # Example: Monday trading day with 18:00-17:00 ET session
            #   -> Session starts Sunday 18:00 ET, ends Monday 17:00 ET
            start_date = trading_day - pd.Timedelta(days=1)
            end_date = trading_day
        else:
            # Same-day session (e.g., equities 09:30 to 16:00)
            start_date = trading_day
            end_date = trading_day

        # Get timezone object if specified
        from hiveq.flow.utils.timezone_utils import get_timezone, local_to_utc
        from datetime import datetime as dt

        tz = get_timezone(timezone)

        # Create local datetime objects
        local_start = dt(
            start_date.year, start_date.month, start_date.day,
            start_hour, start_minute, start_second
        )
        local_end = dt(
            end_date.year, end_date.month, end_date.day,
            end_hour, end_minute, end_second
        )

        # Convert to UTC
        utc_start = local_to_utc(local_start, tz)
        utc_end = local_to_utc(local_end, tz)

        # Format as strings for engine (all in UTC)
        session_start_str = utc_start.strftime('%Y-%m-%d %H:%M:%S')
        session_end_str = utc_end.strftime('%Y-%m-%d %H:%M:%S')
        session_start_ts = pd.Timestamp(utc_start)

        return session_start_str, session_end_str, session_start_ts
