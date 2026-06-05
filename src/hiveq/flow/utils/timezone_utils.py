"""Timezone utilities for hiveq.flow.

This module provides utilities for timezone detection and conversion.
All internal timestamps in hiveq.flow are UTC, but users can configure
a local timezone for display and session time interpretation.
"""

import os
from datetime import datetime, timezone as dt_timezone
from typing import Optional, Union

import pandas as pd

# Use zoneinfo (Python 3.9+) with pytz fallback
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from pytz import timezone as ZoneInfo  # type: ignore


# Module-level cache for configured timezone (avoids repeated config lookups)
_cached_timezone: Optional[ZoneInfo] = None
_cached_timezone_initialized: bool = False


def get_configured_timezone() -> Optional[ZoneInfo]:
    """Get the configured timezone from engine config with caching.

    Returns the timezone configured via hf.init(timezone='...'), or None
    if no timezone is configured (which means UTC will be used).

    The result is cached for performance - repeated calls return the
    cached value without re-checking the config.

    Returns
    -------
    ZoneInfo or None
        The configured timezone object, or None if not configured (UTC)

    Notes
    -----
    Call reset_timezone_cache() when timezone configuration changes
    (automatically done by hf.init()).

    Examples
    --------
    >>> tz = get_configured_timezone()
    >>> if tz:
    ...     print(f"Using timezone: {tz}")
    ... else:
    ...     print("Using UTC")
    """
    global _cached_timezone, _cached_timezone_initialized

    if not _cached_timezone_initialized:
        try:
            import hiveq.flow as hf
            engine_config = hf.config()
            if hasattr(engine_config, 'timezone') and engine_config.timezone:
                _cached_timezone = get_timezone(engine_config.timezone)
        except Exception:
            pass  # Fall through to default below

        # Default to ET when nothing is configured. Matches the C++ side
        # (sigma_config_builder uses 'America/New_York' as its default), so
        # strategy windows like '15:30:00' are interpreted in the same zone
        # as the data adapter and Sigma's market hours config. UTC default
        # silently broke any strategy that compared event timestamps against
        # ET wall-clock window strings.
        if _cached_timezone is None:
            _cached_timezone = get_timezone('America/New_York')

        _cached_timezone_initialized = True

    return _cached_timezone


def reset_timezone_cache() -> None:
    """Reset the cached timezone.

    Called by hf.init() when timezone configuration changes to ensure
    subsequent calls to get_configured_timezone() return the new value.
    """
    global _cached_timezone, _cached_timezone_initialized
    _cached_timezone = None
    _cached_timezone_initialized = False


def get_local_timezone() -> Optional[str]:
    """Auto-detect system timezone from wallclock.

    Attempts multiple methods to detect the local timezone:
    1. tzlocal library (most reliable cross-platform)
    2. Python 3.9+ datetime astimezone
    3. TZ environment variable

    Returns
    -------
    str or None
        Timezone string like 'America/New_York', or None if detection fails.

    Examples
    --------
    >>> tz = get_local_timezone()
    >>> print(tz)  # e.g., 'America/New_York'
    """
    # Method 1: Use tzlocal library (most reliable)
    try:
        from tzlocal import get_localzone
        tz = get_localzone()
        # tzlocal 4.x returns ZoneInfo, 2.x returns pytz
        if hasattr(tz, 'key'):
            return tz.key
        return str(tz)
    except ImportError:
        pass
    except Exception:
        pass

    # Method 2: Python 3.9+ datetime astimezone
    try:
        local_tz = datetime.now().astimezone().tzinfo
        if local_tz is not None:
            if hasattr(local_tz, 'key'):
                return local_tz.key  # zoneinfo has .key attribute
            # Try zone attribute (pytz)
            if hasattr(local_tz, 'zone'):
                return local_tz.zone
    except Exception:
        pass

    # Method 3: Environment variable
    tz_env = os.environ.get('TZ')
    if tz_env:
        return tz_env

    # Method 4: /etc/timezone (Debian/Ubuntu and many distros)
    try:
        with open('/etc/timezone', 'r') as f:
            tz_name = f.read().strip()
            if tz_name:
                return tz_name
    except Exception:
        pass

    # Method 5: /etc/localtime symlink target (most Linux distros)
    try:
        target = os.readlink('/etc/localtime')
        # Typical: /usr/share/zoneinfo/America/New_York → "America/New_York"
        marker = '/zoneinfo/'
        idx = target.find(marker)
        if idx != -1:
            return target[idx + len(marker):]
    except Exception:
        pass

    return None  # Fall back to UTC


def get_timezone(tz_str: Optional[str]) -> Optional[ZoneInfo]:
    """Get ZoneInfo object from timezone string.

    Parameters
    ----------
    tz_str : str or None
        Timezone string (e.g., 'America/New_York') or None for UTC

    Returns
    -------
    ZoneInfo or None
        ZoneInfo object, or None if input is None

    Raises
    ------
    ValueError
        If timezone string is invalid

    Examples
    --------
    >>> tz = get_timezone('America/New_York')
    >>> tz = get_timezone(None)  # Returns None (UTC)
    """
    if tz_str is None:
        return None
    try:
        return ZoneInfo(tz_str)
    except Exception as e:
        raise ValueError(f"Invalid timezone '{tz_str}': {e}")


def utc_to_local(utc_dt: datetime, tz: Optional[ZoneInfo]) -> datetime:
    """Convert UTC datetime to local timezone.

    Parameters
    ----------
    utc_dt : datetime
        UTC datetime (naive or aware)
    tz : ZoneInfo or None
        Target timezone, or None to return UTC

    Returns
    -------
    datetime
        Datetime in target timezone (aware), or UTC if tz is None

    Examples
    --------
    >>> from datetime import datetime
    >>> utc_dt = datetime(2025, 1, 15, 15, 30, 0)  # 3:30 PM UTC
    >>> tz = get_timezone('America/New_York')
    >>> local_dt = utc_to_local(utc_dt, tz)
    >>> print(local_dt)  # 2025-01-15 10:30:00-05:00 (EST)
    """
    if tz is None:
        # Return UTC-aware datetime
        if utc_dt.tzinfo is None:
            return utc_dt.replace(tzinfo=dt_timezone.utc)
        return utc_dt.astimezone(dt_timezone.utc)

    # Make UTC-aware if naive
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=dt_timezone.utc)

    return utc_dt.astimezone(tz)


def local_to_utc(local_dt: datetime, tz: Optional[ZoneInfo]) -> datetime:
    """Convert local datetime to UTC.

    Parameters
    ----------
    local_dt : datetime
        Local datetime (naive assumed to be in tz, or aware)
    tz : ZoneInfo or None
        Source timezone, or None if input is already UTC

    Returns
    -------
    datetime
        UTC datetime (aware)

    Examples
    --------
    >>> from datetime import datetime
    >>> local_dt = datetime(2025, 1, 15, 10, 30, 0)  # 10:30 AM local
    >>> tz = get_timezone('America/New_York')
    >>> utc_dt = local_to_utc(local_dt, tz)
    >>> print(utc_dt)  # 2025-01-15 15:30:00+00:00 (UTC)
    """
    if tz is None:
        # Input is UTC
        if local_dt.tzinfo is None:
            return local_dt.replace(tzinfo=dt_timezone.utc)
        return local_dt.astimezone(dt_timezone.utc)

    # If naive, localize to source timezone
    if local_dt.tzinfo is None:
        # For zoneinfo, we use replace (assumes no DST ambiguity)
        # For pytz, we'd need localize(), but zoneinfo handles this
        local_dt = local_dt.replace(tzinfo=tz)

    return local_dt.astimezone(dt_timezone.utc)


def parse_time_to_utc(
    time_str: str,
    date: pd.Timestamp,
    tz: Optional[ZoneInfo]
) -> pd.Timestamp:
    """Parse HH:MM time string to UTC timestamp.

    Parameters
    ----------
    time_str : str
        Time in 'HH:MM' or 'HH:MM:SS' format
    date : pd.Timestamp
        Date for the time
    tz : ZoneInfo or None
        Timezone of the time_str, or None if UTC

    Returns
    -------
    pd.Timestamp
        UTC timestamp

    Examples
    --------
    >>> date = pd.Timestamp('2025-01-15')
    >>> tz = get_timezone('America/New_York')
    >>> utc_ts = parse_time_to_utc('09:30', date, tz)
    >>> print(utc_ts)  # 2025-01-15 14:30:00+00:00 (UTC)
    """
    parts = time_str.split(':')
    hour = int(parts[0])
    minute = int(parts[1])
    second = int(parts[2]) if len(parts) > 2 else 0

    # Create datetime in local timezone
    local_dt = datetime(
        date.year, date.month, date.day,
        hour, minute, second
    )

    # Convert to UTC
    utc_dt = local_to_utc(local_dt, tz)

    return pd.Timestamp(utc_dt)


def nanos_to_local_datetime(nanos: int, tz: Optional[ZoneInfo]) -> datetime:
    """Convert nanoseconds since epoch to local datetime.

    Parameters
    ----------
    nanos : int
        Nanoseconds since Unix epoch (UTC)
    tz : ZoneInfo or None
        Target timezone, or None for UTC

    Returns
    -------
    datetime
        Datetime in target timezone

    Examples
    --------
    >>> nanos = 1736956200000000000  # 2025-01-15 15:30:00 UTC
    >>> tz = get_timezone('America/New_York')
    >>> local_dt = nanos_to_local_datetime(nanos, tz)
    >>> print(local_dt)  # 2025-01-15 10:30:00-05:00 (EST)
    """
    utc_dt = datetime.fromtimestamp(nanos / 1_000_000_000, tz=dt_timezone.utc)
    return utc_to_local(utc_dt, tz)


def validate_timezone(tz_str: str) -> bool:
    """Validate that a timezone string is valid.

    Parameters
    ----------
    tz_str : str
        Timezone string to validate

    Returns
    -------
    bool
        True if valid, False otherwise

    Examples
    --------
    >>> validate_timezone('America/New_York')
    True
    >>> validate_timezone('Invalid/Timezone')
    False
    """
    try:
        ZoneInfo(tz_str)
        return True
    except Exception:
        return False
