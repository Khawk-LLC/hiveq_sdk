import os
import random
import re
from datetime import datetime, timedelta
from typing import List, Tuple

from hiveq.flow.config import AssetType
from hiveq.flow.utils.date_calendar import TradingCalendar, MarketLocation

__all__ = [
    "TradingCalendar",
    "MarketLocation",
    "pattern_file_list",
    "name_generator",
    "call_before",
    "granularity_to_schema_per_day",
    "split_date_chunks",
    "is_notebook",
    "sec_master_asset_type_to_enum",
    "remove_numeric_suffix",
]

ADJECTIVES = [
    "Swift", "Brave", "Silent", "Clever", "Bold", "Mighty", "Nimble",
    "Fierce", "Calm", "Rapid", "Lucky", "Wild", "Bright", "Sharp",
    "Stealthy", "Fearless", "Cunning", "Valiant", "Daring", "Epic",
    "Glorious", "Legendary", "Savage", "Wise", "Stalwart", "Tenacious",
    "Vigilant", "Loyal", "Dynamic", "Majestic", "Ferocious", "Eternal",
    "Radiant", "Shadowy", "Mystic", "Invincible", "Iron", "Crimson"
]

NOUNS = [
    "Falcon", "Tiger", "Wolf", "Panther", "Hawk", "Shark", "Eagle",
    "Lion", "Dragon", "Bear", "Fox", "Stallion", "Raven", "Cobra",
    "Viper", "Phoenix", "Kraken", "Leopard", "Buffalo", "Owl",
    "Mustang", "Bison", "Jaguar", "Condor", "Griffin", "Hydra",
    "Cheetah", "Mamba", "Puma", "Rhino", "Scorpion", "Cougar",
    "Talon", "Crocodile", "Lynx", "Mammoth", "Raptor", "Serpent"
]

# Precompute all possible combinations
ALL_NAMES = [f"{adj} {noun}" for adj in ADJECTIVES for noun in NOUNS]
random.shuffle(ALL_NAMES)  # shuffle for randomness

def __has_date_pattern(filename: str) -> bool:
    """
    Returns True if the filename ends with a valid date pattern
    (supports YYYYMMDD, DDMMYYYY, MMDDYYYY, etc.).
    """
    supported_date_suffix = ['yyyymmdd', 'ddmmyyyy','mmddyyyy']
    for date_suffix in supported_date_suffix:
        if filename.endswith(date_suffix + '.csv'):
            return True
    return False


def __extract_date_from_filename(filename: str):
    """
    Extracts datetime if filename ends with 8-digit date, otherwise None.
    Supports multiple formats: YYYYMMDD, DDMMYYYY, MMDDYYYY.
    """
    base, _ = os.path.splitext(os.path.basename(filename))
    match = re.search(r"(\d{8})$", base)
    if not match:
        return None

    digits = match.group(1)
    date_formats = ["%Y%m%d", "%d%m%Y", "%m%d%Y"]

    for fmt in date_formats:
        try:
            return datetime.strptime(digits, fmt)
        except ValueError:
            continue
    return None


def pattern_file_list(file_name: str, run_config) -> list[str]:
    csv_files = []
    if __has_date_pattern(file_name) and run_config.start_date and run_config.end_date:
        directory = os.path.dirname(file_name)
        start_date = datetime.strptime(run_config.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(run_config.end_date, "%Y-%m-%d")

        for fname in os.listdir(directory):
            if fname.endswith(".csv"):
                fpath = os.path.join(directory, fname)
                fdate = __extract_date_from_filename(fname)
                if fdate and start_date <= fdate <= end_date:
                    csv_files.append(fpath)

        csv_files.sort()  # ensure chronological order
    elif __has_date_pattern(file_name):
        raise RuntimeError(f'File name {file_name} has date pattern, but no start and end date for run available.')
    else:
        csv_files = [file_name]
    return csv_files


def name_generator():
    """Yield a new unique name until exhausted."""
    for name in ALL_NAMES:
        yield name


def call_before(before_method_name):
    """Decorator to call another method before the main method."""
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            before_method = getattr(self, before_method_name)
            before_method(*args, **kwargs)   # call the "before" method first
            return func(self, *args, **kwargs)
        return wrapper
    return decorator


def granularity_to_schema_per_day(granularity: str) -> int:
    """
    Convert granularity/schema string to estimated data points per day.

    Supports multiple formats:
    - Underscore prefix: '_1s', '_1m', '_1h', '_1d'
    - Bars prefix: 'bars_1s', 'bars_1m', 'bars_1h', 'bars_1d'
    - Just the interval: '1m', '1s', '1h', '1d'

    Returns:
    - '_1s' / 'bars_1s' / '1s'  -> 23400 bars/day (6.5 market hours)
    - '_1m' / 'bars_1m' / '1m'  -> 390 bars/day (6.5 market hours)
    - '_1h' / 'bars_1h' / '1h'  -> 7 bars/day (market hours)
    - '_1d' / 'bars_1d' / '1d'  -> 1 bar/day

    All other schemas (eq_trades, fut_trades, snaps, indices, etc.) default to
    100,000 data points/day for chunking purposes.
    """
    g = granularity.lower()

    # Handle multiple patterns:
    # - _1m, _1s, _1h, _1d (underscore prefix)
    # - bars_1m, bars_1s, bars_1h, bars_1d (bars prefix)
    # - 1m, 1s, 1h, 1d (just interval)
    time_match = re.search(r'(\d+)([smhd])$', g)
    if time_match:
        multiplier = int(time_match.group(1))
        unit = time_match.group(2)
        # Use market hours (6.5 hours = 390 minutes) for more accurate estimation
        if unit == 's':
            return (390 * 60) // multiplier  # seconds in market day
        elif unit == 'm':
            return 390 // multiplier  # minutes in market day
        elif unit == 'h':
            return max(1, 7 // multiplier)  # hours in market day (roughly 7)
        elif unit == 'd':
            return 1  # days per day

    # Default: return 100k for all tick-level schemas and unknown schemas
    return 100_000


def split_date_chunks(symbols: List[str],
                      start_date: datetime,
                      end_date: datetime,
                      schema: str,
                      max_data_per_chunk: int = 100_000
                      ) -> List[Tuple[str, str]]:
    """
    Split symbols and date range into chunks that do not exceed max_data_per_chunk.
    Works with any data type (bars, ticks, index prices, snapshots, etc.)
    Returns: List of (chunk_start, chunk_end) tuples

    This function always succeeds regardless of symbol count - if there are too many
    symbols for even a single day, it returns 1-day chunks and relies on the data
    provider's pagination to handle the volume.

    When start_date or end_date include time components (not midnight), the time
    is preserved in the output for precise session-based data fetching (e.g., futures).
    """
    chunks = []
    data_per_day = granularity_to_schema_per_day(schema)

    # Calculate number of calendar days (inclusive of both start and end dates)
    # Use date portion to avoid truncation issues with timedelta.days
    # e.g., Sep 15 00:00:00 to Sep 17 23:59:59 should be 3 days, not 2
    num_days = (end_date.date() - start_date.date()).days + 1

    # Check if times are specified (not midnight) - if so, preserve them in output
    # This is important for session-based data fetching (e.g., futures with custom session times)
    start_has_time = start_date.hour != 0 or start_date.minute != 0 or start_date.second != 0
    end_has_time = end_date.hour != 0 or end_date.minute != 0 or end_date.second != 0

    # Use datetime format with time if either date has a non-midnight time component
    if start_has_time or end_has_time:
        date_format = '%Y-%m-%d %H:%M:%S'
    else:
        date_format = '%Y-%m-%d'

    # compute total data for all symbols for the whole period
    total_data = len(symbols) * data_per_day * num_days

    if total_data <= max_data_per_chunk:
        chunks.append((start_date.strftime(date_format), end_date.strftime(date_format)))
        # one chunk is enough
        return chunks

    # otherwise split by days to keep each chunk <= max_data_per_chunk
    # minimum 1 day per chunk - data provider pagination handles large single-day volumes
    max_days_per_chunk = max(1, max_data_per_chunk // (len(symbols) * data_per_day))

    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = chunk_start + timedelta(days=max_days_per_chunk)
        if chunk_end > end_date:
            chunk_end = end_date
        chunks.append((chunk_start.strftime(date_format), chunk_end.strftime(date_format)))
        # API treats both start and end as inclusive, so next chunk starts after chunk_end
        # This creates non-overlapping chunks: (Nov 1, Nov 2), (Nov 3, Nov 4), etc.
        chunk_start = chunk_end + timedelta(days=1)

    return chunks


def is_notebook():
    try:
        from IPython import get_ipython
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or JupyterLab
        elif shell == 'TerminalInteractiveShell':
            return False  # IPython in terminal
        else:
            try:
                import marimo._runtime.runtime as rt
                return True
            except ImportError:
                return False
    except (NameError, ImportError):
        return False      # Standard Python interpreter


def sec_master_asset_type_to_enum(sec_asset_type: str) -> AssetType:
    """
    Translate security master asset_type string to HiveQ AssetType enum.

    Security master uses string codes while HiveQ Flow uses enum values.
    This internal utility provides consistent mapping between the two.

    Args:
        sec_asset_type: Security master asset type string
                       ('Eq', 'Opt', 'Fut', 'Crypto')

    Returns:
        Corresponding AssetType enum value

    Raises:
        ValueError: If asset_type string is not recognized

    Examples:
        >>> _sec_master_asset_type_to_enum('Eq')
        AssetType.EQUITY
        >>> _sec_master_asset_type_to_enum('Fut')
        AssetType.FUTURES
        >>> _sec_master_asset_type_to_enum('Opt')
        AssetType.OPTIONS
    """
    # Map security master strings to AssetType enum
    type_map = {
        'Eq': AssetType.EQUITY,
        'Opt': AssetType.OPTIONS,
        'Fut': AssetType.FUTURES,
        'Crypto': AssetType.CRYPTO,
        'Idx': AssetType.INDEX,
    }

    asset_type = type_map.get(sec_asset_type)
    if asset_type is None:
        raise ValueError(
            f"Unknown security master asset_type: '{sec_asset_type}'. "
            f"Expected one of: {list(type_map.keys())}"
        )

    return asset_type


def remove_numeric_suffix(text: str, separator: str = "-") -> str:
    """
    Remove numeric suffixes like -001, -002, etc. from a string.

    Args:
        text: Input string that may contain a numeric suffix
        separator: Character(s) separating the base string from the numeric suffix.
                   Defaults to "-"

    Returns:
        String with the numeric suffix removed

    Examples:
        >>> remove_numeric_suffix("TRADER-001")
        'TRADER'
        >>> remove_numeric_suffix("DEMO-002")
        'DEMO'
        >>> remove_numeric_suffix("ACCOUNT-123")
        'ACCOUNT'
        >>> remove_numeric_suffix("NOSUFFIX")
        'NOSUFFIX'
        >>> remove_numeric_suffix("TEST_001", separator="_")
        'TEST'
    """
    if not text:
        return text

    # Pattern: separator followed by one or more digits at the end of string
    pattern = rf"{re.escape(separator)}\d+$"
    result = re.sub(pattern, "", text)

    return result
