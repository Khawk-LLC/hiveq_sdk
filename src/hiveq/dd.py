"""Stub for hiveq.dd — keyword-style data facade.

The real implementation lives in the data_driver package (HiveQDataDriver)
and is available on the HiveQ platform executor.  These stubs make the
import path resolve so user strategies can be deployed from a local machine
that only has the thin SDK installed.
"""
from __future__ import annotations
from typing import Any, Optional, List


class DateRange:
    """Inclusive calendar-day range."""
    start: str
    end: str
    date_list: list

    def __init__(self, start, end): ...
    def chunks(self, n): ...
    def __iter__(self): ...


class TimeRange:
    """Intraday start / end clock window."""
    start: str
    end: str
    time_list: list

    def __init__(self, start, end): ...
    def __iter__(self): ...


class Driver:
    ...


def load(dataset=None, schema=None, symbols=None, date=None, start=None,
         end=None, time=None, filter_mode=None, split_size=None, columns=None,
         limit=None, path=None, topic=None, keys=None, mode=None,
         drop_duplicates=None, timezone=None, time_out=None):
    ...


def save(df, schema=None, key=None, dataset=None, path=None,
         output_columns=None, column_map=None, defaults=None,
         required_fields=None, operation='add', async_mode=False):
    ...


def stop():
    ...
