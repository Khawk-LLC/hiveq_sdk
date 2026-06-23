"""Stub for hiveq.datetime — DateRange / TimeRange type aliases.

The real implementation lives in the data_driver package and is available
on the HiveQ platform executor.  These stubs make the import path resolve
so user strategies can be deployed from a local machine that only has the
thin SDK installed.
"""
from __future__ import annotations


class DateRange:
    """Inclusive calendar-day date range."""
    def __init__(self, start, end, transform_func=None):
        self.start = str(start).split(' ')[0].replace('-', '.')
        self.end = str(end).split(' ')[0].replace('-', '.')
        self.date_list = []
        self.transform_func = transform_func

    def __iter__(self):
        return iter(self.date_list)


class TimeRange:
    """Intraday start / end clock window."""
    def __init__(self, start, end):
        self.time_list = [start, end]

    def __iter__(self):
        return iter(self.time_list)
