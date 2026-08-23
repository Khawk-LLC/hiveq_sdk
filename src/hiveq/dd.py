"""``hiveq.dd`` — PLATFORM-ONLY IMPORT STUB (keyword-style data facade).

NOTICE: this is **not** the data driver. It is the stub shipped with
``hiveq-sdk`` so driver code can be written and packaged locally; the real
facade runs **only inside a HiveQ platform container**. ``load`` / ``save`` /
``stop`` raise :class:`~hiveq._platform_only.PlatformOnlyError` here instead of
silently returning ``None`` — deploy the code that calls them (see
``hiveq.flow.jobs.deploy_job``) and read the output back from the job.

``DateRange`` / ``TimeRange`` are the exception: they are plain value objects,
re-exported as real code from :mod:`hiveq.datetime`, so query windows can be
built locally before deploying.

The config-section API (``import hiveq.driver as dd``) is preferred over this
keyword facade for anything durable — it can be flipped between transports by
config. See the data-driver reference.
"""
from __future__ import annotations

from hiveq._platform_only import (  # noqa: F401 — re-exported for stub consumers
    NOTICE,
    PlatformOnlyError,
    notice as _notice,
    unavailable as _unavailable,
)
from hiveq.datetime import DateRange, TimeRange  # noqa: F401 — real value objects

_notice()


class Driver:
    """Platform-only: constructing this locally raises."""

    def __init__(self, *args, **kwargs):
        _unavailable("hiveq.dd.Driver()")


def load(dataset=None, schema=None, symbols=None, date=None, start=None,
         end=None, time=None, filter_mode=None, split_size=None, columns=None,
         limit=None, path=None, topic=None, keys=None, mode=None,
         drop_duplicates=None, timezone=None, time_out=None):
    """Platform-only: raises. Deploy the calling code and read the job's result."""
    _unavailable("hiveq.dd.load()")


def save(df=None, schema=None, key=None, dataset=None, path=None,
         output_columns=None, column_map=None, defaults=None,
         required_fields=None, operation='add', async_mode=False):
    """Platform-only: raises. Deploy the calling code and read the job's result."""
    _unavailable("hiveq.dd.save()")


def stop():
    """Platform-only: raises."""
    _unavailable("hiveq.dd.stop()")


__all__ = [
    'DateRange', 'TimeRange', 'Driver',
    'load', 'save', 'stop',
    'NOTICE', 'PlatformOnlyError',
]
