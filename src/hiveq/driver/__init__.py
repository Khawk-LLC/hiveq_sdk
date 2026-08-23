"""``hiveq.driver`` — PLATFORM-ONLY IMPORT STUB.

NOTICE: this is **not** the data driver. It is the stub shipped with
``hiveq-sdk`` so that driver code can be written, imported, and packaged on a
local machine. The real driver — ``HiveQDataDriver`` — runs **only inside a
HiveQ platform container**, where the credentials, transports, and data
endpoints live. Nothing in this module loads or saves data locally.

Importing this module prints a one-time notice; calling ``load`` / ``save`` /
``init`` / ``clear_cache`` / ``alert`` / ``stop`` raises
:class:`~hiveq._platform_only.PlatformOnlyError` rather than silently returning
``None``. Write the driver calls inside a function and deploy it — see
``hiveq.flow.jobs.deploy_job`` and §1.1 of the data-driver reference.

``Cache`` is the exception: it is a plain enum, shipped as real code, so the
argument values can be built locally before deploying.
"""
from __future__ import annotations

from hiveq._platform_only import (  # noqa: F401 — re-exported for stub consumers
    NOTICE,
    PlatformOnlyError,
    notice as _notice,
    unavailable as _unavailable,
)
from hiveq.driver.data_driver_interface import Cache  # real code (pure enum)

_notice()


def load(data_source_id=None, params_tuple=None, cache=None, time_out=None,
         filter_columns=False, drop_duplicates=None, in_memory=False):
    """Platform-only: raises. Deploy the calling code and read the job's result."""
    _unavailable("hiveq.driver.load()")


def save(data_source_id=None, df=None, params_tuple=None, append=False,
         filter_columns=False, in_memory=False):
    """Platform-only: raises. Deploy the calling code and read the job's result."""
    _unavailable("hiveq.driver.save()")


def init(**kwargs):
    """Platform-only: raises."""
    _unavailable("hiveq.driver.init()")


def clear_cache():
    """Platform-only: raises."""
    _unavailable("hiveq.driver.clear_cache()")


def alert(subject=None, message=None, level=None, channel='default'):
    """Platform-only: raises."""
    _unavailable("hiveq.driver.alert()")


def stop():
    """Platform-only: raises."""
    _unavailable("hiveq.driver.stop()")


__all__ = [
    'Cache',
    'load', 'save', 'init', 'clear_cache', 'alert', 'stop',
    'NOTICE', 'PlatformOnlyError',
]
