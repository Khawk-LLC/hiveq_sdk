"""Stub for hiveq_data — HiveQ Data API SDK.

The real implementation lives in the hiveq-data-sdk package and is available
on the HiveQ platform executor.  These stubs make the import path resolve
so user strategies can be deployed from a local machine that only has the
thin SDK installed.
"""
from __future__ import annotations

__version__ = "0.2.9"

from hiveq_data.config import configure
from hiveq_data.historical import Historical
from hiveq_data.instrument_reference import InstrumentReference
from hiveq_data.metadata import Metadata
from hiveq_data.publisher import Publisher
from hiveq_data.live import LiveStream
from hiveq_data.exceptions import HiveQAPIError

__all__ = [
    "configure",
    "Historical",
    "InstrumentReference",
    "Metadata",
    "Publisher",
    "LiveStream",
    "HiveQAPIError",
    "__version__",
]
