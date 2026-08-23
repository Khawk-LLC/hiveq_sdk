"""``hiveq.driver.kdb_subscriber`` — PLATFORM-ONLY IMPORT STUB.

NOTICE: not the real implementation. The HiveQ data driver runs **only inside a
HiveQ platform container**; this stub exists so the import resolves on a local
machine. Using it here raises :class:`~hiveq._platform_only.PlatformOnlyError` —
deploy the code that calls it (``hiveq.flow.jobs.deploy_job``, or a
``hiveq.flow`` strategy) and read the output back from the job.
"""
from __future__ import annotations

from hiveq._platform_only import notice as _notice, unavailable as _unavailable

_notice()


class KDBSubscriber:
    """Platform-only: constructing this locally raises."""

    def __init__(self, *args, **kwargs):
        _unavailable("hiveq.driver.kdb_subscriber.KDBSubscriber()")


__all__ = ['KDBSubscriber']
