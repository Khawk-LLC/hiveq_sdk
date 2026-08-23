"""``hiveq.driver.hiveq_subscriber`` — PLATFORM-ONLY IMPORT STUB.

NOTICE: not the real implementation. The HiveQ data driver runs **only inside a
HiveQ platform container**; this stub exists so the import resolves on a local
machine. Using it here raises :class:`~hiveq._platform_only.PlatformOnlyError` —
deploy the code that calls it (``hiveq.flow.jobs.deploy_job``, or a
``hiveq.flow`` strategy) and read the output back from the job.
"""
from __future__ import annotations

from hiveq._platform_only import notice as _notice, unavailable as _unavailable

_notice()

from typing import List, Optional


class HiveQSubscriber:
    """Platform-only: constructing this locally raises."""

    def __init__(self, topic: str = None, keys: Optional[List[str]] = None, **kwargs) -> None:
        _unavailable("hiveq.driver.hiveq_subscriber.HiveQSubscriber()")

    def load(self, time_out: Optional[float] = None):
        _unavailable("HiveQSubscriber.load()")

    def add_keys(self, keys: List[str]) -> None:
        _unavailable("HiveQSubscriber.add_keys()")

    def stop(self) -> None:
        _unavailable("HiveQSubscriber.stop()")

    def stopped(self) -> bool:
        _unavailable("HiveQSubscriber.stopped()")

    def start(self) -> None:
        _unavailable("HiveQSubscriber.start()")


__all__ = ['HiveQSubscriber']
