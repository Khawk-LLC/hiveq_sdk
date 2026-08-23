"""Shared NOTICE machinery for the data-driver import stubs shipped with hiveq-sdk.

The modules under ``hiveq.driver`` / ``hiveq.dd`` in this wheel are **stubs**:
they exist so code that imports the data driver can be written, type-checked,
and packaged on a local machine, then deployed and executed on the HiveQ
platform — where the real driver lives. Nothing here loads or saves data.

Two things happen so a user is never left guessing:

* importing any data-driver stub prints :data:`NOTICE` once, to stderr;
* actually calling one raises :class:`PlatformOnlyError` with the same message,
  rather than silently returning ``None`` and failing later somewhere confusing.
"""
from __future__ import annotations

import os
import sys

NOTICE = """\
========================================================================
 HIVEQ DATA DRIVER — PLATFORM-ONLY IMPORT STUB (nothing here runs locally)
========================================================================
 What you imported is a stub, not the data driver. It cannot load, save,
 or subscribe to data on this machine. The real driver runs ONLY inside a
 HiveQ platform container, which is where the credentials, transports, and
 data endpoints exist.

 The stub is here so driver code can be written and packaged locally, then
 DEPLOYED and run on the platform:

     import hiveq.flow as hf

     def my_job():
         import hiveq.driver as dd            # the real driver, in-container
         from hiveq.driver import Cache
         df = dd.load('MySection', cache=Cache.PULL_UPDATE_CACHE)
         print(f'{len(df)} rows')             # -> job.logs()
         return {'rows': len(df)}             # -> job.result()['result']

     job = hf.deploy_job(my_job, task_name='my-job', wait=True)
     print(job.result())

 Keep the `hiveq.driver` imports INSIDE the deployed function, so they
 resolve against the container's real driver instead of these stubs.

 Docs: run `hiveq docs`, then data_driver/llms.txt §1.1 (deploying driver
 code) and §1.2 (this contract: what is real vs what raises); llms.txt R13
 and §14.1, plus §11.0 / §11.6 for the job surface.

 Set HIVEQ_SUPPRESS_STUB_NOTICE=1 to silence this notice.
========================================================================"""

_SUPPRESS_ENV = "HIVEQ_SUPPRESS_STUB_NOTICE"
_printed = False


def notice() -> None:
    """Print :data:`NOTICE` to stderr, once per process."""
    global _printed
    if _printed or os.environ.get(_SUPPRESS_ENV):
        return
    _printed = True
    try:
        print(NOTICE, file=sys.stderr, flush=True)
    except Exception:  # never let a notice break a user's import
        pass


class PlatformOnlyError(RuntimeError):
    """Raised when a platform-only data-driver stub is actually called."""


def unavailable(what: str) -> "PlatformOnlyError":
    """Raise :class:`PlatformOnlyError` for ``what``, after printing the notice."""
    notice()
    raise PlatformOnlyError(
        f"{what} cannot run on a local machine — it is an import stub. The HiveQ "
        f"data driver runs only inside a HiveQ platform container. Deploy the code "
        f"that calls it (hiveq.flow.jobs.deploy_job, or a hiveq.flow strategy) and "
        f"read the output back from the job's result/logs. See the notice above, or "
        f"`hiveq docs` -> data_driver/llms.txt §1.1."
    )


__all__ = ["NOTICE", "PlatformOnlyError", "notice", "unavailable"]
