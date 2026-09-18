"""``hiveq.driver.hiveq_data`` — the data-read client, import-safe locally.

The HiveQ data client is distributed standalone and imported as ``hiveq_data``
(see ``data_driver/llms.txt`` §II). This module is the ``hiveq.driver``-scoped
path to it, so the imports the reference promises all resolve with no
ImportError::

    import hiveq.driver
    import hiveq.driver.hiveq_data
    import hiveq_data

When the standalone ``hiveq_data`` distribution is installed, this module simply
re-exports it, so the documented calls (``Historical(...).get_data(...)``,
``Publisher().publish(...)``, ...) are the real thing. When it is not, this
falls back to import stubs — like the rest of ``hiveq.driver`` — so code that
references the client can still be written and packaged locally; the calls then
raise :class:`~hiveq._platform_only.PlatformOnlyError` pointing you at
``deploy_job`` rather than failing at import time.
"""
from __future__ import annotations

try:
    # Prefer the real standalone client when it is installed: the documented
    # read/publish examples then work exactly as written.
    from hiveq_data import *  # noqa: F401,F403 — re-exported public surface
    from hiveq_data import (  # noqa: F401 — submodules `*` does not carry
        config,
        instrument_reference,
    )

    _STUBBED = False
except ImportError:
    # No standalone client here: keep the import resolving, stub the calls.
    from hiveq._platform_only import unavailable as _unavailable

    _STUBBED = True

    class HiveQAPIError(Exception):
        """The error the real client raises on a non-2xx API response.

        Shipped as real code (like ``hiveq.driver.Cache``) so ``except
        hiveq_data.HiveQAPIError`` can be written and imported locally. When the
        real client raises it on-platform it carries ``status_code``,
        ``response_text`` and ``response_json``.
        """

        def __init__(
            self,
            message: str = "",
            status_code=None,
            response_text=None,
            response_json=None,
            **kwargs,
        ):
            super().__init__(message)
            self.status_code = status_code
            self.response_text = response_text
            self.response_json = response_json

    def configure(
        api_key=None, base_url=None, user_id=None, org_id=None, user_name=None
    ) -> None:
        """Record credentials/endpoint. No I/O, so — like the real client — it
        does not raise; it is simply inert until a real client is installed."""
        return None

    class Historical:
        """Stub: constructing this locally raises (install ``hiveq_data`` or deploy)."""

        def __init__(self, *args, **kwargs):
            _unavailable("hiveq.driver.hiveq_data.Historical()")

    class Publisher:
        """Stub: constructing this locally raises (install ``hiveq_data`` or deploy)."""

        def __init__(self, *args, **kwargs):
            _unavailable("hiveq.driver.hiveq_data.Publisher()")

    class _ConfigAccessor:
        """``hiveq_data.config`` — the global config accessor."""

        def get_config(self, *args, **kwargs):
            _unavailable("hiveq.driver.hiveq_data.config.get_config()")

    config = _ConfigAccessor()

    class _InstrumentReference:
        """``hiveq_data.instrument_reference`` — symbol reference helpers."""

        def __getattr__(self, name):
            _unavailable(f"hiveq.driver.hiveq_data.instrument_reference.{name}")

    instrument_reference = _InstrumentReference()

    __all__ = [
        "configure",
        "config",
        "Historical",
        "Publisher",
        "HiveQAPIError",
        "instrument_reference",
    ]
