"""agent_qa — HiveQ's self-extending QA suite.

Not shipped in the wheel: ``pyproject.toml`` uses ``[tool.setuptools.packages.find]``
rooted at ``src/``, so this top-level package is a development-only sibling.

Importing ``agent_qa`` applies the environment profile as a side effect, so that
``HIVEQ_BASE_URL``/``HIVEQ_AUTH_URL`` are in place before anything imports
``hiveq.flow`` and triggers ``~/.hiveq/.env`` loading (which only fills in
variables that are *absent*, so ordering decides which host wins).
``run_all.py`` also exports these into each child's environment; this is the
belt-and-braces path for a test run by hand.
"""

from agent_qa.core.profiles import apply_profile as _apply_profile

_apply_profile()

__all__ = ["core"]
