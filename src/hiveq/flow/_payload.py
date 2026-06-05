"""Private deploy-payload wrapper.

A captured task is wrapped in :class:`_TaskWrapper` before it is sent to the
HiveQ platform. The wrapper lives in its *own* module so it can be cloudpickled
**by value** — the class definition is embedded in the payload bytes, so the
platform executor reconstructs and runs it without importing this module. That
keeps the client free of any platform-internal package: nothing on the server
side needs to know where this class came from.

The executor invokes the payload generically — it calls ``.run()`` and may read
``.target`` to reach the wrapped task — so this minimal shape is all it needs.
"""
from typing import Any, Dict, Optional, Tuple


class _TaskWrapper:
    """Turn any object with an entry method (or a callable) into a runnable task.

    Mirrors the minimal surface the executor relies on: ``target``, ``run()``,
    and (for entry-method dispatch) ``entry_method`` / ``args`` / ``kwargs``.
    """

    def __init__(
        self,
        target: Any,
        entry_method: Optional[str] = "run",
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.target = target
        self.entry_method = entry_method
        self.args = args or ()
        self.kwargs = kwargs or {}

    def run(self) -> Any:
        """Execute the wrapped callable or its entry method."""
        if self.entry_method:
            method = getattr(self.target, self.entry_method)
            return method(*self.args, **self.kwargs)
        if callable(self.target):
            return self.target(*self.args, **self.kwargs)
        if hasattr(self.target, "run"):
            return self.target.run(*self.args, **self.kwargs)
        raise TypeError(
            f"Target {type(self.target).__name__} is not callable and has no "
            f"entry_method specified."
        )

    def __repr__(self) -> str:
        if self.entry_method:
            return f"<_TaskWrapper target={type(self.target).__name__}.{self.entry_method}>"
        return f"<_TaskWrapper target={type(self.target).__name__}>"
