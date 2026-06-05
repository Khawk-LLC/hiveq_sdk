"""Client-side placeholder for the engine's ``SigmaContext``.

The real ``SigmaContext`` is backed by the native ``PySigma`` engine and only
exists on the platform executor. Strategies authored against the thin client SDK
reference it purely for type hints — its methods (``buy_order``, ``is_flat``,
``subscribe_bars`` …) are invoked by the engine at run time, never on the client.

This module therefore provides only a NAME so that ``import hiveq.flow`` and
strategy annotations resolve locally, plus a curated ``sigma_context.pyi`` that
carries the full method signatures for IDEs and type checkers. Constructing or
calling it on the client is a programming error and raises immediately.

This is the Cython model: the ``.pyi`` is the type surface; the real
implementation is the compiled extension that lives only where it runs.
"""
from __future__ import annotations


class _EngineOnly:
    """Descriptor that makes every attribute access raise a clear error."""

    def __get__(self, obj, objtype=None):
        raise RuntimeError(
            "SigmaContext is engine-backed and only runs on the HiveQ platform "
            "executor. The thin hiveq-flow client SDK ships it as a type stub "
            "only — deploy your strategy (hf.run_backtest) "
            "to run it. Install the full hiveq-flow package for local execution."
        )


class SigmaContext:
    """Type-only stub for the engine's strategy execution context.

    See ``sigma_context.pyi`` for the full callable surface. Do not instantiate
    or call this on the client.
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "SigmaContext cannot be constructed on the client. It is provided by "
            "the HiveQ engine to your strategy callbacks at run time. The thin "
            "client SDK ships it as a type stub only; deploy to run, or install "
            "the full hiveq-flow package for local execution."
        )


__all__ = ["SigmaContext"]
