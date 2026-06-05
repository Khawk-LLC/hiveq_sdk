"""Sigma namespace (thin client SDK).

The proprietary ``SigmaOMSAdapter`` / engine bindings (PySigma) are NOT shipped
to clients — they run only on the platform executor (full ``hiveq-flow``). This
package exposes only the engine-free trading-type wrappers (``oms.sigma.types``)
and the ``SigmaContext`` type stub used for authoring strategies.
"""

__all__: list[str] = []
