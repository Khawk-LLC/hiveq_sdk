"""OMS namespace (thin client SDK).

Only the lightweight, engine-free pieces are present here — the trading-type
wrappers under ``oms.sigma.types`` and the ``SigmaContext`` type stub. The OMS
adapters/engine (``oms.sigma.adapter`` etc.) are proprietary and ship only with
the full ``hiveq-flow`` package on the platform executor.
"""
