"""Metrics (thin client SDK).

Only ``PerformanceReport`` is shipped — it wraps the results returned from the
platform over REST (``Run.report()``). The report generator and the multi-run
stitcher (which pull heavy ``quantstats``) are not part of the client.
"""
from .report import PerformanceReport

__all__ = ["PerformanceReport"]
