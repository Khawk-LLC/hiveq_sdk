"""Symbol translation — old-world tickers -> HiveQ canonical symbols.

Pure logic, no external dependencies. Shipped as real code in the SDK so
users can translate legacy symbols locally (e.g. ``ES1!`` → ``ES.c.0``).
For new canonical symbols, prefer explicit ``.v.0`` for quarterly-expiring
products and ``.c.0`` for monthly-expiring products.
"""
import re

OVERRIDES = {}

_CONTINUOUS_RE = re.compile(r'^([A-Za-z]{1,4})(\d+)!$')


def translate_symbol(symbol):
    """Translate a single old-world symbol to HiveQ notation.

    Non-matching symbols (equities, already-canonical futures) pass through
    unchanged.  ``None`` passes through as ``None``.
    """
    if symbol is None:
        return None
    sym = str(symbol).strip()
    if sym in OVERRIDES:
        return OVERRIDES[sym]
    m = _CONTINUOUS_RE.match(sym)
    if m:
        root, rank = m.group(1), int(m.group(2))
        return '%s.c.%d' % (root.upper(), rank - 1)
    return sym


def translate(symbols):
    """Translate a symbol, or a list/tuple of symbols, to HiveQ notation.

    Preserves the input container shape: a ``str`` in → ``str`` out; a list or
    tuple in → list out; ``None`` → ``None``.
    """
    if symbols is None:
        return None
    if isinstance(symbols, str):
        return translate_symbol(symbols)
    return [translate_symbol(s) for s in symbols]
