## Symbols

Old-world continuous-futures notation `<ROOT><rank>!` maps to HiveQ
`<root>.c.<rank-1>` (front month is `.c.0`): `ES1!`→`ES.c.0`, `NQ1!`→`NQ.c.0`,
`ES2!`→`ES.c.1`. Equities (`AAPL`) and already-canonical symbols pass through.
The `hiveq.dd` keyword facade translates automatically; with the section API call
`hiveq.symbol.translate(...)` yourself when needed.

```python
from hiveq import symbol
symbol.translate(['AAPL', 'ES1!'])   # ['AAPL', 'ES.c.0']
```

---

