## 8. Portfolio API  (`SigmaPortfolio` and `SigmaGlobalPortfolio` share this surface)

```python
.position(symbol: str) -> Optional[SigmaPosition]
.net_position(symbol: str) -> float
.is_flat(symbol: str = None) -> bool
.is_net_long(symbol: str) -> bool
.is_net_short(symbol: str) -> bool
.positions() -> List[SigmaPosition]
.realized_pnl(symbol: str = None) -> float       # total when symbol omitted
.unrealized_pnl(symbol: str = None) -> float
.total_pnl(symbol: str = None) -> float          # realized + unrealized
.day_pnl(symbol: str = None) -> float
.net_exposure() -> float                          # signed
.gross_exposure() -> float                        # sum of |values|
.max_drawdown -> float       (property)
.fees -> float               (property)
.initial_capital -> float    (property)           # capital the run started with (fixed base)
.equity -> float             (property)           # account value / NAV = initial_capital + realized + unrealized − fees
.cash -> float               (property)           # uninvested cash (equity − market value of fully-funded holdings)
```
- `SigmaPortfolio` = current strategy only. `SigmaGlobalPortfolio` = summed across all strategies.
- **Account view** (`initial_capital` / `equity` / `cash`): tracked by the engine's cash ledger, not derived in Python. `equity` is asset-agnostic (correct for equities and futures). `cash` reflects that **futures encumber margin, not cash** — buying a future moves only fees out of `cash` (not notional), so `cash ≈ equity` for futures positions while equities reduce `cash` by the full notional. Identity: `equity == initial_capital + realized_pnl + unrealized_pnl` (minus fees). Use `equity` for percent-of-account sizing; there is no built-in `order_target_percent` — size yourself (e.g. `qty = int(pct * ctx.portfolio().equity / (price * ctx.instrument(sym).multiplier))`).

---

