## 13. Config dataclasses

```python
from hiveq.flow import StrategyConfig, BacktestConfig, EngineConfig
```

**StrategyConfig**
| field | type | default |
|---|---|---|
| `name` | str | (required) |
| `type` | str | (required — class name, R2) |
| `symbols` | Optional[List[str]] | None |
| `params` | Dict[str, Any] | {} |

`params` also recognizes rollover-tuning keys, read only when `BacktestConfig(enable_auto_rollover=True)` (§9.1, §15) — they change how the engine rolls a held futures position, not whether it rolls:

| key | type | default | purpose |
|---|---|---|---|
| `rolloverExecutorType` | str | `'POV'` | Executor algo used to exit the old contract / enter the new one: `'POV'` or `'TWAP'`. |
| `useAutoRollOverNotional` | bool | `False` | `True` sizes the new-contract entry to match the dollar notional of the old position instead of carrying the same contract count. |
| `rolloverPovMinOrderQty` / `rolloverPovMaxOrderQty` | long | `10` / `100` | POV child-order size bounds (`rolloverExecutorType='POV'` only). |
| `rolloverPovUpdateIntervalMillis` | long | `20000` | POV re-quote interval (ms). |
| `rolloverPovVolParticipationPct` | float | `2.0` | POV target participation, % of market volume. |
| `rolloverPovAggressivePriceMultiplier` | long | `10` | POV aggressive-price tick multiplier. |
| `rolloverPovTimeDeltaMinutes` | long | `60` | POV executor time budget (minutes). |
| `rolloverTwapMinOrderQty` / `rolloverTwapMaxOrderQty` | long | `1` / `10` | TWAP child-order size bounds (`rolloverExecutorType='TWAP'` only). |
| `rolloverTwapUpdateIntervalMillis` | long | `60000` | TWAP re-quote interval (ms). |
| `rolloverTwapAggressivePriceMultiplier` | long | `1` | TWAP aggressive-price tick multiplier. |
| `rolloverTwapVolParticipationPct` | float | `0.0` | `0` = pure time-based TWAP; `>0` caps slices by % of market volume. |
| `rolloverTwapTimeDeltaMinutes` | long | `60` | TWAP executor time budget (minutes). |

e.g. `StrategyConfig(name=..., type=..., params={'rolloverExecutorType': 'TWAP', 'useAutoRollOverNotional': True})`.

**BacktestConfig** (key fields)
| field | type | default |
|---|---|---|
| `id` | Optional[str] | None |
| `symbols` | list | None |
| `start_date` / `end_date` | Optional[str] | None |
| `initial_capital` | float | 1_000_000.0 |
| `commission` | float | 0.001 |
| `slippage` | float | 0.0 |
| `venue` | str | "SIM" |
| `deploy` | bool | False |
| `benchmark` | Optional[str] | None |
| `risk_free_rate` | float | 0.02 |
| `equity_fee` | float | 0.0011 (per share) |
| `futures_fee` | float | 0.5 (per contract) |
| `crypto_fee` | float | 0.00005 |
| `session_start` / `session_end` | Optional[str] | None (ET "HH:MM", R6) |
| `enable_auto_rollover` | bool | False |
| `auto_flatten_at_close` | bool | False (§15 — force-closes non-option positions at session close; options always settle automatically regardless) |
| `enable_tca` | bool | False |
| `export_orders_csv` | bool | False |
| `extra_config` | Dict[str, Any] | {} |

**EngineConfig**
| field | type | default |
|---|---|---|
| `oms` | str | "SIGMA" (use `OMSType.SIGMA.value`) |
| `timezone` | Optional[str] | None (IANA name; auto-detected if None) |
| `params` | Dict[str, Any] | {} (engine-behavior keys — see §2.1 for the recognized keys) |

Pass an `EngineConfig` (or a plain `config={...}` dict) to `run_backtest` via `**kwargs`; the tunable `params` keys are listed in **§2.1**.

---

