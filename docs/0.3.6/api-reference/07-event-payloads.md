## 7. Event payloads — what `event.data()` returns

### 7.0 EventType → payload map
| event.type | event.data() returns | section |
|---|---|---|
| `BAR` (and `BAR_1_MIN`…`BAR_1_DAY`) | `SigmaBar` | 7.1 |
| `TRADE` | `SigmaTradeTick` | 7.5 |
| `QUOTE` | `SigmaQuoteTick` | 7.6 |
| `SNAP` | `SigmaSnapData` (options) | 7.7 |
| `ORDER_FILLED`, `ORDER_*` | `SigmaOrder` | 7.3 |
| `POSITION_*` | `SigmaPosition` | 7.2 |
| `CUSTOM_DATA` | `SigmaCustomData` | 7.9 |
| `TIMER` | `TimerEventData` | 7.8 |
| `INDEX_PRICE` | `IndexPrice` | 7.11 |
| `ROLLOVER` | `Rollover` | 7.12 |
| `EXECUTOR_EVENT` | executor lifecycle payload (opaque) | 7.13 |
| `SECURITY_EVENT` | security/reference payload (opaque) | 7.13 |

### 7.1 SigmaBar
`symbol:str` · `open:float` · `high:float` · `low:float` · `close:float` · `volume:float` · `interval:str`(human-readable: `'1d'` / `'1m'` / `'1s'` — matches the available bar schemas; no `'1h'`) · `interval_millis:int`(`86400000` / `60000` / `1000`) · `ts_event:int(ns, close)` · `ts_init:int(ns, open)` · `time:datetime?` · `time_utc:datetime?`

### 7.2 SigmaPosition
`symbol:str` · `quantity:float`(signed) · `side:str`("LONG"|"SHORT"|"FLAT") · `avg_price:float` (aliases `entry_price`,`average_price`) · `market_value:float` · `realized_pnl:float` · `unrealized_pnl:float` · `total_pnl:float` · `day_pnl:float` · `notional:float` · `fees:float` · `is_open:bool` · `is_flat:bool` · `is_long:bool` · `is_short:bool` · `ts_event:int`

### 7.3 SigmaOrder
`symbol:str` · `side:OrderSide` · `quantity:float` · `order_type:OrderType` · `time_in_force:str` · `limit_price:float?` · `stop_price:float?` · `order_id:str` · `client_order_id:str` · `status:OrderStatus` · `filled_qty:float` · `leaves_qty:float` · `avg_px:float?` · `last_px:float?` · `last_qty:float?` · `reject_reason:str?` · `last_fill:SigmaFill?` · `commission:float` · `is_buy:bool` · `is_sell:bool` · `is_filled:bool` · `is_open:bool` · `account:str` · `executor_id:str`(empty if placed directly) · `market_center:str` · `ts_event:int` · `ts_init:int` · `time:datetime?` · `time_utc:datetime?`

### 7.4 SigmaFill  (via `order.last_fill`)
`trade_id:str` · `execution_id:str` · `last_qty:float`(alias `filled_qty`) · `last_px:float`(alias `avg_px`) · `commission:float` · `liquidity_side:str`("MAKER"|"TAKER") · `symbol:str` · `side:str`("BUY"|"SELL") · `ts_event:int`

### 7.5 SigmaTradeTick
`symbol:str` · `price:float` · `size:float` · `aggressor_side:str`("BUY"|"SELL"|"NO_AGGRESSOR") · `trade_id:str` · `exchange:str` · `ts_event:int` · `time/time_utc:datetime?`

### 7.6 SigmaQuoteTick
`symbol:str` · `bid_price:float` · `ask_price:float` · `bid_size:float` · `ask_size:float` · `mid_price:float` · `spread:float` · `exchange:str` · `ts_event:int` · `time/time_utc:datetime?`

### 7.7 SigmaSnapData (options snapshot)
`symbol:str`(root) · `chain:str`(OCC) · `underlying:str` · `option_type:str`("C"|"P") · `expiration_date:str` · `strike:float` · `bid_px:float` · `ask_px:float` · `price:float` · `bid_sz:int` · `ask_sz:int` · `size:int` · `mid_price:float` · `spread:float` · `date:str`('YYYY-MM-DD') · `ts_event:int` · `time/time_utc:datetime?` · method `column_data(name, default=None)`

### 7.8 TimerEventData
`timer_id:str` · `ts_event:int` · `ts_init:int` · `time/time_utc:datetime?`

### 7.9 SigmaCustomData
`symbol:str` · `event_id:str`(data source id) · `data:Dict[str,str]`(column→value) · `header:str`(CSV header row) · `row:str`(raw CSV row) · `ts_event:int` · `time/time_utc:datetime?` · method `column_data(name, default=None)`

`column_data(name, default=None)` returns the string value of the named CSV column for this row, or `default` if the column is absent. All values are strings — cast to `float`/`int`/`bool` in strategy code.

**CSV custom data** (`data_type='custom'`): every column in your CSV is accessible by name via `column_data()`. The engine uses `date` + `time` columns (or a `timestamp` column) to determine when each row fires during the backtest; all other columns are user-defined.

**HIVEQ_QUANT_SIGNALS**: rows arrive with a `signal_json` column containing a JSON-encoded string. Parse it with `json.loads(data.column_data("signal_json"))` to access signal fields.

### 7.10 SigmaInstrument  (`ctx.instrument(symbol)`)
`symbol:str` · `last_bar:Bar?` · `multiplier:float` · `exchange` · `min_tick:float` · `asset_type:AssetType` · `current_contract:str`(resolved contract for continuous) · `security_details` · `native_instrument_id` · `tradeStats:SigmaTradeStats?`(`symbol,open,high,low,close,volume`)

### 7.11 IndexPrice
`symbol:str` · `price:float` · `ts_event:int` · `ts_init:int`

### 7.12 Rollover
`continuous_symbol:str`("ES.c.0") · `prev_contract:str`("ESZ5") · `current_contract:str`("ESH6") · `ts_event:int`

### 7.13 EXECUTOR_EVENT / SECURITY_EVENT (advanced)
These fire for executor lifecycle transitions (`on_executor`) and security/reference updates (`on_security_event`). Their payloads are not part of the stable strategy-authoring surface — treat `event.data()` as opaque. For executors, prefer `ctx.executor_state(executor)` (§5.10) to read state rather than parsing the event. Most strategies do not handle these.

---

