from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import export_run_artifacts

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as _get_logger


logger = _get_logger()

SYMBOL = "ES.c.0"
DATA_ID = "order_replay_set_a_skip_dates_v3"
USERDATA_PATH = "userdata/order_replay_set_a/order_events_full_skip_dates_v3.csv"
START_DATE = "2013-01-01"
END_DATE = "2019-12-31"
TASK_NAME = "bt-replay-set-a-skip-dates-v3-rf0-cap-slice"
INITIAL_CAPITAL = 2_500_000.0
RISK_FREE_RATE = 0.0
POSITION_NOTIONAL_FRACTION = 0.2
POSITION_NOTIONAL_CAP = INITIAL_CAPITAL * POSITION_NOTIONAL_FRACTION
OUTPUT_DIR = (
    Path(__file__).resolve().parent
    / "runs"
    / "bars_1m_full_skip_dates_v3_rf0_cap_slice_no_roll_no_close"
)
SENSITIVE_OUTPUT_COLUMNS = {"user_id", "user_name", "org_id", "trader_id", "account_id"}
DEFAULT_BACKTEST_CONFIG = BacktestConfig()


class SdkT48ReplaySetACapSliceV3:
    def __init__(self):
        self.custom_events = 0
        self.orders_requested = 0
        self.last_bar_close = None
        self.last_bar_symbol = None

    def on_start(self, ctx: hf.Context, event):
        ctx.subscribe_bars([SYMBOL], asset_type=AssetType.FUTURES, interval="1m")
        ctx.subscribe_data(DATA_ID)
        logger.info(
            f"[START] subscribed bars_1m symbol={SYMBOL} data_id={DATA_ID} "
            f"userdata_path={USERDATA_PATH}"
        )

    def on_bar(self, ctx, event):
        bar = event.data()
        self.last_bar_close = bar.close
        self.last_bar_symbol = bar.symbol
        logger.debug(
            f"[BAR] time={bar.time} resolved_symbol={bar.symbol} "
            f"close={bar.close} custom_events={self.custom_events}"
        )

    def _latest_price(self, ctx, symbol: str) -> float | None:
        if self.last_bar_close is not None:
            return float(self.last_bar_close)
        instrument = ctx.instrument(symbol)
        last_bar = getattr(instrument, "last_bar", None)
        close = getattr(last_bar, "close", None)
        return None if close is None else float(close)

    def _target_contracts(self, ctx, symbol: str, raw_target: float) -> int:
        if raw_target == 0.0:
            return 0

        price = self._latest_price(ctx, symbol)
        instrument = ctx.instrument(symbol)
        multiplier = float(getattr(instrument, "multiplier", 0.0) or 0.0)
        if price is None or price <= 0.0 or multiplier <= 0.0:
            logger.warning(
                f"[SKIP_SIZE] raw_target={raw_target} symbol={symbol} "
                f"price={price} multiplier={multiplier}"
            )
            return 0

        contracts = int(POSITION_NOTIONAL_CAP / (price * multiplier))
        signed_contracts = contracts if raw_target > 0.0 else -contracts
        logger.debug(
            f"[SIZE] raw_target={raw_target} symbol={symbol} price={price} "
            f"multiplier={multiplier} target_contracts={signed_contracts}"
        )
        return signed_contracts

    def on_custom_data(self, ctx, event):
        data = event.data()
        symbol = data.column_data("sym", default=SYMBOL) or SYMBOL
        raw_target = float(data.column_data("target_quantity", default="0") or "0")
        target_contracts = self._target_contracts(ctx, symbol, raw_target)

        self.custom_events += 1
        logger.debug(
            f"[CUSTOM] raw_target={raw_target} target_contracts={target_contracts} "
            f"symbol={symbol} last_bar={self.last_bar_symbol}@{self.last_bar_close}"
        )

        order = ctx.order_to_target(symbol, target_quantity=target_contracts)
        if order is not None:
            self.orders_requested += 1
            logger.info(
                f"[ORDER_REQUEST] target_contracts={target_contracts} symbol={symbol}"
            )

    def on_order(self, ctx, event):
        order = event.data()
        logger.info(
            f"[ORDER] symbol={order.symbol} status={order.status} "
            f"filled={order.is_filled} avg_px={order.avg_px}"
        )


def _write_table(value: Any, path: Path) -> None:
    if value is None:
        path.write_text("", encoding="utf-8")
        return
    if hasattr(value, "to_csv"):
        cleaned = value
        if hasattr(value, "columns"):
            drop_cols = [c for c in value.columns if str(c) in SENSITIVE_OUTPUT_COLUMNS]
            if drop_cols:
                cleaned = value.drop(columns=drop_cols)
        cleaned.to_csv(path, index=False)
        return
    path.write_text(str(value), encoding="utf-8")


def _total_trades_from_stats(stats: Any) -> Any:
    if stats is None:
        return None
    try:
        return stats.loc["Total Trades"]
    except Exception:
        pass
    try:
        return stats["Total Trades"]
    except Exception:
        pass
    try:
        return stats.loc[stats["metric"] == "Total Trades", "value"].iloc[0]
    except Exception:
        return None


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    local_userdata = Path(__file__).resolve().parent / "data" / USERDATA_PATH
    backtest_config = BacktestConfig(
        symbols=[SYMBOL],
        start_date=START_DATE,
        end_date=END_DATE,
        initial_capital=INITIAL_CAPITAL,
        commission=DEFAULT_BACKTEST_CONFIG.commission,
        futures_fee=DEFAULT_BACKTEST_CONFIG.futures_fee,
        risk_free_rate=RISK_FREE_RATE,
        enable_auto_rollover=False,
        auto_flatten_at_close=False,
        enable_tca=False,
    )

    run = hf.run_backtest(
        strategy_configs=[
            StrategyConfig(
                name="ReplaySetACapSliceV3",
                type="SdkT48ReplaySetACapSliceV3",
            )
        ],
        data_configs=[
            {
                "type": "hiveq_historical",
                "dataset": "HIVEQ_US_FUT",
                "schema": ["bars_1m"],
            },
            {
                "type": "csv",
                "data_type": "custom",
                "id": DATA_ID,
                "path": str(local_userdata),
            },
        ],
        backtest_config=backtest_config,
    )
    print(f"run_id={run.run_id}")
    print(f"task_id={run.task_id}")

    run.wait(progress=False)
    report = run.report()

    metadata = {
        "run_id": run.run_id,
        "task_id": run.task_id,
        "symbol": SYMBOL,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "data_id": DATA_ID,
        "userdata_path": USERDATA_PATH,
        "market_data": {"dataset": "HIVEQ_US_FUT", "schema": ["bars_1m"]},
        "initial_capital": INITIAL_CAPITAL,
        "requested_buying_power": 2_500_000.0,
        "position_notional_fraction": POSITION_NOTIONAL_FRACTION,
        "position_notional_cap": POSITION_NOTIONAL_CAP,
        "commission": DEFAULT_BACKTEST_CONFIG.commission,
        "futures_fee": DEFAULT_BACKTEST_CONFIG.futures_fee,
        "risk_free_rate": RISK_FREE_RATE,
        "enable_auto_rollover": False,
        "auto_flatten_at_close": False,
        "task_name": TASK_NAME,
    }
    (OUTPUT_DIR / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )
    _write_table(getattr(report, "return_stats", None), OUTPUT_DIR / "return_stats.csv")
    _write_table(getattr(report, "run_info", None), OUTPUT_DIR / "run_info.csv")
    _write_table(run.trades(), OUTPUT_DIR / "trades.csv")
    _write_table(run.positions(), OUTPUT_DIR / "positions.csv")

    total_trades = _total_trades_from_stats(getattr(report, "return_stats", None))
    positions = run.positions()
    validation = {
        "symbol": SYMBOL,
        "enable_auto_rollover": False,
        "orders": len(run.orders()),
        "trades": len(run.trades()),
        "positions": len(positions),
        "open_positions": int((positions["quantity"] != 0).sum()),
        "total_trades_stat": total_trades,
        "passed": bool(total_trades and (positions["quantity"] != 0).sum() == 0),
    }
    artifacts = export_run_artifacts(run, validation=validation)
    print(f"total_trades={total_trades}")
    print(f"saved_outputs={OUTPUT_DIR}")
    print(f"run_artifacts={artifacts}")
    if not validation["passed"]:
        raise AssertionError(f"replay validation failed: {validation}")


if __name__ == "__main__":
    main()
