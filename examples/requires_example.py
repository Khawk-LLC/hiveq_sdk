"""HQB-037: staging test of per-job LightGBM and XGBoost installation."""
import uuid
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType
from hiveq.flow.logger import logger as get_logger

logger = get_logger()
modules = ["lightgbm==4.7.0", "xgboost==3.0.5"]


class MLDependencyBacktest:
    def __init__(self):
        self.bars = 0
        self.entered = False
        self.exited = False

    def on_start(self, ctx, event):
        # These imports execute on the platform, after per-job installation.
        import json
        import numpy as np
        import lightgbm as lgb
        import xgboost as xgb

        assert lgb.__version__ == "4.7.0"
        assert xgb.__version__ == "3.0.5"
        # Tiny synthetic training set: a dependency smoke test, not a trading model.
        X = np.arange(16, dtype=float).reshape(8, 2)
        y = np.arange(8, dtype=float)
        a = lgb.train(
            {"objective": "regression", "verbosity": -1,
             "num_threads": 1, "min_data_in_leaf": 1},
            lgb.Dataset(X, label=y), num_boost_round=2)
        b = xgb.train(
            {"objective": "reg:squarederror", "nthread": 1, "device": "cpu"},
            xgb.DMatrix(X, label=y), num_boost_round=2)
        predictions = [float(a.predict(X[-1:])[0]),
                       float(b.predict(xgb.DMatrix(X[-1:]))[0])]
        assert all(np.isfinite(predictions))
        print("ML_DEPENDENCY_OK " + json.dumps({
            "lightgbm": lgb.__version__, "xgboost": xgb.__version__,
            "lightgbm_path": lgb.__file__, "xgboost_path": xgb.__file__,
            "predictions": predictions,
        }))
        ctx.subscribe_bars(["AAPL"], asset_type=AssetType.EQUITY, interval="1m")
        logger.info("Both models fitted; subscribed to AAPL bars")

    def on_bar(self, ctx, event):
        bar = event.data()
        self.bars += 1
        logger.debug(f"bar={self.bars} close={bar.close} entered={self.entered}")
        if not self.entered and ctx.is_flat(bar.symbol):
            ctx.buy_order(bar.symbol, quantity=1.0)
            self.entered = True
            logger.info("Submitted one-share test entry")
        elif self.bars >= 4 and not self.exited and not ctx.is_flat(bar.symbol):
            ctx.close_position(bar.symbol)
            self.exited = True
            logger.info("Submitted test exit")

    def on_order(self, ctx, event):
        order = event.data()
        logger.info(f"order status={order.status} filled={order.is_filled}")


if __name__ == "__main__":
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(
            name="MLDependencyBacktest_Import", type="MLDependencyBacktest")],
        symbols=["AAPL"], start_date="2025-11-03", end_date="2025-11-03",
        data_configs=[{"type": "hiveq_historical", "dataset": "HIVEQ_US_EQ",
                       "schema": ["bars_1m"]}],
        backtest_config=BacktestConfig(session_start="09:30", session_end="09:40"),
        requirements=modules
    )
    run.wait(timeout=600, progress=False)
    report = run.report()
    trades = report.stats.get("Total Trades", 0)
    print(f"run_id={run.run_id}; Total Trades={trades}")
    assert float(trades) > 0, "Backtest did not complete a trade"