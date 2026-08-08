"""Remote callback bar counts match the underlying Data API for equity and futures."""
from collections import Counter
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent)]
from qa_common import completed_checkpoint, emit_checkpoint, finish

import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType


class SdkT42A:
    def on_start(self, ctx, event):
        if not hasattr(self, "counts"):
            self.counts = Counter()
        ctx.subscribe_bars(["AAPL", "MSFT"], asset_type=AssetType.EQUITY, interval="1m")
    def on_bar(self, ctx, event):
        self.counts[event.data().symbol] += 1
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t42_equity", dict(self.counts))


class SdkT42B:
    def on_start(self, ctx, event):
        if not hasattr(self, "counts"):
            self.counts = Counter()
        ctx.subscribe_bars(["ES.c.0", "NQ.c.0"], asset_type=AssetType.FUTURES, interval="1m")
    def on_bar(self, ctx, event):
        self.counts[event.data().symbol] += 1
    def on_stop(self, ctx, event):
        emit_checkpoint(ctx, "t42_futures", dict(self.counts))


def remote_counts(cls, name, symbols, dataset, session_start, session_end):
    data = {"type": "hiveq_historical", "dataset": dataset, "schema": ["bars_1m"]}
    if dataset == "HIVEQ_US_FUT":
        data["filter_mode"] = "continuous"
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name=cls.__name__, type=cls.__name__, symbols=symbols)],
        symbols=symbols,
        start_date="2025-09-15",
        end_date="2025-09-19",
        data_configs=[data],
        backtest_config=BacktestConfig(session_start=session_start, session_end=session_end),
    )
    return completed_checkpoint(run, name)


def api_counts(dataset, symbols, session_start, session_end):
    import hiveq_data

    client = hiveq_data.Historical(timezone="America/New_York")
    result = Counter()
    offset = 0
    page_size = 500_000
    while True:
        response = client.get_data(
            dataset=dataset,
            schema="bars_1m",
            symbols=symbols,
            start=f"2025-09-15 {session_start}:00",
            end=f"2025-09-19 {session_end}:00",
            filter_mode="session",
            limit=page_size,
            offset=offset,
        ) or {}
        rows = response.get("data", [])
        for row in rows:
            symbol = row.get("symbol") or row.get("ticker") or row.get("instrument")
            if symbol:
                result[str(symbol)] += 1
        if len(rows) < page_size:
            break
        offset += page_size
    return dict(result)


def compare(stream, direct):
    keys = set(stream) | set(direct)
    return bool(keys) and all(stream.get(key, 0) == direct.get(key, 0) for key in keys)


if __name__ == "__main__":
    eq_stream = remote_counts(
        SdkT42A, "t42_equity", ["AAPL", "MSFT"], "HIVEQ_US_EQ", "09:30", "16:00"
    )
    eq_api = api_counts("HIVEQ_US_EQ", ["AAPL", "MSFT"], "09:30", "16:00")
    fut_stream = remote_counts(
        SdkT42B, "t42_futures", ["ES.c.0", "NQ.c.0"],
        "HIVEQ_US_FUT", "18:00", "17:00"
    )
    # The Data API resolves continuous aliases to outright symbols just as callbacks do.
    fut_api = api_counts("HIVEQ_US_FUT", ["ES.c.0", "NQ.c.0"], "18:00", "17:00")
    finish("t42_stream_data_api_parity", {
        "equity_stream_nonempty": bool(eq_stream),
        "equity_api_nonempty": bool(eq_api),
        "equity_counts_exact": compare(eq_stream, eq_api),
        "futures_stream_nonempty": bool(fut_stream),
        "futures_api_nonempty": bool(fut_api),
        "futures_counts_exact": compare(fut_stream, fut_api),
    }, extra=f"eq_stream={eq_stream}, eq_api={eq_api}, fut_stream={fut_stream}, fut_api={fut_api}")
