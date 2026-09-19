"""Backtest that trades on the published `hiveq_signal_v1_test` signal.

The signal rows live in quant_features_v3 (published by the first tutorial).
Here they are wired in as a custom data source, so each row fires
`on_custom_data` at its own timestamp, interleaved with SPY bars.

    python3 backtest_on_signal.py

Run backfill_signal_history.py first -- a backtest reads stored history, and
2026-06-01 is chosen because SPY has bars that day.
"""
import hiveq.flow as hf
from hiveq.flow import BacktestConfig, StrategyConfig
from hiveq.flow.config import AssetType

SIGNAL_ID = 'qf3_signals'            # must match the data_configs 'id'
SIGNAL_NAME = 'hiveq_signal_v1_test'  # the `symbol` column in quant_features_v3
INSTRUMENT = 'SPY'

MIN_SCORE = 0.7                       # only act on high-conviction rows
QTY = 100


class SignalFollower:
    def on_start(self, ctx, event):
        # Bars initialise the venue so orders are routable, and give the signal
        # something to trade against.
        ctx.subscribe_bars(ctx.strategy_config.symbols,
                           asset_type=AssetType.EQUITY, interval='1m')
        ctx.subscribe_data(data_id=SIGNAL_ID)
        ctx.add_event_log('subscribed to %s' % SIGNAL_ID, sub_event_type='INIT')

    def on_custom_data(self, ctx, event):
        data = event.data()

        # quant_features_v3 has flat columns -- no `signal_json` to unwrap, which
        # is the difference from a HIVEQ_QUANT_SIGNALS feed.
        ticker = data.column_data('ticker', default=INSTRUMENT)
        signal1 = float(data.column_data('signal1', default='0') or 0)
        score = float(data.column_data('score', default='0') or 0)

        ctx.add_event_log('signal %s signal1=%.0f score=%.2f' % (ticker, signal1, score),
                          sub_event_type='SIGNAL', symbol=str(ticker))

        if signal1 >= 1 and score >= MIN_SCORE and ctx.is_flat(ticker):
            ctx.buy_order(ticker, quantity=QTY)
            ctx.add_event_log('enter long %s' % ticker, symbol=str(ticker))
        elif signal1 < 1 and ctx.is_net_long(ticker):
            ctx.close_position(ticker)
            ctx.add_event_log('exit %s' % ticker, symbol=str(ticker))

    def on_order(self, ctx, event):
        o = event.data()
        if o.is_filled:
            ctx.add_event_log('fill %s qty=%s @ %s' % (o.symbol, o.filled_qty, o.avg_px),
                              symbol=o.symbol)


if __name__ == '__main__':
    run = hf.run_backtest(
        strategy_configs=[StrategyConfig(name='SignalFollower', type='SignalFollower')],
        symbols=[INSTRUMENT],
        start_date='2026-06-01',
        end_date='2026-06-01',
        data_configs=[
            {'type': 'hiveq_historical', 'dataset': 'HIVEQ_US_EQ',
             'schema': ['bars_1m']},
            {'type': 'hiveq_historical', 'dataset': 'HIVEQ_QUANT_FEATURES_V3',
             'schema': ['quant_features_v3'],
             'id': SIGNAL_ID,
             'symbols': [SIGNAL_NAME],
             # The request field `symbols` is sent under. quant_features_v3
             # filters on `symbol`.
             'symbolFilterKey': 'symbol'},
        ],
        backtest_config=BacktestConfig(
            initial_capital=100_000,
            # `signals_datasets` lists datasets that key off this config's
            # `symbols` instead of the run's symbol universe. The default is
            # ['HIVEQ_QUANT_SIGNALS'] only, so quant_features_v3 has to be added
            # or the engine would look for SPY rows in it.
            extra_config={'signals_datasets': ['HIVEQ_QUANT_SIGNALS',
                                               'HIVEQ_QUANT_FEATURES_V3']},
        ),
    )

    run.wait()
    print('status:', run.status())

    report = run.report()
    stats = getattr(report, 'stats', None)
    if callable(stats):
        print(stats())

    for row in run.event_logs().get('logs', [])[:20]:
        print(row)
