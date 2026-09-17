"""Backfill a day of `hiveq_signal_v1_test` history for the backtest to consume.

A backtest reads STORED history, so it needs signal rows already in
quant_features_v3 on a date that also has market data. This publishes eight rows
across 2026-06-01, a day with SPY bars.

Route note: this uses the REST/bulk route (`publishSchema`), not the topic. A
live signal goes on the topic; this is a historical fixture being loaded, which
is exactly what the REST route is for.

    python3 backfill_signal_history.py
"""
import hiveq.flow as hf

SIGNAL_NAME = 'hiveq_signal_v1_test'
INSTRUMENT = 'SPY'
SESSION_DATE = '2026-06-01'

CONFIG = {
    'QuantFeaturesPub': {
        'primary': 'HiveQQuantFeaturesPub',
    },
    'HiveQQuantFeaturesPub': {
        'transport': 'HiveQ',
        'publishSchema': 'quant_features_v3',   # no `topic` -> REST, straight to storage
        'key': SIGNAL_NAME,
        'operation': 'add',
        'async': False,                         # so a failure is visible
    },
}

# (time, side, weight1) -- side 1 is long, 0 is flat.
BARS = [
    ('09:35:00', 1, 0.90),
    ('10:05:00', 1, 0.95),
    ('10:35:00', 0, 0.20),
    ('11:05:00', 1, 0.85),
    ('12:00:00', 1, 0.60),   # below the strategy's threshold on purpose
    ('13:00:00', 0, 0.10),
    ('14:00:00', 1, 0.99),
    ('15:00:00', 0, 0.05),
]


def backfill():
    """Publish the fixture rows. Runs on the platform, where data access lives."""
    import pandas as pd

    from hiveq.driver.data_driver import Driver

    driver = Driver(config=CONFIG)

    rows = []
    for hhmmss, side, weight in BARS:
        ts = '%s %s.000' % (SESSION_DATE, hhmmss)
        row = {
            'symbol': SIGNAL_NAME, 'ticker': INSTRUMENT,
            'norm_ticker': INSTRUMENT, 'root': INSTRUMENT,
            'id': SIGNAL_NAME, 'table': 'quant_features_v3',
            'date': SESSION_DATE, 'time': ts,
            'ts_event': ts, 'recv_event': ts, 'db_event': ts,
            'signal1': float(side), 'signal2': 0.0, 'signal3': 0.0,
            'weight1': float(weight), 'weight2': 0.0, 'weight3': 0.0,
            'score': float(weight), 'side': int(side), 'size': 0.0,
            'price': 0.0, 'stop_px': 0.0, 'target_px': 0.0, 'volume': 0.0,
            'freq': 0, 'flag': 'long' if side == 1 else 'flat',
            'text': 'backtest fixture', 'version': 1.0,
        }
        row.update({'reserved%d' % i: None for i in range(1, 11)})
        rows.append(row)

    df = pd.DataFrame(rows)
    print('Publishing %d fixture row(s) for %s on %s' % (len(df), SIGNAL_NAME, SESSION_DATE))
    print(df[['time', 'symbol', 'ticker', 'side', 'weight1', 'flag']].to_string(index=False))

    saved = driver.save('QuantFeaturesPub', df)
    if saved is None:
        raise RuntimeError('publish failed -- see the [HiveQ] lines in this log')

    return {'signal': SIGNAL_NAME, 'date': SESSION_DATE, 'published': int(len(df))}


if __name__ == '__main__':
    job = hf.deploy_job(backfill, task_name='backfill-hiveq-signal-v1-test',
                        requirements=['pandas'], wait=True)
    print('result:', job.result())
    print(job.logs())
