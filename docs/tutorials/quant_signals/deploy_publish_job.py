"""Deploy the quant_features_v3 publish to the platform as a QUANT_SCRIPTS job.

The route that works from a laptop with ONLY `hiveq-sdk` installed: the function
below is cloudpickled and run inside a platform container, where the real driver
lives. See README.md for why each config property is what it is.

    python3 deploy_publish_job.py
"""
import os

import hiveq.flow as hf

SIGNAL_NAME = 'hiveq_signal_v1_test'
INSTRUMENT = 'SPY'

# Distributor address. Override without editing this file:
#   export HIVEQ_DISTRIBUTOR_HOST=<host>
# In a deployed job the CONTAINER's value wins -- see publish_signal().
WS_HOST = os.environ.get('HIVEQ_DISTRIBUTOR_HOST', 'hiveq-distributor-staging')
WS_PORT = int(os.environ.get('HIVEQ_DISTRIBUTOR_PORT', '8765'))

CONFIG = {
    'QuantFeaturesPub': {
        'primary': 'HiveQQuantFeaturesPub',
    },
    'HiveQQuantFeaturesPub': {
        'transport': 'HiveQ',
        'topic': 'prod.quants.quant_features_v3',   # `topic` -> WebSocket route, to live subscribers
        'keyField': 'symbol',                       # per-row key = the signal name, not `id`
        'wsHost': WS_HOST,                          # a named service; localhost:8765 is refused
        'wsPort': WS_PORT,
    },
    'QuantFeaturesRead': {
        'primary': 'HiveQQuantFeaturesRead',
    },
    'HiveQQuantFeaturesRead': {
        'transport': 'HiveQ',
        'dataset': 'HIVEQ_QUANT_FEATURES_V3',       # a read section needs dataset + schema
        'schema': 'quant_features_v3',
        'columns': ('date,time,symbol,ticker,norm_ticker,root,signal1,weight1,'
                    'score,price,side,text,id,version'),   # else you get 10 of 38 columns
        'limit': 1000,
        'timezone': 'UTC',                          # 'UTC' = do not convert; the default shifts rows -4h
    },
}


def publish_signal():
    """Publish one row to the topic, then REST-read the stored history."""
    import collections
    import datetime

    import pandas as pd

    # Imports inside the function, so they resolve against the container's real
    # driver rather than the deploying machine's stubs.
    # Driver(config=...), never the dd.* facade: its logger does
    # makedirs('<cwd>/logs') and the container's cwd /app is read-only.
    from hiveq.driver.data_driver import Driver
    from hiveq.driver import Cache
    from hiveq.datetime import DateRange

    # Let the container's own distributor address win over the one baked in at
    # deploy time, so the host can change without redeploying this code.
    import os
    cfg = {k: dict(v) for k, v in CONFIG.items()}
    cfg['HiveQQuantFeaturesPub'].update(
        wsHost=os.environ.get('HIVEQ_DISTRIBUTOR_HOST', WS_HOST),
        wsPort=int(os.environ.get('HIVEQ_DISTRIBUTOR_PORT', WS_PORT)),
    )
    print('distributor: %s:%s' % (cfg['HiveQQuantFeaturesPub']['wsHost'],
                                  cfg['HiveQQuantFeaturesPub']['wsPort']))

    driver = Driver(config=cfg)

    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S.000')
    today = now.strftime('%Y-%m-%d')

    # 38 columns, 17 NOT NULL. Timestamps are naive ET -- never convert from UTC.
    row = {
        'symbol':      SIGNAL_NAME,        # the signal's NAME
        'ticker':      INSTRUMENT,         # the instrument it refers to
        'norm_ticker': INSTRUMENT,
        'root':        INSTRUMENT,
        'id':          SIGNAL_NAME,
        'table':       'quant_features_v3',

        'date':        today,
        'time':        ts,
        'ts_event':    ts,
        'recv_event':  ts,
        'db_event':    ts,

        'signal1':     1.0,
        'signal2':     0.0,
        'signal3':     0.0,
        'weight1':     1.0,
        'weight2':     0.0,
        'weight3':     0.0,
        'score':       0.42,
        'side':        1,
        'size':        0.0,

        'price':       655.25,
        'stop_px':     0.0,
        'target_px':   0.0,
        'volume':      0.0,

        'freq':        0,
        'flag':        '',
        'text':        '%s via QUANT_SCRIPTS' % SIGNAL_NAME,
        'version':     1.0,
    }
    row.update({'reserved%d' % i: None for i in range(1, 11)})

    df = pd.DataFrame([row])
    print('Publishing %d row(s) of %s (%d columns) to topic %s'
          % (len(df), SIGNAL_NAME, len(row),
             CONFIG['HiveQQuantFeaturesPub']['topic']))

    # save() returns the frame on success and None on failure -- it never raises.
    saved = driver.save('QuantFeaturesPub', df)
    if saved is None:
        raise RuntimeError('publish failed -- see the [HiveQ] lines in this log')
    print('Published (distributor acked every row).')

    # A downstream consumer persists topic traffic, measured at 5-10s. Wait past
    # that or the row above is genuinely not there yet.
    import time
    time.sleep(12)

    Params = collections.namedtuple('Params', ['date', 'symbol'])
    params = Params(DateRange(today, today), [SIGNAL_NAME])

    back = driver.load('QuantFeaturesRead', params_tuple=params, cache=Cache.NO_CACHE)
    found = 0 if back is None else len(back)
    print('Stored history: %d row(s) for %s on %s' % (found, SIGNAL_NAME, today))
    if found:
        print(back.to_string(index=False))

    # Returned dict -> job.result()['result']; prints -> job.logs().
    return {'signal': SIGNAL_NAME, 'published': int(len(df)), 'stored_history': int(found)}


if __name__ == '__main__':
    # Credentials are HIVEQ_API_KEY only; identity resolves server-side.
    job = hf.deploy_job(
        publish_signal,
        task_name='publish-hiveq-signal-v1-test',
        requirements=['pandas'],
        wait=True,
    )

    print('task_id: %s' % job.task_id)
    print('result : %s' % job.result())
    print('--- logs ---')
    print(job.logs())
