"""Realtime round-trip on the quant_features_v3 topic: subscribe, publish, receive.

Use this to validate a REALTIME subscription -- delivery confirmed in
milliseconds, off the wire. A topic publish is also persisted by a downstream
consumer, but that takes 5-10s, so a REST read is the wrong tool for confirming
a publish you just made (see README §4d).

Three routes, three purposes:

    publishing                      -> topic (WebSocket, via the distributor)
    validating stored history       -> REST, dataset + schema
    validating a realtime feed      -> topic subscription  <-- this file

    python3 realtime_subscribe_job.py

Runs as a QUANT_SCRIPTS job, because the distributor is a service on the
platform network and is not reachable from a laptop.
"""
import os

import hiveq.flow as hf

SIGNAL_NAME = 'hiveq_signal_v1_test'
INSTRUMENT = 'SPY'

TOPIC = 'prod.quants.quant_features_v3'

# Distributor address. This runs IN a platform container, so the service name
# applies; from the VM it is 172.16.60.192:28765 instead.
WS_HOST = os.environ.get('HIVEQ_DISTRIBUTOR_HOST', 'hiveq-distributor-staging')
WS_PORT = int(os.environ.get('HIVEQ_DISTRIBUTOR_PORT', '8765'))

# Publisher and subscriber share the topic. That is the point: what one sends,
# the other receives.
CONFIG = {
    'QuantFeaturesPub': {
        'primary': 'HiveQQuantFeaturesPub',
    },
    'HiveQQuantFeaturesPub': {
        'transport': 'HiveQ',
        'topic': TOPIC,
        'keyField': 'symbol',      # per-row key, read from df['symbol']
        'wsHost': WS_HOST,
        'wsPort': WS_PORT,
    },
    'QuantFeaturesSub': {
        'primary': 'HiveQQuantFeaturesSub',
    },
    'HiveQQuantFeaturesSub': {
        'transport': 'HiveQ',
        'topic': TOPIC,
        'keyField': 'symbol',      # for a subscriber: the params field holding the keys
        'wsHost': WS_HOST,
        'wsPort': WS_PORT,
    },
}


def realtime_roundtrip():
    """Subscribe to the topic, publish onto it, and receive what we published."""
    import collections
    import datetime

    import pandas as pd

    from hiveq.driver.data_driver import Driver

    # Publisher and subscriber must agree, so resolve once and set both.
    import os
    host = os.environ.get('HIVEQ_DISTRIBUTOR_HOST', WS_HOST)
    port = int(os.environ.get('HIVEQ_DISTRIBUTOR_PORT', WS_PORT))
    cfg = {k: dict(v) for k, v in CONFIG.items()}
    for section in ('HiveQQuantFeaturesPub', 'HiveQQuantFeaturesSub'):
        cfg[section].update(wsHost=host, wsPort=port)
    print('distributor: %s:%s' % (host, port))

    driver = Driver(config=cfg)

    Params = collections.namedtuple('Params', ['symbol'])
    params = Params([SIGNAL_NAME])

    # 1. Open the subscription FIRST -- the distributor only forwards to
    #    already-connected subscribers, so 0 rows here is correct.
    before = driver.load('QuantFeaturesSub', params_tuple=params, time_out=3000)
    n_before = 0 if before is None else len(before)
    print('subscription open; %d row(s) buffered before publishing' % n_before)

    # 2. Publish one row onto the same topic.
    now = datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S.000')
    marker = 'realtime-%s' % now.strftime('%H%M%S')

    row = {
        'symbol': SIGNAL_NAME, 'ticker': INSTRUMENT,
        'norm_ticker': INSTRUMENT, 'root': INSTRUMENT,
        'id': SIGNAL_NAME, 'table': 'quant_features_v3',
        'date': now.strftime('%Y-%m-%d'), 'time': ts,
        'ts_event': ts, 'recv_event': ts, 'db_event': ts,
        'signal1': 1.0, 'signal2': 0.0, 'signal3': 0.0,
        'weight1': 1.0, 'weight2': 0.0, 'weight3': 0.0,
        'score': 0.42, 'side': 1, 'size': 0.0,
        'price': 655.25, 'stop_px': 0.0, 'target_px': 0.0, 'volume': 0.0,
        'freq': 0, 'flag': '', 'text': marker, 'version': 1.0,
    }
    row.update({'reserved%d' % i: None for i in range(1, 11)})

    df = pd.DataFrame([row])
    saved = driver.save('QuantFeaturesPub', df)
    if saved is None:
        raise RuntimeError('publish failed -- see the [HiveQ] lines in this log')
    print('published marker %s to %s' % (marker, TOPIC))

    # 3. Poll until our marker arrives. `time_out` does NOT wait for rows -- it
    #    blocks only until the initial connect completes, so once the subscriber
    #    is up every load() returns immediately with whatever is buffered.
    #    Polling is what makes this deterministic rather than a race.
    import time
    deadline = time.time() + 15
    after, n_after, got_marker = None, 0, False
    while time.time() < deadline:
        after = driver.load('QuantFeaturesSub', params_tuple=params)
        n_after = 0 if after is None else len(after)
        got_marker = bool(n_after and 'text' in after.columns
                          and (after['text'] == marker).any())
        if got_marker:
            break
        time.sleep(0.5)

    print('received %d row(s) after publishing' % n_after)
    if n_after:
        print(after.to_string(index=False))
    print('round-trip %s' % ('CONFIRMED' if got_marker else 'NOT confirmed'))

    return {'signal': SIGNAL_NAME, 'topic': TOPIC, 'marker': marker,
            'buffered_before': int(n_before), 'received_after': int(n_after),
            'round_trip': got_marker}


if __name__ == '__main__':
    job = hf.deploy_job(
        realtime_roundtrip,
        task_name='realtime-subscribe-hiveq-signal-v1-test',
        requirements=['pandas'],
        wait=True,
    )

    print('task_id: %s' % job.task_id)
    print('result : %s' % job.result())
    print('--- logs ---')
    print(job.logs())
