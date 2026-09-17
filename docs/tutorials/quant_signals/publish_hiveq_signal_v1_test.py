"""Publish one row of the signal `hiveq_signal_v1_test` into quant_features_v3.

Run this where the REAL data driver exists AND the network is provisioned:

    E2  any HiveQ platform container - marimo, Jupyter or VS Code; the driver is
                                      installed and the distributor is on the network
    E3  your company VM            - install the wheel from the internal systems
                                     team; the VM's network is entitled to the data

On a laptop the driver installs but reaches nothing: data access is IP restricted
and the distributor is not exposed publicly. Publish from the VM, or deploy the
job (deploy_publish_job.py), which runs on the platform.

On a machine with only `hiveq-sdk` installed, `hiveq.driver` is an import stub:
the imports below succeed and the first driver CALL raises PlatformOnlyError.
That is the designed contract, not a broken install.

    python3 publish_hiveq_signal_v1_test.py
"""
import datetime
import os

import pandas as pd

import hiveq.driver as dd

# Distributor address, as reachable from the VM / provisioned network.
#
# TEMPORARY: this default is the internal address used while the public endpoint
# is being opened up. It becomes staging.hiveq.ai:8765 -- at which point this
# default changes and the override below is no longer needed.
# Inside a platform container the address is hiveq-distributor-staging:8765.
WS_HOST = os.environ.get('HIVEQ_DISTRIBUTOR_HOST', '172.16.60.192')
WS_PORT = int(os.environ.get('HIVEQ_DISTRIBUTOR_PORT', '28765'))

# The signal name. In quant_features_v3 the `symbol` column carries the NAME of
# the signal, not a tradeable instrument -- the instrument lives in `ticker` /
# `norm_ticker` / `root`. Getting these two backwards is the most common mistake.
SIGNAL_NAME = 'hiveq_signal_v1_test'
INSTRUMENT = 'SPY'

CONFIG = {
    'QuantFeaturesPub': {
        'primary': 'HiveQQuantFeaturesPub',
    },
    'HiveQQuantFeaturesPub': {
        'transport': 'HiveQ',
        'topic': 'prod.quants.quant_features_v3',   # `topic` -> WebSocket route, to live subscribers
        'keyField': 'symbol',                       # per-row key = the signal name, not `id`
        'wsHost': WS_HOST,                          # see the note above; not reachable off-network
        'wsPort': WS_PORT,
    },
}


def build_row(now=None):
    """One quant_features_v3 row.

    quant_features_v3 has 38 columns, 17 of them NOT NULL. Every non-nullable
    column is supplied below; the nullable ones are set explicitly anyway so the
    published frame is self-documenting.

    Timestamps are naive US/Eastern. HiveQ data is already ET -- never convert
    from UTC here.
    """
    now = now or datetime.datetime.now()
    ts = now.strftime('%Y-%m-%d %H:%M:%S.000')

    return {
        # --- identity (all NOT NULL) ---------------------------------------
        'symbol':      SIGNAL_NAME,        # the signal's name
        'ticker':      INSTRUMENT,         # the instrument it refers to
        'norm_ticker': INSTRUMENT,
        'root':        INSTRUMENT,
        'id':          SIGNAL_NAME,        # record identifier
        'table':       'quant_features_v3',

        # --- time (all NOT NULL) -------------------------------------------
        'date':        now.strftime('%Y-%m-%d'),
        'time':        ts,
        'ts_event':    ts,                 # when the signal happened
        'recv_event':  ts,                 # when you received it
        'db_event':    ts,                 # when it was written

        # --- the signal itself ---------------------------------------------
        'signal1':     1.0,                # NOT NULL
        'signal2':     0.0,
        'signal3':     0.0,
        'weight1':     1.0,                # NOT NULL
        'weight2':     0.0,
        'weight3':     0.0,
        'score':       0.42,
        'side':        1,                  # NOT NULL
        'size':        0.0,

        # --- pricing context ------------------------------------------------
        'price':       655.25,
        'stop_px':     0.0,
        'target_px':   0.0,
        'volume':      0.0,

        # --- bookkeeping (freq/flag/text NOT NULL) --------------------------
        'freq':        0,
        'flag':        '',
        'text':        '%s smoke row' % SIGNAL_NAME,
        'version':     1.0,

        # --- reserved1..reserved10, all nullable ----------------------------
        **{'reserved%d' % i: None for i in range(1, 11)},
    }


def main():
    dd.init(config=CONFIG)

    df = pd.DataFrame([build_row()])
    print('Publishing %d row(s) of %s:' % (len(df), SIGNAL_NAME))
    print(df[['date', 'time', 'symbol', 'ticker', 'signal1', 'score']].to_string(index=False))

    # Returns the frame on success and None if any row failed -- never raises.
    saved = dd.save('QuantFeaturesPub', df)
    if saved is None:
        raise RuntimeError('publish failed -- check the driver log for [HiveQ]')

    print('Published %d row(s) to %s (distributor acked).'
          % (len(saved), CONFIG['HiveQQuantFeaturesPub']['topic']))


if __name__ == '__main__':
    main()
