"""Read `hiveq_signal_v1_test` back out of quant_features_v3.

A publish that returns success is not proof the row is queryable. Run this after
publish_hiveq_signal_v1_test.py, in the same environment -- a HiveQ platform
container (marimo, Jupyter, VS Code) or your company VM. Data access is IP
restricted, so from a laptop this returns nothing.

    python3 verify_read_back.py
"""
import collections
import datetime

import hiveq.driver as dd
from hiveq.driver import Cache
from hiveq.datetime import DateRange

SIGNAL_NAME = 'hiveq_signal_v1_test'

# Validating stored history is the REST route: a read section needs dataset +
# schema. A row published to the topic takes 5-10s to appear here, so a read run
# immediately after publishing can legitimately miss it.
CONFIG = {
    'QuantFeaturesRead': {
        'primary': 'HiveQQuantFeaturesRead',
    },
    'HiveQQuantFeaturesRead': {
        'transport': 'HiveQ',
        'dataset': 'HIVEQ_QUANT_FEATURES_V3',
        'schema': 'quant_features_v3',
        'columns': ('date,time,symbol,ticker,norm_ticker,root,signal1,weight1,'
                    'score,price,side,text,id,version'),   # else you get 10 of 38 columns
        'limit': 1000,
        'timezone': 'UTC',      # 'UTC' = do not convert; the default shifts rows -4h
    },
}

# The driver matches params_tuple fields BY TYPE, not by name: the DateRange
# becomes the date window and the first list/str becomes the symbol list. The
# field names below are for your benefit only.
Params = collections.namedtuple('Params', ['date', 'symbol'])


def main():
    dd.init(config=CONFIG)

    today = datetime.date.today().strftime('%Y-%m-%d')
    params = Params(DateRange(today, today), [SIGNAL_NAME])

    # NO_CACHE so we read the platform, not a cache file confirming our own write.
    df = dd.load('QuantFeaturesRead', params_tuple=params, cache=Cache.NO_CACHE)

    if df is None or len(df) == 0:
        raise SystemExit('No rows for %s on %s -- the publish did not land.'
                         % (SIGNAL_NAME, today))

    print('%d row(s) for %s on %s' % (len(df), SIGNAL_NAME, today))
    print(df.to_string(index=False))


if __name__ == '__main__':
    main()
