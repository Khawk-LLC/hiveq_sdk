"""``hiveq.driver.date_time_utils`` — PLATFORM-ONLY IMPORT STUB.

NOTICE: not the real implementation. The HiveQ data driver runs **only inside a
HiveQ platform container**; this stub exists so the import resolves on a local
machine. Using it here raises :class:`~hiveq._platform_only.PlatformOnlyError` —
deploy the code that calls it (``hiveq.flow.jobs.deploy_job``, or a
``hiveq.flow`` strategy) and read the output back from the job.
"""
from __future__ import annotations

from hiveq._platform_only import notice as _notice, unavailable as _unavailable

_notice()


def current_date_string(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.current_date_string()")

def get_prev_date(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_prev_date()")

def get_datetime_from_datetime_string(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_datetime_from_datetime_string()")

def get_time_delta_seconds(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_time_delta_seconds()")

def now_time(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.now_time()")

def get_datetime_from_time_str(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_datetime_from_time_str()")

def convert_time_zone(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.convert_time_zone()")

def get_datetime_from_milliseconds(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_datetime_from_milliseconds()")

def get_datetime_str_from_millisecods(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_datetime_str_from_millisecods()")

def get_date_from_date_str(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_date_from_date_str()")

def get_nyse_holidays(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_nyse_holidays()")

def get_pre_post_date(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_pre_post_date()")

def get_date_list(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_date_list()")

def get_date_list_from_str(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_date_list_from_str()")

def get_time_as_string(*args, **kwargs):
    _unavailable("hiveq.driver.date_time_utils.get_time_as_string()")


__all__ = ['current_date_string', 'get_prev_date', 'get_datetime_from_datetime_string', 'get_time_delta_seconds', 'now_time', 'get_datetime_from_time_str', 'convert_time_zone', 'get_datetime_from_milliseconds', 'get_datetime_str_from_millisecods', 'get_date_from_date_str', 'get_nyse_holidays', 'get_pre_post_date', 'get_date_list', 'get_date_list_from_str', 'get_time_as_string']
