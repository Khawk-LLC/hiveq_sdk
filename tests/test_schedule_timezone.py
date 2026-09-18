"""Schedule timezones: a job's start_time is Eastern unless the caller says otherwise.

start_time is a wall-clock time, so the zone attached to it decides when the job
actually fires. Everything else a HiveQ user sees is Eastern, so a schedule is
too -- and an abbreviation like "EST" has to keep meaning US Eastern (DST and
all) rather than the fixed -05:00 zone the tz database hands back for that name.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from hiveq.flow import Schedule, ScheduleFrequency
from hiveq.flow._client import _normalize_schedule, resolve_schedule_timezone


@pytest.fixture(autouse=True)
def _no_env_override(monkeypatch):
    monkeypatch.delenv("HIVEQ_SCHEDULE_TIMEZONE", raising=False)


def _daily(**kwargs) -> Schedule:
    return Schedule(frequency=ScheduleFrequency.DAILY, start_time="16:05", **kwargs)


def test_default_is_eastern():
    assert _daily().to_dict()["timezone"] == "America/New_York"


def test_abbreviations_alias_onto_the_dst_aware_zone():
    for name in ("EST", "est", "EDT", "ET", "US/Eastern"):
        assert _daily(timezone=name).timezone == "America/New_York"

    # The point of the aliasing: ZoneInfo('EST') is a fixed -05:00, so 16:05
    # "EST" would fire at 17:05 wall clock all summer.
    tz = ZoneInfo(resolve_schedule_timezone("EST"))
    assert datetime(2026, 1, 15, 16, 5, tzinfo=tz).utcoffset() != datetime(
        2026, 7, 15, 16, 5, tzinfo=tz
    ).utcoffset()


def test_explicit_zones_pass_through():
    assert _daily(timezone="UTC").timezone == "UTC"
    assert _daily(timezone="Europe/London").timezone == "Europe/London"


def test_local_resolves_to_the_client_machine_zone(monkeypatch):
    monkeypatch.setenv("TZ", "Europe/Berlin")
    assert _daily(timezone="local").timezone == "Europe/Berlin"


def test_env_var_sets_the_default_without_overriding_the_caller(monkeypatch):
    monkeypatch.setenv("HIVEQ_SCHEDULE_TIMEZONE", "Asia/Tokyo")
    assert _daily().timezone == "Asia/Tokyo"
    assert _daily(timezone="ET").timezone == "America/New_York"


def test_unknown_zone_fails_at_construction():
    with pytest.raises(ValueError, match="Mars/Olympus"):
        _daily(timezone="Mars/Olympus")


def test_dict_schedules_get_the_same_treatment():
    # A plain dict is an accepted stand-in for Schedule, so it must not reach
    # the platform with a missing or abbreviated timezone.
    assert _normalize_schedule(
        {"frequency": ScheduleFrequency.DAILY, "start_time": "16:05"}
    ) == {"frequency": "DAILY", "start_time": "16:05", "timezone": "America/New_York"}

    assert _normalize_schedule({"frequency": "DAILY", "start_time": "16:05",
                                "timezone": "PT"})["timezone"] == "America/Los_Angeles"
    assert _normalize_schedule(None) is None


# --- the job clock ----------------------------------------------------------

def test_job_clock_follows_the_schedule_timezone(monkeypatch):
    from hiveq.flow.jobs import job_now, job_timezone, job_today

    assert job_timezone() == "America/New_York"
    assert job_now().utcoffset() == datetime.now(ZoneInfo("America/New_York")).utcoffset()
    assert job_now("UTC").utcoffset().total_seconds() == 0
    assert job_today() == job_now().date()

    # Inside a deployed job this is set from the schedule (see submit()).
    monkeypatch.setenv("HIVEQ_SCHEDULE_TIMEZONE", "Asia/Tokyo")
    assert job_timezone() == "Asia/Tokyo"
    assert job_now().tzinfo is not None


def test_submit_hands_the_job_its_schedule_timezone(monkeypatch):
    # The script's own time checks have to agree with the schedule that woke
    # it, so the resolved zone travels in the payload.
    import io
    import os
    import pickle

    import cloudpickle

    from hiveq.flow._client import _JOB_TIMEZONE_ENV, _TaskWrapper
    from hiveq.flow.jobs import job_now

    def task():
        return job_now().tzinfo is not None

    wrapper = _TaskWrapper(task, entry_method=None,
                           env={_JOB_TIMEZONE_ENV: "Asia/Tokyo"})

    class _ExecutorWithoutSDK(pickle.Unpickler):
        """The executor deliberately has no thin-client SDK installed."""

        def find_class(self, module, name):
            if module.startswith("hiveq"):
                raise ModuleNotFoundError(f"No module named {module!r}")
            return super().find_class(module, name)

    monkeypatch.delenv(_JOB_TIMEZONE_ENV, raising=False)
    restored = _ExecutorWithoutSDK(io.BytesIO(cloudpickle.dumps(wrapper))).load()
    assert restored.run() is True
    assert os.environ[_JOB_TIMEZONE_ENV] == "Asia/Tokyo"


def test_wrapper_without_env_touches_nothing(monkeypatch):
    # Backtest payloads go through the same wrapper and must be unaffected.
    from hiveq.flow._client import _TaskWrapper

    monkeypatch.delenv("HIVEQ_SCHEDULE_TIMEZONE", raising=False)
    assert _TaskWrapper(lambda: "ok", entry_method=None).run() == "ok"
    assert "HIVEQ_SCHEDULE_TIMEZONE" not in __import__("os").environ
