"""``hiveq.driver.config_property_manager`` — PLATFORM-ONLY IMPORT STUB.

NOTICE: not the real implementation. The HiveQ data driver runs **only inside a
HiveQ platform container**; this stub exists so the import resolves on a local
machine. Using it here raises :class:`~hiveq._platform_only.PlatformOnlyError` —
deploy the code that calls it (``hiveq.flow.jobs.deploy_job``, or a
``hiveq.flow`` strategy) and read the output back from the job.
"""
from __future__ import annotations

from hiveq._platform_only import notice as _notice, unavailable as _unavailable

_notice()


def load_env_file(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.load_env_file()")

def load_hiveq_env(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.load_hiveq_env()")

def get_logger(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.get_logger()")

def read_config_file(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.read_config_file()")

def configure(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.configure()")

def use_dict(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.use_dict()")

def reset(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.reset()")

def snapshot(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.snapshot()")

def get_properties(*args, **kwargs):
    _unavailable("hiveq.driver.config_property_manager.get_properties()")


class DictProperties:
    """Platform-only: constructing this locally raises."""

    def __init__(self, *args, **kwargs):
        _unavailable("DictProperties()")

    def get_property_as_string(self, *args, **kwargs):
        _unavailable("DictProperties.get_property_as_string()")

    def get_property_as_boolean(self, *args, **kwargs):
        _unavailable("DictProperties.get_property_as_boolean()")

    def get_property_as_int(self, *args, **kwargs):
        _unavailable("DictProperties.get_property_as_int()")

    def get_property_as_float(self, *args, **kwargs):
        _unavailable("DictProperties.get_property_as_float()")

    def get_property_keys_as_list(self, *args, **kwargs):
        _unavailable("DictProperties.get_property_keys_as_list()")


__all__ = ['load_env_file', 'load_hiveq_env', 'get_logger', 'read_config_file', 'configure', 'use_dict', 'reset', 'snapshot', 'get_properties', 'DictProperties']
