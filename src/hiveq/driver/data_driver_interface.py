"""Stub for hiveq.driver.data_driver_interface — ships the Cache enum as real code."""
from enum import Enum


class Cache(Enum):
    NO_CACHE = 1
    ONLY_CACHE = 2
    CACHE_FORCE_PULL = 3
    PULL_UPDATE_CACHE = 4
    IN_MEMORY = 5
