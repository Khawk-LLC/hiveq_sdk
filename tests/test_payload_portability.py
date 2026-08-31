import io
import pickle

import cloudpickle

from hiveq.flow import ScheduleFrequency
from hiveq.flow._payload import _TaskWrapper


class _ExecutorWithoutThinClient(pickle.Unpickler):
    """Model an executor which deliberately has no SDK ``_client`` module."""

    def find_class(self, module, name):
        if module == "hiveq.flow._client":
            raise ModuleNotFoundError("No module named 'hiveq.flow._client'")
        return super().find_class(module, name)


def test_callable_capturing_public_client_value_deserializes_without_client_module():
    # A lambda is serialized by value, just like a user-authored scheduled job.
    # Its referenced SDK enum previously left a by-reference _client import in
    # the payload and failed before the user's code could start on the executor.
    task = lambda: ScheduleFrequency.DAILY  # noqa: E731
    payload = cloudpickle.dumps(_TaskWrapper(task, entry_method=None))

    restored = _ExecutorWithoutThinClient(io.BytesIO(payload)).load()

    assert restored.target().value == "DAILY"
