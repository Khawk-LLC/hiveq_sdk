#!/usr/bin/env python3
"""Push a reusable function to the HiveQ function registry.

Run this once to register ``zscore`` in your namespace; then
``functions_run.py`` loads and uses it on the platform.

Run:  python functions_push.py
"""
import hiveq.flow as hf


def zscore(values, window=20):
    """Z-score of the latest value over a trailing window."""
    import numpy as np

    a = np.asarray(values, dtype=float)[-window:]
    sd = a.std()
    return float((a[-1] - a.mean()) / sd) if sd else 0.0


if __name__ == "__main__":
    # The function is cloudpickled and stored in YOUR namespace. `requirements`
    # are the packages it needs wherever it runs. Versions are immutable — bump
    # the version to publish changes (re-pushing the same one is a no-op here).
    try:
        info = hf.push_function(zscore, version="1.0.0", requirements=["numpy"])
        print("Registered:", info)  # {'function_id', 'namespace', 'name', 'version'}
    except RuntimeError as e:
        if "already exists" not in str(e):
            raise
        print("zscore v1.0.0 is already registered.")

    # Inspect what's there.
    print("Versions :", hf.function_versions("zscore"))
    print("In namespace:", [f.get("name") for f in hf.list_functions()])
