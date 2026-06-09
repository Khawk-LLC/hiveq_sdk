#!/usr/bin/env python3
"""Use a registered function on the platform via a QUANT_SCRIPTS task.

``functions_push.py`` registered ``zscore``. Here we submit a small job that
runs ON THE PLATFORM: it loads ``zscore`` from the registry and applies it.

``hf.run_function(job, ...)`` cloudpickles ``job``, runs it on the platform as a
QUANT_SCRIPTS task, and returns the job's return value. The job loads the
registered function with the platform's function-registry client (available in
the execution sandbox), so the same ``zscore`` you pushed is what runs.

Run:  python functions_run.py   (after functions_push.py)
"""
import hiveq.flow as hf


def latest_zscore(prices):
    """Runs on the platform: load `zscore` from the registry and apply it."""
    from hiveq_function_registry_client import get_client

    client = get_client()  # configured from the sandbox env (your identity)
    namespace = client.list_namespaces().get("own_namespace")
    zscore = client.load("zscore", namespace=namespace)  # latest version
    return {"n": len(prices), "zscore": zscore(prices, window=20)}


if __name__ == "__main__":
    prices = [
        100, 101, 102, 99, 105, 110, 108, 112, 115, 120,
        118, 121, 125, 130, 128, 132, 135, 140, 138, 145,
    ]
    # numpy is needed in the sandbox because zscore uses it.
    result = hf.run_function(latest_zscore, prices, requirements=["numpy"])
    print("Result from the platform:", result)
