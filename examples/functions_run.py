#!/usr/bin/env python3
"""Use a registered function on the platform via a QUANT_SCRIPTS task.

``functions_push.py`` registered ``zscore``. Here we fetch it back from the
registry (``hf.load_function``) and run it ON THE PLATFORM with
``hf.run_function``, which cloudpickles it, executes it as a QUANT_SCRIPTS task,
and returns its return value. ``requirements`` are installed in the sandbox.

Run:  python functions_run.py   (after functions_push.py)
"""
import hiveq.flow as hf


if __name__ == "__main__":
    prices = [
        100, 101, 102, 99, 105, 110, 108, 112, 115, 120,
        118, 121, 125, 130, 128, 132, 135, 140, 138, 145,
    ]

    # Fetch the function you registered earlier, then run it on the platform.
    zscore = hf.load_function("zscore")  # latest version from your namespace
    result = hf.run_function(zscore, prices, window=20, requirements=["numpy"])
    print("Result from the platform:", result)
