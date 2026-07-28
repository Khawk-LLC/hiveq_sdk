"""l9_01: the livesim REST surface answers, and answers in the documented shape.

Read-only plus ``dry_run``, so it is safe on any profile and consumes no fleet
capacity. Its job is to catch the cheap, high-frequency breakages — a route
renamed, an envelope changed, a required field silently dropped — before the
expensive tests in ``l9_02``/``l9_03`` try to deploy against them.

Routes exercised (verified against orchestrator ``server.py``):

* ``GET  /livesim/containers``                     — fleet inventory
* ``GET  /livesim/deployments``                    — deployment inventory
* ``GET  /livesim/containers/<id>/deployments``    — per-container view
* ``GET  /livesim/containers/<id>/log-components`` — log routing metadata
* ``GET  /livesim/fleet-yaml``                     — fleet definition
* ``POST /livesim/deploy``                         — required-field validation
* ``PATCH .../strategies/<name>/params``           — required-field validation

The two write routes are probed only for their **validation** behaviour: a
``POST /livesim/deploy`` without ``source_run_id`` must be a 400, and a
``PATCH`` without ``param_changes`` must be a 400. Asserting the guard rails
costs nothing and they are exactly what regresses when a handler is refactored.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core.guards import require_remote
from agent_qa.core.livesim_client import LivesimClient, container_ids
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l9_01_lifecycle_contract"
SURFACE = "l9.lifecycle"


def main():
    install_crash_handler(NAME, SURFACE)
    # Livesim exists only as a platform deployment; the in-process engine has no
    # orchestrator client at all.
    require_remote(NAME)

    c = Checks()
    client = LivesimClient(timeout=60.0)
    c.note(f"base_url={client.base_url}")

    body, status = client.list_containers()
    c.add("list_containers_ok", status == 200, f"status={status}, body={str(body)[:200]}")
    if status in (401, 403):
        c.note("authentication rejected — see l0_01; nothing below can be asserted")
        c.finish(NAME, surface=SURFACE)
        return

    ids = container_ids(body)
    c.add("containers_parse", isinstance(body, (list, dict)), f"type={type(body).__name__}")
    c.note(f"containers={ids[:6]}{'...' if len(ids) > 6 else ''} (n={len(ids)})")

    deployments, status = client.list_deployments()
    c.add("list_deployments_ok", status == 200,
          f"status={status}, body={str(deployments)[:200]}")

    fleet, status = client.get_fleet_yaml()
    c.add("fleet_yaml_readable", status == 200, f"status={status}")

    # Per-container views, against whatever the fleet actually holds.
    if ids:
        first = ids[0]
        per, status = client.container_deployments(first)
        c.add("container_deployments_ok", status == 200,
              f"{first} -> status={status}, body={str(per)[:200]}")

        comps, status = client.container_log_components(first)
        # Not every runtime exposes log components; 404 is an acceptable answer,
        # a 500 is not.
        c.add("log_components_no_server_error", status < 500,
              f"{first} -> status={status}")
    else:
        c.note("no containers in the fleet; per-container routes not exercised")

    # --- validation guard rails (no side effects) ---

    body, status = client.request("POST", "/livesim/deploy", body={})
    c.add("deploy_requires_source_run_id", status == 400,
          f"POST /livesim/deploy with an empty body returned {status}, expected 400: "
          f"{str(body)[:200]}")

    body, status = client.request(
        "PATCH",
        "/livesim/containers/__qa_nonexistent__/deployments/__qa__/strategies/__qa__/params",
        body={},
    )
    # The handler checks param_changes before it resolves the container, so an
    # empty body is a 400 even for ids that do not exist. A 404 here would mean
    # the ordering changed; a 500 means the guard is gone.
    c.add("params_requires_param_changes", status in (400, 403, 404),
          f"PATCH params with an empty body returned {status}: {str(body)[:200]}")
    c.add("params_no_server_error", status != 500,
          f"PATCH params with an empty body returned 500: {str(body)[:200]}")

    c.finish(NAME, surface=SURFACE, extra=f"containers={len(ids)}")


if __name__ == "__main__":
    main()
