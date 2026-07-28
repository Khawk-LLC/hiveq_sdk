"""l9_03: only the run's creator can promote it to livesim.

``POST /livesim/deploy`` enforces a strict-creator ACL server-side, and the
handler's own comment explains why the check cannot live in the UI: *"Frontend
hides the deploy icon for non-owners, but enforce server-side too since the UI
guard is bypassable."* An authorization check that is only ever exercised by
hand is an authorization check that regresses quietly, so it gets a test.

Three properties, all negative-path and none of them consuming fleet capacity:

* An unknown ``source_run_id`` must **not** be promotable. The failure must be a
  clean 403/404, never a 500 and never a success.
* ``payload_id`` must not be a way around the ownership check. The handler
  deliberately looks the task up even when ``payload_id`` is supplied, precisely
  so a caller who knows a payload id cannot sidestep the ACL — that bypass is
  what this asserts is closed.
* A missing ``source_run_id`` is a 400.

Negative-path tests are the right shape here: proving a *real* cross-user denial
would need two accounts, which the suite does not have. What it can prove is
that the guard fires rather than falling open — and falling open is the failure
mode that matters.
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core.guards import require_remote
from agent_qa.core.livesim_client import LivesimClient
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l9_03_promotion_acl"
SURFACE = "l9.promotion"

#: Well-formed but certainly not ours.
FOREIGN_RUN_ID = f"qa-nonexistent-{uuid.uuid4()}"
FOREIGN_PAYLOAD_ID = f"qa-nonexistent-{uuid.uuid4()}"

#: Anything in here means the guard fired. 403 = ownership denied,
#: 404 = source run not found. Both refuse; neither leaks.
REFUSED = (400, 403, 404)


def main():
    install_crash_handler(NAME, SURFACE)
    require_remote(NAME)

    c = Checks()
    client = LivesimClient(timeout=60.0)
    c.note(f"base_url={client.base_url}")

    # Sanity: are we authenticated at all? Without this, every 401 below would
    # look like a passing ACL check when it is really a broken environment.
    _, probe_status = client.list_deployments()
    if probe_status in (401, 403):
        c.add("authenticated", False,
              f"GET /livesim/deployments -> {probe_status}; see l0_01. An "
              "unauthenticated caller is refused for the wrong reason, so the "
              "ACL cannot be distinguished from the auth failure")
        c.finish(NAME, surface=SURFACE)
        return
    c.add("authenticated", True)

    # 1. Missing source_run_id -> 400.
    body, status = client.request("POST", "/livesim/deploy", body={})
    c.add("missing_source_run_id_is_400", status == 400,
          f"got {status}: {str(body)[:200]}")

    # 2. Unknown source_run_id must be refused, not promoted.
    body, status = client.promote(FOREIGN_RUN_ID, instance_name="qa_acl", dry_run=True)
    c.add("unknown_run_refused", status in REFUSED,
          f"got {status}: {str(body)[:250]}")
    c.add("unknown_run_not_promoted", status not in (200, 201, 202),
          f"an unknown source_run_id was accepted with {status}: {str(body)[:250]}")
    c.add("unknown_run_no_server_error", status != 500,
          f"the ACL path 500s instead of refusing: {str(body)[:250]}")

    # 3. payload_id must not bypass the ownership lookup.
    body, status = client.promote(FOREIGN_RUN_ID, payload_id=FOREIGN_PAYLOAD_ID,
                                  instance_name="qa_acl", dry_run=True)
    c.add("payload_id_does_not_bypass_acl", status in REFUSED,
          f"supplying payload_id returned {status}: {str(body)[:250]}")
    c.add("payload_id_bypass_no_server_error", status != 500,
          f"payload_id path 500s: {str(body)[:250]}")

    c.finish(NAME, surface=SURFACE, extra=f"foreign_run_id={FOREIGN_RUN_ID}")


if __name__ == "__main__":
    main()
