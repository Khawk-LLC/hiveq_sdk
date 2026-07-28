"""l0_01: credentials resolve and the platform answers.

Runs first and cheaply so that a misconfigured environment produces one clear
failure here instead of forty opaque ones downstream. In particular it catches
the most common footgun: an API key issued by one environment used against
another. A local-stack key returns HTTP 401 from staging and vice versa, and
that 401 otherwise surfaces deep inside an unrelated data test.

Never triggers the interactive browser sign-in — a QA process that blocks five
minutes waiting for a human is a hung job, not a test.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core.guards import require_remote
from agent_qa.core.livesim_client import LivesimClient
from agent_qa.core.profiles import api_key, detect_engine, profile_name, resolved_host
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l0_01_auth_endpoints"
SURFACE = "l0.auth"


def main():
    install_crash_handler(NAME, SURFACE)
    # The fat engine package has no platform REST client, so credentials and
    # endpoint reachability can only be checked from the thin SDK side.
    require_remote(NAME)

    c = Checks()
    profile = profile_name()
    engine = detect_engine()
    c.note(f"profile={profile}, engine={engine}")

    key = api_key()
    c.add("api_key_present", bool(key),
          "no HIVEQ_API_KEY in env or ~/.hiveq/.env — run any SDK command once "
          "to sign in, then re-run")

    auth = os.environ.get("HIVEQ_AUTH_URL", "")
    # Resolve the deploy target exactly the way _Client does, so the scorecard
    # always states where backtests actually went. Under the default `env`
    # profile this is whatever ~/.hiveq/.env configures.
    target = resolved_host()
    c.note(f"deploy target={target}")
    c.add("deploy_target_resolved", bool(target),
          "neither HIVEQ_BASE_URL nor HIVEQ_AUTH_URL is set")

    if not key:
        c.finish(NAME, surface=SURFACE, extra=f"auth_url={auth}")
        return

    client = LivesimClient(timeout=20.0)

    # /health is deliberately unauthenticated, so it proves reachability only.
    body, status = client.health()
    c.add("orchestrator_reachable", status == 200, f"status={status}, body={body!r}")
    healthy = isinstance(body, dict) and str(body.get("status", "")).lower() == "healthy"
    c.add("orchestrator_reports_healthy", healthy, f"body={body!r}")

    # An authenticated route is what actually validates the key. Without this
    # check a stale key sails past preflight and then silently starves every
    # data test of rows — the strategy runs, the callbacks fire, and zero bars
    # arrive, which reads as a product bug rather than an auth problem.
    authed_body, authed_status = client.list_deployments()
    c.add("api_key_accepted", authed_status not in (401, 403),
          f"GET /livesim/deployments -> {authed_status}: {authed_body!r}")

    if authed_status in (401, 403):
        c.note(
            f"the stored HIVEQ_API_KEY is rejected by profile={profile}. Every "
            "data-dependent test will report zero rows until this is fixed. "
            "Re-run the SDK sign-in against this environment "
            f"({target}) to mint a fresh key."
        )

    c.finish(NAME, surface=SURFACE)


if __name__ == "__main__":
    main()
