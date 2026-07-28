"""l1_04: every entitled (dataset, schema) pair is reachable and accounted for.

The exhaustiveness test. Rather than running one backtest per dataset — which
would cost hours and still go stale the moment a dataset is added — this sweeps
the *live* catalog and answers two questions cheaply:

1. **Reachability**: does each entitled pair expose a field list? A pair that is
   advertised but cannot be described is broken before any strategy touches it.
2. **Coverage**: which pairs does the behavioural corpus actually assert on?

The second output is the point. It writes the uncovered pairs into the run
report, and they become the QA agent's work-list — so "all supported data is
covered" stops being a claim someone has to re-verify by reading test files and
becomes a number this test prints. New datasets appear here automatically the
first time they are entitled.

Never fails on an uncovered pair — that is a backlog item, not a defect. It
fails only when the catalog itself is unreachable or self-inconsistent.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from agent_qa.core import catalog, ledger
from agent_qa.core.guards import require_remote
from agent_qa.core.result import Checks, install_crash_handler

NAME = "l1_04_catalog_coverage"
SURFACE = "l1.data"

#: Pairs the behavioural suite asserts delivery for today. Each entry names the
#: test that covers it, so a stale claim is traceable to a file rather than
#: being folklore. The QA agent appends here when it lands a new delivery test.
BEHAVIOURAL_COVERAGE = {
    ("HIVEQ_US_EQ", "bars_1m"): "l1_01_equity_streams",
    ("HIVEQ_US_EQ", "eq_trades"): "l1_01_equity_streams",
    ("HIVEQ_US_EQ", "tbbo"): "l1_01_equity_streams",
    ("HIVEQ_US_FUT", "bars_1m"): "l1_02_futures_symbology",
}


def main():
    install_crash_handler(NAME, SURFACE)
    # The metadata client lives in the `hiveq` namespace, which only the thin
    # SDK ships; the fat engine package has no catalog access at all.
    require_remote(NAME)

    c = Checks()

    try:
        entries = catalog.load()
    except catalog.CatalogUnavailable as exc:
        c.add("catalog_reachable", False, str(exc))
        c.finish(NAME, surface=SURFACE)
        return

    c.add("catalog_reachable", bool(entries), f"n_datasets={len(entries)}")
    if not entries:
        c.finish(NAME, surface=SURFACE)
        return

    pairs = catalog.pairs(entries)
    c.add("catalog_has_schemas", bool(pairs), f"n_pairs={len(pairs)}")

    # A dataset advertised with an empty schema list is a catalog defect: it
    # cannot be subscribed to, so nothing downstream can ever use it.
    schemaless = [e["dataset"] for e in entries if not e.get("schemas")]
    c.add("no_schemaless_datasets", not schemaless, f"datasets={schemaless}")

    covered = [p for p in pairs if p in BEHAVIOURAL_COVERAGE]
    uncovered = [p for p in pairs if p not in BEHAVIOURAL_COVERAGE]

    # A coverage claim for a pair the account is no longer entitled to means the
    # table above has drifted from reality — worth knowing, and cheap to detect.
    stale = [p for p in BEHAVIOURAL_COVERAGE if p not in set(pairs)]
    c.add("no_stale_coverage_claims", not stale,
          f"claimed but not in catalog: {[f'{d}/{s}' for d, s in stale]}")

    pct = (100.0 * len(covered) / len(pairs)) if pairs else 0.0
    c.note(f"behavioural coverage {len(covered)}/{len(pairs)} pairs ({pct:.0f}%)")

    # Record the work-list where the agent reads it.
    for dataset, schema in uncovered:
        ledger.record_coverage(
            "l1.uncovered",
            f"{dataset}/{schema}",
            dataset=dataset,
            schema=schema,
            status="no_behavioural_test",
        )

    c.finish(
        NAME,
        surface=SURFACE,
        extra=(
            f"datasets={len(entries)}, pairs={len(pairs)}, covered={len(covered)}, "
            f"uncovered={len(uncovered)}; "
            f"next={[f'{d}/{s}' for d, s in uncovered[:8]]}"
        ),
    )


if __name__ == "__main__":
    main()
