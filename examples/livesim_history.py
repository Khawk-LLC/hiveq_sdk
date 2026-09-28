#!/usr/bin/env python3
"""Pull a prior day's LiveSim data by date, strategy and container.

Why this example exists
-----------------------
A ``Deployment`` handle (``hf.get_deployment``) reaches only its *current*
container session, and stops resolving once the deployment is terminated. So it
cannot answer "what did my strategy do last Tuesday", and it cannot help if you
never kept the ``deployment_id``.

``hf.livesim_history`` is addressed by the day instead. It covers every
container session of that day: a container restarted twice gives three
sessions, each with its own ``run_id``, and every row says which one it came
from.

    date only            all of your rows that day, every container, every restart
    + container          only that container's sessions
    + strategy           that strategy in every container
    + both               that strategy in that container

``date`` is an America/New_York calendar day. You see your own rows;
organization owners, managers and admins see the whole organization.

Usage
-----
    python examples/livesim_history.py 2026-09-25
    python examples/livesim_history.py 2026-09-25 --strategy MyStrategy
    python examples/livesim_history.py 2026-09-25 --container livesim-futures-1 --csv orders.csv

Prerequisites: ``pip install hiveq-sdk`` and a HiveQ API key (§2 of docs/llms.txt).
"""

import argparse
from collections import Counter

import hiveq.flow as hf


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("date", help="ET calendar day, YYYY-MM-DD")
    parser.add_argument("--strategy", help="strategy instance name")
    parser.add_argument("--container", help="bare container name, e.g. livesim-futures-1")
    parser.add_argument("--csv", help="also write the day's orders to this CSV file")
    args = parser.parse_args()

    day = hf.livesim_history(args.date, strategy=args.strategy, container=args.container)
    print(day)

    # One row per container session: a restart shows up as a second run_id.
    sessions = day.runs()
    print(f"\n{len(sessions)} container session(s):")
    for session in sessions:
        print(
            f"  {session['container']:<32} run {session['run_id'][:8]}  "
            f"{session['first_event_ts'][:19]} -> {session['last_event_ts'][:19]} UTC  "
            f"{', '.join(session['strategies'])}"
        )

    # Every resource spans all of those sessions. Each call is a complete pull.
    orders = day.orders()
    print(f"\n{len(orders)} order(s)")
    by_status = Counter(order.get("status") for order in orders)
    for status, count in by_status.most_common():
        print(f"  {status:<14} {count}")

    trades = day.trades()
    pnl = sum(float(trade.get("realized_pnl") or 0) for trade in trades)
    print(f"\n{len(trades)} trade(s), realized P&L {pnl:,.2f}")
    print(f"{len(day.positions())} position(s)")
    print(f"{len(day.events())} event log row(s)")

    if args.csv:
        with open(args.csv, "w") as handle:
            handle.write(day.orders(format="csv"))
        print(f"\norders written to {args.csv}")


if __name__ == "__main__":
    main()
