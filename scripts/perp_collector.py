"""Run the perpetual-futures cross-section collector.

A thin entry point: the sweep, its skip rule, its refusal codes and its store
contract all live in :mod:`market_propagation.perp.collector`, so the command
line and any programmatic caller exercise one implementation rather than two.

One sweep::

    uv run --no-sync python scripts/perp_collector.py --once

The priority universe continuously::

    uv run --no-sync python scripts/perp_collector.py --loop

What is held, and what each sweep could not collect::

    uv run --no-sync python scripts/perp_collector.py --report
"""

from __future__ import annotations

import argparse
import json
import sys

from market_propagation.perp import collector
from market_propagation.perp.store import DEFAULT_ROOT, SnapshotStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect PerpDexList cross-sections.")
    parser.add_argument("--config", default=collector.CONFIG_PATH)
    parser.add_argument("--root", default=DEFAULT_ROOT, help="snapshot store root")
    parser.add_argument("--once", action="store_true", help="one sweep, then exit")
    parser.add_argument("--loop", action="store_true", help="sweep repeatedly")
    parser.add_argument("--all", action="store_true", help="full universe instead of the ranking")
    parser.add_argument("--limit", type=int, default=None, help="cap the asset count")
    parser.add_argument("--no-funding", action="store_true", help="skip funding-history pages")
    parser.add_argument("--force", action="store_true", help="sweep even if the build is unchanged")
    parser.add_argument("--report", action="store_true", help="summarise what is held")
    args = parser.parse_args(argv)

    config = collector.load_config(args.config)
    store = SnapshotStore.open(args.root)

    if args.report:
        print(json.dumps(collector.report(store), indent=2, default=str))
        return 0

    def emit(record: dict[str, object]) -> None:
        print(json.dumps(record, indent=2, default=str), flush=True)

    with collector.build_transport(config) as transport:
        return collector.collect(
            transport,
            store,
            config,
            limit=args.limit,
            use_all=args.all,
            include_funding=not args.no_funding,
            force=args.force,
            # Neither flag means one sweep: an unattended loop has to be asked for.
            once=not args.loop,
            interval_minutes=collector.interval_minutes(config),
            on_record=emit,
        )


if __name__ == "__main__":
    sys.exit(main())
