#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from market_propagation.acquisition import Coordinator, load_settings, loop


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/evidence_acquisition_v1.yaml")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--loop", action="store_true")
    mode.add_argument("--report", action="store_true")
    parser.add_argument("--now", help="ISO-8601 instant, for bounded operator checks")
    args = parser.parse_args()
    now = dt.datetime.fromisoformat(args.now) if args.now else None
    coordinator = Coordinator(load_settings(args.config))
    if args.report:
        print(json.dumps(coordinator.status(now), indent=2, sort_keys=True))
        return 0
    if args.once:
        result = coordinator.locked_run(now)
        print(json.dumps(result, indent=2, sort_keys=True))
        blocked = result.get("status") == "blocked" or any(
            item.get("status") in {"blocked", "backoff", "retry_exhausted"}
            for item in result.get("results", [])
        )
        return 2 if blocked else 0
    loop(coordinator)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
