"""Funding differentials and cross-venue basis from the held perp cross-sections.

This is the Study D report. The collector stores one file per ``(asset, build)``; the
source rebuilds its whole site as one unit and stamps every page, so every file sharing
a build stamp is a genuine cross-section and files from different builds are not
simultaneous. The module this calls is explicit that differentials computed within one
build are simultaneous and across builds are not, so the report measures the two things
that distinction actually permits:

* **Gross spreads, within one build.** For each build, every ordered venue pair for an
  asset, with the annualised spread, the per-hour normalisation, and the refusals that
  stopped a pair being formed.
* **Persistence, across builds.** For one asset over its held builds, how often a pair
  is admissible at all and how far its spread moves. This is not a time series of a
  simultaneous quantity and is not reported as one: two observations of the same asset
  in different builds are two cross-sections, and the report says so.

Two limits are carried into the output rather than left to the reader:

* Every differential carries ``execution_cost_not_observable_from_this_source``. The
  cost layer is not on these pages, so a spread here is a quoted spread and never an
  executable opportunity. Nothing in this report is a net return, a fee, a slippage
  estimate, a fill probability or a capacity figure.
* A refusal is a count, not a dropped row. A venue whose rate the source did not
  publish is counted under its own reason code, which is the opposite of absent.

Run::

    uv run --no-sync python scripts/perp_differentials_report.py \\
      --root data/perp --output .audit/e2e/perp
"""

from __future__ import annotations

import argparse
import collections
import json
import pathlib
import statistics
from decimal import Decimal
from typing import Any

import pyarrow.parquet as pq

from market_propagation.perp.differentials import (
    REFUSAL_EXECUTION_COST_UNAVAILABLE,
    cross_venue_basis,
    funding_differentials,
)
from market_propagation.perp.parse import MarketSnapshot, VenueQuote
from market_propagation.perp.store import DEFAULT_ROOT, SnapshotStore

EXIT_OK = 0
EXIT_BLOCKED = 2


def _refusals(text: Any) -> tuple[str, ...]:
    """The stored ``|``-joined refusal codes, back as a tuple."""
    if text is None or text == "":
        return ()
    return tuple(part for part in str(text).split("|") if part)


def _quotes(rows: list[dict[str, Any]]) -> tuple[VenueQuote, ...]:
    """Rebuild the parsed quotes so the module's own arithmetic is what runs."""
    return tuple(
        VenueQuote(
            asset_class=str(row["asset_class"]),
            asset=str(row["asset"]),
            venue=str(row["venue"]),
            symbol=str(row["symbol"]),
            price=row["price"],
            volume_24h_usd=row["volume_24h_usd"],
            open_interest_usd=row["open_interest_usd"],
            funding_rate=row["funding_rate"],
            funding_apr=row["funding_apr"],
            paid_24h=row["paid_24h"],
            paid_7d=row["paid_7d"],
            paid_30d=row["paid_30d"],
            refusals=_refusals(row["refusals"]),
        )
        for row in rows
    )


def load_build(
    store: SnapshotStore, asset_class: str, asset: str, build: str
) -> tuple[MarketSnapshot, dict[str, int]]:
    """One stored market build as a parsed snapshot, plus its settlement counts.

    ``settlement_counts`` keys on venue alone because a venue settles all its contracts
    on one schedule, while a market page reports one row per ``(venue, symbol)``.
    """
    market_path = store.root / "market" / asset_class / asset / f"{build}.parquet"
    rows = pq.read_table(market_path).to_pylist()
    if not rows:
        raise ValueError(
            f"{market_path} holds no quote row; a build with no row is not a cross-section"
        )
    settlements: dict[str, int] = {}
    funding_path = store.root / "funding" / asset_class / asset / f"{build}.parquet"
    if funding_path.is_file():
        for row in pq.read_table(funding_path).to_pylist():
            if row.get("settlements") is not None:
                settlements[str(row["venue"])] = int(row["settlements"])
    snapshot = MarketSnapshot(
        asset_class=asset_class,
        asset=asset,
        build_time=rows[0]["build_time"],
        quotes=_quotes(rows),
        received_at=rows[0]["received_at"],
    )
    return snapshot, settlements


def _decimal_or_none(value: Any) -> Decimal | None:
    return value if isinstance(value, Decimal) else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=DEFAULT_ROOT, help="perp snapshot store root")
    parser.add_argument("--output", required=True, help="directory to write the report into")
    args = parser.parse_args()

    store = SnapshotStore.open(args.root)
    assets = sorted(
        (path.parent.parent.name, path.parent.name)
        for path in store.root.glob("market/*/*/*.parquet")
    )
    if not assets:
        print(
            json.dumps(
                {"error": f"no market build held under {store.root}; nothing to measure"},
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    builds_held: list[str] = []
    refusal_counts: collections.Counter[str] = collections.Counter()
    spreads_by_asset: dict[tuple[str, str], list[Decimal]] = collections.defaultdict(list)
    pair_direction_by_asset: dict[tuple[str, str], list[int]] = collections.defaultdict(list)
    basis_by_asset: dict[tuple[str, str], list[float]] = collections.defaultdict(list)
    differentials_total = 0
    basis_total = 0
    per_build: list[dict[str, Any]] = []

    for asset_class, asset in sorted(set(assets)):
        key = (asset_class, asset)
        builds = store.market_builds(asset_class, asset)
        builds_held.extend(f"{asset_class}/{asset}/{build}" for build in builds)
        for build in builds:
            snapshot, settlements = load_build(store, asset_class, asset, build)
            differentials = funding_differentials(snapshot, settlement_counts=settlements)
            basis = cross_venue_basis(snapshot)
            differentials_total += len(differentials)
            basis_total += len(basis)
            build_spreads: list[Decimal] = []
            for differential in differentials:
                for code in differential.refusals:
                    refusal_counts[code] += 1
                spread = _decimal_or_none(differential.apr_spread)
                if spread is None:
                    continue
                build_spreads.append(spread)
                spreads_by_asset[key].append(spread)
                pair_direction_by_asset[key].append(1 if spread >= 0 else -1)
            for item in basis:
                basis_by_asset[key].append(item.log_basis)
            per_build.append(
                {
                    "asset_class": asset_class,
                    "asset": asset,
                    "build": build,
                    "venues_in_snapshot": len({quote.venue for quote in snapshot.quotes}),
                    "differentials": len(differentials),
                    "admissible_spreads": len(build_spreads),
                    "basis_pairs": len(basis),
                }
            )

    assets_with_admissible_spread = sum(1 for key, values in spreads_by_asset.items() if values)
    persistence = [
        {
            "asset_class": asset_class,
            "asset": asset,
            "builds_held": len(store.market_builds(asset_class, asset)),
            "admissible_spread_observations": len(spreads_by_asset.get((asset_class, asset), [])),
            "spread_min_apr": str(min(spreads_by_asset[(asset_class, asset)])),
            "spread_max_apr": str(max(spreads_by_asset[(asset_class, asset)])),
            "spread_median_apr": str(statistics.median(spreads_by_asset[(asset_class, asset)])),
            "sign_stable_across_observations": len(
                set(pair_direction_by_asset.get((asset_class, asset), []))
            )
            <= 1,
            "basis_observations": len(basis_by_asset.get((asset_class, asset), [])),
        }
        for asset_class, asset in sorted(spreads_by_asset)
    ]

    document: dict[str, Any] = {
        "produced_by": "perp_differentials_report",
        "store_root": str(store.root),
        "distinct_source_build_stamps": len(
            {path.stem for path in store.root.glob("market/*/*/*.parquet")}
        ),
        "asset_build_pairs": len(builds_held),
        "assets_held": len(set(assets)),
        "assets_observed_at_more_than_one_build": sum(
            1
            for asset_class, asset in set(assets)
            if len(store.market_builds(asset_class, asset)) > 1
        ),
        "differentials_formed": differentials_total,
        "basis_pairs_formed": basis_total,
        "assets_with_at_least_one_admissible_spread": assets_with_admissible_spread,
        "refusals_by_code": dict(sorted(refusal_counts.items())),
        "claim_limits": {
            REFUSAL_EXECUTION_COST_UNAVAILABLE: (
                "the cost layer is not observable from this source, so every spread here is "
                "a quoted spread and not an executable opportunity"
            ),
            "net_return_or_capacity_claimed": False,
            "differentials_within_one_build_are_simultaneous": True,
            "differentials_across_builds_are_not_simultaneous": True,
        },
        "persistence_across_builds": persistence,
        "per_build": per_build,
    }

    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "perp_differentials.json"
    report_path.write_text(
        json.dumps(document, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )

    print(
        json.dumps(
            {
                "distinct_source_build_stamps": document["distinct_source_build_stamps"],
                "asset_build_pairs": document["asset_build_pairs"],
                "assets_held": document["assets_held"],
                "assets_at_more_than_one_build": document["assets_observed_at_more_than_one_build"],
                "differentials_formed": differentials_total,
                "basis_pairs_formed": basis_total,
                "assets_with_at_least_one_admissible_spread": assets_with_admissible_spread,
                "refusals_by_code": document["refusals_by_code"],
                "report_path": str(report_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    for row in persistence[:3]:
        print(
            f"persistence: {row['asset_class']}/{row['asset']} over {row['builds_held']} build(s): "
            f"{row['admissible_spread_observations']} admissible spread(s), "
            f"median {row['spread_median_apr']}, sign stable {row['sign_stable_across_observations']}"
        )
    print(
        "every spread is a quoted spread: the execution-cost layer is unobservable from this source, "
        "so nothing here is a net return, a cost estimate or a capacity figure"
    )
    return EXIT_OK if assets_with_admissible_spread else EXIT_BLOCKED


if __name__ == "__main__":
    raise SystemExit(main())
