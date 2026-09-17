"""The Kalshi contract universe, defined as the union of two observation paths.

The universe used to be whatever the archived markets glob happened to hold, which
made a contract's presence in a third-party transcription of the venue's listing into
a study admission rule. It is now the union of that archive with this repository's own
prospective capture of the venue's listing, and the archive is one observation
mechanism among two rather than the definition of existence.

This module is the single place that defines those layers. It exists so the graph
builder, the study panel and the cross-venue matcher cannot drift into three subtly
different universes, which is what four separate glob constants had already started
to do.

Nothing here decides eligibility. A contract is admitted by the study's own
eligibility rule, and this module only reports which observation paths saw it, so the
two admission criteria cannot be confused with one another.
"""

from __future__ import annotations

import glob
from collections import Counter
from collections.abc import Sequence
from typing import Any

import duckdb

from ..neighbors import (
    ORIGIN_ARCHIVED_AND_LIVE,
    ORIGIN_ARCHIVED_ONLY,
    ORIGIN_LIVE_ONLY,
    observation_origin,
)

#: The layer this repository captures prospectively from the venue's own listing.
LIVE_LAYER = "kalshi_own_markets"
LIVE_MARKETS_GLOB = "data/external/kalshi-own/markets/markets-*.parquet"

#: The third-party archive layer.
ARCHIVE_LAYER = "kalshi_markets"
ARCHIVE_MARKETS_GLOB = "data/external/kalshi-trades/markets-*.parquet"

#: The layers the universe is drawn from. The universe is their union and never their
#: intersection, and the order resolves a field disagreement: this repository's own
#: capture is read first because ``configs/external_history_v1.yaml`` already names
#: ``kalshi_own_markets`` as the two-source authority for a window it covers, since it
#: is the listing as the venue served it, read forward, rather than a later vendor
#: transcription of the same listing.
MARKET_LAYERS: tuple[tuple[str, str], ...] = (
    (LIVE_LAYER, LIVE_MARKETS_GLOB),
    (ARCHIVE_LAYER, ARCHIVE_MARKETS_GLOB),
)

#: The columns the graph's predicate and the study panel are read from, projected
#: identically from every layer so the two paths cannot disagree about a schema.
DEFAULT_COLUMNS: tuple[str, ...] = (
    "ticker",
    "event_ticker",
    "yes_sub_title",
    "no_sub_title",
    "title",
    "status",
    "open_time",
    "close_time",
)

#: The columns the cross-venue matcher reads a predicate from.
PREDICATE_COLUMNS: tuple[str, ...] = ("ticker", "event_ticker", "yes_sub_title", "title")

#: Timestamp columns are projected as text rather than as timestamps. The layers store
#: them as UTC-typed parquet, and reading one as a timestamp makes DuckDB import pytz,
#: which is not a dependency of this project. The callers parse these with their own
#: instant reader anyway, so the text form is the one they already consume and the one
#: that keeps the reader's timezone handling in one place.
TIMESTAMP_COLUMNS: tuple[str, ...] = ("open_time", "close_time", "created_time")


class UniverseError(ValueError):
    """The declared markets layers do not describe a universe that can be read."""


def layer_shards(pattern: str) -> list[str]:
    """The shard paths one layer holds, sorted, or none when it holds nothing."""
    return sorted(glob.glob(pattern))


def layer_presence(layers: Sequence[tuple[str, str]] = MARKET_LAYERS) -> dict[str, int]:
    """How many shards each declared layer holds, present or not.

    A layer with no shard is reported as zero rather than omitted, so a checkout with
    no capture yet reads as an empty live path instead of an undeclared one.
    """
    return {name: len(layer_shards(pattern)) for name, pattern in layers}


def _layer_rows(
    pattern: str,
    columns: Sequence[str],
    policy_series: Sequence[str],
) -> list[dict[str, Any]]:
    """Declared-series rows from one markets layer, projected to ``columns``.

    A layer holding no shard yields no rows: a checkout with no capture yet still has
    an archive to read, and which layers were present is reported beside the universe
    rather than inferred from an empty one.
    """
    shards = layer_shards(pattern)
    if not shards:
        return []
    projection = ", ".join(
        f"{column}::VARCHAR AS {column}" if column in TIMESTAMP_COLUMNS else column
        for column in columns
    )
    placeholders = ", ".join("?" for _ in policy_series)
    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone='UTC'")
        rows = connection.execute(
            f"""
            SELECT {projection}
            FROM read_parquet({shards!r})
            WHERE regexp_extract(ticker, '^[A-Z]+') IN ({placeholders})
            """,
            list(policy_series),
        ).fetchall()
    finally:
        connection.close()
    return [dict(zip(columns, row, strict=True)) for row in rows]


def union_market_rows(
    policy_series: Sequence[str],
    *,
    columns: Sequence[str] = DEFAULT_COLUMNS,
    layers: Sequence[tuple[str, str]] = MARKET_LAYERS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Every declared-series contract, observed through either admissible path.

    A contract both layers hold is one contract with two observations rather than two
    contracts, and its fields come from the first layer that states a non-null value,
    so a disagreement is resolved by the declared order instead of by whichever shard
    happened to be read last.

    Each row carries ``seen_archive``, ``seen_live`` and ``observation_origin``, so a
    consumer can condition on how a contract was observed without the population being
    redefined after the fact.
    """
    rows_by_layer = {name: _layer_rows(pattern, columns, policy_series) for name, pattern in layers}
    held = {name: {str(row["ticker"]) for row in rows} for name, rows in rows_by_layer.items()}

    merged: dict[str, dict[str, Any]] = {}
    for name, _ in layers:
        for row in rows_by_layer[name]:
            ticker = str(row["ticker"])
            current = merged.get(ticker)
            if current is None:
                merged[ticker] = dict(row)
                continue
            for field, value in row.items():
                if value is not None and current.get(field) is None:
                    current[field] = value

    markets: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for ticker in sorted(merged):
        row = merged[ticker]
        seen_archive = ticker in held[ARCHIVE_LAYER]
        seen_live = ticker in held[LIVE_LAYER]
        row["seen_archive"] = seen_archive
        row["seen_live"] = seen_live
        row["observation_origin"] = observation_origin(
            seen_archive=seen_archive, seen_live=seen_live
        )
        counts[row["observation_origin"]] += 1
        markets.append(row)

    diagnostics: dict[str, Any] = {
        "layers": [name for name, _ in layers],
        "shards_by_layer": {name: len(layer_shards(pattern)) for name, pattern in layers},
        "layers_present": sorted(name for name, rows in rows_by_layer.items() if rows),
        "layers_empty": sorted(name for name, rows in rows_by_layer.items() if not rows),
        "archived_contracts": len(held[ARCHIVE_LAYER]),
        "live_contracts": len(held[LIVE_LAYER]),
        "archived_and_live_contracts": len(held[ARCHIVE_LAYER] & held[LIVE_LAYER]),
        "union_contracts": len(merged),
        "provenance_counts": {
            ORIGIN_ARCHIVED_ONLY: counts[ORIGIN_ARCHIVED_ONLY],
            ORIGIN_LIVE_ONLY: counts[ORIGIN_LIVE_ONLY],
            ORIGIN_ARCHIVED_AND_LIVE: counts[ORIGIN_ARCHIVED_AND_LIVE],
        },
        "population_note": (
            "the union of both observation paths: archive presence is an observation "
            "mechanism and never a membership rule"
        ),
    }
    assert_union_identity(diagnostics)
    return markets, diagnostics


def declared_layer_origin(pattern: str) -> str | None:
    """The observation origin one declared layer glob names, or ``None`` for any other.

    A caller-supplied path is not one of the declared observation paths, so it claims
    no provenance rather than borrowing one: a fixture directory is not an archive
    observation, and labelling it as one would write a fabricated path into a
    population count.
    """
    for name, layer_pattern in MARKET_LAYERS:
        if pattern == layer_pattern:
            return observation_origin(
                seen_archive=name == ARCHIVE_LAYER, seen_live=name == LIVE_LAYER
            )
    return None


def assert_union_identity(diagnostics: dict[str, Any]) -> None:
    """Fail when the provenance counts and the union size disagree.

    Asserted rather than reported, because counts that do not sum to the universe mean
    a contract's observation paths and its membership disagree, and every downstream
    count would inherit the disagreement silently.
    """
    total = sum(diagnostics["provenance_counts"].values())
    if total != diagnostics["union_contracts"]:
        raise UniverseError(
            "provenance counts do not sum to the union size, so a contract's observation "
            f"paths and its membership disagree: {diagnostics['provenance_counts']} against "
            f"{diagnostics['union_contracts']}"
        )
    overlap = diagnostics["archived_and_live_contracts"]
    if overlap > min(diagnostics["archived_contracts"], diagnostics["live_contracts"]):
        raise UniverseError(
            "the archived-and-live count exceeds one of the layers' own counts, so the "
            "overlap is not a subset of both"
        )
    if overlap > diagnostics["union_contracts"]:
        raise UniverseError("the archived-and-live count exceeds the union itself")
