"""Assemble the cross-venue candidate universe and grade every pair in it.

This is the driver the matching layer deliberately does not contain. That layer is
pure: it grades the records it is handed and never reads a file. Something has to
decide *which* records are handed to it, and that decision fixes the denominator a
match count is measured against, so it is made here where it can be declared,
recorded and checked.

Four decisions shape this module, and each answers something measured rather than
assumed:

* **The candidate filter is a selection rule, never evidence.** The first venue's
  candidates are the contracts whose series the cohort declares. The second venue's
  are the records whose own identity column matches a declared pattern, and the
  pattern, the totals available and the cap actually applied are all recorded beside
  the counts. No grade and no refusal is ever expressed in terms of that pattern:
  the matching layer still computes no similarity score at all. A contract the
  filter excludes is outside the candidate universe, not refused for resembling
  anything, and the two must not be confused in a report.
* **A component the record does not publish is passed as absent.** The venue's own
  market record publishes no reference period, and the rule records that would
  publish settlement semantics carry no per-contract entry on this checkout. Both
  are therefore passed as ``None``, which the matching layer records as unobserved
  and treats as a refusal. Deriving a reference period from a contract's own ticker
  would invent the component this project has already measured to matter.
* **The second venue declares no parser, so its reads are refusals.** That is the
  measured state of the archive rather than a placeholder: the cleaned local layer
  carries a slug and market metadata and no settlement-rule text. Every pair is
  therefore refused, and the command exists to record that funnel precisely.
* **The cap is stated with what it hid.** A bounded candidate universe reports the
  total available beside the count supplied, so a match count can never be read as
  though the whole layer had been searched.
"""

from __future__ import annotations

import collections
import datetime as dt
import glob
import pathlib
import re
from dataclasses import dataclass
from typing import Any

import duckdb
import yaml

from .ingest.audit import series_of
from .ingest.kalshi_universe import (
    MARKET_LAYERS,
    PREDICATE_COLUMNS,
    declared_layer_origin,
    union_market_rows,
)
from .matching import (
    MatchingSettings,
    MatchRegistry,
    PredicateRead,
    build_registry,
    read_predicate,
)

#: The declared match rules, the declared candidate series and the declared calendar.
MATCH_CONFIG_PATH = "configs/matching_v1.yaml"
COHORT_CONFIG_PATH = "configs/cohort_v2.yaml"
GRAPH_CONFIG_PATH = "configs/neighbor_graph_v2.yaml"

#: The two local layers the candidate universe is drawn from.
KALSHI_MARKETS_GLOB = "data/external/kalshi-trades/markets-*.parquet"
POLYMARKET_GLOB = "data/external/polymarket-v1/daily_aligned_multi/*.parquet"

#: The second venue's own identity column, and the cap applied when nothing else is
#: declared. Both are overridable; the effective values are recorded in the result.
DEFAULT_SECOND_VENUE_IDENTITY_COLUMN = "market_slug"
DEFAULT_SECOND_VENUE_LIMIT = 250


class CrossVenueError(ValueError):
    """The declared inputs do not describe a candidate universe that can be graded."""


def load_config(path: str | pathlib.Path) -> dict[str, Any]:
    """Read one declared YAML configuration."""
    resolved = pathlib.Path(path)
    if not resolved.is_file():
        raise CrossVenueError(f"{resolved} does not exist")
    return yaml.safe_load(resolved.read_text(encoding="utf-8"))


def declared_policy_series(cohort_config: dict[str, Any]) -> tuple[str, ...]:
    """The policy series the cohort declares, in declaration order.

    Read from the configuration rather than matched by substring: the cohort's own
    note records that a ``LIKE 'FED-%'`` test drops the sibling ``FEDDECISION``
    series, so membership is decided by :func:`~market_propagation.ingest.audit.series_of`
    against this list.
    """
    series = cohort_config.get("policy_series")
    if not isinstance(series, list) or not series:
        raise CrossVenueError(
            f"{COHORT_CONFIG_PATH} declares no `policy_series`; the candidate universe for the "
            "first venue is undefined without it"
        )
    return tuple(str(name) for name in series)


def declared_calendar(graph_config: dict[str, Any]) -> dict[tuple[int, int], dt.date]:
    """The declared decision calendar as a month map, which is what a horizon needs."""
    block = graph_config.get("decision_calendar") or {}
    dates = [dt.date.fromisoformat(str(name)) for name in block.get("dates") or []]
    if not dates:
        raise CrossVenueError(
            f"{GRAPH_CONFIG_PATH} declares no `decision_calendar.dates`; a contract's decision "
            "instant cannot be placed on the exposure axis without one"
        )
    if len(set(dates)) != len(dates):
        raise CrossVenueError("the declared calendar names a meeting date twice")
    return {(date.year, date.month): date for date in dates}


def first_venue_records(
    markets_glob: str | None = None,
    series: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Every declared-series contract with the text its predicate is read from.

    Read from the declared union of observation paths by default, so a contract the
    venue listed but the archive omits is still a candidate and the archive is one
    observation mechanism rather than the definition of existence. An explicit glob
    narrows to one layer, which a caller does when a window has to be resolved to a
    single path; a glob that is not one of the declared layer globs is not an
    observation path, so those records claim no provenance instead of borrowing one.

    The series test is applied in Python through the declared parser rather than in
    SQL, so the membership rule has exactly one implementation and a ticker that
    merely contains the letters is not a match.
    """
    if markets_glob is None:
        rows, _population = union_market_rows(series, columns=PREDICATE_COLUMNS)
        if not rows:
            raise CrossVenueError(
                "no market records matched any declared layer: "
                + ", ".join(f"{name}={pattern}" for name, pattern in MARKET_LAYERS)
            )
    else:
        files = sorted(glob.glob(markets_glob))
        if not files:
            raise CrossVenueError(f"no market records matched {markets_glob}")
        connection = duckdb.connect()
        try:
            connection.execute("SET TimeZone='UTC'")
            raw = connection.execute(
                f"""
                SELECT ticker, event_ticker, yes_sub_title, title
                FROM read_parquet({files!r})
                """
            ).fetchall()
        finally:
            connection.close()
        origin = declared_layer_origin(markets_glob)
        rows = [
            {**dict(zip(PREDICATE_COLUMNS, row, strict=True)), "observation_origin": origin}
            for row in raw
        ]

    declared = set(series)
    records = [
        {
            "contract_id": str(row["ticker"]),
            "event_ticker": str(row["event_ticker"] or ""),
            "yes_sub_title": str(row["yes_sub_title"] or ""),
            "title": str(row["title"] or ""),
            "observation_origin": row["observation_origin"],
        }
        for row in rows
        if series_of(str(row["ticker"])) in declared
    ]
    return sorted(records, key=lambda record: record["contract_id"])


def second_venue_records(
    layer_glob: str,
    *,
    identity_column: str,
    slug_pattern: str | None,
    limit: int | None,
) -> tuple[list[str], dict[str, Any]]:
    """The second venue's candidate identities, and the selection that produced them.

    The selection block travels with the identities so a reader can see both what the
    candidate universe was and what it was not: the total available, the pattern that
    reduced it, and the cap applied. A pattern that matched nothing yields an empty
    candidate universe with the total recorded, which reads as an empty search rather
    than as an absence of contracts.
    """
    files = sorted(glob.glob(layer_glob))
    if not files:
        raise CrossVenueError(f"no records matched {layer_glob}")
    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone='UTC'")
        available = [
            str(row[0])
            for row in connection.execute(
                f"SELECT DISTINCT {identity_column} FROM read_parquet({files!r}) "
                f"WHERE {identity_column} IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
    finally:
        connection.close()

    selected = available
    if slug_pattern:
        try:
            compiled = re.compile(slug_pattern)
        except re.error as error:
            raise CrossVenueError(
                f"the declared candidate pattern is not a regex: {error}"
            ) from error
        selected = [name for name in available if compiled.search(name)]
    matched = len(selected)
    applied = limit if limit is not None else DEFAULT_SECOND_VENUE_LIMIT
    if applied is not None and applied >= 0:
        selected = selected[:applied]

    selection = {
        "layer_glob": layer_glob,
        "identity_column": identity_column,
        "records_available": len(available),
        "records_matching_the_declared_pattern": matched,
        "candidates_supplied": len(selected),
        "slug_pattern": slug_pattern,
        "limit_applied": applied,
        "cap_hid_records": bool(matched > len(selected)),
        "pattern_is_a_selection_rule_and_not_evidence": True,
    }
    return selected, selection


def first_venue_reads(
    records: list[dict[str, Any]],
    *,
    settings: Any,
    calendar: dict[tuple[int, int], dt.date],
) -> tuple[PredicateRead, ...]:
    """Read one predicate per first-venue contract, or the reason there is none.

    ``reference_period`` and ``settlement_criterion`` are passed as ``None`` on
    purpose. The venue's own market record publishes neither, and the rule records
    that would carry settlement semantics hold no per-contract entry on this
    checkout, so both land as unobserved components and every pair that needs them is
    refused. Supplying a reference period read off the contract's own ticker would be
    this driver inventing the component, which is the one thing the matching layer
    exists to prevent.
    """
    declaration = settings.declaration(settings.venue_names[0])
    reads: list[PredicateRead] = []
    for record in records:
        reads.append(
            read_predicate(
                declaration,
                contract_id=record["contract_id"],
                text_fields={
                    "yes_sub_title": record["yes_sub_title"],
                    "title": record["title"],
                },
                event_ticker=record["event_ticker"] or None,
                calendar=calendar,
                reference_period=None,
                settlement_criterion=None,
            )
        )
    return tuple(reads)


def second_venue_reads(identities: list[str], *, settings: Any) -> tuple[PredicateRead, ...]:
    """Read one predicate per second-venue record, which is a refusal for each.

    The second venue's declaration carries no parser, so each read returns the named
    refusal for that fact. It is called rather than short-circuited so the recorded
    reason is the matching layer's own, with one implementation and one code.
    """
    declaration = settings.declaration(settings.venue_names[1])
    return tuple(read_predicate(declaration, contract_id=identity) for identity in identities)


@dataclass(frozen=True, slots=True)
class CrossVenueResult:
    """The graded candidate universe and the selection that formed it."""

    registry: MatchRegistry
    selection: dict[str, Any]
    calendar_months: tuple[str, ...]
    reads_by_venue: dict[str, int]

    def refusal_counts(self) -> dict[str, int]:
        """Per-pair refusal reasons, counted whole.

        A pair can be refused for more than one reason, so these counts do not sum to
        the pair count and are not presented as though they did.
        """
        counts: collections.Counter[str] = collections.Counter()
        for pair in self.registry.pairs:
            for reason in pair.reasons:
                counts[str(reason)] += 1
        return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))

    def read_refusals(self) -> dict[str, int]:
        """Per-record refusal reasons, which decide the candidate coverage."""
        counts: collections.Counter[str] = collections.Counter()
        for read in self.registry.reads:
            if read.reason is not None:
                counts[str(read.reason)] += 1
        return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))

    def summary(self) -> dict[str, Any]:
        """Everything except the pair list, which a caller writes to a file instead."""
        whole = self.registry.as_dict()
        return {
            "version": whole["version"],
            "digest": whole["digest"],
            "settings_digest": whole["settings_digest"],
            "candidate_selection": self.selection,
            "calendar_months": list(self.calendar_months),
            "reads_by_venue": self.reads_by_venue,
            "coverage": whole["coverage"],
            "counts": whole["counts"],
            "read_refusals": self.read_refusals(),
            "pair_refusals": self.refusal_counts(),
            "rule_vintage": whole["rule_vintage"],
        }

    def as_dict(self) -> dict[str, Any]:
        """The whole registry, pairs and reads included."""
        whole = self.registry.as_dict()
        whole["candidate_selection"] = self.selection
        whole["calendar_months"] = list(self.calendar_months)
        whole["reads_by_venue"] = self.reads_by_venue
        whole["pair_refusals"] = self.refusal_counts()
        return whole


def run_cross_venue_matching(
    *,
    match_config_path: str | pathlib.Path = MATCH_CONFIG_PATH,
    cohort_config_path: str | pathlib.Path = COHORT_CONFIG_PATH,
    graph_config_path: str | pathlib.Path = GRAPH_CONFIG_PATH,
    markets_glob: str | None = None,
    second_venue_glob: str = POLYMARKET_GLOB,
    second_venue_limit: int | None = None,
    slug_pattern: str | None = None,
) -> CrossVenueResult:
    """Form the candidate universe, read every candidate and grade every pair."""
    raw = load_config(match_config_path)
    settings = MatchingSettings.from_config(raw)
    cohort = load_config(cohort_config_path)
    graph = load_config(graph_config_path)
    series = declared_policy_series(cohort)
    calendar = declared_calendar(graph)

    selection_block = raw.get("candidate_selection") or {}
    second_block = selection_block.get("second_venue") or {}
    identity_column = str(
        second_block.get("contract_identity_column", DEFAULT_SECOND_VENUE_IDENTITY_COLUMN)
    )
    pattern = slug_pattern if slug_pattern is not None else second_block.get("slug_pattern")
    # The declared cap is the default; a caller's explicit limit overrides it.
    if second_venue_limit is None and second_block.get("max_candidates") is not None:
        limit = int(second_block["max_candidates"])
    else:
        limit = second_venue_limit

    records = first_venue_records(markets_glob, series)
    identities, selection = second_venue_records(
        second_venue_glob,
        identity_column=identity_column,
        slug_pattern=str(pattern) if pattern else None,
        limit=limit,
    )
    selection["declared_policy_series"] = list(series)
    selection["contracts_available_in_the_first_venue"] = len(records)
    selection["contracts_supplied_from_the_first_venue"] = len(records)
    # The first venue's candidate universe is now the union of two observation paths, so
    # the selection records how many contracts came through each. A contract read from a
    # caller-supplied glob claims no declared path and is labelled as such rather than
    # being folded into one of the real paths.
    first_venue_provenance = collections.Counter(
        "not_a_declared_observation_path"
        if record["observation_origin"] is None
        else str(record["observation_origin"])
        for record in records
    )
    selection["first_venue_observation_provenance"] = dict(sorted(first_venue_provenance.items()))
    selection["first_venue_universe_is_the_union_of_observation_paths"] = markets_glob is None

    reads = (
        *first_venue_reads(records, settings=settings, calendar=calendar),
        *second_venue_reads(identities, settings=settings),
    )
    registry = build_registry(reads, settings=settings)
    reads_by_venue = {
        venue: sum(1 for read in reads if read.venue == venue) for venue in settings.venue_names
    }
    return CrossVenueResult(
        registry=registry,
        selection=selection,
        calendar_months=tuple(sorted({f"{year:04d}-{month:02d}" for year, month in calendar})),
        reads_by_venue=reads_by_venue,
    )


__all__ = [
    "COHORT_CONFIG_PATH",
    "DEFAULT_SECOND_VENUE_IDENTITY_COLUMN",
    "DEFAULT_SECOND_VENUE_LIMIT",
    "GRAPH_CONFIG_PATH",
    "KALSHI_MARKETS_GLOB",
    "MATCH_CONFIG_PATH",
    "POLYMARKET_GLOB",
    "CrossVenueError",
    "CrossVenueResult",
    "declared_calendar",
    "declared_policy_series",
    "first_venue_reads",
    "first_venue_records",
    "load_config",
    "run_cross_venue_matching",
    "second_venue_reads",
    "second_venue_records",
]
