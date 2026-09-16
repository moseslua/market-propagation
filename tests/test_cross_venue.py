"""Tests for the cross-venue candidate driver.

The matching layer's own tests cover grading. These cover what the driver adds and
what the matching layer deliberately does not do: which records become candidates,
what the selection hid, and that a component the record does not publish is passed
as absent instead of being read off a ticker.

The match rules are the committed ``configs/matching_v1.yaml``, because the driver's
job is to feed that declaration rather than to restate it. Only the two record layers
and the cohort and calendar declarations are synthetic.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.cross_venue import (
    CrossVenueError,
    declared_calendar,
    declared_policy_series,
    first_venue_records,
    run_cross_venue_matching,
    second_venue_records,
)

MARKET_COLUMNS = ("ticker", "event_ticker", "yes_sub_title", "title")

#: Two readable contracts and one whose text states no payout this repository reads.
#: The fourth is in a series the cohort does not declare.
MARKETS = (
    (
        "KXFEDDECISION-25JAN-T4.25",
        "KXFEDDECISION-25JAN",
        "Above 4.25%",
        "Will the target federal funds rate be above 4.25%?",
    ),
    (
        "KXFEDDECISION-25JAN-NC",
        "KXFEDDECISION-25JAN",
        "No change",
        "Will the target federal funds rate be unchanged?",
    ),
    (
        "KXFEDDECISION-25JAN-T9.99",
        "KXFEDDECISION-25JAN",
        "25 bps decrease",
        "Will the Federal Reserve decrease interest rates by 25 bps?",
    ),
    ("KXCPI-25JAN-T300", "KXCPI-25JAN", "Above 300", "Will CPI be above 300?"),
)

#: The legacy sibling series. A ``LIKE 'FED-%'`` membership test drops it, which is
#: the trap the cohort's own note records.
LEGACY = (
    "FEDDECISION-25JAN-T4.25",
    "FEDDECISION-25JAN",
    "Above 4.25%",
    "Will the target federal funds rate be above 4.25%?",
)

SLUGS = (
    "fed-decreases-interest-rates-by-25-bps-after-january-2025-meeting",
    "no-change-in-fed-interest-rates-after-january-2025-meeting",
    "will-donald-trump-be-inaugurated",
    "highest-temperature-in-nyc-on-january-1",
)


def write_markets(path: Path, rows) -> None:
    table = pa.table(
        {
            name: pa.array([row[index] for row in rows], type=pa.string())
            for index, name in enumerate(MARKET_COLUMNS)
        }
    )
    pq.write_table(table, path)


def write_slugs(path: Path, slugs) -> None:
    pq.write_table(pa.table({"market_slug": pa.array(list(slugs), type=pa.string())}), path)


def write_config(path: Path, payload: dict) -> None:
    import yaml

    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


@pytest.fixture
def layers(tmp_path, monkeypatch):
    """A synthetic pair of layers, addressed by the relative globs the driver takes."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "markets").mkdir()
    (tmp_path / "poly").mkdir()
    write_markets(tmp_path / "markets" / "markets-0000.parquet", [*MARKETS, LEGACY])
    write_slugs(tmp_path / "poly" / "2025_01_01.parquet", SLUGS)
    write_config(
        tmp_path / "cohort.yaml",
        {"policy_series": ["KXFEDDECISION", "FEDDECISION"]},
    )
    write_config(
        tmp_path / "graph.yaml",
        {"decision_calendar": {"dates": ["2025-01-29", "2025-03-19"]}},
    )
    return tmp_path


def run(layers, **overrides):
    options = {
        "match_config_path": Path(__file__).resolve().parents[1] / "configs/matching_v1.yaml",
        "cohort_config_path": "cohort.yaml",
        "graph_config_path": "graph.yaml",
        "markets_glob": "markets/*.parquet",
        "second_venue_glob": "poly/*.parquet",
    }
    options.update(overrides)
    return run_cross_venue_matching(**options)


# --------------------------------------------------------------------------- #
# The declared inputs.
# --------------------------------------------------------------------------- #


def test_a_cohort_that_declares_no_series_is_refused():
    with pytest.raises(CrossVenueError, match="policy_series"):
        declared_policy_series({})


def test_a_calendar_with_no_dates_is_refused():
    with pytest.raises(CrossVenueError, match="decision_calendar"):
        declared_calendar({})


def test_a_calendar_naming_one_meeting_twice_is_refused():
    with pytest.raises(CrossVenueError, match="twice"):
        declared_calendar({"decision_calendar": {"dates": ["2025-01-29", "2025-01-29"]}})


# --------------------------------------------------------------------------- #
# Candidate formation.
# --------------------------------------------------------------------------- #


def test_series_membership_includes_the_legacy_sibling_a_substring_test_drops(layers):
    records = first_venue_records("markets/*.parquet", ("KXFEDDECISION", "FEDDECISION"))
    ids = [record["contract_id"] for record in records]
    assert "FEDDECISION-25JAN-T4.25" in ids


def test_series_membership_excludes_a_series_the_cohort_does_not_declare(layers):
    records = first_venue_records("markets/*.parquet", ("KXFEDDECISION", "FEDDECISION"))
    ids = [record["contract_id"] for record in records]
    # KXCPI is a real series and not a declared one, so it is not a candidate. A
    # substring test on "FED" would not have admitted it either; the point is that
    # membership is decided by the declaration rather than by the letters.
    assert not any(ticker.startswith("KXCPI") for ticker in ids)


def test_a_rooted_pattern_is_read_because_the_cli_hands_over_a_resolved_layer(layers):
    """A window-scoped run resolves its layer and passes a pattern that carries its anchor.

    ``match-cross-venue`` given a window resolves which declared market layer governs it
    and passes ``<root>/<path_pattern>`` rather than the configured relative glob.
    Globbing that against the working directory raises instead of reading, so both
    anchored and relative patterns have to be readable here.
    """
    rooted = str(layers / "markets" / "*.parquet")
    records = first_venue_records(rooted, ("KXFEDDECISION", "FEDDECISION"))
    assert "FEDDECISION-25JAN-T4.25" in [record["contract_id"] for record in records]


def test_a_rooted_second_venue_pattern_is_read_too(layers):
    """The same hazard on the other layer, which no current caller passes rooted."""
    rooted = str(layers / "poly" / "*.parquet")
    names, selection = second_venue_records(
        rooted, identity_column="market_slug", slug_pattern=None, limit=None
    )
    assert names == sorted(SLUGS)
    assert selection["records_available"] == len(SLUGS)


def test_a_layer_with_no_records_is_refused_rather_than_reported_empty():
    with pytest.raises(CrossVenueError, match="no market records matched"):
        first_venue_records("nothing/here/*.parquet", ("KXFEDDECISION",))


def test_the_second_venue_uses_its_own_identity_column(layers):
    names, selection = second_venue_records(
        "poly/*.parquet", identity_column="market_slug", slug_pattern=None, limit=None
    )
    assert names == sorted(SLUGS)
    assert selection["identity_column"] == "market_slug"
    assert selection["records_available"] == len(SLUGS)


def test_the_selection_records_the_total_available_before_any_cap(layers):
    names, selection = second_venue_records(
        "poly/*.parquet", identity_column="market_slug", slug_pattern="fed", limit=2
    )
    assert len(names) == 2
    assert selection["records_available"] == len(SLUGS)
    assert selection["records_matching_the_declared_pattern"] == 2
    assert selection["candidates_supplied"] == 2
    assert selection["limit_applied"] == 2
    assert selection["cap_hid_records"] is False


def test_the_selection_says_when_its_cap_hid_records(layers):
    _, selection = second_venue_records(
        "poly/*.parquet", identity_column="market_slug", slug_pattern="fed", limit=1
    )
    assert selection["records_matching_the_declared_pattern"] == 2
    assert selection["candidates_supplied"] == 1
    # A count from a bounded universe must not read as a count from the whole layer.
    assert selection["cap_hid_records"] is True


def test_a_pattern_that_matches_nothing_is_an_empty_search_not_an_absence(layers):
    names, selection = second_venue_records(
        "poly/*.parquet", identity_column="market_slug", slug_pattern="banana", limit=None
    )
    assert names == []
    assert selection["records_available"] == len(SLUGS)
    assert selection["records_matching_the_declared_pattern"] == 0


def test_an_invalid_pattern_is_refused_rather_than_treated_as_no_pattern(layers):
    with pytest.raises(CrossVenueError, match="not a regex"):
        second_venue_records(
            "poly/*.parquet", identity_column="market_slug", slug_pattern="(", limit=None
        )


# --------------------------------------------------------------------------- #
# The whole driver.
# --------------------------------------------------------------------------- #


def test_the_selection_travels_with_the_counts_and_is_marked_as_selection(layers):
    result = run(layers, slug_pattern="fed")
    selection = result.selection
    assert selection["slug_pattern"] == "fed"
    assert selection["pattern_is_a_selection_rule_and_not_evidence"] is True
    assert selection["declared_policy_series"] == ["KXFEDDECISION", "FEDDECISION"]
    assert selection["contracts_supplied_from_the_first_venue"] == 4


def test_a_readable_contract_is_read_and_an_unreadable_one_is_refused_by_name(layers):
    result = run(layers, slug_pattern="fed")
    reasons = result.read_refusals()
    # Three of the four supplied contracts parsed; the fourth states no readable payout.
    assert reasons.get("published_contract_text_states_no_readable_payout") == 1
    kalshi = next(entry for entry in result.summary()["coverage"] if entry["venue"] == "kalshi")
    assert kalshi["supplied"] == 4
    assert kalshi["readable"] == 3
    assert kalshi["refused"] == 1


def test_every_pair_is_kept_and_graded_reject_when_a_venue_declares_no_parser(layers):
    result = run(layers, slug_pattern="fed")
    counts = result.summary()["counts"]
    assert counts["by_grade"]["EXACT"] == 0
    assert counts["primary_analysis"]["pairs"] == 0
    # A refused pair keeps its place rather than being dropped, so the denominator
    # states the universe that was searched.
    assert counts["candidate_pairs"] == 4 * 2
    assert result.summary()["pair_refusals"]["one_side_states_no_readable_payoff_predicate"] == 8


def test_the_second_venues_records_are_all_refused_for_the_declared_reason(layers):
    result = run(layers, slug_pattern="fed")
    reads = result.read_refusals()
    assert reads["venue_payout_text_has_no_parser_declared_in_this_repository"] == 2
    polymarket = next(
        entry for entry in result.summary()["coverage"] if entry["venue"] == "polymarket"
    )
    assert polymarket["readable"] == 0
    assert polymarket["refused"] == 2


def test_the_registry_is_deterministic_across_runs(layers):
    first = run(layers, slug_pattern="fed")
    second = run(layers, slug_pattern="fed")
    assert first.summary()["digest"] == second.summary()["digest"]
    assert first.summary()["settings_digest"] == second.summary()["settings_digest"]


def test_the_whole_registry_carries_the_selection_beside_its_pairs(layers):
    whole = run(layers, slug_pattern="fed").as_dict()
    assert len(whole["pairs"]) == 8
    assert whole["candidate_selection"]["records_available"] == len(SLUGS)
    assert whole["calendar_months"] == ["2025-01", "2025-03"]
    assert whole["reads_by_venue"] == {"kalshi": 4, "polymarket": 2}
    # JSON-representable end to end, because that is how it is written.
    json.dumps(whole)
