"""Tests for the cross-venue candidate driver.

The matching layer's own tests cover grading. These cover what the driver adds and
what the matching layer deliberately does not do: which records become candidates,
what the selection hid, and that a component the record does not publish is passed
as absent instead of being read off a ticker.

The match rules are the committed ``configs/matching_v1.yaml``, because the driver's
job is to feed that declaration rather than to restate it. Only the two record layers,
the cohort and calendar declarations, and the store root the venue's metadata
declaration resolves to are synthetic.
"""

from __future__ import annotations

import hashlib
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
from market_propagation.matching import (
    REASON_SECOND_VENUE_RECORD_NOT_HELD,
    REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER,
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


def condition_id_for(slug: str) -> str:
    """The contract key the store keys a record by, derived from the slug.

    A candidate is identified by the venue's own contract key and selected by its
    slug, so the fixture layer has to carry both. The key is a hash of the slug
    rather than a restatement of it, because the two are different fields on the real
    layer — 163,289 distinct slugs and 163,289 distinct keys — and a key read out of
    the name it identifies would make the ambiguity this driver refuses invisible to
    the fixture. It is derived rather than listed so the file stays reproducible.
    """
    return "0x" + hashlib.sha256(slug.encode("utf-8")).hexdigest()


def write_markets(path: Path, rows) -> None:
    table = pa.table(
        {
            name: pa.array([row[index] for row in rows], type=pa.string())
            for index, name in enumerate(MARKET_COLUMNS)
        }
    )
    pq.write_table(table, path)


def write_contracts(path: Path, slugs) -> None:
    """The second venue's cleaned layer: a slug that names it and the key that identifies it."""
    pq.write_table(
        pa.table(
            {
                "market_slug": pa.array(list(slugs), type=pa.string()),
                "condition_id": pa.array(
                    [condition_id_for(slug) for slug in slugs], type=pa.string()
                ),
            }
        ),
        path,
    )


def write_config(path: Path, payload: dict) -> None:
    import yaml

    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def write_acquisition_plan(root: Path) -> None:
    """The committed metadata plan, re-declared with a store root inside ``root``.

    The second venue's reads come from the metadata store, and the store resolves its
    plan from the matching configuration by the relative path the module declares
    rather than from the path the driver was handed, so a fixture that only wrote the
    layers would have the store read whichever plan the working directory happened to
    hold. The real block is copied from the committed configuration and its store root
    re-declared inside the temporary tree, so this test exercises the declared plan and
    the store it names is the empty one this fixture is about rather than whatever a
    machine with acquired metadata would answer.
    """
    import yaml

    committed = Path(__file__).resolve().parents[1] / "configs/matching_v1.yaml"
    payload = yaml.safe_load(committed.read_text(encoding="utf-8"))
    venue = next(entry for entry in payload["venues"] if entry["id"] == "polymarket")
    venue["metadata_acquisition"]["store_root"] = str(root / "polymarket-metadata")
    path = root / "configs" / "matching_v1.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


@pytest.fixture
def layers(tmp_path, monkeypatch):
    """A synthetic pair of layers, addressed by the relative globs the driver takes."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "markets").mkdir()
    (tmp_path / "poly").mkdir()
    write_markets(tmp_path / "markets" / "markets-0000.parquet", [*MARKETS, LEGACY])
    write_contracts(tmp_path / "poly" / "2025_01_01.parquet", SLUGS)
    write_config(
        tmp_path / "cohort.yaml",
        {"policy_series": ["KXFEDDECISION", "FEDDECISION"]},
    )
    write_config(
        tmp_path / "graph.yaml",
        {"decision_calendar": {"dates": ["2025-01-29", "2025-03-19"]}},
    )
    write_acquisition_plan(tmp_path)
    return tmp_path


def run(layers, **overrides):
    options = {
        "match_config_path": layers / "configs/matching_v1.yaml",
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
        rooted,
        identity_column="condition_id",
        slug_pattern=None,
        limit=None,
        pattern_column="market_slug",
    )
    assert names == sorted(condition_id_for(slug) for slug in SLUGS)
    assert selection["records_available"] == len(SLUGS)


def test_a_layer_with_no_records_is_refused_rather_than_reported_empty():
    with pytest.raises(CrossVenueError, match="no market records matched"):
        first_venue_records("nothing/here/*.parquet", ("KXFEDDECISION",))


def test_the_second_venue_uses_its_own_identity_column(layers):
    """The identity a candidate is handed comes from the key column, not from its name.

    The two are separate fields: the slug is what a human reads and the condition id
    is what the venue's own records are keyed by. Handing the pattern's column back as
    the identity would hand every later reader a name to look a contract up by, which
    is the confusion this driver refuses when a name maps to two keys.
    """
    names, selection = second_venue_records(
        "poly/*.parquet",
        identity_column="condition_id",
        slug_pattern=None,
        limit=None,
        pattern_column="market_slug",
    )
    assert names == sorted(condition_id_for(slug) for slug in SLUGS)
    assert selection["identity_column"] == "condition_id"
    assert selection["pattern_column"] == "market_slug"
    assert selection["records_available"] == len(SLUGS)


def test_the_selection_records_the_total_available_before_any_cap(layers):
    names, selection = second_venue_records(
        "poly/*.parquet",
        identity_column="condition_id",
        slug_pattern="fed",
        limit=2,
        pattern_column="market_slug",
    )
    assert len(names) == 2
    assert selection["records_available"] == len(SLUGS)
    assert selection["records_matching_the_declared_pattern"] == 2
    assert selection["candidates_supplied"] == 2
    assert selection["limit_applied"] == 2
    assert selection["cap_hid_records"] is False


def test_the_selection_says_when_its_cap_hid_records(layers):
    _, selection = second_venue_records(
        "poly/*.parquet",
        identity_column="condition_id",
        slug_pattern="fed",
        limit=1,
        pattern_column="market_slug",
    )
    assert selection["records_matching_the_declared_pattern"] == 2
    assert selection["candidates_supplied"] == 1
    # A count from a bounded universe must not read as a count from the whole layer.
    assert selection["cap_hid_records"] is True


def test_a_pattern_that_matches_nothing_is_an_empty_search_not_an_absence(layers):
    names, selection = second_venue_records(
        "poly/*.parquet",
        identity_column="condition_id",
        slug_pattern="banana",
        limit=None,
        pattern_column="market_slug",
    )
    assert names == []
    assert selection["records_available"] == len(SLUGS)
    assert selection["records_matching_the_declared_pattern"] == 0


def test_an_invalid_pattern_is_refused_rather_than_treated_as_no_pattern(layers):
    with pytest.raises(CrossVenueError, match="not a regex"):
        second_venue_records(
            "poly/*.parquet",
            identity_column="condition_id",
            slug_pattern="(",
            limit=None,
            pattern_column="market_slug",
        )


def test_a_name_that_maps_to_more_than_one_contract_key_is_refused(layers):
    """Which contract was meant would be a guess, so the universe is refused whole.

    The two columns are separate fields on the real layer and nothing makes the map
    one-to-one there either, so a pattern that reaches one name and two keys is a
    statement about the acquisition rather than a pair the driver may pick a side of.
    """
    duplicated = layers / "poly" / "2025_01_02.parquet"
    shared = "fed-decreases-interest-rates"
    pq.write_table(
        pa.table(
            {
                "market_slug": pa.array([shared, shared], type=pa.string()),
                "condition_id": pa.array(
                    [condition_id_for(shared), "0x" + "ab" * 32],
                    type=pa.string(),
                ),
            }
        ),
        duplicated,
    )
    with pytest.raises(CrossVenueError, match="more than one contract key"):
        second_venue_records(
            "poly/*.parquet",
            identity_column="condition_id",
            slug_pattern="fed-decreases-interest-rates",
            limit=None,
            pattern_column="market_slug",
        )


# --------------------------------------------------------------------------- #
# The whole driver.
# --------------------------------------------------------------------------- #


def test_the_selection_travels_with_the_counts_and_is_marked_as_selection(layers):
    """The selection names the key, the pattern's field, and what the cap was applied to.

    A candidate is identified by the venue's contract key and selected by its slug, so
    the block has to carry both field names rather than one: a reader that saw only the
    identity column could not tell which field the pattern reduced. The cap is stated
    with the thing it bounded, so a count of identities never reads as a count of names.
    """
    result = run(layers, slug_pattern="fed")
    selection = result.selection
    assert selection["slug_pattern"] == "fed"
    assert selection["identity_column"] == "condition_id"
    assert selection["pattern_column"] == "market_slug"
    assert selection["cap_applied_to"] == "contract_identity"
    assert selection["pattern_is_a_selection_rule_and_not_evidence"] is True
    assert selection["declared_policy_series"] == ["KXFEDDECISION", "FEDDECISION"]
    assert selection["contracts_supplied_from_the_first_venue"] == 4
    # The totals available and the pattern-matched count are reported whole, beside
    # the capped count, so the pattern's effect is legible rather than inferred.
    assert selection["records_available"] == len(SLUGS)
    assert selection["records_matching_the_declared_pattern"] == 2
    assert selection["candidates_supplied"] == 2


def test_a_readable_contract_is_read_and_an_unreadable_one_is_refused_by_name(layers):
    result = run(layers, slug_pattern="fed")
    reasons = result.read_refusals()
    # Three of the four supplied contracts parsed; the fourth states no readable payout.
    assert reasons.get("published_contract_text_states_no_readable_payout") == 1
    kalshi = next(entry for entry in result.summary()["coverage"] if entry["venue"] == "kalshi")
    assert kalshi["supplied"] == 4
    assert kalshi["readable"] == 3
    assert kalshi["refused"] == 1


def test_every_pair_is_kept_and_graded_reject_when_one_side_has_no_held_metadata(layers):
    """A pair whose second side has no held metadata is graded REJECT, not dropped.

    The second venue now declares a grammar, so the old premise that every pair is
    refused for want of a parser is gone. What is still true and still worth pinning is
    that a refused side does not remove the pair: the denominator keeps the universe
    that was searched, and the pair records the sides' own reasons so a reader sees
    which side was unreadable and why rather than only that the pair did not match.
    """
    result = run(layers, slug_pattern="fed")
    counts = result.summary()["counts"]
    assert counts["by_grade"]["EXACT"] == 0
    assert counts["primary_analysis"]["pairs"] == 0
    # A refused pair keeps its place rather than being dropped, so the denominator
    # states the universe that was searched.
    assert counts["candidate_pairs"] == 4 * 2
    assert counts["by_grade"]["REJECT"] == 4 * 2
    refusals = result.summary()["pair_refusals"]
    assert refusals["one_side_states_no_readable_payoff_predicate"] == 8
    assert refusals[REASON_SECOND_VENUE_RECORD_NOT_HELD] == 8
    for pair in result.registry.pairs:
        assert pair.grade.value == "REJECT"
        # The pair names its own sides' refusal codes rather than one generic reason.
        assert "one_side_states_no_readable_payoff_predicate" in pair.reasons
        assert REASON_SECOND_VENUE_RECORD_NOT_HELD in pair.reasons


def test_the_second_venues_candidates_are_identified_by_their_contract_key_and_refused(layers):
    """Every supplied candidate refuses for want of held metadata, under its own key.

    With no metadata held the read has nothing to state a payout from, and the refusal
    is named for that fact. The identifier is the venue's contract key rather than the
    slug, because the slug only names the contract and this layer has already declared
    that a name is not evidence.
    """
    result = run(layers, slug_pattern="fed")
    reads = result.read_refusals()
    assert reads[REASON_SECOND_VENUE_RECORD_NOT_HELD] == 2
    polymarket_reads = [read for read in result.registry.reads if read.venue == "polymarket"]
    assert len(polymarket_reads) == 2
    for read in polymarket_reads:
        assert read.reason == REASON_SECOND_VENUE_RECORD_NOT_HELD
        assert read.predicate is None
    assert [read.contract_id for read in polymarket_reads] == sorted(
        condition_id_for(slug) for slug in SLUGS if "fed" in slug
    )
    polymarket = next(
        entry for entry in result.summary()["coverage"] if entry["venue"] == "polymarket"
    )
    assert polymarket["readable"] == 0
    assert polymarket["refused"] == 2
    # The declared configuration can no longer produce the no-parser code, so it
    # appears nowhere in the registry: not as a read reason, not as a pair reason, and
    # not in the serialized document a consumer reads.
    assert REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER not in reads
    assert all(
        REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER not in pair.reasons
        for pair in result.registry.pairs
    )
    assert REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER not in json.dumps(result.as_dict())


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
