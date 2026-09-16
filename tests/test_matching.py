"""Acceptance tests for the cross-venue predicate-matching layer.

Each test here defends one way a match could be manufactured out of a resemblance
rather than out of a parsed predicate:

* An identical predicate pair grades ``EXACT``, and the grade is assigned from the
  archived text the two records state rather than from any property of their
  identifiers or titles.
* A near-miss threshold on the same event does not grade ``EXACT``. It is
  ``APPROXIMATE`` with ``threshold_differs``, because the neighbouring strike is the
  most correlated contract on the venue and therefore the one substitution the design
  forbids.
* A pair mismatching on the reference period is ``REJECT`` with a named reason, not
  ``APPROXIMATE``: the two payoffs resolve on different figures however alike the
  contracts look.
* A settlement-semantics mismatch is refused by name. One contract settling on a
  revised figure and another on the first release is
  ``settlement_revision_vintage_differs``, which is the refusal this project's
  already-measured revision-vintage result requires.
* Two unobserved components are not an agreement. A component neither record
  publishes refuses the pair with
  ``required_component_is_unobserved_on_both_records``, which is the same rule that
  made the repository remove a placeholder string from a matching field.
* A venue whose record states no declared payout form yields no predicate, so the
  cross-venue pair is ``REJECT`` with that named reason and the registry reports it
  per venue instead of reporting an empty match set as an absence of matches.
* The registry's grade counts reconcile with the pairs it lists, and only ``EXACT``
  feeds the primary analysis.

Two second-venue declarations are fixtured, because the local archive only exhibits
one of them. ``POLYMARKET`` is the measured state of the acquired layer: its cleaned
column list carries a market slug and market metadata and no settlement-rule text, and
no parser for that vocabulary exists in this repository, so its records yield no
predicate. The layer still has to express the populated case, so ``ALTERNATE_VENUE``
stands for a second venue whose record states the same declared payout grammar, and
that is stated openly here rather than imitated from the measured one. Every parity
claim is graded across those two venues, never within one.

The fixtures are synthetic. No test here reaches the network or reads the acquired
archives, and the one test that reads a configuration file reads the declaration this
change adds, which is a source file rather than a payload.
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from market_propagation.ingest.policy_predicates import (
    RATE_FEDERAL_FUNDS,
    RATE_TARGET_CHANGE_BPS,
    RATE_TARGET_UPPER_BOUND,
    REASON_MONTH_NOT_IN_CALENDAR,
    REASON_RATE_SUBJECT_UNREADABLE,
    REASON_TEXT_UNREADABLE,
)
from market_propagation.matching import (
    AGREEMENT_AGREE,
    AGREEMENT_EQUIVALENT,
    AGREEMENT_UNOBSERVED,
    COMPONENTS,
    GRADES,
    MATCH_CONFIG_PATH,
    PARSER_DECLARED_POLICY_FORMS,
    PARSER_NONE_DECLARED,
    PROGRAMME_REQUIRED_COMPONENTS,
    REASON_COMPONENT_UNOBSERVED,
    REASON_ECONOMICALLY_EQUIVALENT,
    REASON_GRADED_EXACT,
    REASON_PREDICATE_REFUSED,
    REASON_RECORD_FIELD_UNPUBLISHED,
    REASON_REFERENCE_HORIZON_DIFFERS,
    REASON_REFERENCE_PERIOD_DIFFERS,
    REASON_SETTLEMENT_CRITERION_DIFFERS,
    REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS,
    REASON_THRESHOLD_DIFFERS,
    REASON_UNDERLYING_EVENT_DIFFERS,
    REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER,
    REASONS,
    REGISTRY_VERSION,
    SETTLEMENT_VINTAGE_FIRST_RELEASE,
    SETTLEMENT_VINTAGE_REVISED_FIGURE,
    GradeDefinition,
    MatchGrade,
    MatchingSettings,
    PredicateComponent,
    SettlementCriterion,
    VenueDeclaration,
    build_registry,
    grade_pair,
    load_matching_settings,
    read_predicate,
    registry_digest,
    settings_digest,
)

#: The declared decision calendar the synthetic contracts are placed on. It is
#: supplied independently of the contracts, because a calendar read off a ticker
#: cannot tell a missing meeting from one that was never handed in.
CALENDAR: dict[tuple[int, int], dt.date] = {
    (2025, 1): dt.date(2025, 1, 29),
    (2025, 3): dt.date(2025, 3, 19),
    (2025, 5): dt.date(2025, 5, 7),
}

JAN_MEETING = dt.date(2025, 1, 29)
MAR_MEETING = dt.date(2025, 3, 19)

#: The declared settlement basis the synthetic records state.
CASH_AT_LISTED_PAYOUT = "cash_settlement_at_the_listed_payout"

KALSHI = VenueDeclaration(
    venue="kalshi",
    payout_text_fields=("yes_sub_title", "title"),
    parser=PARSER_DECLARED_POLICY_FORMS,
    documentation_verified=True,
)

#: A second venue whose own record states the same declared payout grammar. It is a
#: fixtured venue for the populated case, not a claim about any real venue's schema.
ALTERNATE_VENUE = VenueDeclaration(
    venue="alternate_venue",
    payout_text_fields=("yes_sub_title", "title"),
    parser=PARSER_DECLARED_POLICY_FORMS,
    documentation_verified=False,
)

#: The second venue this repository actually has. Its cleaned local layer carries a
#: slug and no settlement-rule text column, and no parser for that vocabulary exists
#: here, so its records yield no predicate.
POLYMARKET = VenueDeclaration(
    venue="polymarket",
    payout_text_fields=("market_slug",),
    parser=PARSER_NONE_DECLARED,
    documentation_verified=False,
)


def _grade(grade: MatchGrade, definition: str, certifies: bool) -> GradeDefinition:
    return GradeDefinition(grade=grade, definition=definition, certifies_the_same_payoff=certifies)


def settings(
    *,
    second_venue: VenueDeclaration = ALTERNATE_VENUE,
    required: tuple[PredicateComponent, ...] = PROGRAMME_REQUIRED_COMPONENTS,
    primary_grades: tuple[MatchGrade, ...] = (MatchGrade.EXACT,),
) -> MatchingSettings:
    """The declared rules, built directly so a test does not depend on a file's text."""
    return MatchingSettings(
        config_version="matching_v1",
        venues=(KALSHI, second_venue),
        required_components=required,
        grades=(
            _grade(MatchGrade.EXACT, "every component agrees", True),
            _grade(MatchGrade.ECONOMICALLY_EQUIVALENT, "same level in another unit", True),
            _grade(MatchGrade.APPROXIMATE, "same event, different boundary", False),
            _grade(MatchGrade.REJECT, "a required component differs", False),
        ),
        primary_analysis_grades=primary_grades,
        threshold_units=(("percent", Decimal(1)), ("basis_points", Decimal("0.01"))),
    )


def criterion(
    vintage: str = SETTLEMENT_VINTAGE_FIRST_RELEASE,
    basis: str = CASH_AT_LISTED_PAYOUT,
) -> SettlementCriterion:
    return SettlementCriterion(basis=basis, vintage=vintage)


def read_at(
    declaration: VenueDeclaration,
    contract_id: str,
    subtitle: str,
    title: str,
    *,
    event_ticker: str = "KXFED-25JAN",
    reference_period: str | None = "2024-12",
    settlement: SettlementCriterion | None = None,
    strike: Decimal | None = None,
    strike_unit: str | None = None,
):
    """One contract read from its own archived payout text, on a declared venue."""
    return read_predicate(
        declaration,
        contract_id=contract_id,
        text_fields={"yes_sub_title": subtitle, "title": title},
        event_ticker=event_ticker,
        calendar=CALENDAR,
        reference_period=reference_period,
        settlement_criterion=settlement if settlement is not None else criterion(),
        strike=strike,
        strike_unit=strike_unit,
    )


def kalshi_read(contract_id: str, subtitle: str, title: str, **kwargs):
    return read_at(KALSHI, contract_id, subtitle, title, **kwargs)


def alternate_read(contract_id: str, subtitle: str, title: str, **kwargs):
    return read_at(ALTERNATE_VENUE, contract_id, subtitle, title, **kwargs)


def polymarket_read(contract_id: str, slug: str, *, event_ticker: str = "KXFED-25JAN"):
    """One Polymarket record, read through its declared (absent) parser."""
    return read_predicate(
        POLYMARKET,
        contract_id=contract_id,
        text_fields={"market_slug": slug},
        event_ticker=event_ticker,
        calendar=CALENDAR,
        reference_period="2024-12",
        settlement_criterion=criterion(),
    )


LEVEL_TITLE = "Will the target federal funds rate be above 3.00%?"


def test_settings_load_from_the_declaration() -> None:
    """The committed configuration parses into rules this module accepts."""
    loaded = load_matching_settings("configs/matching_v1.yaml")
    assert loaded.config_version == "matching_v1"
    assert loaded.venue_names == ("kalshi", "polymarket")
    assert set(PROGRAMME_REQUIRED_COMPONENTS) <= set(loaded.required_components)
    assert loaded.supplies_the_primary_analysis(MatchGrade.EXACT) is True
    assert loaded.supplies_the_primary_analysis(MatchGrade.ECONOMICALLY_EQUIVALENT) is False
    assert loaded.supplies_the_primary_analysis(MatchGrade.APPROXIMATE) is False
    assert loaded.supplies_the_primary_analysis(MatchGrade.REJECT) is False
    assert loaded.unit_scale("basis_points") == Decimal("0.01")
    assert settings_digest(loaded) == settings_digest(load_matching_settings(MATCH_CONFIG_PATH))
    assert loaded.grade_definition(MatchGrade.EXACT).certifies_the_same_payoff is True


def test_the_declaration_requires_the_programme_components() -> None:
    """A configuration that drops a required component is refused, not honoured.

    The gate a run reports itself as applying has to be the gate it applied, and a
    configuration requiring only the threshold would admit a pair whose payoffs
    resolve on different figures.
    """
    with pytest.raises(ValueError, match="omits"):
        settings(required=(PredicateComponent.THRESHOLD,))


def test_primary_analysis_cannot_be_fed_from_a_near_miss() -> None:
    """APPROXIMATE never feeds the primary analysis, however the config is written."""
    with pytest.raises(ValueError, match="primary analysis cannot be fed"):
        settings(primary_grades=(MatchGrade.EXACT, MatchGrade.APPROXIMATE))


def test_an_identical_predicate_pair_grades_exact() -> None:
    """Two records stating the same predicate are one claim, and grade EXACT.

    The two contracts are deliberately unlike in every identifier: different venues,
    different tickers. Matching on the parsed predicate is what makes them a pair.
    """
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = alternate_read("ALT-B3.00", "Above 3.00%", LEVEL_TITLE)
    assert left.readable and right.readable
    assert (left.venue, right.venue) == ("kalshi", "alternate_venue")

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.EXACT
    assert pair.reasons == (REASON_GRADED_EXACT,)
    assert tuple(entry.component for entry in pair.components) == COMPONENTS
    assert {entry.verdict for entry in pair.components} == {AGREEMENT_AGREE}
    threshold = next(
        entry for entry in pair.components if entry.component is PredicateComponent.THRESHOLD
    )
    assert (threshold.left, threshold.right) == ("3.00 percent", "3.00 percent")


def test_a_near_miss_threshold_does_not_grade_exact() -> None:
    """The neighbouring strike is a different claim, not a rounding difference."""
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = alternate_read(
        "ALT-A3.25", "Above 3.25%", "Will the target federal funds rate be above 3.25%?"
    )

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.APPROXIMATE
    assert pair.grade is not MatchGrade.EXACT
    assert REASON_THRESHOLD_DIFFERS in pair.reasons
    threshold = next(
        entry for entry in pair.components if entry.component is PredicateComponent.THRESHOLD
    )
    assert (threshold.left, threshold.right) == ("3.00 percent", "3.25 percent")
    assert threshold.verdict != AGREEMENT_AGREE


def test_a_reference_period_mismatch_is_rejected_by_name() -> None:
    """Different reference periods are REJECT, not APPROXIMATE."""
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = alternate_read("ALT-A3.00", "Above 3.00%", LEVEL_TITLE, reference_period="2025-01")

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.REJECT
    assert REASON_REFERENCE_PERIOD_DIFFERS in pair.reasons
    period = next(
        entry for entry in pair.components if entry.component is PredicateComponent.REFERENCE_PERIOD
    )
    assert (period.left, period.right) == ("2024-12", "2025-01")
    assert REASON_REFERENCE_PERIOD_DIFFERS == "reference_period_differs"


def test_a_settlement_revision_vintage_mismatch_is_refused_by_name() -> None:
    """Same basis, different print: the measured revision result makes this a refusal.

    Every other component agrees, so nothing but the settlement criterion separates
    the two contracts. One settles on the revised figure and the other on the first
    release, and the refusal names that fact rather than a generic criterion
    difference.
    """
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = alternate_read(
        "ALT-A3.00",
        "Above 3.00%",
        LEVEL_TITLE,
        settlement=criterion(SETTLEMENT_VINTAGE_REVISED_FIGURE),
    )

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.REJECT
    assert REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS in pair.reasons
    assert REASON_SETTLEMENT_CRITERION_DIFFERS not in pair.reasons, (
        "the specific refusal is not repeated as the generic one"
    )
    assert pair.reasons[0] == REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS
    assert "vintage" in pair.detail
    assert REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS == "settlement_revision_vintage_differs"


def test_a_different_settlement_basis_is_a_named_refusal() -> None:
    """A different payout basis is refused, and is not reported as a vintage issue."""
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = alternate_read(
        "ALT-A3.00",
        "Above 3.00%",
        LEVEL_TITLE,
        settlement=criterion(basis="physical_delivery_of_the_reference_rate"),
    )
    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.REJECT
    assert REASON_SETTLEMENT_CRITERION_DIFFERS in pair.reasons
    assert REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS not in pair.reasons


def test_a_different_economic_variable_is_rejected() -> None:
    """The realized effective rate and the target range are different claims.

    The parser separates them, and this test defends that the separation reaches the
    grade: the two records state the same number and still are not one claim.
    """
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = alternate_read(
        "ALT-EFFR3.00", "Above 3.00%", "Will the federal funds rate be above 3.00%?"
    )
    assert left.predicate is not None and right.predicate is not None
    assert left.predicate.underlying_event == RATE_TARGET_UPPER_BOUND
    assert right.predicate.underlying_event == RATE_FEDERAL_FUNDS

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.REJECT
    assert REASON_UNDERLYING_EVENT_DIFFERS in pair.reasons
    assert REASON_UNDERLYING_EVENT_DIFFERS == "underlying_economic_event_differs"


def test_a_different_horizon_is_rejected() -> None:
    """Two months' contracts are different horizons, resolved from the calendar."""
    left = kalshi_read("KXFED-25JAN-A3.00", "Above 3.00%", LEVEL_TITLE, event_ticker="KXFED-25JAN")
    right = alternate_read("ALT-25MAR-A3.00", "Above 3.00%", LEVEL_TITLE, event_ticker="ALT-25MAR")
    assert left.predicate is not None and right.predicate is not None
    assert (left.predicate.reference_horizon, right.predicate.reference_horizon) == (
        JAN_MEETING,
        MAR_MEETING,
    )

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.REJECT
    assert REASON_REFERENCE_HORIZON_DIFFERS in pair.reasons
    assert REASON_REFERENCE_HORIZON_DIFFERS == "reference_horizon_differs"


def test_an_unobserved_component_is_never_an_agreement() -> None:
    """Two records publishing nothing on a component cannot be shown to agree."""
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE, reference_period=None)
    right = alternate_read("ALT-A3.00", "Above 3.00%", LEVEL_TITLE, reference_period=None)

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.REJECT
    assert REASON_COMPONENT_UNOBSERVED in pair.reasons
    period = next(
        entry for entry in pair.components if entry.component is PredicateComponent.REFERENCE_PERIOD
    )
    assert period.verdict == AGREEMENT_UNOBSERVED
    assert (period.left, period.right) == ("unobserved", "unobserved")


def test_a_unit_restatement_is_economically_equivalent_and_not_exact() -> None:
    """One level in two declared units: the same payoff, and a separate grade.

    The scale is applied from the declaration and the grade says so, so a reader sees
    the conversion rather than inferring one. It is kept out of the primary analysis
    because admitting it is a separate decision.
    """
    left = kalshi_read(
        "KXFEDDECISION-A",
        "Hike 25bps",
        "Will the target federal funds rate be hiked by 25bps?",
        event_ticker="KXFEDDECISION-25JAN",
    )
    assert left.predicate is not None
    assert left.predicate.underlying_event == RATE_TARGET_CHANGE_BPS
    assert left.predicate.threshold_unit == "basis_points"

    right = alternate_read(
        "ALT-DECISION-A",
        "Hike 25bps",
        "Will the target federal funds rate be hiked by 25bps?",
        event_ticker="ALT-25JAN",
        strike=Decimal("0.25"),
        strike_unit="percent",
    )
    assert right.readable

    pair = grade_pair(left, right, settings=settings())
    assert pair.grade is MatchGrade.ECONOMICALLY_EQUIVALENT
    assert pair.reasons == (REASON_ECONOMICALLY_EQUIVALENT,)
    threshold = next(
        entry for entry in pair.components if entry.component is PredicateComponent.THRESHOLD
    )
    assert threshold.verdict == AGREEMENT_EQUIVALENT
    assert (threshold.left, threshold.right) == ("25 basis_points", "0.25 percent")
    assert settings().supplies_the_primary_analysis(pair.grade) is False


def test_a_stated_strike_in_an_undeclared_unit_is_refused() -> None:
    """A unit the configuration does not scale is refused at the boundary."""
    with pytest.raises(ValueError, match="strike_unit"):
        kalshi_read(
            "KXFED-C",
            "Above 3.00%",
            LEVEL_TITLE,
            strike=Decimal("300"),
            strike_unit="per_myriad",
        )


def test_an_unreadable_payout_text_yields_a_named_null() -> None:
    """A contract whose text states no declared payout yields no predicate."""
    read = kalshi_read("KXFED-X", "No cut/hike", "Will the target federal funds rate change?")
    assert read.readable is False
    assert read.reason == REASON_TEXT_UNREADABLE
    assert read.predicate is None
    assert "No cut/hike" in read.detail


def test_a_month_the_calendar_does_not_date_is_unplaceable() -> None:
    """The horizon comes from the declared calendar and never from the ticker."""
    read = kalshi_read("KXFED-25APR-A3.00", "Above 3.00%", LEVEL_TITLE, event_ticker="KXFED-25APR")
    assert read.readable is False
    assert read.reason == REASON_MONTH_NOT_IN_CALENDAR
    assert "2025-04" in read.detail


def test_a_title_with_no_settlement_subject_is_refused() -> None:
    """A readable payout on an unrecognized subject is still not a predicate."""
    read = kalshi_read("KXODD-A3.00", "Above 3.00%", "Will something else be above 3.00%?")
    assert read.readable is False
    assert read.reason == REASON_RATE_SUBJECT_UNREADABLE


def test_a_venue_with_no_declared_parser_yields_no_predicate() -> None:
    """Reading a slug as a predicate is the title similarity this layer refuses."""
    read = polymarket_read(
        "0xabc", "will-the-fed-decrease-interest-rates-by-25-bps-after-its-may-meeting"
    )
    assert read.readable is False
    assert read.reason == REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER
    assert "slug" in read.detail


def test_a_record_missing_a_declared_text_field_is_refused() -> None:
    """A record omitting a column the declared parser reads yields no predicate."""
    read = read_predicate(
        KALSHI,
        contract_id="KXFED-PARTIAL",
        text_fields={"yes_sub_title": "Above 3.00%"},
        event_ticker="KXFED-25JAN",
        calendar=CALENDAR,
    )
    assert read.readable is False
    assert read.reason == REASON_RECORD_FIELD_UNPUBLISHED
    assert "'title'" in read.detail


def test_an_unreadable_side_rejects_the_pair_with_its_own_reason() -> None:
    """A cross-venue pair is refused on the parse, and carries the parser's code."""
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = polymarket_read("0xabc", "fed-rate-cut-by-march-2026-meeting")

    pair = grade_pair(left, right, settings=settings(second_venue=POLYMARKET))
    assert pair.grade is MatchGrade.REJECT
    assert REASON_PREDICATE_REFUSED in pair.reasons
    assert REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER in pair.reasons
    assert pair.components == ()
    assert "polymarket" in pair.detail


def test_the_declaration_and_this_module_agree_on_every_code() -> None:
    """Every code a registry can carry resolves in the declaration, and the reverse.

    A code written by the module and absent from the configuration is a reason a
    reader cannot look up; a code declared and never written is a refusal the run
    claims to be able to record. Both are drift between the two files, and this test
    is what stops one of them being edited without the other.
    """
    declared = yaml.safe_load(Path(MATCH_CONFIG_PATH).read_text(encoding="utf-8"))
    assert isinstance(declared, dict)

    grade_codes = {entry["code"] for entry in declared["grade_reasons"]}
    refusal_codes = {entry["code"] for entry in declared["refusals"]}
    assert grade_codes | refusal_codes == set(REASONS), (
        "the declaration and the module's reason vocabulary have drifted"
    )
    by_grade = {definition.grade.value: definition for definition in settings().grades}
    for entry in declared["grade_reasons"]:
        assert by_grade[entry["grade"]].certifies_the_same_payoff is True

    declared_grades = {entry["grade"] for entry in declared["grades"]}
    assert declared_grades == {grade.value for grade in GRADES}
    assert [
        entry["grade"] for entry in declared["grades"] if entry["admits_into_primary_analysis"]
    ] == [MatchGrade.EXACT.value]
    assert declared["primary_analysis"]["grades"] == [MatchGrade.EXACT.value]
    assert declared["similarity_measures_declared"] == [], (
        "no similarity measure is computed, so none can be declared"
    )
    assert declared["components"]["all"] == [str(component) for component in COMPONENTS]
    assert tuple(declared["components"]["required_for_agreement"]) == tuple(
        str(component) for component in PROGRAMME_REQUIRED_COMPONENTS
    )


def test_the_grade_cannot_be_moved_by_a_title() -> None:
    """Titles influence nothing: the grade is read off the parsed predicate alone.

    Two contracts whose titles are character-for-character identical and whose
    predicates differ are not a match, and two whose titles share no wording but whose
    predicates are identical are. A similarity measure over the text cannot produce
    either verdict, which is why this layer computes none.
    """
    same_title_left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    same_title_right = alternate_read("ALT-A3.25", "Above 3.25%", LEVEL_TITLE)
    assert same_title_left.predicate is not None and same_title_right.predicate is not None
    assert same_title_left.predicate.title == same_title_right.predicate.title
    assert same_title_left.predicate.yes_sub_title != same_title_right.predicate.yes_sub_title
    assert grade_pair(same_title_left, same_title_right, settings=settings()).grade is (
        MatchGrade.APPROXIMATE
    )

    unlike_left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    unlike_right = alternate_read(
        "ALT-B3.00",
        "Above 3.00%",
        "Target federal funds rate above 3.00%?",
    )
    assert unlike_left.predicate is not None and unlike_right.predicate is not None
    assert unlike_left.predicate.title != unlike_right.predicate.title
    assert grade_pair(unlike_left, unlike_right, settings=settings()).grade is MatchGrade.EXACT

    # A pair with identical titles is still refused when a component the title does not
    # state differs, so the refusal is not readable off the text either: the reference
    # period is not in the title at all, and it is a required component.
    other_period = alternate_read(
        "ALT-C3.00", "Above 3.00%", LEVEL_TITLE, reference_period="2025-01"
    )
    assert other_period.predicate is not None
    assert other_period.predicate.title == unlike_left.predicate.title
    refused = grade_pair(unlike_left, other_period, settings=settings())
    assert refused.grade is MatchGrade.REJECT
    assert REASON_REFERENCE_PERIOD_DIFFERS in refused.reasons


def test_grade_pair_refuses_a_same_venue_comparison() -> None:
    """This layer compares across venues; a same-venue pair is another module's question."""
    left = kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE)
    right = kalshi_read("KXFED-B3.00", "Above 3.00%", LEVEL_TITLE)
    with pytest.raises(ValueError, match="cross-venue comparison"):
        grade_pair(left, right, settings=settings())


def test_the_registry_counts_reconcile_with_the_pairs_it_lists() -> None:
    """Every pair is graded exactly once, and the counts are the listed pairs' counts."""
    reads = [
        kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE),
        kalshi_read(
            "KXFED-B3.25", "Above 3.25%", "Will the target federal funds rate be above 3.25%?"
        ),
        alternate_read("ALT-C3.00", "Above 3.00%", LEVEL_TITLE),
        alternate_read(
            "ALT-D3.50", "Above 3.50%", "Will the target federal funds rate be above 3.50%?"
        ),
    ]
    registry = build_registry(reads, settings=settings())

    assert len(registry.pairs) == 4, "the candidate universe is the cross-venue product"
    counts = registry.counts_by_grade()
    assert set(counts) == {grade.value for grade in GRADES}
    assert sum(counts.values()) == len(registry.pairs)
    for grade in GRADES:
        assert counts[grade.value] == len(registry.pairs_with_grade(grade))

    assert all(pair.reasons for pair in registry.pairs)
    assert len({(pair.left_contract_id, pair.right_contract_id) for pair in registry.pairs}) == len(
        registry.pairs
    )
    assert counts["EXACT"] == 1
    assert counts["APPROXIMATE"] == 3
    assert counts["REJECT"] == 0
    assert registry.primary_analysis_pairs() == registry.exact_pairs()
    assert len(registry.primary_analysis_pairs()) == 1


def test_registry_counts_reconcile_when_a_venue_contributes_no_predicate() -> None:
    """The measured cross-venue state: one readable venue and one that states nothing."""
    reads = [
        kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE),
        kalshi_read("KXFED-B3.00", "Above 3.00%", LEVEL_TITLE),
        polymarket_read("0xabc", "fed-rate-cut-by-march-2026-meeting"),
        polymarket_read("0xdef", "fed-rate-hike-in-2025"),
    ]
    registry = build_registry(reads, settings=settings(second_venue=POLYMARKET))

    counts = registry.counts_by_grade()
    assert len(registry.pairs) == 4
    assert sum(counts.values()) == len(registry.pairs)
    assert counts["REJECT"] == 4
    assert counts["EXACT"] == 0
    assert counts["APPROXIMATE"] == 0
    assert registry.primary_analysis_pairs() == ()
    for grade in GRADES:
        assert counts[grade.value] == len(registry.pairs_with_grade(grade))

    coverage = {entry.venue: entry for entry in registry.coverage}
    assert coverage["kalshi"].readable == 2
    assert coverage["polymarket"].readable == 0
    assert coverage["polymarket"].refused == 2
    assert coverage["polymarket"].reasons == ((REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER, 2),)
    payload = registry.as_dict()
    assert payload["counts"]["by_grade"] == counts
    assert payload["counts"]["primary_analysis"] == {"grades": ["EXACT"], "pairs": 0}


def test_coverage_reports_why_a_contract_was_refused() -> None:
    """A venue that yielded nothing on some records is reported by reason."""
    reads = [
        kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE),
        kalshi_read("KXFED-X", "No cut/hike", "Will the target federal funds rate change?"),
        alternate_read("ALT-C3.00", "Above 3.00%", LEVEL_TITLE),
    ]
    registry = build_registry(reads, settings=settings())
    coverage = {entry.venue: entry for entry in registry.coverage}

    assert coverage["kalshi"].readable == 1
    assert coverage["kalshi"].refused == 1
    assert coverage["kalshi"].supplied == 2
    assert coverage["kalshi"].reasons == ((REASON_TEXT_UNREADABLE, 1),)
    assert coverage["alternate_venue"].readable == 1
    assert coverage["alternate_venue"].refused == 0
    assert registry.reasons_by_contract() == {"kalshi|KXFED-X": REASON_TEXT_UNREADABLE}


def test_an_undeclared_venue_is_refused_rather_than_dropped() -> None:
    """A candidate the configuration does not declare is refused, not silently ignored."""
    other = VenueDeclaration(
        venue="other_venue",
        payout_text_fields=("yes_sub_title", "title"),
        parser=PARSER_DECLARED_POLICY_FORMS,
        documentation_verified=False,
    )
    read = read_at(other, "X-1", "Above 3.00%", LEVEL_TITLE)
    with pytest.raises(ValueError, match="does not declare"):
        build_registry([read], settings=settings())


def test_the_registry_digest_moves_with_the_pairs() -> None:
    """The digest identifies the pairs, and an unchanged registry keeps its digest."""
    reads = [
        kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE),
        alternate_read("ALT-C3.00", "Above 3.00%", LEVEL_TITLE),
    ]
    first = build_registry(reads, settings=settings())
    again = build_registry(reads, settings=settings())
    assert first.as_dict()["digest"] == again.as_dict()["digest"]
    assert registry_digest(first) == registry_digest(again)

    changed = build_registry(
        [
            reads[0],
            alternate_read(
                "ALT-C3.25", "Above 3.25%", "Will the target federal funds rate be above 3.25%?"
            ),
        ],
        settings=settings(),
    )
    assert changed.as_dict()["digest"] != first.as_dict()["digest"]
    assert changed.grade_for("KXFED-A3.00", "ALT-C3.25") is MatchGrade.APPROXIMATE
    assert first.version == REGISTRY_VERSION
    assert json.loads(json.dumps(first.as_dict()))["pairs"] == first.as_dict()["pairs"]


def test_an_unverified_rule_is_not_a_verified_match() -> None:
    """A predicate-identical pair is a grade; a *verified* match needs the rule window.

    No synthetic predicate here carries an attested rule version, so the verified set
    is empty although the EXACT set is not. The two claims stay separate, which is how
    the exposure graph's own rule-vintage requirement is read here.
    """
    reads = [
        kalshi_read("KXFED-A3.00", "Above 3.00%", LEVEL_TITLE),
        alternate_read("ALT-C3.00", "Above 3.00%", LEVEL_TITLE),
    ]
    registry = build_registry(reads, settings=settings())
    assert len(registry.exact_pairs()) == 1
    assert (
        registry.verified_matches(
            dt.datetime(2025, 1, 1, tzinfo=dt.UTC), dt.datetime(2025, 2, 1, tzinfo=dt.UTC)
        )
        == ()
    )
    assert registry.as_dict()["rule_vintage"]["reason"].startswith("the rule-vintage requirement")
