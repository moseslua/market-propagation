"""Tests for the Polymarket payout-predicate grammar.

Every piece of venue text in this file is verbatim, fetched from
``https://gamma-api.polymarket.com/public-search`` on 2026-09-17, with the
``condition_id`` the venue itself returned. The descriptions are reproduced in full
rather than excerpted, because the paragraph structure is exactly what the
resolution-sentence reader and the meeting reader depend on, and a paraphrase would
test a sentence this grammar will never see.

Two cases carry the weight.

The first is the March 2024 50+ market: the venue's slug says ``...-by-50-bps-...``
while its own rule says "50 *or more*". A grammar that read the slug would read a
strike that is wrong in the direction that matters, so the slug test at the end of
this file refuses a record whose only readable field *is* the slug. Check that test
first if this module is ever changed.

The second is the horizon. The study's declared calendar begins at 2024-11-07, so
March 2024 is a month the declaration does not date, and the March candidates still
refuse -- with ``contract_month_is_not_dated_by_the_declared_calendar`` rather than
with the vault-wide ``venue_payout_text_has_no_parser_declared_in_this_repository``.
That is the difference this module makes: the refusal now names the fact that is
actually missing.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from market_propagation.ingest.policy_predicates import (
    RATE_TARGET_CHANGE_BPS,
    RATE_TARGET_UPPER_BOUND,
    REASON_MONTH_NOT_IN_CALENDAR,
    REASON_TEXT_UNREADABLE,
    YES_AXIS_CHANGE,
    YES_AXIS_LEVEL,
    PredicateError,
)
from market_propagation.ingest.polymarket_predicates import (
    REASON_MEETING_UNREADABLE,
    REASON_MEETINGS_DISAGREE,
    REASON_RESOLUTION_BASIS_UNREADABLE,
    REASON_SETTLEMENT_SUBJECT_UNREADABLE,
    REASON_THRESHOLDS_DISAGREE,
    REASON_YES_SIDE_UNREADABLE,
    REASON_YES_SIDES_DISAGREE,
    SETTLEMENT_BASIS_CASH_AT_LISTED_PAYOUT,
    parse_polymarket_predicate,
    study_declared_calendar,
)
from market_propagation.matching import UNIT_BASIS_POINTS, UNIT_PERCENT

#: A calendar that dates every meeting these fixtures state, standing in for a
#: declaration that reached back to 2024. The real declaration is exercised separately,
#: because the interesting fact about it is which months it does *not* date.
DATED = {
    (2024, 1): dt.date(2024, 1, 31),
    (2024, 3): dt.date(2024, 3, 20),
    (2025, 9): dt.date(2025, 9, 17),
    (2026, 12): dt.date(2026, 12, 9),
}


MARCH_50_PLUS = {
    "slug": "will-the-fed-decrease-interest-rates-by-50-bps-after-its-march-2024-meeting",
    "question": "Will the Fed decrease interest rates by 50+ bps after its March 2024 meeting?",
    "group_item_title": "50+ bps decrease",
    "condition_id": "0xd4a957e7b51fc2e74c4f1909583972011772eda01aada975251aeaffbbc73f56",
    "end_date": "2024-03-18T00:00:00Z",
    "event_title": "Fed Interest Rates: March 2024",
    "description": (
        'The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to “Yes” if following the Federal Reserve\'s March 2024 meeting the upper bound of the target federal funds rate is decreased by 50 or more basis points below the level it was prior to the meeting. Otherwise, it will resolve to “No.”\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for March 19 - 20, 2024 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their March meeting with relevant data is issued. If no statement is released by March 31, 2024, 11:59 PM ET, this market will resolve to the "No change" bracket.'
    ),
}

MARCH_25 = {
    "slug": "will-the-fed-decrease-interest-rates-by-25-bps-after-its-2024-march-meeting",
    "question": "Will the Fed decrease interest rates by 25 bps after its 2024 March meeting?",
    "group_item_title": "25 bps decrease",
    "condition_id": "0x70ee5e1d18a794f36d3c50b7215bd203e00f702d71fa0f0014466f98afe35a09",
    "end_date": "2024-03-18T00:00:00Z",
    "event_title": "Fed Interest Rates: March 2024",
    "description": (
        'The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to “Yes” if following the Federal Reserve\'s March 2024 meeting the upper bound of the target federal funds rate is decreased by exactly 25 basis points below the level it was prior to the meeting. Otherwise, it will resolve to “No.”\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for March 19 - 20, 2024 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their March meeting with relevant data is issued. If no statement is released by March 31, 2024, 11:59 PM ET, this market will resolve to the "No change" bracket.'
    ),
}

MARCH_NO_CHANGE = {
    "slug": "no-change-in-fed-raise-interest-rates-after-its-2024-march-meeting",
    "question": "No change in Fed raise interest rates after its 2024 March meeting?",
    "group_item_title": "No change",
    "condition_id": "0xa1d06c273030c75b85ecfee08b2ff1f0e510c07f12152f81c707eb83deaab8ba",
    "end_date": "2024-03-18T00:00:00Z",
    "event_title": "Fed Interest Rates: March 2024",
    "description": (
        'The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to “Yes” if following the Federal Reserve\'s March 2024 meeting the upper bound of the target federal funds rate is exactly the same as the level it was prior to the meeting (namely it increased 0 bps). Otherwise, it will resolve to “No.”\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for March 19 - 20, 2024 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their March meeting with relevant data is issued. If no statement is released by March 31, 2024, 11:59 PM ET, this market will resolve to the "No change" bracket.'
    ),
}

MARCH_25_PLUS_INCREASE = {
    "slug": "will-the-fed-raise-interest-rates-by-25-bps-after-its-2024-march-meeting",
    "question": "Will the Fed raise interest rates by 25+ bps after its 2024 March meeting?",
    "group_item_title": "25+ bps increase",
    "condition_id": "0xdf3b361f2617570e0abc10ece5e350a979c5e1e58ad5a2b5147e023ade4d9311",
    "end_date": "2024-03-18T00:00:00Z",
    "event_title": "Fed Interest Rates: March 2024",
    "description": (
        'The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to “Yes” if following the Federal Reserve\'s March 2024 meeting the upper bound of the target federal funds rate is increased by 25 or more basis points above the level it was prior to the meeting. Otherwise, it will resolve to “No.”\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for March 19 - 20, 2024 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their March meeting with relevant data is issued. If no statement is released by March 31, 2024, 11:59 PM ET, this market will resolve to the "No change" bracket.'
    ),
}

JANUARY_25 = {
    "slug": "will-the-fed-decrease-interest-rates-by-25-bps-after-its-january-meeting",
    "question": "Will the Fed decrease interest rates by 25 bps after its January meeting?",
    "group_item_title": "25 bps decrease?",
    "condition_id": "0x0b7c6545b913133ccde713feb3599cae65325030bbe3e1bdf99943e5f5ef3464",
    "end_date": "2024-01-31T00:00:00Z",
    "event_title": "Fed Interest Rates: January 2024",
    "description": (
        "The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to “Yes” if following the Federal Reserve's January 2024 meeting the upper bound of the target federal funds rate is decreased exactly 25 basis points below the level it was prior to the meeting. Otherwise, it will resolve to “No.”\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for January 30 - 31, 2024 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their December meeting with relevant data is issued. If no statement is released by February 7, 2024, 11:59 PM ET, this market will resolve 50-50."
    ),
}

JANUARY_NO_CHANGE = {
    "slug": "will-the-fed-raise-interest-rates-by-0-bps-after-its-january-meeting",
    "question": "Will the Fed raise interest rates by 0 bps after its January meeting?",
    "group_item_title": "0 bps increase?",
    "condition_id": "0x95153ddd005326dc5dcadbeccc51ffad40e0fa9dac5bd18b242b42eed61d3a6c",
    "end_date": "2024-01-31T00:00:00Z",
    "event_title": "Fed Interest Rates: January 2024",
    "description": (
        "The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to “Yes” if following the Federal Reserve's January 2024 meeting the upper bound of the target federal funds rate is exactly the same as the level it was prior to the meeting (namely it increased 0 bps). Otherwise, it will resolve to “No.”\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for January 30 - 31, 2024 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their December meeting with relevant data is issued. If no statement is released by February 7, 2024, 11:59 PM ET, this market will resolve 50-50."
    ),
}

SEPTEMBER_2025_50_PLUS = {
    "slug": "fed-decreases-interest-rates-by-50-bps-after-september-2025-meeting",
    "question": "Fed decreases interest rates by 50+ bps after September 2025 meeting?",
    "group_item_title": "50+ bps decrease",
    "condition_id": "0x6c3d5fba5442f3e725eb89b7cffcc341ca3105cddaac09c1e4bc8e7010f0fe40",
    "end_date": "2025-09-17T12:00:00Z",
    "event_title": "Fed decision in September?",
    "description": (
        "The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to the amount of basis points the upper bound of the target federal funds rate is changed by versus the level it was prior to the Federal Reserve's September 2025 meeting.\n\nIf the target federal funds rate is changed to a level not expressed in the displayed options, the change will be rounded up to the nearest 25 and will resolve to the relevant bracket. (e.g. if there's a cut/increase of 12.5 bps it will be considered to be 25 bps)\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for September 16 - 17, 2025 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their September meeting with relevant data is issued. If no statement is released by the end date of the next scheduled meeting, this market will resolve to the \"No change\" bracket."
    ),
}

SEPTEMBER_2025_NO_CHANGE = {
    "slug": "no-change-in-fed-interest-rates-after-september-2025-meeting",
    "question": "No change in Fed interest rates after September 2025 meeting?",
    "group_item_title": "No change",
    "condition_id": "0x02dc46f4354c1cc447577db82ba62cca1bda737bd67b73fe966ce1bfe580868f",
    "end_date": "2025-09-17T12:00:00Z",
    "event_title": "Fed decision in September?",
    "description": (
        "The FED interest rates are defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve to the amount of basis points the upper bound of the target federal funds rate is changed by versus the level it was prior to the Federal Reserve's September 2025 meeting.\n\nIf the target federal funds rate is changed to a level not expressed in the displayed options, the change will be rounded up to the nearest 25 and will resolve to the relevant bracket. (e.g. if there's a cut/increase of 12.5 bps it will be considered to be 25 bps)\n\nThe resolution source for this market is the FOMC\u2019s statement after its meeting scheduled for September 16 - 17, 2025 according to the official calendar: https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm.\n\nThe level and change of the target federal funds rate is also published at the official website of the Federal Reserve at https://www.federalreserve.gov/monetarypolicy/openmarket.htm.\n\nThis market may resolve as soon as the FOMC\u2019s statement for their September meeting with relevant data is issued. If no statement is released by the end date of the next scheduled meeting, this market will resolve to the \"No change\" bracket."
    ),
}

LEVEL_DECEMBER_2026 = {
    "slug": "will-the-upper-bound-of-the-target-federal-funds-rate-be-2pt25-at-the-end-of-2026-971",
    "question": "Will the upper bound of the target federal funds rate be 2.25% at the end of 2026?",
    "group_item_title": "2.25%",
    "condition_id": "0xcefba5a5ca20b0fee51103c05388828b138cc2199a36fe5a7702049dbc9e5222",
    "end_date": "2026-12-09T00:00:00Z",
    "event_title": "What will the Fed rate be at the end of 2026?",
    "description": (
        "The FED rate is defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve according to the upper bound of the Federal Reserve\u2019s target federal funds range after the December 2026 Federal Open Market Committee (FOMC) meeting, currently scheduled for December 8-9, 2026.\n\nThis market may resolve immediately after the statement for the FOMC\u2019s December meeting, with relevant information about the FOMC\u2019s decision on the target federal funds range, has been issued. If no FOMC decision on the target federal funds range for their December meeting has been issued by December 31, 2026, 11:59 PM ET, this market will resolve according to the upper bound of the target federal funds range at that time.\n\nThe upper bound of the target federal funds range will be rounded to the nearest 25 basis points for resolution of this market. If the upper bound of the target federal funds range falls exactly between two listed options, it will be rounded away from zero (e.g. if the upper bound is 2.875, with listed options of 3.0 & 2.75, this market will resolve to 3.0).\n\nThe primary resolution source for this market will be official information from the Federal Reserve (https://www.federalreserve.gov/monetarypolicy/openmarket.htm)."
    ),
}


LEVEL_HIGH_2026 = {
    "slug": "will-the-upper-bound-of-the-target-federal-funds-rate-be-4pt5-at-the-end-of-2026-139",
    "question": "Will the upper bound of the target federal funds rate be ≥ 4.5% at the end of 2026?",
    "group_item_title": "≥ 4.5%",
    "condition_id": "0x3d20f26deb9b9cc7e24e5e06c10234a722d93bac095ce1105c59b44b503078d7",
    "end_date": "2026-12-09T00:00:00Z",
    "event_title": "What will the Fed rate be at the end of 2026?",
    "description": (
        "The FED rate is defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve according to the upper bound of the Federal Reserve\u2019s target federal funds range after the December 2026 Federal Open Market Committee (FOMC) meeting, currently scheduled for December 8-9, 2026.\n\nThis market may resolve immediately after the statement for the FOMC\u2019s December meeting, with relevant information about the FOMC\u2019s decision on the target federal funds range, has been issued. If no FOMC decision on the target federal funds range for their December meeting has been issued by December 31, 2026, 11:59 PM ET, this market will resolve according to the upper bound of the target federal funds range at that time.\n\nThe upper bound of the target federal funds range will be rounded to the nearest 25 basis points for resolution of this market. If the upper bound of the target federal funds range falls exactly between two listed options, it will be rounded away from zero (e.g. if the upper bound is 2.875, with listed options of 3.0 & 2.75, this market will resolve to 3.0).\n\nThe primary resolution source for this market will be official information from the Federal Reserve (https://www.federalreserve.gov/monetarypolicy/openmarket.htm)."
    ),
}

LEVEL_LOW_2026 = {
    "slug": "will-the-upper-bound-of-the-target-federal-funds-rate-be-1pt0-at-the-end-of-2026-434",
    "question": "Will the upper bound of the target federal funds rate be ≤1.0% at the end of 2026?",
    "group_item_title": "≤1.0%",
    "condition_id": "0x857a2ae8088bc256184d6684adfdc7832d072e526c1e8f0d218c1160770c20e9",
    "end_date": "2026-12-09T00:00:00Z",
    "event_title": "What will the Fed rate be at the end of 2026?",
    "description": (
        "The FED rate is defined in this market by the upper bound of the target federal funds range. The decisions on the target federal fund range are made by the Federal Open Market Committee (FOMC) meetings.\n\nThis market will resolve according to the upper bound of the Federal Reserve\u2019s target federal funds range after the December 2026 Federal Open Market Committee (FOMC) meeting, currently scheduled for December 8-9, 2026.\n\nThis market may resolve immediately after the statement for the FOMC\u2019s December meeting, with relevant information about the FOMC\u2019s decision on the target federal funds range, has been issued. If no FOMC decision on the target federal funds range for their December meeting has been issued by December 31, 2026, 11:59 PM ET, this market will resolve according to the upper bound of the target federal funds range at that time.\n\nThe upper bound of the target federal funds range will be rounded to the nearest 25 basis points for resolution of this market. If the upper bound of the target federal funds range falls exactly between two listed options, it will be rounded away from zero (e.g. if the upper bound is 2.875, with listed options of 3.0 & 2.75, this market will resolve to 3.0).\n\nThe primary resolution source for this market will be official information from the Federal Reserve (https://www.federalreserve.gov/monetarypolicy/openmarket.htm)."
    ),
}


def read(market: dict, *, calendar=DATED):
    return parse_polymarket_predicate(
        question=market["question"],
        description=market["description"],
        group_item_title=market["group_item_title"],
        market_slug=market["slug"],
        calendar=calendar,
    )


def refusal(market: dict, *, calendar=DATED, **overrides):
    arguments = {
        "question": market["question"],
        "description": market["description"],
        "group_item_title": market["group_item_title"],
        "market_slug": market["slug"],
        "calendar": calendar,
    }
    arguments.update(overrides)
    with pytest.raises(PredicateError) as raised:
        parse_polymarket_predicate(**arguments)
    return raised.value


def test_the_march_2024_50_plus_market_reads_a_strict_decrease():
    """The venue's own rule for the market whose slug says "50 bps" says "50 or more"."""
    parsed = read(MARCH_50_PLUS)

    assert parsed.underlying_event == RATE_TARGET_CHANGE_BPS
    assert parsed.yes_axis == YES_AXIS_CHANGE
    assert parsed.orientation == 1
    assert parsed.threshold == Decimal(-50)
    assert parsed.threshold_unit == UNIT_BASIS_POINTS
    assert parsed.operator == "below"
    assert parsed.reference_period == "2024-03"
    assert parsed.reference_horizon == dt.date(2024, 3, 20)
    assert parsed.settlement_criterion.basis == SETTLEMENT_BASIS_CASH_AT_LISTED_PAYOUT
    assert parsed.settlement_criterion.vintage == "first_release"


def test_the_25_bps_decrease_states_an_exact_change_and_no_direction():
    """An exact stated change is recorded with no direction rather than a fabricated one."""
    parsed = read(MARCH_25)

    assert parsed.threshold == Decimal(-25)
    assert parsed.operator is None
    assert parsed.underlying_event == RATE_TARGET_CHANGE_BPS
    assert "is decreased by exactly 25 basis points" in parsed.threshold_text


def test_the_no_change_market_states_a_zero_change():
    parsed = read(MARCH_NO_CHANGE)

    assert parsed.threshold == Decimal(0)
    assert parsed.operator is None
    assert parsed.underlying_event == RATE_TARGET_CHANGE_BPS
    # The yes side prices the stated change itself, and a zero change states its own
    # direction, so the change family carries +1 in this branch too.
    assert parsed.orientation == 1
    assert parsed.reference_period == "2024-03"


def test_an_increase_is_positive_and_a_strict_one_carries_a_direction():
    parsed = read(MARCH_25_PLUS_INCREASE)

    assert parsed.threshold == Decimal(25)
    assert parsed.operator == "above"
    assert parsed.orientation == 1
    # The description states "increased by 25 or more basis points"; the question states
    # "raise interest rates by 25+ bps"; the two agree, so the reading stands.
    assert "or more" in parsed.threshold_text


def test_a_description_that_states_the_bracket_form_reads_its_number_from_the_question():
    """The September 2025 rule states no number at all; the question and title do."""
    parsed = read(SEPTEMBER_2025_50_PLUS)

    assert parsed.threshold == Decimal(-50)
    assert parsed.operator == "below"
    assert parsed.reference_period == "2025-09"
    assert parsed.reference_horizon == dt.date(2025, 9, 17)


def test_the_level_family_reads_the_high_side_as_orientation_plus_one():
    """The venue states the side with a sign: "≥ 4.5%" prices the high side."""
    parsed = read(LEVEL_HIGH_2026)

    assert parsed.underlying_event == RATE_TARGET_UPPER_BOUND
    assert parsed.yes_axis == YES_AXIS_LEVEL
    assert parsed.threshold == Decimal("4.5")
    assert parsed.threshold_unit == UNIT_PERCENT
    assert parsed.operator is None
    assert parsed.orientation == 1
    assert parsed.reference_period == "2026-12"
    assert parsed.reference_horizon == dt.date(2026, 12, 9)


def test_the_level_family_reads_the_low_side_as_orientation_minus_one():
    """The same event's complement: "≤1.0%" prices the low side, and is not a match."""
    parsed = read(LEVEL_LOW_2026)

    assert parsed.underlying_event == RATE_TARGET_UPPER_BOUND
    assert parsed.yes_axis == YES_AXIS_LEVEL
    assert parsed.threshold == Decimal("1.0")
    assert parsed.orientation == -1
    # The two sides share an event, an axis and a period, so orientation is the only
    # component that keeps one from being matched against the other.
    assert read(LEVEL_HIGH_2026).orientation != parsed.orientation


def test_a_level_market_that_states_a_rate_but_no_side_is_refused():
    """The equality form prices an exact level; it states no side, so no side is invented.

    Every market in this event shares one description, and that description states no
    side. The side-stating markets state theirs in the question and the title, so a
    market that states its rate without a side anywhere is a finding: it is refused
    rather than read as a high or a low side, because a direction defaulted here would
    let the contract be matched against its own complement.
    """
    refused = refusal(LEVEL_DECEMBER_2026)

    assert refused.reason == REASON_YES_SIDE_UNREADABLE
    assert "2.25" in refused.detail
    assert "complement" in refused.detail


def test_a_level_market_that_states_its_side_in_words_reads_that_side():
    """ "be above 2.5%" and "2.5% or lower" are the two sides in prose rather than a sign."""
    base = dict(LEVEL_DECEMBER_2026)
    high = read(
        base
        | {
            "question": "Will the upper bound of the target federal funds rate be above 2.5% "
            "at the end of 2026?",
            "group_item_title": "above 2.5%",
        }
    )
    low = read(
        base
        | {
            "question": "Will the upper bound of the target federal funds rate be 2.5% or lower "
            "at the end of 2026?",
            "group_item_title": "2.5%",
        }
    )

    assert high.orientation == 1
    assert low.orientation == -1
    # The question alone states the side for the low one; the title states only the rate and
    # is silent about the side, which is not a contradiction.
    assert high.threshold == low.threshold == Decimal("2.5")


def test_two_level_fields_that_state_different_sides_are_refused():
    """A question on the high side and a title on the low side is a contradiction."""
    refused = refusal(LEVEL_HIGH_2026, group_item_title="\u2264 4.5%")

    assert refused.reason == REASON_YES_SIDES_DISAGREE
    assert "high side" in refused.detail
    assert "low side" in refused.detail


def test_every_component_carries_the_venue_text_it_was_read_from():
    parsed = read(MARCH_50_PLUS)

    for text in (
        parsed.underlying_event_text,
        parsed.threshold_text,
        parsed.reference_period_text,
        parsed.settlement_criterion_text,
        parsed.reference_horizon_text,
    ):
        assert text and text.strip()
    assert parsed.underlying_event_text in MARCH_50_PLUS["description"]
    assert parsed.threshold_text in MARCH_50_PLUS["description"]
    assert parsed.reference_period_text in MARCH_50_PLUS["description"]
    assert parsed.settlement_criterion_text in MARCH_50_PLUS["description"]
    assert parsed.as_dict()["threshold"] == "-50"
    assert parsed.as_dict()["reference_horizon"] == "2024-03-20"


def test_a_question_and_a_group_item_title_that_state_the_same_threshold_agree():
    """Three fields state "-25" and the agreement is what admits the reading."""
    parsed = read(MARCH_25)

    assert parsed.threshold == Decimal(-25)
    assert "25 basis points" in parsed.threshold_text
    assert MARCH_25["question"].startswith("Will the Fed decrease interest rates by 25 bps")
    assert MARCH_25["group_item_title"] == "25 bps decrease"


def test_fields_that_state_different_thresholds_are_refused_rather_than_resolved():
    """A disagreement is refused by name instead of picking the convenient field."""
    with pytest.raises(PredicateError) as raised:
        parse_polymarket_predicate(
            question="Will the Fed decrease interest rates by 50+ bps after its March 2024 meeting?",
            description=MARCH_25["description"],
            group_item_title="50+ bps decrease",
            market_slug=MARCH_25["slug"],
            calendar=DATED,
        )

    assert raised.value.reason == REASON_THRESHOLDS_DISAGREE
    # Both stated values are named, so a reader sees which two facts conflicted.
    assert "-25" in raised.value.detail
    assert "-50" in raised.value.detail


def test_a_question_that_states_a_bare_month_states_no_reference_period():
    """The January markets say "after its January meeting" and state no year."""
    parsed = read(JANUARY_25)

    assert parsed.reference_period == "2024-01"
    # The year comes from the description ("the Federal Reserve's January 2024 meeting"),
    # not from the question, which states the month alone.
    assert "January 2024" in parsed.reference_period_text
    assert "January 2024" not in JANUARY_25["question"]


def test_a_description_that_states_no_resolution_basis_is_refused_by_name():
    sentinel = MARCH_50_PLUS["description"].split("The resolution source")[0]
    refused = refusal(MARCH_50_PLUS, description=sentinel)

    assert refused.reason == REASON_RESOLUTION_BASIS_UNREADABLE
    assert "resolution source" in refused.detail


def test_a_month_the_declared_calendar_does_not_date_is_refused():
    """The study's real declaration begins at 2024-11-07, so March 2024 is undated."""
    assert study_declared_calendar().get((2024, 3)) is None

    with pytest.raises(PredicateError) as raised:
        parse_polymarket_predicate(
            question=MARCH_50_PLUS["question"],
            description=MARCH_50_PLUS["description"],
            group_item_title=MARCH_50_PLUS["group_item_title"],
            market_slug=MARCH_50_PLUS["slug"],
        )

    assert raised.value.reason == REASON_MONTH_NOT_IN_CALENDAR
    assert "2024-03" in raised.value.detail
    # The refusal names the missing declaration, not the venue's payout text.
    assert "declared calendar dates no meeting" in raised.value.detail


def test_a_month_the_declared_calendar_does_date_resolves_through_it():
    """September 2025 is declared, so no calendar is supplied and the study's is read."""
    parsed = parse_polymarket_predicate(
        question=SEPTEMBER_2025_50_PLUS["question"],
        description=SEPTEMBER_2025_50_PLUS["description"],
        group_item_title=SEPTEMBER_2025_50_PLUS["group_item_title"],
        market_slug=SEPTEMBER_2025_50_PLUS["slug"],
    )

    assert parsed.reference_period == "2025-09"
    assert parsed.reference_horizon == study_declared_calendar()[(2025, 9)]
    assert parsed.reference_horizon == dt.date(2025, 9, 17)


def test_a_description_that_names_no_governed_rate_is_refused_by_name():
    refused = refusal(
        MARCH_50_PLUS,
        description="This market resolves to Yes if it rains in March 2024.",
    )

    assert refused.reason == REASON_SETTLEMENT_SUBJECT_UNREADABLE


def test_a_meeting_the_fields_disagree_on_is_refused_rather_than_resolved():
    refused = refusal(MARCH_50_PLUS, question=MARCH_50_PLUS["question"].replace("March", "May"))

    assert refused.reason == REASON_MEETINGS_DISAGREE
    assert "2024-03" in refused.detail
    assert "2024-05" in refused.detail


def test_a_record_stating_no_meeting_at_all_states_no_reference_period():
    """A bare month names no period a calendar could date, and no year is inferred."""
    refused = refusal(
        MARCH_50_PLUS,
        question="Will the Fed decrease interest rates by 50+ bps after its meeting?",
        description=MARCH_50_PLUS["description"].replace(
            "Federal Reserve's March 2024 meeting", "Federal Reserve's March meeting"
        ),
    )

    assert refused.reason == REASON_MEETING_UNREADABLE
    assert "month and a year" in refused.detail


def test_a_record_stating_no_threshold_at_all_is_refused_rather_than_defaulted():
    """A bracket rule with no question and no title states no strike, and none is invented."""
    refused = refusal(
        SEPTEMBER_2025_50_PLUS,
        question="Will the Fed change rates after September 2025 meeting?",
        group_item_title=None,
    )

    assert refused.reason == REASON_TEXT_UNREADABLE
    assert "not defaulted to zero" in refused.detail


def test_the_slug_is_never_a_component_source():
    """The load-bearing test: a slug that states everything still reads as nothing.

    ``will-the-fed-decrease-interest-rates-by-25-bps-after-its-march-2024-meeting``
    states a strike and a meeting plainly. This record's question and description state
    neither, so every component is unreadable and the record is refused. If this
    module ever consulted the slug it would return a predicate here, and this assertion
    is what would fail.
    """
    refused = refusal(
        MARCH_25,
        question="",
        description="",
        group_item_title=None,
        market_slug="will-the-fed-decrease-interest-rates-by-25-bps-after-its-march-2024-meeting",
    )

    assert refused.reason == REASON_SETTLEMENT_SUBJECT_UNREADABLE


def test_a_slug_that_contradicts_the_rule_does_not_move_the_reading():
    """The March 50+ slug says "50 bps"; its rule says "50 or more". The rule wins.

    A grammar that read the slug would return a threshold of exactly -50 with no
    direction. This asserts the direction the rule states, which is the fact that
    distinguishes a strict extension from an exact change.
    """
    parsed = read(MARCH_50_PLUS)

    assert "by-50-bps" in MARCH_50_PLUS["slug"]
    assert "or more" in MARCH_50_PLUS["description"]
    assert parsed.threshold == Decimal(-50)
    assert parsed.operator == "below"
    assert parsed.operator_text == "or more"


def test_the_module_declares_every_reason_it_can_emit():
    """A refusal has to resolve to a declared code rather than to a bare string."""
    from market_propagation.ingest import polymarket_predicates

    assert REASON_MONTH_NOT_IN_CALENDAR in polymarket_predicates.REASONS
    assert REASON_TEXT_UNREADABLE in polymarket_predicates.REASONS
    assert polymarket_predicates.REASONS == (
        REASON_SETTLEMENT_SUBJECT_UNREADABLE,
        REASON_TEXT_UNREADABLE,
        REASON_THRESHOLDS_DISAGREE,
        REASON_MEETING_UNREADABLE,
        REASON_MEETINGS_DISAGREE,
        REASON_RESOLUTION_BASIS_UNREADABLE,
        REASON_YES_SIDE_UNREADABLE,
        REASON_YES_SIDES_DISAGREE,
        REASON_MONTH_NOT_IN_CALENDAR,
    )


def test_the_zero_change_market_reads_its_zero_from_the_rule_not_the_parenthetical():
    """The January no-change rule states "exactly the same ... (namely it increased 0 bps)".

    The parenthetical is a gloss on a zero change, not a second strike, and the reading
    has to come out as a single stated zero rather than as a disagreement between the
    "exactly the same" clause and the "increased 0 bps" one.
    """
    parsed = read(JANUARY_NO_CHANGE)

    assert parsed.threshold == Decimal(0)
    assert parsed.operator is None
    assert parsed.reference_period == "2024-01"
    assert "increased 0 bps" in JANUARY_NO_CHANGE["description"]


def test_a_no_change_market_whose_rule_states_the_bracket_form_reads_zero():
    """The September 2025 rule states no number at all; "No change" is stated twice over."""
    parsed = read(SEPTEMBER_2025_NO_CHANGE)

    assert parsed.threshold == Decimal(0)
    assert parsed.operator is None
    assert parsed.underlying_event == RATE_TARGET_CHANGE_BPS
    assert parsed.reference_period == "2025-09"
    assert parsed.reference_horizon == dt.date(2025, 9, 17)


def test_the_grammar_reads_every_market_in_the_march_2024_event_without_its_slug():
    """All four markets of one event read from their own text, with the slug passed but unused.

    Two of the four share the neighbour ``50`` and ``25`` so the numbers cannot be read
    off one another, and the event's fourth market states an increase rather than a
    decrease, so a sign read from the event title would have been wrong for it.
    """
    for market, threshold, operator in (
        (MARCH_50_PLUS, Decimal(-50), "below"),
        (MARCH_25, Decimal(-25), None),
        (MARCH_NO_CHANGE, Decimal(0), None),
        (MARCH_25_PLUS_INCREASE, Decimal(25), "above"),
    ):
        parsed = read(market)
        assert parsed.threshold == threshold, market["slug"]
        assert parsed.operator == operator, market["slug"]
        assert parsed.reference_period == "2024-03", market["slug"]
