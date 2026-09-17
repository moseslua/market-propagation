"""Polymarket payout predicates, read from the venue's own stated market text.

The second venue's cleaned local trade layer carries ``market_slug``,
``outcome_label``, ``condition_id`` and ``category``, and no settlement-rule text at
all, which is why the matching layer refuses every one of its candidates with
``venue_payout_text_has_no_parser_declared_in_this_repository``. That refusal is a
statement about *that layer*, not about the venue: the venue publishes each market's
own ``question``, ``description`` and ``groupItemTitle``, and both the ``question``
and the ``description`` state the claim's predicate verbatim. This module is the
grammar that reads those fields, so the refusal can become a reading.

Six components are read, from the venue's own stated text and from nothing else:

``underlying_event``
    The rate the claim resolves on and the axis its yes side prices, derived the way
    :mod:`market_propagation.ingest.policy_predicates` derives them. A description
    that resolves on the *change* the FOMC's statement makes ("the upper bound of the
    target federal funds rate is decreased by 50 or more basis points below the
    level it was prior to the meeting", or the bracket form "will resolve to the
    amount of basis points the upper bound ... is changed by") is the existing
    :data:`~market_propagation.ingest.policy_predicates.RATE_TARGET_CHANGE_BPS` on
    :data:`~market_propagation.ingest.policy_predicates.YES_AXIS_CHANGE`; one that
    resolves on the *level* ("will resolve according to the upper bound of the
    Federal Reserve's target federal funds range after the December 2026 ...
    meeting") is ``RATE_TARGET_UPPER_BOUND`` on ``YES_AXIS_LEVEL``. The two are never
    inferred from the wording of a slug, because the venue writes slugs like
    ``will-the-fed-decrease-interest-rates-by-50-bps-after-its-march-2024-meeting``
    for a market whose payout is "50 *or more*": the slug states ``50`` where the rule
    states ``50 or more``, and a reading taken from it would be wrong in the direction
    that matters.

``threshold``
    The stated strike as an exact ``Decimal``, signed, together with the unit the
    record states it in. A decrease is negative and an increase positive: "decreased
    by exactly 25 basis points" is ``-25``, "increased by 25 or more basis points" is
    ``+25``, and "exactly the same as the level it was prior to the meeting" is ``0``.

``operator``
    The comparison direction, or ``None`` when the change is stated exactly. This
    mirrors :mod:`~market_propagation.ingest.policy_predicates`: a stated extension
    ("50 or more", "50+") is a real inequality, and an exact stated change is recorded
    with no direction rather than with a fabricated one.

``orientation``
    Which side of the stated value the *yes* side prices, read from the venue's own
    text rather than derived from the axis. A change market's yes side is the stated
    change itself, whose sign is already in the signed threshold, so it is ``+1`` —
    the same value the policy grammar's change branches carry. A level market states
    its side in the question or the group item title ("\u2265 4.5%" and "≤1.0%" for
    one event's high and low sides), so the side is read from that field; a level
    market that states a rate but no side of it is **refused** with
    ``market_text_states_no_payoff_direction_for_the_yes_side`` rather than defaulted,
    because a level contract read as its own complement is the match this component
    exists to prevent.

``reference_period``
    The meeting's period as ``YYYY-MM``, read from the venue's own stated meeting
    ("after its March 2024 meeting", "after the December 2026 Federal Open Market
    Committee (FOMC) meeting", "prior to the Federal Reserve's September 2025
    meeting"). The ``question`` states the year for some markets and omits it for
    others ("after its January meeting"), so the period is read from whichever of the
    three fields states a month *and* a year, and no year is inferred from a bare
    month.

``settlement_criterion``
    The declared basis ``cash_settlement_at_the_listed_payout`` on the
    ``first_release`` vintage, which the description's own resolution wording states
    ("The resolution source for this market is the FOMC's statement after its meeting
    scheduled for ...", or "The primary resolution source for this market will be
    official information from the Federal Reserve").

``reference_horizon``
    The decision instant the claim resolves on, resolved through the study's declared
    calendar by :func:`market_propagation.cross_venue.declared_calendar`, exactly as
    the first venue's horizon already is. A month the declared calendar does not date
    is unplaceable, and the contract yields no predicate rather than a horizon read
    off the venue's own stated date. This is deliberate and it is why the March 2024
    candidates still refuse: the declared calendar begins at 2024-11-07, so the March
    2024 meeting is a month the study has not declared, and its window falls before
    anything a dated rule capture can bound.

Three rules this module enforces, because each is a way a graph quietly matches
contracts that are not comparable:

**A component the venue's text does not state is refused by name, never defaulted.**
A record whose text states no resolution basis, no meeting, or no readable threshold
yields no predicate, and the reason names the fact that could not be established.

**A field that states a threshold is compared against every other field that states
one.** The ``question``, the ``description`` and the ``groupItemTitle`` often state
the same strike three times over ("50+ bps decrease", "by 50+ bps", "decreased by 50
or more basis points"). Where they agree, the agreement is what makes the reading
trustworthy; where they disagree, the record is refused rather than resolved by
picking the field that reads more conveniently.

**The slug is never a component source.** ``market_slug`` is accepted by
:func:`parse_polymarket_predicate` for one purpose only: naming the contract in a
refusal message. It is never assigned to a local that the component readers see, the
readers take only the ``question``, ``description`` and ``group_item_title`` texts,
and the module holds no pattern that could read a predicate out of a slug. A record
whose three text fields are unreadable is refused even when its slug states the
strike and the meeting perfectly, because a slug is a name and this repository has
already declared (``configs/matching_v1.yaml``: ``title_text_is_a_lead_only``) that a
name is not evidence. The same rule is what makes the tests load-bearing: a grammar
that ever consulted the slug would read the March 2024 50+ market as a 50, and a
grammar that refuses it is the only one that can be checked for doing so.
"""

from __future__ import annotations

import datetime as dt
import functools
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from types import MappingProxyType
from typing import Any

from ..cross_venue import GRAPH_CONFIG_PATH, declared_calendar, load_config
from ..matching import (
    SETTLEMENT_VINTAGE_FIRST_RELEASE,
    UNIT_BASIS_POINTS,
    UNIT_PERCENT,
    SettlementCriterion,
)
from ..neighbors import INEQUALITIES
from .policy_predicates import (
    RATE_TARGET_CHANGE_BPS,
    RATE_TARGET_UPPER_BOUND,
    REASON_MONTH_NOT_IN_CALENDAR,
    REASON_TEXT_UNREADABLE,
    YES_AXIS_CHANGE,
    YES_AXIS_LEVEL,
    PredicateError,
    threshold_decimal,
)

__all__ = [
    "REASONS",
    "REASON_MEETINGS_DISAGREE",
    "REASON_MEETING_UNREADABLE",
    "REASON_RESOLUTION_BASIS_UNREADABLE",
    "REASON_SETTLEMENT_SUBJECT_UNREADABLE",
    "REASON_THRESHOLDS_DISAGREE",
    "REASON_YES_SIDES_DISAGREE",
    "REASON_YES_SIDE_UNREADABLE",
    "SETTLEMENT_BASIS_CASH_AT_LISTED_PAYOUT",
    "ParsedPolymarketPredicate",
    "parse_polymarket_predicate",
    "study_declared_calendar",
]

#: The payout basis the venue's own resolution wording states. A Polymarket market
#: pays a fixed cash amount at the listed price on the outcome its rule determines;
#: the string is the declared basis in ``configs/matching_v1.yaml``
#: (``settlement_criteria.basis``), and the vintage is the declared first print
#: because the FOMC's own statement is the figure the rule resolves on, not a revision
#: of it. Both are existing declarations rather than new ones: only the basis has no
#: Python constant elsewhere in the repository, so it is named here once.
SETTLEMENT_BASIS_CASH_AT_LISTED_PAYOUT = "cash_settlement_at_the_listed_payout"

#: Reasons a second-venue record yields no predicate. Each names one distinct fact,
#: so a blocked reading reports which one applied rather than one undifferentiated
#: rejection. ``published_contract_text_states_no_readable_payout`` and
#: ``contract_month_is_not_dated_by_the_declared_calendar`` are reused from the policy
#: grammar, so one unreadable contract has one name whichever venue stated it.
REASON_SETTLEMENT_SUBJECT_UNREADABLE = "description_states_no_readable_settlement_subject"
REASON_THRESHOLDS_DISAGREE = "stated_thresholds_disagree_across_the_market_record"
REASON_MEETING_UNREADABLE = "venue_text_states_no_readable_meeting_reference_period"
REASON_MEETINGS_DISAGREE = "stated_meeting_reference_periods_disagree_across_the_market_record"
REASON_RESOLUTION_BASIS_UNREADABLE = "description_states_no_resolution_basis"
REASON_YES_SIDE_UNREADABLE = "market_text_states_no_payoff_direction_for_the_yes_side"
REASON_YES_SIDES_DISAGREE = "stated_yes_sides_disagree_across_the_market_record"

#: The reason vocabulary this module can write, in the order it applies it.
REASONS: tuple[str, ...] = (
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

#: The unit each stated settlement subject's numbers are published in, which the
#: matching layer needs beside every threshold because a threshold without a unit is
#: not a comparison. The matching layer declares the same mapping for the definitions
#: it compares; it is restated here for the two this grammar can state rather than
#: reaching for that module's private name.
_UNIT_BY_DEFINITION: Mapping[str, str] = {
    RATE_TARGET_CHANGE_BPS: UNIT_BASIS_POINTS,
    RATE_TARGET_UPPER_BOUND: UNIT_PERCENT,
}

_MONTH_NUMBERS: Mapping[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

_MONTH_PATTERN = "(?:" + "|".join(_MONTH_NUMBERS) + ")"

#: The rate the claim resolves on, as the venue's description names it. The description
#: writes the range in one clause and the rate in another, and both are the target
#: funds bound this study governs.
_SUBJECT_RE = re.compile(
    r"upper bound of (?:the )?(?:federal reserve[\u2019']s? )?target federal funds (?:range|rate)",
    re.IGNORECASE,
)

#: The two forms the description uses to say what its yes side prices. The change form
#: is tested first because a change description also names the level it changes from.
_CHANGE_FORM_RE = re.compile(
    r"\bis (?:decreased|increased|changed)\b|amount of basis points|"
    r"exactly the same as the level",
    re.IGNORECASE,
)
_LEVEL_FORM_RE = re.compile(r"resolve(?:s)? (?:according to|to) the upper bound", re.IGNORECASE)

#: The description's own statement of the strike, in the two wordings the venue uses:
#: a signed change with an optional stated extension, and no change at all. The bracket
#: form ("will resolve to the amount of basis points ... is changed by") states no
#: number and is deliberately not matched here; its number is stated in the question
#: and the group item title, and is read from there.
_DESCRIPTION_CHANGE_RE = re.compile(
    r"\bis (?P<direction>decreased|increased)(?: by)?(?: exactly)? "
    r"(?P<bps>[0-9]+(?:\.[0-9]+)?)(?P<extension> or more|\+)? basis points",
    re.IGNORECASE,
)
_DESCRIPTION_NO_CHANGE_RE = re.compile(r"\bis exactly the same as the level", re.IGNORECASE)

#: The question's own statement of the strike. The direction word and the number are
#: both stated there, and the number may carry the "+" the venue uses for an extension.
_QUESTION_CHANGE_RE = re.compile(
    r"\b(?P<direction>decreas\w*|cut\w*|lower\w*|reduc\w*|increas\w*|raise\w*|hike\w*)\b"
    r"[^.?]{0,80}?\bby (?P<bps>[0-9]+(?:\.[0-9]+)?)(?P<extension>\+)? bps\b",
    re.IGNORECASE,
)
_QUESTION_NO_CHANGE_RE = re.compile(r"\bno change\b", re.IGNORECASE)

#: The group item title's own statement of the strike, in the four forms the venue
#: uses for a decision market. An unrecognized title contributes nothing rather than a
#: refusal: the title is one of three fields, and the other two still state the claim.
_GROUP_ITEM_CHANGE_RE = re.compile(
    r"^(?P<bps>[0-9]+(?:\.[0-9]+)?)(?P<extension>\+)? bps (?P<direction>decrease|increase)"
    r"\??$",
    re.IGNORECASE,
)
_GROUP_ITEM_NO_CHANGE_RE = re.compile(r"^no change\??$", re.IGNORECASE)

#: The level family's own statement of the strike, which is a rate rather than a change,
#: with the side of the level the yes side prices where the field states one. The venue
#: writes the high side as "above" / the "≥" sign / a trailing "or more" or "or higher",
#: the low side as "below" / "≤" / "or lower" or "or less", and a field that states only
#: the rate ("be 2.25%", "2.25%") states no side at all.
_WORD_SIDES: Mapping[str, int] = {
    "above": 1,
    "high": 1,
    "or more": 1,
    "or higher": 1,
    "below": -1,
    "low": -1,
    "or lower": -1,
    "or less": -1,
}
_SYMBOL_SIDES: Mapping[str, int] = {"\u2265": 1, "\u2264": -1}
_SIDE_WORD_PATTERN = "|".join(_WORD_SIDES)

_QUESTION_LEVEL_RE = re.compile(
    r"\b(?:be|hit|reach|stay)\s+(?:"
    r"(?P<side_word>" + _SIDE_WORD_PATTERN + r")\s+(?P<rate_word>[0-9]+(?:\.[0-9]+)?)\s*%"
    r"|(?P<side_symbol>\u2265|\u2264)\s*(?P<rate_symbol>[0-9]+(?:\.[0-9]+)?)\s*%"
    r"|(?P<rate_plain>[0-9]+(?:\.[0-9]+)?)\s*%(?:\s*(?P<side_trailing>"
    + _SIDE_WORD_PATTERN
    + r"))?"
    r")",
    re.IGNORECASE,
)
_GROUP_ITEM_LEVEL_RE = re.compile(
    r"^(?:"
    r"(?P<side_word>" + _SIDE_WORD_PATTERN + r")\s+(?P<rate_word>[0-9]+(?:\.[0-9]+)?)\s*%"
    r"|(?P<side_symbol>\u2265|\u2264)\s*(?P<rate_symbol>[0-9]+(?:\.[0-9]+)?)\s*%"
    r"|(?P<rate_plain>[0-9]+(?:\.[0-9]+)?)\s*%(?:\s*(?P<side_trailing>"
    + _SIDE_WORD_PATTERN
    + r"))?"
    r")$",
    re.IGNORECASE,
)

#: The meeting the claim resolves on, as the question and the description state it.
#: The month and the year both have to be stated: a bare month ("their March meeting",
#: "after its January meeting") names no period a calendar could date, and the year is
#: not inferred from the market's own listing window.
_MEETING_RE = re.compile(
    r"(?:(?P<month_first>" + _MONTH_PATTERN + r")\s+(?P<year_after>[0-9]{4})"
    r"|(?P<year_first>[0-9]{4})\s+(?P<month_after>" + _MONTH_PATTERN + r"))"
    r"[ ,]{0,2}(?:federal open market committee \(fomc\) )?meeting",
    re.IGNORECASE,
)

#: The description's own resolution wording, and the official source it names. The
#: sentence has to name a served official figure, because "this market resolves
#: somehow" is not a basis a payout can be computed from.
_RESOLUTION_SOURCE_RE = re.compile(r"resolution source", re.IGNORECASE)
_SERVED_STATEMENT_RE = re.compile(
    r"fomc[\u2019']?s? statement|official information from the federal reserve",
    re.IGNORECASE,
)

_PERIOD_RE = re.compile(r"^(?P<year>[0-9]{4})-(?P<month>0[1-9]|1[0-2])$")


@dataclass(frozen=True, slots=True)
class _ThresholdStatement:
    """One field's own statement of the strike, with the text it was read from.

    ``value`` is signed and ``strict`` states whether the field extends the threshold
    ("50 or more", "50+") or states it exactly. The two are compared separately because
    a disagreement about *how much* and a disagreement about *or more* are different
    facts, and neither is resolved by picking a field.

    ``yes_side`` is the side of the level the yes side prices, stated only by the level
    family: ``+1`` high, ``-1`` low, ``None`` when the field states a rate without a
    side. It is not part of :attr:`key`, because a field that is *silent* about the side
    does not contradict one that states it -- the venue's level markets share a
    description that states no side and a question that states one. Two fields that
    state *different* sides are a disagreement, and that is checked separately.
    """

    value: Decimal
    strict: bool
    marker_text: str | None
    text: str
    source: str
    yes_side: int | None = None

    @property
    def key(self) -> tuple[Decimal, bool]:
        return (self.value, self.strict)

    def render(self) -> str:
        bound = "a stated extension" if self.strict else "stated exactly"
        if self.yes_side is None:
            return f"{self.source} states {self.value} ({bound}) in {self.text!r}"
        side = "the high side" if self.yes_side > 0 else "the low side"
        return f"{self.source} states {self.value} on {side} ({bound}) in {self.text!r}"


@dataclass(frozen=True, slots=True)
class _PeriodStatement:
    """One field's own statement of the meeting, with the text it was read from."""

    year: int
    month: int
    text: str
    source: str

    @property
    def period(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def key(self) -> tuple[int, int]:
        return (self.year, self.month)

    def render(self) -> str:
        return f"{self.source} states {self.period} in {self.text!r}"


@dataclass(frozen=True, slots=True)
class ParsedPolymarketPredicate:
    """One market's payout predicate, with the venue text each component was read from.

    The text travels with every component so a reader can re-derive the reading
    instead of trusting a slug, a question, or this module. ``operator`` is ``None``
    when the change is stated exactly, which is a stated state and not an absence;
    ``threshold_unit`` travels beside ``threshold`` because a threshold without a unit
    is not a comparison.

    ``orientation`` is which side of the stated value the *yes* side prices, read from
    the venue's own text rather than derived from the axis. The change family always
    states its direction in the signed threshold, so its yes side is ``+1``; a level
    market states its side in the question or the group item title ("\u2265 4.5%",
    "above 2.5%") and is refused rather than defaulted when it states none.
    """

    underlying_event: str
    threshold: Decimal
    threshold_unit: str
    operator: str | None
    reference_period: str
    settlement_criterion: SettlementCriterion
    reference_horizon: dt.date
    yes_axis: str
    orientation: int
    underlying_event_text: str
    threshold_text: str
    operator_text: str | None
    reference_period_text: str
    settlement_criterion_text: str
    reference_horizon_text: str

    def __post_init__(self) -> None:
        if self.underlying_event not in _UNIT_BY_DEFINITION:
            raise ValueError(
                f"ParsedPolymarketPredicate.underlying_event must be one of "
                f"{sorted(_UNIT_BY_DEFINITION)}, got {self.underlying_event!r}"
            )
        if not isinstance(self.threshold, Decimal) or isinstance(self.threshold, float):
            raise TypeError(
                "ParsedPolymarketPredicate.threshold must be an exact Decimal; a binary float "
                "cannot represent a published strike exactly"
            )
        if self.threshold_unit not in (UNIT_BASIS_POINTS, UNIT_PERCENT):
            raise ValueError(
                f"ParsedPolymarketPredicate.threshold_unit must be one of "
                f"{(UNIT_BASIS_POINTS, UNIT_PERCENT)}, got {self.threshold_unit!r}"
            )
        if self.operator is not None and self.operator not in INEQUALITIES:
            raise ValueError(
                f"ParsedPolymarketPredicate.operator must be one of {INEQUALITIES} or None, got "
                f"{self.operator!r}"
            )
        if self.yes_axis not in (YES_AXIS_CHANGE, YES_AXIS_LEVEL):
            raise ValueError(
                f"ParsedPolymarketPredicate.yes_axis must be one of "
                f"{(YES_AXIS_CHANGE, YES_AXIS_LEVEL)}, got {self.yes_axis!r}"
            )
        if isinstance(self.orientation, bool) or self.orientation not in (-1, 1):
            raise ValueError(
                f"ParsedPolymarketPredicate.orientation must be +1 or -1, got {self.orientation!r}"
            )
        if _PERIOD_RE.match(self.reference_period) is None:
            raise ValueError(
                f"ParsedPolymarketPredicate.reference_period {self.reference_period!r} is not a "
                "canonical YYYY-MM period"
            )
        if not isinstance(self.reference_horizon, dt.date) or isinstance(
            self.reference_horizon, dt.datetime
        ):
            raise TypeError(
                "ParsedPolymarketPredicate.reference_horizon must be a date, got "
                f"{type(self.reference_horizon).__name__}"
            )
        if not isinstance(self.settlement_criterion, SettlementCriterion):
            raise TypeError(
                "ParsedPolymarketPredicate.settlement_criterion must be a SettlementCriterion, "
                f"got {type(self.settlement_criterion).__name__}"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "underlying_event": self.underlying_event,
            "threshold": str(self.threshold),
            "threshold_unit": self.threshold_unit,
            "operator": self.operator,
            "reference_period": self.reference_period,
            "settlement_criterion": self.settlement_criterion.as_dict(),
            "reference_horizon": self.reference_horizon.isoformat(),
            "yes_axis": self.yes_axis,
            "orientation": self.orientation,
            "underlying_event_text": self.underlying_event_text,
            "threshold_text": self.threshold_text,
            "operator_text": self.operator_text,
            "reference_period_text": self.reference_period_text,
            "settlement_criterion_text": self.settlement_criterion_text,
            "reference_horizon_text": self.reference_horizon_text,
        }


@functools.lru_cache(maxsize=1)
def study_declared_calendar() -> Mapping[tuple[int, int], dt.date]:
    """The study's declared decision calendar, as the month map a horizon needs.

    It is read through :mod:`market_propagation.cross_venue`, which is where the
    declaration lives, rather than through a second reader that could drift from it.
    The result is cached and read-only because the declaration is a property of the
    study and not of a request.
    """
    return MappingProxyType(declared_calendar(load_config(GRAPH_CONFIG_PATH)))


def _refuse(reason: str, detail: str) -> PredicateError:
    """A refusal carrying a declared reason code and the fact that could not be read."""
    return PredicateError(reason, detail)


def _contract_named(market_slug: str | None) -> str:
    """The contract as a refusal message names it, and the only use of a slug here.

    Nothing this function returns is ever a component. It exists so a blocked reading
    says which market it was about, and a slug that named a threshold would still
    reach no reader: the readers below take the three stated text fields directly.
    """
    return f"{market_slug!r}" if market_slug else "this market"


def _sign_of(direction: str) -> int:
    """The sign a stated direction word carries, refusing a word that states none."""
    lowered = direction.lower()
    if any(lowered.startswith(prefix) for prefix in ("decreas", "cut", "lower", "reduc")):
        return -1
    if any(lowered.startswith(prefix) for prefix in ("increas", "raise", "hike")):
        return 1
    raise _refuse(
        REASON_TEXT_UNREADABLE,
        f"the stated direction {direction!r} is neither an increase nor a decrease, so the sign "
        "of the stated change cannot be read from it",
    )


def _signed(direction: str, magnitude: Decimal) -> Decimal:
    """A signed change. A zero change is stated unsigned rather than as a negative zero."""
    if magnitude == 0:
        return Decimal(0)
    sign = _sign_of(direction)
    return magnitude if sign > 0 else -magnitude


def _level_rate_and_side(match: re.Match[str]) -> tuple[Decimal, int | None]:
    """The rate a level field states, and the side of it the yes side prices.

    The side comes from the field's own wording: a word ("above", "below", "or lower")
    or a sign ("\u2265", "\u2264"). A field that states only the rate states no side, and
    ``None`` is returned rather than a defaulted direction, because a level contract
    read as its own complement is exactly the match this component exists to prevent.
    """
    rate = match.group("rate_word") or match.group("rate_symbol") or match.group("rate_plain")
    value = threshold_decimal(rate)
    word = match.group("side_word") or match.group("side_trailing")
    if word is not None:
        return value, _WORD_SIDES[word.lower()]
    symbol = match.group("side_symbol")
    if symbol is not None:
        return value, _SYMBOL_SIDES[symbol]
    return value, None


def _threshold_statements(
    *, question: str, description: str, group_item_title: str | None, definition: str
) -> list[_ThresholdStatement]:
    """Every field's own statement of the strike, in the order this module trusts them.

    The description is read first because it is the market's rule rather than its
    title, then the question, then the group item title. All three are collected and
    compared, so a disagreement is visible instead of being hidden by the precedence.
    """
    statements: list[_ThresholdStatement] = []
    if definition == RATE_TARGET_CHANGE_BPS:
        match = _DESCRIPTION_CHANGE_RE.search(description)
        if match:
            statements.append(
                _ThresholdStatement(
                    value=_signed(match.group("direction"), threshold_decimal(match.group("bps"))),
                    strict=bool(match.group("extension")),
                    marker_text=(match.group("extension") or "").strip() or None,
                    text=match.group(0),
                    source="the market description",
                )
            )
        elif (no_change := _DESCRIPTION_NO_CHANGE_RE.search(description)) is not None:
            statements.append(
                _ThresholdStatement(
                    value=Decimal(0),
                    strict=False,
                    marker_text=None,
                    text=no_change.group(0),
                    source="the market description",
                )
            )
        match = _QUESTION_CHANGE_RE.search(question)
        if match:
            statements.append(
                _ThresholdStatement(
                    value=_signed(match.group("direction"), threshold_decimal(match.group("bps"))),
                    strict=bool(match.group("extension")),
                    marker_text=(match.group("extension") or "").strip() or None,
                    text=match.group(0),
                    source="the market question",
                )
            )
        elif (no_change := _QUESTION_NO_CHANGE_RE.search(question)) is not None:
            statements.append(
                _ThresholdStatement(
                    value=Decimal(0),
                    strict=False,
                    marker_text=None,
                    text=no_change.group(0),
                    source="the market question",
                )
            )
        if group_item_title is not None:
            match = _GROUP_ITEM_CHANGE_RE.match(group_item_title.strip())
            if match:
                statements.append(
                    _ThresholdStatement(
                        value=_signed(
                            match.group("direction"), threshold_decimal(match.group("bps"))
                        ),
                        strict=bool(match.group("extension")),
                        marker_text=(match.group("extension") or "").strip() or None,
                        text=group_item_title,
                        source="the group item title",
                    )
                )
            elif _GROUP_ITEM_NO_CHANGE_RE.match(group_item_title.strip()):
                statements.append(
                    _ThresholdStatement(
                        value=Decimal(0),
                        strict=False,
                        marker_text=None,
                        text=group_item_title,
                        source="the group item title",
                    )
                )
        return statements
    match = _QUESTION_LEVEL_RE.search(question)
    if match:
        rate, side = _level_rate_and_side(match)
        statements.append(
            _ThresholdStatement(
                value=rate,
                strict=False,
                marker_text=None,
                text=match.group(0),
                source="the market question",
                yes_side=side,
            )
        )
    if group_item_title is not None:
        match = _GROUP_ITEM_LEVEL_RE.match(group_item_title.strip())
        if match:
            rate, side = _level_rate_and_side(match)
            statements.append(
                _ThresholdStatement(
                    value=rate,
                    strict=False,
                    marker_text=None,
                    text=group_item_title,
                    source="the group item title",
                    yes_side=side,
                )
            )
    return statements


def _single_threshold(
    statements: list[_ThresholdStatement], *, market_slug: str | None
) -> _ThresholdStatement:
    """The one strike every stating field agrees on, or a refusal naming the disagreement."""
    if not statements:
        raise _refuse(
            REASON_TEXT_UNREADABLE,
            f"neither the question, the description nor the group item title of "
            f"{_contract_named(market_slug)} states a threshold this grammar reads, so the "
            "contract's boundary is not established and is not defaulted to zero",
        )
    keys = {statement.key for statement in statements}
    if len(keys) > 1:
        stated = "; ".join(statement.render() for statement in statements)
        raise _refuse(
            REASON_THRESHOLDS_DISAGREE,
            f"the fields of {_contract_named(market_slug)} state more than one threshold and this "
            f"grammar refuses to choose one: {stated}",
        )
    sides = {statement.yes_side for statement in statements if statement.yes_side is not None}
    if len(sides) > 1:
        stated = "; ".join(statement.render() for statement in statements)
        raise _refuse(
            REASON_YES_SIDES_DISAGREE,
            f"the fields of {_contract_named(market_slug)} state different sides of the same level "
            f"and this grammar refuses to choose one: {stated}",
        )
    if len(sides) == 1 and statements[0].yes_side is None:
        # One field stated the side and another was silent about it. The silence is not a
        # contradiction, so the agreed side travels on the record the reader returns.
        return replace(statements[0], yes_side=next(iter(sides)))
    return statements[0]


def _period_statements(*, question: str, description: str) -> list[_PeriodStatement]:
    """Every field's own statement of the meeting, each with the text it was read from."""
    statements: list[_PeriodStatement] = []
    for source, text in (
        ("the market description", description),
        ("the market question", question),
    ):
        for match in _MEETING_RE.finditer(text):
            month_name = (match.group("month_first") or match.group("month_after")).lower()
            year = match.group("year_after") or match.group("year_first")
            statements.append(
                _PeriodStatement(
                    year=int(year),
                    month=_MONTH_NUMBERS[month_name],
                    text=match.group(0),
                    source=source,
                )
            )
    return statements


def _single_period(
    statements: list[_PeriodStatement], *, market_slug: str | None
) -> _PeriodStatement:
    """The one meeting every stating field agrees on, or a refusal naming the disagreement."""
    if not statements:
        raise _refuse(
            REASON_MEETING_UNREADABLE,
            f"neither the question nor the description of {_contract_named(market_slug)} states a "
            "meeting as a month and a year, so the reference period is not established. A bare "
            "month names no period a calendar could date, and the market's own listing window "
            "is not the meeting it resolves on",
        )
    keys = {statement.key for statement in statements}
    if len(keys) > 1:
        stated = "; ".join(statement.render() for statement in statements)
        raise _refuse(
            REASON_MEETINGS_DISAGREE,
            f"the fields of {_contract_named(market_slug)} state more than one meeting and this "
            f"grammar refuses to choose one: {stated}",
        )
    return statements[0]


def _resolution_sentence(description: str) -> str:
    """The description's own sentence stating what the market resolves from, or a refusal.

    The sentence is read from the paragraph that carries it rather than by splitting on
    periods, because the resolution paragraph itself contains a URL whose dots would
    cut it in half.
    """
    for paragraph in description.split("\n\n"):
        if _RESOLUTION_SOURCE_RE.search(paragraph):
            return paragraph.strip()
    raise _refuse(
        REASON_RESOLUTION_BASIS_UNREADABLE,
        "the market description states no resolution source, so the basis its payout is computed "
        "from is not established and is not assumed from the listing venue",
    )


def parse_polymarket_predicate(
    *,
    question: str,
    description: str,
    group_item_title: str | None = None,
    market_slug: str | None = None,
    calendar: Mapping[tuple[int, int], dt.date] | None = None,
) -> ParsedPolymarketPredicate:
    """The payout predicate the venue's own market text states, or a refusal.

    ``question``, ``description`` and ``group_item_title`` are the three fields the
    venue states its claim in, and they are the only sources any component is read
    from. ``market_slug`` is accepted for one purpose — naming the market in a refusal
    message — and is never passed to a reader; a record whose three text fields are
    unreadable is refused even when its slug states the strike and the meeting exactly.

    ``calendar`` defaults to the study's declared calendar, which is what
    :func:`study_declared_calendar` reads, and it is the only source of the
    ``reference_horizon``: the venue's own stated date is never turned into a horizon.
    """
    question_text = str(question or "")
    description_text = str(description or "")
    title_text = None if group_item_title is None else str(group_item_title)

    subject = _SUBJECT_RE.search(description_text)
    if subject is None:
        raise _refuse(
            REASON_SETTLEMENT_SUBJECT_UNREADABLE,
            f"the description of {_contract_named(market_slug)} names neither the upper bound of "
            "the target federal funds rate nor its range, so the payout resolves on no rate this "
            "study governs",
        )
    if _CHANGE_FORM_RE.search(description_text):
        definition = RATE_TARGET_CHANGE_BPS
        axis = YES_AXIS_CHANGE
    elif _LEVEL_FORM_RE.search(description_text):
        definition = RATE_TARGET_UPPER_BOUND
        axis = YES_AXIS_LEVEL
    else:
        raise _refuse(
            REASON_SETTLEMENT_SUBJECT_UNREADABLE,
            f"the description of {_contract_named(market_slug)} names the target funds bound but "
            "states neither a change to it nor a level of it, so the axis its yes side prices is "
            "not established",
        )

    statement = _single_threshold(
        _threshold_statements(
            question=question_text,
            description=description_text,
            group_item_title=title_text,
            definition=definition,
        ),
        market_slug=market_slug,
    )
    operator = None
    if definition == RATE_TARGET_CHANGE_BPS and statement.strict:
        # A stated extension is a real inequality; an exact stated change is recorded
        # with no direction rather than with a fabricated one.
        operator = "above" if statement.value > 0 else "below"

    if definition == RATE_TARGET_CHANGE_BPS:
        # The yes side of a change market prices the stated change itself, and its sign
        # already lives in the signed threshold. This mirrors the policy grammar, whose
        # change branches carry orientation +1.
        orientation = 1
    elif statement.yes_side is not None:
        orientation = statement.yes_side
    else:
        # The fields stated the rate but none of them stated a side of it, and the venue's
        # level markets do state one ("\u2265 4.5%", "above 2.5%", "2.5% or lower") when the
        # yes side prices a side. A market that states no side anywhere is refused rather
        # than read as its own complement.
        raise _refuse(
            REASON_YES_SIDE_UNREADABLE,
            f"the text of {_contract_named(market_slug)} states the level "
            f"{statement.value} but names no side of it, so which outcome the yes side prices "
            "is not established. Defaulting a direction would let the contract be matched "
            "against its own complement",
        )

    period = _single_period(
        _period_statements(question=question_text, description=description_text),
        market_slug=market_slug,
    )
    declared = study_declared_calendar() if calendar is None else calendar
    horizon = declared.get(period.key)
    if horizon is None:
        raise _refuse(
            REASON_MONTH_NOT_IN_CALENDAR,
            f"the declared calendar dates no meeting in {period.period}, so the contract "
            f"{_contract_named(market_slug)} cannot be placed on the exposure axis. Resolving it "
            "from the date the venue states would put it on a meeting this study never declared",
        )

    resolution = _resolution_sentence(description_text)
    if _SERVED_STATEMENT_RE.search(resolution) is None:
        raise _refuse(
            REASON_RESOLUTION_BASIS_UNREADABLE,
            f"the description of {_contract_named(market_slug)} names a resolution source that is "
            f"no served official statement ({resolution!r}), so the basis its payout is computed "
            "from is not established",
        )

    return ParsedPolymarketPredicate(
        underlying_event=definition,
        threshold=statement.value,
        threshold_unit=_UNIT_BY_DEFINITION[definition],
        operator=operator,
        reference_period=period.period,
        settlement_criterion=SettlementCriterion(
            basis=SETTLEMENT_BASIS_CASH_AT_LISTED_PAYOUT,
            vintage=SETTLEMENT_VINTAGE_FIRST_RELEASE,
        ),
        reference_horizon=horizon,
        yes_axis=axis,
        orientation=orientation,
        underlying_event_text=subject.group(0),
        threshold_text=statement.text,
        operator_text=statement.marker_text,
        reference_period_text=period.text,
        settlement_criterion_text=resolution,
        reference_horizon_text=(
            f"declared calendar {period.period} -> {horizon.isoformat()}"
            + ("" if calendar is None else " (supplied calendar)")
        ),
    )
