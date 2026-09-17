"""Cross-venue contract matching on parsed payout predicates, never on titles.

Pooling two venues' contracts requires evidence that they state the *same claim*,
and a similar title is not that evidence. ``reports/contract_rule_registry.json``
declares the gate this module implements: ``all_required_fields_must_match`` is
true, ``similar_titles_sufficient`` is false and ``title_text_is_a_lead_only`` is
true, so the title is a lead to a contract and never a reason to match one. Two
contracts are therefore compared on the predicate components the venue's own
archived text yields, and a contract whose text states no readable payout is a
NULL with a named reason rather than a guess.

Six components, in the order this module compares them:

``underlying_event``
    The economic variable and yes-side axis the payout resolves on. It is the
    existing ``rate_definition`` derivation from
    :mod:`market_propagation.ingest.policy_predicates`, whose whole point is that a
    contract about the target range and a contract about the realized effective
    rate are different claims even when their numbers coincide. ``yes_axis`` is
    carried and compared with it because the parser derives both in one branch.
``reference_period``
    The ``YYYY-MM`` period the underlying figure covers. The registry requires
    exact equality after canonicalization to ISO year-month, so a value that is not
    already canonical is refused at the boundary rather than normalized by a second
    month vocabulary living here.
``threshold``
    The stated strike as an exact ``Decimal``, together with the unit the record
    states it in. A threshold without a unit is not a comparison, and the unit is
    what makes a cross-venue restatement recognizable (see the grade definitions).
``operator``
    The comparison direction, taken as ``inequality`` and ``orientation`` together,
    which is how :data:`market_propagation.neighbors.MATCH_FIELDS` treats a payoff
    direction. ``None`` is a stated state and not an absence: the parser records an
    exact stated rate change with no direction rather than a fabricated one.
``settlement_criterion``
    What the payout is computed from, as a basis plus the revision vintage it
    settles on. The vintage is a separate component of one criterion because this
    project has already measured that the vintage matters
    (:mod:`market_propagation.ingest.expectations` refuses a forecast scored against
    a revision, and :mod:`market_propagation.ingest.macro_releases` keeps disclosed
    revisions out of the first-print values). One contract settling on a revised
    figure and another on the first release is therefore a named refusal, not a
    rounding difference.
``reference_horizon``
    The decision instant the claim resolves on, resolved through the declared
    calendar by the existing
    :func:`market_propagation.ingest.policy_predicates.decision_date_for`. A month
    the declared calendar does not date is unplaceable, and the contract yields no
    predicate rather than a horizon read off its own ticker.

Four grades, declared in ``configs/matching_v1.yaml``:

``EXACT``
    Every component agrees on its stated value.
``ECONOMICALLY_EQUIVALENT``
    Every component agrees except that the threshold is stated in a different
    declared unit, and the two states denote the same quantity under the exact
    declared conversion. The payoff set is identical; the representation is not,
    which is why this is a separate grade rather than EXACT.
``APPROXIMATE``
    Every required component agrees, but the threshold or the operator differs, so
    the two contracts are the same event at a different strike or with a different
    boundary rule. A reported near-miss, not a match.
``REJECT``
    A required component differs, a component nobody published cannot be shown to
    agree, or one side's payout text states no readable predicate at all. The
    headers are the specific reasons, and the pair is still listed: a candidate that
    was refused is a recorded NULL, never a dropped row.

Two rules are load-bearing and each is a way this layer could quietly manufacture a
match.

**A near-miss is never a substitute for a match.** The neighbouring strike on the
same meeting is the most correlated contract on the venue, which is exactly the
substitution the design forbids. It is graded ``APPROXIMATE`` with
``threshold_differs`` and it never reaches the primary analysis.

**Two unknowns are not an agreement.** A component neither record publishes cannot
be shown to agree, so it is refused with
``required_component_is_unobserved_on_both_records``. The same rule is enforced
across this repository because a placeholder string in a matching field once made
two unread contracts compare equal.

The measured state of the inputs, which is a result rather than a fault:

* Only Kalshi's records state a payout this project can read. On the cleaned local
  Polymarket layer the verified column list of ``daily_aligned`` carries
  ``market_slug`` and metadata but no settlement-rule text column, and
  :mod:`market_propagation.ingest.polymarket_public` is fail-closed here because no
  documented host answered. The Polymarket declaration in the configuration
  therefore names no parser, and every cross-venue pair on the current inputs is
  ``REJECT``. The registry reports that per venue instead of reporting an empty
  match set as an absence of matches.
* The published payout grain is discrete. On the archived policy series every
  readable level strike is a multiple of 0.25 percent and every stated rate change
  is a multiple of 25 basis points, so a threshold difference between two contracts
  on one event is a genuinely different claim rather than a rounding artefact.
* The second venue's own record is used for nothing else. This module reads a
  venue's declared payout-text columns and the record's own structured strike; it
  never reads a slug as a predicate and never reaches the network.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from .ingest.policy_predicates import (
    RATE_FEDERAL_FUNDS,
    RATE_TARGET_CHANGE_BPS,
    RATE_TARGET_UPPER_BOUND,
    REASON_MONTH_NOT_IN_CALENDAR,
    REASON_RATE_SUBJECT_UNREADABLE,
    REASON_TEXT_UNREADABLE,
    ParsedPredicate,
    PredicateError,
    decision_date_for,
    parse_predicate,
)
from .neighbors import INEQUALITIES, REASON_RULE_VINTAGE_UNVERIFIED, ContractPredicate

__all__ = [
    "COMPONENTS",
    "GRADES",
    "MATCH_CONFIG_PATH",
    "PARSER_DECLARED_SECOND_VENUE_MARKET_TEXT",
    "POLICY_TEXT_FIELDS",
    "REASONS",
    "REGISTRY_VERSION",
    "SECOND_VENUE_REASONS",
    "SETTLEMENT_VINTAGES",
    "THRESHOLD_UNITS",
    "ComponentAgreement",
    "GradeDefinition",
    "MatchGrade",
    "MatchRegistry",
    "MatchingSettings",
    "PairMatch",
    "PredicateComponent",
    "PredicateRead",
    "SettlementCriterion",
    "VenueCoverage",
    "VenueDeclaration",
    "VenuePredicate",
    "build_registry",
    "grade_pair",
    "load_matching_settings",
    "read_predicate",
    "registry_digest",
    "settings_digest",
]

#: The pipeline configuration this module reads its match rules from.
MATCH_CONFIG_PATH = "configs/matching_v1.yaml"

#: Version of the matching rules. It travels beside the digest rather than inside
#: it, so bumping the rules does not pretend that an unchanged registry changed.
REGISTRY_VERSION = "matching_v1"

#: The archived columns a Kalshi payout predicate is read from, which are the
#: ``predicates.text_columns`` declared by ``configs/neighbor_graph_v2.yaml``. A
#: venue whose payout text is stated in the declared policy forms has to carry
#: both, or the direction and the subject cannot be read at all.
POLICY_TEXT_FIELDS: tuple[str, str] = ("yes_sub_title", "title")

#: The payout-grammar declarations a venue may carry. ``none_declared`` is a real
#: state: it says the venue's record states no payout this repository can parse,
#: and a reader that answered from a title anyway would be inventing a match.
PARSER_DECLARED_POLICY_FORMS = "declared_policy_predicate_forms"
PARSER_NONE_DECLARED = "none_declared_in_this_repository"
#: The grammar for a venue whose own market record states the claim in its
#: ``question`` and ``description`` text rather than in a subtitle and a title. It is
#: a second grammar and not an extension of the first: this layer never reads one
#: venue's text through another venue's grammar, because the two venues write their
#: claims in different vocabularies and a shared reader would read one of them wrong.
PARSER_DECLARED_SECOND_VENUE_MARKET_TEXT = "declared_second_venue_market_text"

#: The permitted parser declarations, in the order they are checked.
PARSERS: tuple[str, ...] = (
    PARSER_DECLARED_POLICY_FORMS,
    PARSER_DECLARED_SECOND_VENUE_MARKET_TEXT,
    PARSER_NONE_DECLARED,
)

#: The units a stated threshold may be published in, and the exact scale that
#: carries each onto the canonical unit (percent). The scales live in the
#: configuration and are validated against this vocabulary, because a conversion
#: invented at a call site is a silent rescaling of a matching field.
UNIT_PERCENT = "percent"
UNIT_BASIS_POINTS = "basis_points"

#: The declared threshold units.
THRESHOLD_UNITS: tuple[str, ...] = (UNIT_PERCENT, UNIT_BASIS_POINTS)

#: The canonical unit every conversion lands on.
CANONICAL_THRESHOLD_UNIT = UNIT_PERCENT

#: The unit each stated settlement subject's numbers are published in. A level is
#: published as a percent and a decision as a basis-point change, which is the
#: parser's own statement and is never re-derived from the magnitude of the number.
_UNIT_BY_RATE_DEFINITION: Mapping[str, str] = {
    RATE_TARGET_UPPER_BOUND: UNIT_PERCENT,
    RATE_FEDERAL_FUNDS: UNIT_PERCENT,
    RATE_TARGET_CHANGE_BPS: UNIT_BASIS_POINTS,
}

#: The revision vintages a settlement criterion may resolve on. They are separate
#: declarations because this project has measured that scoring against a revision
#: measures the revision rather than the news the release delivered.
SETTLEMENT_VINTAGE_FIRST_RELEASE = "first_release"
SETTLEMENT_VINTAGE_REVISED_FIGURE = "revised_figure"

#: The declared settlement vintages.
SETTLEMENT_VINTAGES: tuple[str, ...] = (
    SETTLEMENT_VINTAGE_FIRST_RELEASE,
    SETTLEMENT_VINTAGE_REVISED_FIGURE,
)


class PredicateComponent(StrEnum):
    """One component of a contract's payout predicate, in comparison order."""

    UNDERLYING_EVENT = "underlying_event"
    REFERENCE_PERIOD = "reference_period"
    THRESHOLD = "threshold"
    OPERATOR = "operator"
    SETTLEMENT_CRITERION = "settlement_criterion"
    REFERENCE_HORIZON = "reference_horizon"


#: The components, in the order this module compares and reports them.
COMPONENTS: tuple[PredicateComponent, ...] = tuple(PredicateComponent)

#: The components the programme requires a pair to agree on before it is a pair at
#: all. A configuration may require more than these and may never require fewer:
#: dropping one would silently widen the gate the run reports itself as applying.
PROGRAMME_REQUIRED_COMPONENTS: tuple[PredicateComponent, ...] = (
    PredicateComponent.UNDERLYING_EVENT,
    PredicateComponent.REFERENCE_PERIOD,
    PredicateComponent.SETTLEMENT_CRITERION,
    PredicateComponent.REFERENCE_HORIZON,
)


class MatchGrade(StrEnum):
    """What was established about a candidate pair, best first."""

    EXACT = "EXACT"
    ECONOMICALLY_EQUIVALENT = "ECONOMICALLY_EQUIVALENT"
    APPROXIMATE = "APPROXIMATE"
    REJECT = "REJECT"


#: The grades, in reported order.
GRADES: tuple[MatchGrade, ...] = tuple(MatchGrade)

#: The grades that certify the two contracts state the same payoff. A
#: configuration may feed the primary analysis from these and from nothing else:
#: ``APPROXIMATE`` is a reported near-miss and ``REJECT`` is a refusal, and neither
#: is a match however convenient pooling them would be.
AGREEMENT_GRADES: tuple[MatchGrade, ...] = (MatchGrade.EXACT, MatchGrade.ECONOMICALLY_EQUIVALENT)

#: Reasons a read yields no predicate. The three from the predicate parser are the
#: parser's own codes, reused so one unreadable contract has one name across the
#: repository rather than one name per consumer.
REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER = (
    "venue_payout_text_has_no_parser_declared_in_this_repository"
)
REASON_RECORD_FIELD_UNPUBLISHED = "record_does_not_publish_a_required_predicate_field"

#: Reasons a second-venue record yields no predicate. They are declared here rather
#: than in the grammar that raises them, because this module is where a
#: ``PredicateRead`` validates the code it carries: a grammar able to raise a code
#: this module does not declare would produce a refusal no consumer could branch on.
#: The grammar imports them from here, which is also the direction that keeps the
#: import graph acyclic — it already reads this module for ``SettlementCriterion``,
#: so this module must never read it back at module scope.
REASON_SETTLEMENT_SUBJECT_UNREADABLE = "description_states_no_readable_settlement_subject"
REASON_THRESHOLDS_DISAGREE = "stated_thresholds_disagree_across_the_market_record"
REASON_MEETING_UNREADABLE = "venue_text_states_no_readable_meeting_reference_period"
REASON_MEETINGS_DISAGREE = "stated_meeting_reference_periods_disagree_across_the_market_record"
REASON_RESOLUTION_BASIS_UNREADABLE = "description_states_no_resolution_basis"
REASON_YES_SIDE_UNREADABLE = "market_text_states_no_payoff_direction_for_the_yes_side"
REASON_YES_SIDES_DISAGREE = "stated_yes_sides_disagree_across_the_market_record"
#: The venue's metadata holds no record for the contract at all. It is a distinct
#: fact from a record that omits a field, and the two must not share a code: a
#: contract the sweep never reached is a coverage statement about the acquisition,
#: while a reached contract whose text omits a field is a statement about the venue.
REASON_SECOND_VENUE_RECORD_NOT_HELD = "the_venue_metadata_holds_no_record_for_this_contract"

#: The second-venue refusal codes as one tuple, so the grammar re-exports the
#: vocabulary from its single declaration instead of restating the strings.
SECOND_VENUE_REASONS: tuple[str, ...] = (
    REASON_SETTLEMENT_SUBJECT_UNREADABLE,
    REASON_THRESHOLDS_DISAGREE,
    REASON_MEETING_UNREADABLE,
    REASON_MEETINGS_DISAGREE,
    REASON_RESOLUTION_BASIS_UNREADABLE,
    REASON_YES_SIDE_UNREADABLE,
    REASON_YES_SIDES_DISAGREE,
    REASON_SECOND_VENUE_RECORD_NOT_HELD,
)

#: Reasons a pair is refused or graded down. Each names one distinct fact, so a
#: blocked registry reports which one applied rather than one undifferentiated
#: rejection.
REASON_PREDICATE_REFUSED = "one_side_states_no_readable_payoff_predicate"
REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS = "settlement_revision_vintage_differs"
REASON_SETTLEMENT_CRITERION_DIFFERS = "settlement_criterion_differs"
REASON_UNDERLYING_EVENT_DIFFERS = "underlying_economic_event_differs"
REASON_REFERENCE_PERIOD_DIFFERS = "reference_period_differs"
REASON_REFERENCE_HORIZON_DIFFERS = "reference_horizon_differs"
REASON_COMPONENT_UNOBSERVED = "required_component_is_unobserved_on_both_records"
REASON_THRESHOLD_DIFFERS = "threshold_differs"
REASON_OPERATOR_DIFFERS = "comparison_operator_differs"
REASON_GRADED_EXACT = "every_component_agrees_on_the_stated_value"
REASON_ECONOMICALLY_EQUIVALENT = "threshold_denotes_the_same_quantity_in_a_declared_unit"

#: The reason vocabulary this module can write, in the precedence order it applies.
REASONS: tuple[str, ...] = (
    REASON_PREDICATE_REFUSED,
    REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS,
    REASON_SETTLEMENT_CRITERION_DIFFERS,
    REASON_UNDERLYING_EVENT_DIFFERS,
    REASON_REFERENCE_PERIOD_DIFFERS,
    REASON_REFERENCE_HORIZON_DIFFERS,
    REASON_COMPONENT_UNOBSERVED,
    REASON_THRESHOLD_DIFFERS,
    REASON_OPERATOR_DIFFERS,
    REASON_GRADED_EXACT,
    REASON_ECONOMICALLY_EQUIVALENT,
    REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER,
    REASON_RECORD_FIELD_UNPUBLISHED,
    REASON_TEXT_UNREADABLE,
    REASON_RATE_SUBJECT_UNREADABLE,
    REASON_MONTH_NOT_IN_CALENDAR,
    *SECOND_VENUE_REASONS,
)

#: Agreement verdicts, one per component of a graded pair.
AGREEMENT_AGREE = "agree"
AGREEMENT_EQUIVALENT = "equivalent_under_a_declared_unit_conversion"
AGREEMENT_DIFFER = "differ"
AGREEMENT_UNOBSERVED = "unobserved_on_at_least_one_record"

#: The agreement verdicts, in reported order.
AGREEMENT_VERDICTS: tuple[str, ...] = (
    AGREEMENT_AGREE,
    AGREEMENT_EQUIVALENT,
    AGREEMENT_DIFFER,
    AGREEMENT_UNOBSERVED,
)

#: The precedence a specific reason is reported in when several apply, so the
#: headline detail is the most specific fact rather than the first compared.
_REASON_ORDER: Mapping[str, int] = {reason: index for index, reason in enumerate(REASONS)}

#: The component whose difference each mismatch reason names.
_MISMATCH_REASON: Mapping[PredicateComponent, str] = {
    PredicateComponent.UNDERLYING_EVENT: REASON_UNDERLYING_EVENT_DIFFERS,
    PredicateComponent.REFERENCE_PERIOD: REASON_REFERENCE_PERIOD_DIFFERS,
    PredicateComponent.SETTLEMENT_CRITERION: REASON_SETTLEMENT_CRITERION_DIFFERS,
    PredicateComponent.REFERENCE_HORIZON: REASON_REFERENCE_HORIZON_DIFFERS,
    PredicateComponent.THRESHOLD: REASON_THRESHOLD_DIFFERS,
    PredicateComponent.OPERATOR: REASON_OPERATOR_DIFFERS,
}

_PERIOD_RE = re.compile(r"^(?P<year>[0-9]{4})-(?P<month>0[1-9]|1[0-2])$")


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty str, got {value!r}")
    return value


def _strict_decimal(value: object, *, field_name: str) -> Decimal:
    """A threshold as an exact decimal, refusing a float.

    A binary float cannot represent a published strike exactly, and a matching
    field compared through one is a comparison whose result depends on the
    conversion rather than on the contract.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or not isinstance(value, (int, str, Decimal)):
        raise ValueError(
            f"{field_name} must be an exact Decimal, an int or a decimal string, got "
            f"{type(value).__name__}; a float threshold is a matching field compared "
            "through a binary conversion"
        )
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError) as exc:
        raise ValueError(f"{field_name} {value!r} is not a decimal") from exc


def _canonical_period(value: str | None, *, field_name: str) -> str | None:
    """A reference period in the ``YYYY-MM`` form the archive stores, or a refusal.

    The registry compares this field for exact equality after canonicalization to
    ISO year-month, and ``market_propagation.sample`` already refuses anything that
    is not a ``YYYY-MM`` period. Normalizing a second spelling here would mean two
    calendars' worth of month vocabulary in one repository, so a non-canonical value
    is refused where it was supplied.
    """
    if value is None:
        return None
    text = _text(value, field_name=field_name).strip()
    if _PERIOD_RE.match(text) is None:
        raise ValueError(
            f"{field_name} {text!r} is not a canonical YYYY-MM reference period; the archive "
            "stores this field canonically and a second spelling is not normalized here"
        )
    return text


@dataclass(frozen=True, slots=True)
class SettlementCriterion:
    """What a payout is computed from, and which print of the figure it resolves on.

    ``basis`` states the payout rule the venue's text declares, for example a cash
    payout at the listed amount. ``vintage`` states which publication of the
    underlying figure settles it. They are one component together because a revised
    figure and a first release are different settlements of the same basis, and the
    difference between them is exactly the kind that must be refused by name rather
    than averaged away.
    """

    basis: str
    vintage: str

    def __post_init__(self) -> None:
        _text(self.basis, field_name="SettlementCriterion.basis")
        object.__setattr__(
            self, "basis", _text(self.basis, field_name="SettlementCriterion.basis").strip()
        )
        vintage = _text(self.vintage, field_name="SettlementCriterion.vintage").strip()
        if vintage not in SETTLEMENT_VINTAGES:
            raise ValueError(
                f"SettlementCriterion.vintage must be one of {SETTLEMENT_VINTAGES}, got "
                f"{vintage!r}; an undeclared vintage cannot be compared with a declared one"
            )
        object.__setattr__(self, "vintage", vintage)

    @property
    def settles_on_a_revision(self) -> bool:
        """Whether this criterion resolves on a later print rather than the first."""
        return self.vintage != SETTLEMENT_VINTAGE_FIRST_RELEASE

    def as_dict(self) -> dict[str, Any]:
        return {"basis": self.basis, "vintage": self.vintage}


@dataclass(frozen=True, slots=True)
class VenueDeclaration:
    """One venue's declared payout record: where its text is and what reads it.

    ``payout_text_fields`` names the columns the venue's own record states its
    payout in, in the order the parser reads them. ``parser`` names the grammar that
    reads them, and ``none_declared_in_this_repository`` is a real declaration: this
    repository has no parser for that venue's payout vocabulary, so its records yield
    no predicate rather than one read through another venue's grammar.
    """

    venue: str
    payout_text_fields: tuple[str, ...]
    parser: str
    documentation_verified: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", _text(self.venue, field_name="VenueDeclaration.venue"))
        fields = tuple(
            _text(name, field_name="VenueDeclaration.payout_text_fields")
            for name in self.payout_text_fields
        )
        if len(set(fields)) != len(fields):
            raise ValueError(
                f"VenueDeclaration {self.venue!r} names a payout text field twice: {list(fields)}"
            )
        if self.parser not in PARSERS:
            raise ValueError(
                f"VenueDeclaration.parser must be one of {PARSERS}, got {self.parser!r}"
            )
        if self.parser == PARSER_DECLARED_POLICY_FORMS and fields != POLICY_TEXT_FIELDS:
            raise ValueError(
                f"VenueDeclaration {self.venue!r} declares the policy predicate parser, which "
                f"reads {list(POLICY_TEXT_FIELDS)}; the record names {list(fields)}"
            )
        if not isinstance(self.documentation_verified, bool):
            raise ValueError(
                "VenueDeclaration.documentation_verified must be a bool; a schema assumed from "
                f"documentation is not a verified schema, got {self.documentation_verified!r}"
            )
        object.__setattr__(self, "payout_text_fields", fields)

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "payout_text_fields": list(self.payout_text_fields),
            "parser": self.parser,
            "documentation_verified": self.documentation_verified,
        }


@dataclass(frozen=True, slots=True)
class VenuePredicate:
    """One contract's payout predicate as the six comparable components.

    ``unobserved`` names the components the record does not publish. It is a set of
    components rather than a set of null values because ``operator`` has a legal
    ``None``: the parser records an exact stated rate change with no direction
    rather than a fabricated one, and reading that ``None`` as an absence would
    refuse a contract whose text was read perfectly.

    ``contract`` carries the
    :class:`~market_propagation.neighbors.ContractPredicate` built from the same
    parsed predicate when the caller has one, so the rule-vintage check reuses that
    record's own ``rule_verified_over`` rather than a second interval comparison.
    """

    venue: str
    contract_id: str
    underlying_event: str
    reference_period: str | None
    threshold: Decimal | None
    threshold_unit: str | None
    inequality: str | None
    orientation: int
    settlement_criterion: SettlementCriterion | None
    reference_horizon: dt.date | None
    yes_axis: str
    yes_sub_title: str
    title: str
    unobserved: frozenset[PredicateComponent] = frozenset()
    contract: ContractPredicate | None = None

    def __post_init__(self) -> None:
        for name in ("venue", "contract_id", "underlying_event", "yes_axis"):
            object.__setattr__(
                self, name, _text(getattr(self, name), field_name=f"VenuePredicate.{name}")
            )
        for name in ("yes_sub_title", "title"):
            object.__setattr__(self, name, str(getattr(self, name) or "").strip())
        observed = frozenset(self.unobserved)
        unknown = sorted(str(component) for component in observed - set(COMPONENTS))
        if unknown:
            raise ValueError(
                f"VenuePredicate.unobserved names components this module does not declare: {unknown}"
            )
        if PredicateComponent.UNDERLYING_EVENT in observed:
            raise ValueError(
                "VenuePredicate.underlying_event cannot be unobserved: a contract whose record "
                "states no economic variable yields no predicate rather than one compared on an "
                "unknown event"
            )
        object.__setattr__(self, "unobserved", observed)
        if self.threshold is not None:
            object.__setattr__(
                self,
                "threshold",
                _strict_decimal(self.threshold, field_name="VenuePredicate.threshold"),
            )
        if self.threshold_unit is not None and self.threshold_unit not in THRESHOLD_UNITS:
            raise ValueError(
                f"VenuePredicate.threshold_unit must be one of {THRESHOLD_UNITS} or None, got "
                f"{self.threshold_unit!r}"
            )
        if PredicateComponent.THRESHOLD in observed:
            if self.threshold is not None or self.threshold_unit is not None:
                raise ValueError(
                    "VenuePredicate.threshold is unobserved and must carry neither a value nor a "
                    "unit; a threshold without a unit is not a comparison"
                )
        elif self.threshold is None or self.threshold_unit is None:
            raise ValueError(
                "VenuePredicate must state an observed threshold with its unit, or name the "
                "component unobserved"
            )
        if self.inequality is not None and self.inequality not in INEQUALITIES:
            raise ValueError(
                f"VenuePredicate.inequality must be one of {INEQUALITIES} or None, got "
                f"{self.inequality!r}"
            )
        if isinstance(self.orientation, bool) or self.orientation not in (-1, 1):
            raise ValueError(
                f"VenuePredicate.orientation must be +1 or -1, got {self.orientation!r}"
            )
        if PredicateComponent.SETTLEMENT_CRITERION in observed:
            if self.settlement_criterion is not None:
                raise ValueError(
                    "VenuePredicate.settlement_criterion is unobserved and must carry no criterion"
                )
        elif self.settlement_criterion is None:
            raise ValueError(
                "VenuePredicate must state a settlement criterion, or name the component unobserved"
            )
        object.__setattr__(
            self,
            "reference_period",
            _canonical_period(self.reference_period, field_name="VenuePredicate.reference_period"),
        )
        if PredicateComponent.REFERENCE_PERIOD in observed and self.reference_period is not None:
            raise ValueError(
                "VenuePredicate.reference_period is unobserved and must carry no period"
            )
        if self.reference_horizon is not None and (
            not isinstance(self.reference_horizon, dt.date)
            or isinstance(self.reference_horizon, dt.datetime)
        ):
            raise TypeError(
                "VenuePredicate.reference_horizon must be a date, got "
                f"{type(self.reference_horizon).__name__}"
            )
        if PredicateComponent.REFERENCE_HORIZON in observed and self.reference_horizon is not None:
            raise ValueError(
                "VenuePredicate.reference_horizon is unobserved and must carry no horizon"
            )
        if self.contract is not None and not isinstance(self.contract, ContractPredicate):
            raise TypeError(
                "VenuePredicate.contract must be a ContractPredicate or None, got "
                f"{type(self.contract).__name__}"
            )

    def component(self, name: PredicateComponent) -> object:
        """One component's value as this record states it."""
        return {
            PredicateComponent.UNDERLYING_EVENT: self.underlying_event,
            PredicateComponent.REFERENCE_PERIOD: self.reference_period,
            PredicateComponent.THRESHOLD: self.threshold,
            PredicateComponent.OPERATOR: self.inequality,
            PredicateComponent.SETTLEMENT_CRITERION: self.settlement_criterion,
            PredicateComponent.REFERENCE_HORIZON: self.reference_horizon,
        }[name]

    def component_text(self, name: PredicateComponent) -> str:
        """One component as the text a registry records, with an absent value stated.

        A component this record does not publish renders as the words that say so
        rather than as an empty string, so a reader of the registry cannot mistake
        an unobserved component for a stated value that happens to be blank.
        """
        if name in self.unobserved:
            return "unobserved"
        value = self.component(name)
        if value is None:
            return "no direction stated"
        if isinstance(value, SettlementCriterion):
            return f"{value.basis} on the {value.vintage}"
        if isinstance(value, dt.date):
            return value.isoformat()
        if name is PredicateComponent.THRESHOLD:
            return f"{value} {self.threshold_unit}"
        return str(value)

    def unit_scale(self, scales: Mapping[str, Decimal]) -> Decimal:
        """The exact scale carrying this threshold onto the canonical unit."""
        return scales[str(self.threshold_unit)]

    def rule_verified_over(self, start: dt.datetime, end: dt.datetime) -> bool:
        """Whether a verified rule version is in force across a window.

        The verdict comes from the carried
        :meth:`~market_propagation.neighbors.ContractPredicate.rule_verified_over`, so
        one interval comparison governs both this module and the exposure graph. A
        read that carries no contract predicate has no rule evidence at all and
        answers ``False`` rather than assuming one.
        """
        if self.contract is None:
            return False
        return self.contract.rule_verified_over(start, end)

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "contract_id": self.contract_id,
            "components": {str(name): self.component_text(name) for name in COMPONENTS},
            "unobserved": sorted(str(name) for name in self.unobserved),
            "yes_axis": self.yes_axis,
            "orientation": self.orientation,
            "payout_text": {"yes_sub_title": self.yes_sub_title, "title": self.title},
        }


@dataclass(frozen=True, slots=True)
class PredicateRead:
    """What reading one contract's record established, whether or not it did.

    ``predicate`` and ``reason`` are the two outcomes of one read, so exactly one of
    them is set, mirroring
    :class:`~market_propagation.neighbors.GraphDecision`. A contract whose text
    states no readable payout is carried as a decision rather than dropped, because
    the candidate universe a match set is measured against has to stay visible.
    """

    venue: str
    contract_id: str
    predicate: VenuePredicate | None
    reason: str | None
    detail: str

    def __post_init__(self) -> None:
        _text(self.venue, field_name="PredicateRead.venue")
        _text(self.contract_id, field_name="PredicateRead.contract_id")
        _text(self.detail, field_name="PredicateRead.detail")
        if (self.predicate is None) == (self.reason is None):
            raise ValueError(
                "PredicateRead must state either the predicate it read or the reason it read "
                f"none, got predicate {self.predicate!r} with reason {self.reason!r}"
            )
        if self.predicate is not None:
            if not isinstance(self.predicate, VenuePredicate):
                raise TypeError(
                    "PredicateRead.predicate must be a VenuePredicate, got "
                    f"{type(self.predicate).__name__}"
                )
            if (self.predicate.venue, self.predicate.contract_id) != (self.venue, self.contract_id):
                raise ValueError(
                    "PredicateRead names "
                    f"{(self.venue, self.contract_id)!r} and carries the predicate of "
                    f"{(self.predicate.venue, self.predicate.contract_id)!r}"
                )
        elif self.reason not in REASONS:
            raise ValueError(
                f"PredicateRead.reason must be one of the {len(REASONS)} declared reasons, got "
                f"{self.reason!r}; an undeclared reason is a refusal nobody can branch on"
            )

    @property
    def readable(self) -> bool:
        """Whether this read yielded a predicate."""
        return self.predicate is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "contract_id": self.contract_id,
            "readable": self.readable,
            "reason": self.reason,
            "detail": self.detail,
            "predicate": self.predicate.as_dict() if self.predicate is not None else None,
        }


@dataclass(frozen=True, slots=True)
class ComponentAgreement:
    """One component's verdict on a pair, with both stated values as evidence."""

    component: PredicateComponent
    left: str
    right: str
    verdict: str

    def __post_init__(self) -> None:
        if self.verdict not in AGREEMENT_VERDICTS:
            raise ValueError(
                f"ComponentAgreement.verdict must be one of {AGREEMENT_VERDICTS}, got "
                f"{self.verdict!r}"
            )
        _text(self.left, field_name="ComponentAgreement.left")
        _text(self.right, field_name="ComponentAgreement.right")

    def as_dict(self) -> dict[str, Any]:
        return {
            "component": str(self.component),
            "left": self.left,
            "right": self.right,
            "verdict": self.verdict,
        }


@dataclass(frozen=True, slots=True)
class PairMatch:
    """One candidate pair's grade, the reasons for it, and the component evidence.

    The grade is checked against the recorded verdicts rather than trusted: ``EXACT``
    requires every component to have agreed, ``ECONOMICALLY_EQUIVALENT`` requires an
    equivalent threshold and no differing component, and ``APPROXIMATE`` requires a
    difference that is not on a required component. A pair refused because a side
    states no predicate carries no component evidence, because there is nothing to
    compare, and its reasons name that refusal.
    """

    left_venue: str
    left_contract_id: str
    right_venue: str
    right_contract_id: str
    grade: MatchGrade
    reasons: tuple[str, ...]
    detail: str
    components: tuple[ComponentAgreement, ...] = ()

    def __post_init__(self) -> None:
        for name in ("left_venue", "left_contract_id", "right_venue", "right_contract_id"):
            object.__setattr__(
                self, name, _text(getattr(self, name), field_name=f"PairMatch.{name}")
            )
        _text(self.detail, field_name="PairMatch.detail")
        if self.left_venue == self.right_venue:
            raise ValueError(
                f"PairMatch is a cross-venue comparison, got {self.left_venue!r} on both sides"
            )
        if not self.reasons:
            raise ValueError("PairMatch must state the reasons it was assigned its grade")
        undeclared = sorted({reason for reason in self.reasons if reason not in REASONS})
        if undeclared:
            raise ValueError(f"PairMatch states reasons this module does not declare: {undeclared}")
        refused = REASON_PREDICATE_REFUSED in self.reasons
        if not self.components:
            # A pair refused before any comparison was made has no component evidence,
            # because there is nothing to compare. It is still a graded pair.
            if self.grade is not MatchGrade.REJECT or not refused:
                raise ValueError(
                    "PairMatch with no component evidence must be a refusal naming "
                    f"{REASON_PREDICATE_REFUSED!r}, got {self.grade.value!r} with {self.reasons!r}"
                )
            return
        if refused:
            raise ValueError(
                "PairMatch cannot both refuse a side for stating no predicate and report "
                "component verdicts"
            )
        if tuple(entry.component for entry in self.components) != COMPONENTS:
            raise ValueError(
                "PairMatch.components must record every component in order, got "
                f"{[str(entry.component) for entry in self.components]}"
            )
        verdicts = {entry.component: entry.verdict for entry in self.components}
        if self.grade is MatchGrade.EXACT and any(
            verdict != AGREEMENT_AGREE for verdict in verdicts.values()
        ):
            raise ValueError(
                "PairMatch grades a pair EXACT while a component did not agree: "
                f"{sorted(str(name) for name, verdict in verdicts.items() if verdict != AGREEMENT_AGREE)}"
            )
        if self.grade is MatchGrade.ECONOMICALLY_EQUIVALENT and (
            verdicts[PredicateComponent.THRESHOLD] != AGREEMENT_EQUIVALENT
            or any(verdict == AGREEMENT_DIFFER for verdict in verdicts.values())
        ):
            raise ValueError(
                "PairMatch grades a pair ECONOMICALLY_EQUIVALENT without an equivalent threshold "
                "and no differing component"
            )
        if self.grade is MatchGrade.APPROXIMATE:
            differing = {
                name
                for name, verdict in verdicts.items()
                if verdict in (AGREEMENT_DIFFER, AGREEMENT_UNOBSERVED)
            }
            if not differing:
                raise ValueError(
                    "PairMatch grades a pair APPROXIMATE with no differing component; a near-miss "
                    "has one, and an unexplained grade is an assertion"
                )
        if self.grade is MatchGrade.REJECT and not any(
            verdict in (AGREEMENT_DIFFER, AGREEMENT_UNOBSERVED) for verdict in verdicts.values()
        ):
            raise ValueError(
                "PairMatch grades a pair REJECT with every component agreeing; a refusal has a "
                "reason that is visible in the evidence"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "left": {"venue": self.left_venue, "contract_id": self.left_contract_id},
            "right": {"venue": self.right_venue, "contract_id": self.right_contract_id},
            "grade": self.grade.value,
            "reasons": list(self.reasons),
            "detail": self.detail,
            "components": [entry.as_dict() for entry in self.components],
        }


@dataclass(frozen=True, slots=True)
class VenueCoverage:
    """How many of one venue's supplied records yielded a predicate, and why not.

    A venue whose records were never supplied and a venue whose records all refused
    are different states, and the readable count is what separates them from an
    observed absence of matches.
    """

    venue: str
    readable: int
    refused: int
    reasons: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        _text(self.venue, field_name="VenueCoverage.venue")
        for name in ("readable", "refused"):
            value = getattr(self, name)
            if isinstance(value, bool) or value < 0:
                raise ValueError(f"VenueCoverage.{name} must be a non-negative int, got {value!r}")
        pairs = tuple((str(reason), int(count)) for reason, count in self.reasons)
        if sum(count for _, count in pairs) != self.refused:
            raise ValueError(
                f"VenueCoverage for {self.venue!r} counts {self.refused} refusals but its reasons "
                f"account for {sum(count for _, count in pairs)}"
            )
        object.__setattr__(self, "reasons", pairs)

    @property
    def supplied(self) -> int:
        """How many of this venue's records the caller supplied."""
        return self.readable + self.refused

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "supplied": self.supplied,
            "readable": self.readable,
            "refused": self.refused,
            "reasons": dict(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class GradeDefinition:
    """One grade's declared meaning and whether it certifies the same payoff."""

    grade: MatchGrade
    definition: str
    certifies_the_same_payoff: bool

    def __post_init__(self) -> None:
        _text(self.definition, field_name="GradeDefinition.definition")
        if not isinstance(self.certifies_the_same_payoff, bool):
            raise ValueError(
                "GradeDefinition.certifies_the_same_payoff must be a bool, got "
                f"{self.certifies_the_same_payoff!r}"
            )
        expected = self.grade in AGREEMENT_GRADES
        if self.certifies_the_same_payoff is not expected:
            raise ValueError(
                f"GradeDefinition for {self.grade.value!r} states "
                f"certifies_the_same_payoff={self.certifies_the_same_payoff!r}; this module's "
                f"agreement grades are {[grade.value for grade in AGREEMENT_GRADES]}"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "grade": self.grade.value,
            "definition": self.definition,
            "certifies_the_same_payoff": self.certifies_the_same_payoff,
        }


@dataclass(frozen=True, slots=True)
class MatchingSettings:
    """The declared match rules a registry is built under.

    ``required_components`` decides whether a stated difference is a refusal or a
    reported near-miss; it never decides whether an unobserved component is a
    refusal, because an unobserved component is one either way.
    """

    config_version: str
    venues: tuple[VenueDeclaration, ...]
    required_components: tuple[PredicateComponent, ...]
    grades: tuple[GradeDefinition, ...]
    primary_analysis_grades: tuple[MatchGrade, ...]
    threshold_units: tuple[tuple[str, Decimal], ...]

    def __post_init__(self) -> None:
        _text(self.config_version, field_name="MatchingSettings.config_version")
        venues = tuple(self.venues)
        if len(venues) < 2:
            raise ValueError(
                "MatchingSettings must declare at least two venues; this module only compares "
                "contracts across venues, and a one-venue configuration would report a match set "
                "for a comparison it cannot make"
            )
        names = [declaration.venue for declaration in venues]
        if len(set(names)) != len(names):
            raise ValueError(f"MatchingSettings declares a venue twice: {names}")
        object.__setattr__(self, "venues", venues)
        required = tuple(self.required_components)
        missing = [name for name in PROGRAMME_REQUIRED_COMPONENTS if name not in required]
        if missing:
            raise ValueError(
                "MatchingSettings.required_components omits "
                f"{[str(name) for name in missing]}, which the programme requires a pair to agree "
                "on; a configuration may require more components and never fewer"
            )
        if len(set(required)) != len(required):
            raise ValueError("MatchingSettings.required_components names a component twice")
        object.__setattr__(self, "required_components", required)
        grades = tuple(self.grades)
        if sorted(definition.grade for definition in grades) != sorted(GRADES):
            raise ValueError(
                "MatchingSettings.grades must define every declared grade exactly once, got "
                f"{[definition.grade.value for definition in grades]}"
            )
        object.__setattr__(self, "grades", grades)
        primary = tuple(self.primary_analysis_grades)
        if not primary:
            raise ValueError(
                "MatchingSettings.primary_analysis_grades is empty; a primary analysis that "
                "admits no grade has no declared match set"
            )
        inadmissible = [grade.value for grade in primary if grade not in AGREEMENT_GRADES]
        if inadmissible:
            raise ValueError(
                f"the primary analysis cannot be fed from {inadmissible}; only "
                f"{[grade.value for grade in AGREEMENT_GRADES]} certify that two contracts state "
                "the same payoff, and APPROXIMATE is a reported near-miss"
            )
        object.__setattr__(self, "primary_analysis_grades", primary)
        scales = tuple(
            (str(unit), _strict_decimal(scale, field_name=f"threshold_units.{unit}"))
            for unit, scale in self.threshold_units
        )
        if sorted(unit for unit, _ in scales) != sorted(THRESHOLD_UNITS):
            raise ValueError(
                "MatchingSettings.threshold_units must declare exactly the units "
                f"{list(THRESHOLD_UNITS)}, got {sorted(unit for unit, _ in scales)}"
            )
        if any(scale <= 0 for _, scale in scales):
            raise ValueError("every declared threshold unit scale must be positive")
        canonical = dict(scales)[CANONICAL_THRESHOLD_UNIT]
        if canonical != Decimal(1):
            raise ValueError(
                f"the canonical threshold unit {CANONICAL_THRESHOLD_UNIT!r} must scale by exactly "
                f"1, got {canonical}"
            )
        object.__setattr__(self, "threshold_units", scales)

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> MatchingSettings:
        """Read the match rules from a parsed ``configs/matching_v1.yaml``.

        Every block is mandatory. A configuration that omitted the required
        components or the grade definitions would leave this module to supply its own
        defaults, and a run would then report a gate it never read.
        """
        if not isinstance(config, Mapping):
            raise TypeError(
                "MatchingSettings.from_config expects the parsed configuration mapping, got "
                f"{type(config).__name__}"
            )
        version = config.get("config_version")
        if version != Path(MATCH_CONFIG_PATH).stem:
            raise ValueError(
                f"the configuration declares config_version {version!r}; this module reads "
                f"{Path(MATCH_CONFIG_PATH).stem!r} and a registry built from another "
                "declaration would name rules it did not apply"
            )
        components = config.get("components")
        if not isinstance(components, Mapping):
            raise ValueError(
                "the configuration carries no `components` mapping; the required components come "
                f"from {MATCH_CONFIG_PATH} and this module defines no fallback"
            )
        declared = components.get("all")
        if not isinstance(declared, list) or [str(name) for name in declared] != [
            str(name) for name in COMPONENTS
        ]:
            raise ValueError(
                f"components.all must declare the {len(COMPONENTS)} components in order "
                f"{[str(name) for name in COMPONENTS]}, got {declared!r}"
            )
        required_raw = components.get("required_for_agreement")
        if not isinstance(required_raw, list) or not required_raw:
            raise ValueError(
                "components.required_for_agreement must be a non-empty list; a configuration that "
                "requires nothing admits every pair"
            )
        try:
            required = tuple(PredicateComponent(str(name)) for name in required_raw)
        except ValueError as exc:
            raise ValueError(
                f"components.required_for_agreement names an undeclared component: {required_raw!r}"
            ) from exc
        venues_raw = config.get("venues")
        if not isinstance(venues_raw, list) or not venues_raw:
            raise ValueError("the configuration must declare the venues it compares")
        venues: list[VenueDeclaration] = []
        for entry in venues_raw:
            if not isinstance(entry, Mapping):
                raise ValueError(f"each declared venue must be a mapping, got {entry!r}")
            fields = entry.get("payout_text_fields")
            if not isinstance(fields, list):
                raise ValueError(
                    f"venue {entry.get('id')!r} must declare the payout text fields its record "
                    "states, even when it declares no parser for them"
                )
            venues.append(
                VenueDeclaration(
                    venue=str(entry.get("id") or ""),
                    payout_text_fields=tuple(str(name) for name in fields),
                    parser=str(entry.get("parser") or ""),
                    documentation_verified=entry.get("documentation_verified") is True,
                )
            )
        grades_raw = config.get("grades")
        if not isinstance(grades_raw, list) or not grades_raw:
            raise ValueError("the configuration must define every declared grade")
        grades: list[GradeDefinition] = []
        for entry in grades_raw:
            if not isinstance(entry, Mapping):
                raise ValueError(f"each grade definition must be a mapping, got {entry!r}")
            try:
                grade = MatchGrade(str(entry.get("grade") or ""))
            except ValueError as exc:
                raise ValueError(f"undeclared grade {entry.get('grade')!r}") from exc
            grades.append(
                GradeDefinition(
                    grade=grade,
                    definition=str(entry.get("definition") or ""),
                    certifies_the_same_payoff=entry.get("certifies_the_same_payoff") is True,
                )
            )
        primary = config.get("primary_analysis")
        if not isinstance(primary, Mapping) or not isinstance(primary.get("grades"), list):
            raise ValueError(
                "the configuration must declare primary_analysis.grades; which grades feed the "
                "primary analysis is a decision the run reports rather than one it makes"
            )
        try:
            primary_grades = tuple(MatchGrade(str(name)) for name in primary["grades"])
        except ValueError as exc:
            raise ValueError(
                f"primary_analysis.grades names an undeclared grade: {primary['grades']!r}"
            ) from exc
        units_raw = config.get("threshold_units")
        if not isinstance(units_raw, Mapping):
            raise ValueError(
                "the configuration must declare the scale from each threshold unit onto the "
                "canonical unit"
            )
        return cls(
            config_version=str(version),
            venues=tuple(venues),
            required_components=required,
            grades=tuple(grades),
            primary_analysis_grades=primary_grades,
            threshold_units=tuple((str(unit), scale) for unit, scale in units_raw.items()),
        )

    @property
    def venue_names(self) -> tuple[str, ...]:
        """The declared venues, in declaration order."""
        return tuple(declaration.venue for declaration in self.venues)

    def declaration(self, venue: str) -> VenueDeclaration:
        """One venue's declaration."""
        for declaration in self.venues:
            if declaration.venue == venue:
                return declaration
        raise KeyError(f"venue {venue!r} is not declared by this configuration")

    def unit_scale(self, unit: str) -> Decimal:
        """The exact scale carrying a declared unit onto the canonical unit."""
        scales = dict(self.threshold_units)
        if unit not in scales:
            raise ValueError(
                f"threshold unit {unit!r} is not declared; declared units are "
                f"{list(THRESHOLD_UNITS)}"
            )
        return scales[unit]

    def grade_definition(self, grade: MatchGrade) -> GradeDefinition:
        """One grade's declared definition."""
        for definition in self.grades:
            if definition.grade == grade:
                return definition
        raise KeyError(f"grade {grade.value!r} is not declared")

    def supplies_the_primary_analysis(self, grade: MatchGrade) -> bool:
        """Whether a grade may feed the primary analysis under this declaration."""
        return grade in self.primary_analysis_grades

    def as_dict(self) -> dict[str, Any]:
        return {
            "config_version": self.config_version,
            "venues": [declaration.as_dict() for declaration in self.venues],
            "required_components": [str(name) for name in self.required_components],
            "grades": [definition.as_dict() for definition in self.grades],
            "primary_analysis_grades": [grade.value for grade in self.primary_analysis_grades],
            "threshold_units": {unit: str(scale) for unit, scale in self.threshold_units},
        }


def load_matching_settings(config_path: str | Path = MATCH_CONFIG_PATH) -> MatchingSettings:
    """Load the match rules from a pipeline configuration file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"matching configuration not found: {path}")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"matching configuration at {path} is not valid YAML: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"matching configuration at {path} must be a mapping, got {type(payload).__name__}"
        )
    return MatchingSettings.from_config(payload)


def settings_digest(settings: MatchingSettings) -> str:
    """Stable sha256 of the settings, so a registry names the rules it was built under."""
    payload = json.dumps(settings.as_dict(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _refusal(
    declaration: VenueDeclaration, contract_id: str, reason: str, detail: str
) -> PredicateRead:
    return PredicateRead(
        venue=declaration.venue,
        contract_id=contract_id,
        predicate=None,
        reason=reason,
        detail=detail,
    )


def read_predicate(
    declaration: VenueDeclaration,
    *,
    contract_id: str,
    text_fields: Mapping[str, str] | None = None,
    event_ticker: str | None = None,
    calendar: Mapping[tuple[int, int], dt.date] | None = None,
    reference_period: str | None = None,
    settlement_criterion: SettlementCriterion | None = None,
    strike: Decimal | None = None,
    strike_unit: str | None = None,
    contract: ContractPredicate | None = None,
) -> PredicateRead:
    """Read one contract's payout predicate from its own record, or the reason there is none.

    The direction and the settlement subject come from the record's declared payout
    text through the existing
    :func:`~market_propagation.ingest.policy_predicates.parse_predicate`, and the
    horizon comes from the declared calendar through the existing
    :func:`~market_propagation.ingest.policy_predicates.decision_date_for`. Neither is
    re-derived here.

    ``strike`` and ``strike_unit`` are supplied together when the record states the
    level in its own unit outside the payout text, as a structured strike field does.
    They replace the parsed value and unit and are never used to invent a direction:
    the payout text still has to state one, so a record with a strike field and no
    readable payout text is refused. ``reference_period`` and
    ``settlement_criterion`` come from the caller that joined the release and rule
    records, because the venue's own market record does not publish either.
    """
    _text(contract_id, field_name="read_predicate.contract_id")
    if (strike is None) != (strike_unit is None):
        raise ValueError(
            "read_predicate takes a structured strike and its unit together; a value without a "
            "unit is not a comparison"
        )
    if strike is not None:
        strike = _strict_decimal(strike, field_name="read_predicate.strike")
        if strike_unit not in THRESHOLD_UNITS:
            raise ValueError(
                f"read_predicate.strike_unit must be one of {THRESHOLD_UNITS}, got {strike_unit!r}"
            )
    if declaration.parser == PARSER_NONE_DECLARED:
        return _refusal(
            declaration,
            contract_id,
            REASON_VENUE_PAYOUT_TEXT_HAS_NO_DECLARED_PARSER,
            f"{declaration.venue} declares no payout parser in this repository, so the record for "
            f"{contract_id} states no payout this project can read: the cleaned local layer carries "
            f"{list(declaration.payout_text_fields)} as its payout text columns and none of them "
            "states a declared payout form. Reading a title or a slug as a predicate would be a "
            "match inferred from a name",
        )
    fields = text_fields if text_fields is not None else {}
    for name in declaration.payout_text_fields:
        value = fields.get(name)
        if not isinstance(value, str) or not value.strip():
            return _refusal(
                declaration,
                contract_id,
                REASON_RECORD_FIELD_UNPUBLISHED,
                f"the record for {contract_id} publishes no {name!r}, which the declared "
                f"{declaration.parser} grammar reads",
            )
    if declaration.parser == PARSER_DECLARED_SECOND_VENUE_MARKET_TEXT:
        return _read_second_venue_market_text(declaration, contract_id=contract_id, fields=fields)
    try:
        parsed: ParsedPredicate = parse_predicate(
            yes_sub_title=str(fields[POLICY_TEXT_FIELDS[0]]),
            title=str(fields[POLICY_TEXT_FIELDS[1]]),
        )
        if event_ticker is None or calendar is None:
            raise PredicateError(
                REASON_MONTH_NOT_IN_CALENDAR,
                f"the record for {contract_id} states no event ticker or no declared calendar, so "
                "the decision it resolves on cannot be placed on the exposure axis. A horizon "
                "read off the contract's own identifier would be an invented schedule",
            )
        horizon = decision_date_for(event_ticker, calendar)
    except PredicateError as error:
        return _refusal(declaration, contract_id, error.reason, error.detail)

    threshold = parsed.threshold if strike is None else strike
    unit = _UNIT_BY_RATE_DEFINITION.get(parsed.rate_definition)
    if unit is None:
        raise ValueError(
            f"the parser stated rate definition {parsed.rate_definition!r}, which this module "
            f"declares no threshold unit for; declared definitions are "
            f"{sorted(_UNIT_BY_RATE_DEFINITION)}"
        )
    unit = unit if strike_unit is None else strike_unit
    if contract is not None:
        stated = (
            contract.rate_definition,
            contract.threshold,
            contract.inequality,
            contract.orientation,
        )
        parsed_values = (
            parsed.rate_definition,
            parsed.threshold,
            parsed.inequality,
            parsed.orientation,
        )
        if stated != parsed_values:
            raise ValueError(
                f"the carried ContractPredicate for {contract_id} states {stated!r} while the "
                f"archived text states {parsed_values!r}; one of the two was not read from this "
                "contract's own text"
            )
    unobserved = set()
    if reference_period is None:
        unobserved.add(PredicateComponent.REFERENCE_PERIOD)
    if settlement_criterion is None:
        unobserved.add(PredicateComponent.SETTLEMENT_CRITERION)
    return PredicateRead(
        venue=declaration.venue,
        contract_id=contract_id,
        predicate=VenuePredicate(
            venue=declaration.venue,
            contract_id=contract_id,
            underlying_event=parsed.rate_definition,
            reference_period=reference_period,
            threshold=threshold,
            threshold_unit=unit,
            inequality=parsed.inequality,
            orientation=parsed.orientation,
            settlement_criterion=settlement_criterion,
            reference_horizon=horizon,
            yes_axis=parsed.yes_axis,
            yes_sub_title=parsed.yes_sub_title,
            title=parsed.title,
            unobserved=frozenset(unobserved),
            contract=contract,
        ),
        reason=None,
        detail=(
            f"the archived text of {contract_id} states a "
            f"{parsed.rate_definition} payout that this repository's declared forms read"
        ),
    )


def _read_second_venue_market_text(
    declaration: VenueDeclaration,
    *,
    contract_id: str,
    fields: Mapping[str, str],
) -> PredicateRead:
    """One second-venue record read through its own declared market text.

    The grammar is imported here rather than at module scope deliberately. It reads
    this module for its refusal vocabulary and for ``SettlementCriterion``, and it
    reads ``cross_venue`` for the declared calendar, and ``cross_venue`` reads this
    module back: a module-scope import would close that loop. A cycle that happens to
    import cleanly today breaks on whichever module a future entry point imports
    first, so the one import that would close it is kept inside the one function that
    needs it.

    Every component is read from the venue's own stated text, including the
    ``reference_period`` and the ``settlement_criterion`` that the policy path has to
    receive from its caller. Neither is left unobserved here, because this venue's own
    text states both, and an unobserved component refuses every pair it appears in —
    so leaving them unobserved would report a readable record as an unreadable one.
    """
    from .ingest.polymarket_predicates import parse_polymarket_predicate

    try:
        parsed = parse_polymarket_predicate(
            question=fields["question"],
            description=fields["description"],
            group_item_title=fields.get("group_item_title"),
        )
    except PredicateError as error:
        return _refusal(declaration, contract_id, error.reason, error.detail)
    return PredicateRead(
        venue=declaration.venue,
        contract_id=contract_id,
        predicate=VenuePredicate(
            venue=declaration.venue,
            contract_id=contract_id,
            underlying_event=parsed.underlying_event,
            reference_period=parsed.reference_period,
            threshold=parsed.threshold,
            threshold_unit=parsed.threshold_unit,
            inequality=parsed.operator,
            orientation=parsed.orientation,
            settlement_criterion=parsed.settlement_criterion,
            reference_horizon=parsed.reference_horizon,
            yes_axis=parsed.yes_axis,
            yes_sub_title=fields["question"],
            title=fields["description"],
            unobserved=frozenset(),
            contract=None,
        ),
        reason=None,
        detail=(
            f"the venue's own market text for {contract_id} states a "
            f"{parsed.underlying_event} payout that this repository's declared second-venue "
            "grammar reads"
        ),
    )


def _threshold_verdict(
    left: VenuePredicate, right: VenuePredicate, settings: MatchingSettings
) -> str:
    """Whether two stated strikes denote the same level, and how that was established.

    A difference only in the stated unit is established as equivalent when the
    declared scales carry both onto the same canonical value. Any other difference is
    a different strike, which on the measured payout grain is a different claim.
    """
    if left.threshold == right.threshold and left.threshold_unit == right.threshold_unit:
        return AGREEMENT_AGREE
    left_canonical = left.threshold * settings.unit_scale(str(left.threshold_unit))
    right_canonical = right.threshold * settings.unit_scale(str(right.threshold_unit))
    if left_canonical == right_canonical:
        return AGREEMENT_EQUIVALENT
    return AGREEMENT_DIFFER


def _agreement(
    component: PredicateComponent,
    left: VenuePredicate,
    right: VenuePredicate,
    settings: MatchingSettings,
) -> ComponentAgreement:
    """One component's verdict, with both records' own statements as evidence.

    ``unobserved`` is checked first and for every component, so an absent value is
    never compared as though it were one: equal blanks are not an agreement.
    """
    if component in left.unobserved or component in right.unobserved:
        verdict = AGREEMENT_UNOBSERVED
    elif component is PredicateComponent.THRESHOLD:
        verdict = _threshold_verdict(left, right, settings)
    elif component is PredicateComponent.UNDERLYING_EVENT:
        # The economic variable and the yes-side axis the parser derived together:
        # the target range and the realized rate differ, and so do the two sides of a
        # level contract's own payoff.
        verdict = (
            AGREEMENT_AGREE
            if (left.underlying_event, left.yes_axis) == (right.underlying_event, right.yes_axis)
            else AGREEMENT_DIFFER
        )
    elif component is PredicateComponent.OPERATOR:
        # The stated inequality together with the orientation, which is how the
        # exposure graph's own MATCH_FIELDS treat one payoff direction.
        verdict = (
            AGREEMENT_AGREE
            if (left.inequality, left.orientation) == (right.inequality, right.orientation)
            else AGREEMENT_DIFFER
        )
    elif component is PredicateComponent.REFERENCE_PERIOD:
        verdict = (
            AGREEMENT_AGREE if left.reference_period == right.reference_period else AGREEMENT_DIFFER
        )
    elif component is PredicateComponent.REFERENCE_HORIZON:
        verdict = (
            AGREEMENT_AGREE
            if left.reference_horizon == right.reference_horizon
            else AGREEMENT_DIFFER
        )
    else:
        verdict = (
            AGREEMENT_AGREE
            if left.settlement_criterion == right.settlement_criterion
            else AGREEMENT_DIFFER
        )
    return ComponentAgreement(
        component=component,
        left=left.component_text(component),
        right=right.component_text(component),
        verdict=verdict,
    )


def grade_pair(
    left: PredicateRead, right: PredicateRead, *, settings: MatchingSettings
) -> PairMatch:
    """Grade one cross-venue candidate pair, with the reasons the grade was assigned.

    The order the checks run in is declared rather than incidental. A settlement
    difference is reported as the revision-vintage refusal when that is what it is,
    because that fact is the one this project has already measured and the generic
    criterion difference would lose it. Required-component differences are reported
    together, so a pair mismatching on two of them names both. An unobserved
    component refuses the pair whatever its requiredness: two records that publish
    nothing cannot be shown to agree, and equal blanks are not a match.
    """
    if (left.venue, left.contract_id) == (right.venue, right.contract_id):
        raise ValueError(
            f"grade_pair compares two contracts, got {(left.venue, left.contract_id)!r} twice"
        )
    if left.venue == right.venue:
        raise ValueError(
            f"grade_pair is a cross-venue comparison; both sides are on {left.venue!r}"
        )
    undeclared = sorted({read.venue for read in (left, right)} - set(settings.venue_names))
    if undeclared:
        raise ValueError(
            f"grade_pair was handed a venue the configuration does not declare: {undeclared}"
        )

    if left.predicate is None or right.predicate is None:
        refused_codes = tuple(read.reason for read in (left, right) if read.reason is not None)
        details = "; ".join(
            f"{read.venue} {read.contract_id}: {read.detail}"
            for read in (left, right)
            if read.predicate is None
        )
        return PairMatch(
            left_venue=left.venue,
            left_contract_id=left.contract_id,
            right_venue=right.venue,
            right_contract_id=right.contract_id,
            grade=MatchGrade.REJECT,
            reasons=(REASON_PREDICATE_REFUSED, *refused_codes),
            detail=(
                "no pair is formed on predicate content because a side states no readable payoff "
                f"predicate. {details}"
            ),
        )

    left_predicate = left.predicate
    right_predicate = right.predicate
    components = tuple(
        _agreement(component, left_predicate, right_predicate, settings) for component in COMPONENTS
    )
    verdicts = {entry.component: entry.verdict for entry in components}
    required = set(settings.required_components)

    reasons: list[str] = []
    left_criterion = left_predicate.settlement_criterion
    right_criterion = right_predicate.settlement_criterion
    revision_vintage_differs = (
        PredicateComponent.SETTLEMENT_CRITERION not in left_predicate.unobserved
        and PredicateComponent.SETTLEMENT_CRITERION not in right_predicate.unobserved
        and left_criterion is not None
        and right_criterion is not None
        and left_criterion.basis == right_criterion.basis
        and left_criterion.vintage != right_criterion.vintage
    )
    if revision_vintage_differs:
        reasons.append(REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS)

    for component in COMPONENTS:
        verdict = verdicts[component]
        if verdict == AGREEMENT_UNOBSERVED:
            reasons.append(REASON_COMPONENT_UNOBSERVED)
        elif verdict == AGREEMENT_DIFFER and component in required:
            # The revision-vintage refusal already names this difference more
            # precisely, so the generic criterion reason is not repeated beside it.
            if component is PredicateComponent.SETTLEMENT_CRITERION and revision_vintage_differs:
                continue
            reasons.append(_MISMATCH_REASON[component])

    if reasons:
        unique = tuple(sorted(set(reasons), key=lambda reason: _REASON_ORDER[reason]))
        unobserved = [
            str(name) for name, verdict in verdicts.items() if verdict == AGREEMENT_UNOBSERVED
        ]
        detail = _refusal_detail(
            unique, left_predicate, right_predicate, left, right, settings, unobserved
        )
        return PairMatch(
            left_venue=left.venue,
            left_contract_id=left.contract_id,
            right_venue=right.venue,
            right_contract_id=right.contract_id,
            grade=MatchGrade.REJECT,
            reasons=unique,
            detail=detail,
            components=components,
        )

    differing = [
        str(name)
        for name, verdict in verdicts.items()
        if verdict in (AGREEMENT_DIFFER, AGREEMENT_EQUIVALENT)
    ]
    if not differing:
        return PairMatch(
            left_venue=left.venue,
            left_contract_id=left.contract_id,
            right_venue=right.venue,
            right_contract_id=right.contract_id,
            grade=MatchGrade.EXACT,
            reasons=(REASON_GRADED_EXACT,),
            detail=(
                f"{left.contract_id} and {right.contract_id} state every component of the payout "
                "predicate identically, so they are one claim on the parsed predicate rather than "
                "on a resemblance between their titles"
            ),
            components=components,
        )
    threshold_equivalent = verdicts[PredicateComponent.THRESHOLD] == AGREEMENT_EQUIVALENT
    operator_agrees = verdicts[PredicateComponent.OPERATOR] == AGREEMENT_AGREE
    if threshold_equivalent and operator_agrees:
        return PairMatch(
            left_venue=left.venue,
            left_contract_id=left.contract_id,
            right_venue=right.venue,
            right_contract_id=right.contract_id,
            grade=MatchGrade.ECONOMICALLY_EQUIVALENT,
            reasons=(REASON_ECONOMICALLY_EQUIVALENT,),
            detail=(
                f"{left.contract_id} states {left_predicate.component_text(PredicateComponent.THRESHOLD)} "
                f"and {right.contract_id} states "
                f"{right_predicate.component_text(PredicateComponent.THRESHOLD)}; the declared "
                "scales carry both onto the same canonical level, so the payoff sets are identical "
                "and only the stated unit differs"
            ),
            components=components,
        )
    graded_reasons = tuple(
        sorted(
            {
                _MISMATCH_REASON[component]
                for component in COMPONENTS
                if verdicts[component] == AGREEMENT_DIFFER
            },
            key=lambda reason: _REASON_ORDER[reason],
        )
    )
    return PairMatch(
        left_venue=left.venue,
        left_contract_id=left.contract_id,
        right_venue=right.venue,
        right_contract_id=right.contract_id,
        grade=MatchGrade.APPROXIMATE,
        reasons=graded_reasons,
        detail=(
            f"{left.contract_id} and {right.contract_id} agree on every required component of the "
            f"predicate but differ on {list(graded_reasons)}; they are the same "
            "economic event at a different boundary, which the primary analysis does not pool"
        ),
        components=components,
    )


def _refusal_detail(
    reasons: Sequence[str],
    left_predicate: VenuePredicate,
    right_predicate: VenuePredicate,
    left: PredicateRead,
    right: PredicateRead,
    settings: MatchingSettings,
    unobserved: Sequence[str],
) -> str:
    """The refusal's headline sentence, written from the reason that outranks the rest."""
    head = reasons[0]
    where = f"{left.venue} {left.contract_id} against {right.venue} {right.contract_id}"
    if head == REASON_SETTLEMENT_REVISION_VINTAGE_DIFFERS:
        return (
            f"{where} settle the same basis on different publication vintages: "
            f"{left_predicate.component_text(PredicateComponent.SETTLEMENT_CRITERION)} against "
            f"{right_predicate.component_text(PredicateComponent.SETTLEMENT_CRITERION)}. This "
            "project has measured that a revised figure is a different settlement from the first "
            "release, so the pair is refused by name rather than approximated"
        )
    if head == REASON_COMPONENT_UNOBSERVED:
        return (
            f"{where} cannot be shown to agree on {list(unobserved)}: at least one record publishes "
            "no such component, and two unobserved components are not an agreement. The candidate "
            "is reported rather than dropped, and it is not a match"
        )
    if head == REASON_UNDERLYING_EVENT_DIFFERS:
        return (
            f"{where} resolve on different economic variables: "
            f"{left_predicate.underlying_event} against {right_predicate.underlying_event}. "
            "Contracts on different settlement subjects are never each other's match, however "
            "close their numbers are"
        )
    if head == REASON_REFERENCE_PERIOD_DIFFERS:
        return (
            f"{where} cover different reference periods: {left_predicate.reference_period} against "
            f"{right_predicate.reference_period}, so their payoffs resolve on different figures"
        )
    if head == REASON_REFERENCE_HORIZON_DIFFERS:
        return (
            f"{where} resolve on different horizons: {left_predicate.reference_horizon} against "
            f"{right_predicate.reference_horizon}"
        )
    if head == REASON_SETTLEMENT_CRITERION_DIFFERS:
        return (
            f"{where} settle on different criteria: "
            f"{left_predicate.component_text(PredicateComponent.SETTLEMENT_CRITERION)} against "
            f"{right_predicate.component_text(PredicateComponent.SETTLEMENT_CRITERION)}"
        )
    if head == REASON_THRESHOLD_DIFFERS:
        return (
            f"{where} state different strikes: "
            f"{left_predicate.component_text(PredicateComponent.THRESHOLD)} against "
            f"{right_predicate.component_text(PredicateComponent.THRESHOLD)}, and the threshold is "
            "a component the configuration requires for agreement. The nearest strike is the most "
            "correlated contract, which is the substitution this gate exists to refuse"
        )
    return (
        f"{where} was refused with reasons {list(reasons)} under the declaration in "
        f"{MATCH_CONFIG_PATH}, which requires agreement on "
        f"{[str(name) for name in settings.required_components]}"
    )


@dataclass(frozen=True, slots=True)
class MatchRegistry:
    """Every candidate pair with its grade, and the counts a consumer branches on.

    ``reads`` is the candidate universe the pairs were drawn from, kept whole so a
    refusal stays visible and so the rule-vintage check has the contracts it needs.
    ``counts_by_grade`` counts pairs over that universe rather than over some
    unobserved population: a zero EXACT count states that no pair of the supplied
    candidates graded EXACT, not that none exists.
    """

    version: str
    settings_digest: str
    primary_analysis_grades: tuple[MatchGrade, ...]
    pairs: tuple[PairMatch, ...]
    reads: tuple[PredicateRead, ...]
    coverage: tuple[VenueCoverage, ...]

    def __post_init__(self) -> None:
        _text(self.version, field_name="MatchRegistry.version")
        _text(self.settings_digest, field_name="MatchRegistry.settings_digest")
        if not self.primary_analysis_grades:
            raise ValueError("MatchRegistry must name the grades the primary analysis may use")
        pairs = tuple(self.pairs)
        ordered = tuple(
            sorted(
                pairs,
                key=lambda pair: (
                    pair.left_venue,
                    pair.left_contract_id,
                    pair.right_venue,
                    pair.right_contract_id,
                ),
            )
        )
        object.__setattr__(self, "pairs", ordered)
        reads = tuple(self.reads)
        keys = [(read.venue, read.contract_id) for read in reads]
        if len(set(keys)) != len(keys):
            raise ValueError("MatchRegistry.reads must name each venue contract once")
        object.__setattr__(self, "reads", reads)
        candidates = set(keys)
        named = {(pair.left_venue, pair.left_contract_id) for pair in ordered} | {
            (pair.right_venue, pair.right_contract_id) for pair in ordered
        }
        outside = sorted(named - candidates)
        if outside:
            raise ValueError(
                "MatchRegistry holds pairs naming contracts outside its candidate universe: "
                f"{outside}"
            )
        coverage = tuple(self.coverage)
        if sorted(entry.venue for entry in coverage) != sorted({venue for venue, _ in keys}):
            raise ValueError(
                "MatchRegistry.coverage must report every venue that has records, covering exactly "
                f"{sorted({venue for venue, _ in keys})}"
            )
        for entry in coverage:
            supplied = sum(1 for venue, _ in keys if venue == entry.venue)
            if supplied != entry.supplied:
                raise ValueError(
                    f"MatchRegistry.coverage for {entry.venue!r} counts {entry.supplied} supplied "
                    f"records while the candidate universe holds {supplied}"
                )
            readable = sum(1 for read in reads if read.venue == entry.venue and read.readable)
            if readable != entry.readable:
                raise ValueError(
                    f"MatchRegistry.coverage for {entry.venue!r} counts {entry.readable} readable "
                    f"records while the candidate universe holds {readable}"
                )
        object.__setattr__(self, "coverage", coverage)

    def counts_by_grade(self) -> dict[str, int]:
        """How many pairs were graded each way, with every grade stated including zeros.

        A zero is stated rather than omitted so a reader sees the count that was
        measured instead of inferring one from an absent key.
        """
        counts = {grade.value: 0 for grade in GRADES}
        for pair in self.pairs:
            counts[pair.grade.value] += 1
        return counts

    def pairs_with_grade(self, grade: MatchGrade) -> tuple[PairMatch, ...]:
        """Every pair graded ``grade``."""
        return tuple(pair for pair in self.pairs if pair.grade is grade)

    def exact_pairs(self) -> tuple[PairMatch, ...]:
        """Every pair graded EXACT, which is the primary analysis's own match set."""
        return self.pairs_with_grade(MatchGrade.EXACT)

    def grade_for(self, left_contract_id: str, right_contract_id: str) -> MatchGrade:
        """The grade recorded for one pair, in either argument order.

        A registry grades each cross-venue pair once and identifies it by its two
        contract ids, so a lookup that matches more or fewer than one pair is
        ambiguous and is refused rather than resolved by position.
        """
        wanted = {left_contract_id, right_contract_id}
        if len(wanted) != 2:
            raise KeyError(
                f"grade_for joins two contracts, and both arguments were {left_contract_id!r}"
            )
        matched = [
            pair for pair in self.pairs if {pair.left_contract_id, pair.right_contract_id} == wanted
        ]
        if len(matched) != 1:
            raise KeyError(
                f"no single pair joins {left_contract_id!r} to {right_contract_id!r} in this "
                f"registry; {len(matched)} pairs do"
            )
        return matched[0].grade

    def primary_analysis_pairs(self) -> tuple[PairMatch, ...]:
        """Every pair the declared primary analysis may use.

        The grades come from the configuration rather than from this method, and the
        configuration may not name a grade that fails to certify the same payoff.
        """
        admitted = set(self.primary_analysis_grades)
        return tuple(pair for pair in self.pairs if pair.grade in admitted)

    def reasons_by_contract(self) -> dict[str, str]:
        """The refusal reason for every contract that yielded no predicate."""
        return {
            f"{read.venue}|{read.contract_id}": str(read.reason)
            for read in self.reads
            if not read.readable
        }

    def verified_matches(self, start: dt.datetime, end: dt.datetime) -> tuple[PairMatch, ...]:
        """The pairs whose both sides carry a verified rule version across a window.

        A predicate-identical pair and a *verified* identical pair are different
        claims, and this project's exposure graph already refuses an edge whose rule
        vintage is not verified. The same requirement is applied here through the
        carried :class:`~market_propagation.neighbors.ContractPredicate`, so a match
        set can be reported as unverified rather than silently promoted.
        """
        by_key = {(read.venue, read.contract_id): read for read in self.reads}
        out: list[PairMatch] = []
        for pair in self.primary_analysis_pairs():
            left = by_key[(pair.left_venue, pair.left_contract_id)]
            right = by_key[(pair.right_venue, pair.right_contract_id)]
            if (
                left.predicate is not None
                and right.predicate is not None
                and left.predicate.rule_verified_over(start, end)
                and right.predicate.rule_verified_over(start, end)
            ):
                out.append(pair)
        return tuple(out)

    def as_dict(self) -> dict[str, Any]:
        """The registry as JSON-representable data, with its digest beside the pairs."""
        primary = self.primary_analysis_pairs()
        counts = self.counts_by_grade()
        return {
            "version": self.version,
            "digest": registry_digest(self),
            "settings_digest": self.settings_digest,
            "coverage": [entry.as_dict() for entry in self.coverage],
            "counts": {
                "candidate_pairs": len(self.pairs),
                "by_grade": counts,
                "primary_analysis": {
                    "grades": [grade.value for grade in self.primary_analysis_grades],
                    "pairs": len(primary),
                },
                "denominator": (
                    "candidate pairs formed from the venue records this registry was handed"
                ),
            },
            "rule_vintage": {
                "verified_looking": any(
                    read.readable
                    and read.predicate is not None
                    and read.predicate.contract is not None
                    for read in self.reads
                ),
                "reason": (
                    "the rule-vintage requirement is declared in configs/neighbor_graph_v2.yaml and "
                    f"is applied through {REASON_RULE_VINTAGE_UNVERIFIED}"
                ),
                "note": (
                    "a pair in this registry is identical on the parsed predicate; whether it is a "
                    "verified identical claim is answered by verified_matches over the declared "
                    "window, not by the grade"
                ),
            },
            "pairs": [pair.as_dict() for pair in self.pairs],
            "reads": [read.as_dict() for read in self.reads],
        }


def registry_digest(registry: MatchRegistry) -> str:
    """Stable sha256 over the canonically sorted JSON of the registry's pairs.

    The version travels beside the digest rather than inside it, exactly as
    :func:`market_propagation.neighbors.graph_digest` does, so bumping the rules does
    not pretend that an unchanged registry changed.
    """
    payload = json.dumps(
        [pair.as_dict() for pair in registry.pairs], sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_registry(
    reads: Sequence[PredicateRead],
    *,
    settings: MatchingSettings,
    version: str = REGISTRY_VERSION,
) -> MatchRegistry:
    """Grade every cross-venue candidate pair drawn from the supplied records.

    The candidate universe is the cross-venue product of the venue records the caller
    supplied, so every cross-venue pair among them is graded exactly once and every
    refusal is reported. Within-venue pairs are not candidates: this module compares
    contracts across venues, and a same-venue pair is the exposure graph's question
    rather than this one's.

    A venue the configuration does not declare is refused rather than ignored, because
    a candidate a caller handed in and the registry silently dropped is a match set
    measured against an undisclosed universe.
    """
    supplied = tuple(reads)
    declared = set(settings.venue_names)
    undeclared = sorted({read.venue for read in supplied} - declared)
    if undeclared:
        raise ValueError(
            f"the candidate records name venues the configuration does not declare: {undeclared}"
        )
    seen: set[tuple[str, str]] = set()
    for read in supplied:
        key = (read.venue, read.contract_id)
        if key in seen:
            raise ValueError(f"two candidate records name {key!r}; the universe is ambiguous")
        seen.add(key)

    pairs: list[PairMatch] = []
    for index, left_venue in enumerate(settings.venue_names):
        for right_venue in settings.venue_names[index + 1 :]:
            left_reads = sorted(
                (read for read in supplied if read.venue == left_venue),
                key=lambda read: read.contract_id,
            )
            right_reads = sorted(
                (read for read in supplied if read.venue == right_venue),
                key=lambda read: read.contract_id,
            )
            for left in left_reads:
                for right in right_reads:
                    pairs.append(grade_pair(left, right, settings=settings))

    coverage: list[VenueCoverage] = []
    for venue in settings.venue_names:
        venue_reads = [read for read in supplied if read.venue == venue]
        if not venue_reads:
            continue
        counts: dict[str, int] = {}
        for read in venue_reads:
            if read.reason is not None:
                counts[read.reason] = counts.get(read.reason, 0) + 1
        coverage.append(
            VenueCoverage(
                venue=venue,
                readable=sum(1 for read in venue_reads if read.readable),
                refused=sum(1 for read in venue_reads if not read.readable),
                reasons=tuple(sorted(counts.items())),
            )
        )

    return MatchRegistry(
        version=version,
        settings_digest=settings_digest(settings),
        primary_analysis_grades=settings.primary_analysis_grades,
        pairs=tuple(pairs),
        reads=supplied,
        coverage=tuple(coverage),
    )
