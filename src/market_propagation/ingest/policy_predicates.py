"""Policy-rate payoff predicates, read from the archived contract text.

A neighbour graph is only as honest as the predicates it matches on, so the
predicate is read from the venue's own archived text rather than from the shape of
a ticker. Kalshi states each contract's payout in ``yes_sub_title`` ("Above 0.25%",
"Cut 25bps") and its settlement subject in ``title`` ("Will the **target federal
funds rate** be above 0.25%?"), and the archive carries both verbatim. A ticker
that looks alike is not evidence: two contracts can share a series and a month and
still settle on different rates.

Three rules this module enforces, because each is a way a graph quietly matches
contracts that are not comparable:

**The decision date comes from a declared calendar, never from the ticker.** A
contract's expiry code names a month; which meeting that month's contract is about
is a fact about the Federal Reserve's schedule, so the caller passes the declared
calendar and a month the calendar does not date is unplaceable rather than assumed
to be the middle of the month.

**An unparseable predicate is refused, not defaulted.** A contract whose published
text does not state a payout this module can read yields no predicate, and the
reason names the text. Reading it as a threshold of zero would make it a donor for
every other unreadable contract.

**The stated unit is part of the predicate.** The level contracts state a rate in
percent and the decision contracts state a change in basis points, so they carry
different ``rate_definition`` values and can never be each other's donor even when
their numbers coincide.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

#: The rate each stated settlement subject resolves on. They are separate
#: definitions rather than one, because a contract about the target range and a
#: contract about the realized effective rate are not the same claim.
RATE_TARGET_UPPER_BOUND = "upper_bound_federal_funds_target_rate"
RATE_FEDERAL_FUNDS = "federal_funds_rate"
RATE_TARGET_CHANGE_BPS = "target_rate_change_bps"

#: The declared rate definitions a predicate may state.
RATE_DEFINITIONS: tuple[str, ...] = (
    RATE_TARGET_UPPER_BOUND,
    RATE_FEDERAL_FUNDS,
    RATE_TARGET_CHANGE_BPS,
)

#: The outcome axis each family prices. The event axis is the contract's own yes
#: side, so the projection is the identity for both families.
YES_AXIS_LEVEL = "yes_pays_if_target_upper_bound_above_threshold"
YES_AXIS_CHANGE = "yes_pays_if_target_change_at_or_beyond_threshold"

#: Reasons a contract yields no predicate.
REASON_TEXT_UNREADABLE = "published_contract_text_states_no_readable_payout"
REASON_MONTH_NOT_IN_CALENDAR = "contract_month_is_not_dated_by_the_declared_calendar"
REASON_RATE_SUBJECT_UNREADABLE = "contract_title_states_no_readable_settlement_subject"


class PredicateError(ValueError):
    """A contract whose predicate cannot be read, with the reason that says why."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class ParsedPredicate:
    """One contract's payout predicate, with the archived text it was read from.

    The text travels with the predicate so a reader can re-derive the match instead
    of trusting a ticker.
    """

    rate_definition: str
    threshold: Decimal
    inequality: str | None
    yes_axis: str
    orientation: int
    yes_sub_title: str
    title: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "rate_definition": self.rate_definition,
            "threshold": str(self.threshold),
            "inequality": self.inequality,
            "yes_axis": self.yes_axis,
            "orientation": self.orientation,
            "yes_sub_title": self.yes_sub_title,
            "title": self.title,
        }


_ABOVE_RE = re.compile(r"^Above\s+(?P<rate>[0-9]+(?:\.[0-9]+)?)%\s*$")
_BELOW_RE = re.compile(r"^(?P<rate>[0-9]+(?:\.[0-9]+)?)%\s+or\s+below\s*$")
_CHANGE_RE = re.compile(
    r"^(?P<direction>Cut|Hike)\s+(?P<strict>>)?\s*(?P<bps>[0-9]+)\s*bps\s*$",
    re.IGNORECASE,
)
_MAINTAIN_RE = re.compile(r"^(Fed maintains rate|No change)$", re.IGNORECASE)
_TARGET_SUBJECT = "target federal funds rate"
_FUNDS_SUBJECT = "federal funds rate"
_MONTH_CODES: Mapping[str, int] = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_EXPIRY_RE = re.compile(r"^(?P<series>[A-Z]+)-(?P<yy>[0-9]{2})(?P<mon>[A-Z]{3})$")


def _rate_subject(title: str) -> str:
    """The settlement subject a contract's title states, or a refusal.

    ``title`` is read case-insensitively on its plain text. The target range and the
    realized rate are matched in that order, because the target phrase contains the
    shorter one and a substring test in the other order would confuse the two.
    """
    lowered = str(title).lower()
    if _TARGET_SUBJECT in lowered:
        return RATE_TARGET_UPPER_BOUND
    if _FUNDS_SUBJECT in lowered:
        return RATE_FEDERAL_FUNDS
    raise PredicateError(
        REASON_RATE_SUBJECT_UNREADABLE,
        f"the contract title {title!r} states neither the target federal funds rate nor the "
        "federal funds rate, so its payout resolves on no rate this study governs",
    )


def parse_predicate(*, yes_sub_title: str, title: str) -> ParsedPredicate:
    """The payout predicate the archived contract text states, or a refusal."""
    subtitle = str(yes_sub_title or "").strip()
    match = _ABOVE_RE.match(subtitle)
    if match:
        return ParsedPredicate(
            rate_definition=_rate_subject(title),
            threshold=Decimal(match.group("rate")),
            inequality="above",
            yes_axis=YES_AXIS_LEVEL,
            orientation=1,
            yes_sub_title=subtitle,
            title=str(title),
        )
    match = _BELOW_RE.match(subtitle)
    if match:
        # The no side of a level contract. It is recorded with its own orientation so
        # a contract and its complement are never matched to each other.
        return ParsedPredicate(
            rate_definition=_rate_subject(title),
            threshold=Decimal(match.group("rate")),
            inequality="below",
            yes_axis=YES_AXIS_LEVEL,
            orientation=-1,
            yes_sub_title=subtitle,
            title=str(title),
        )
    if _MAINTAIN_RE.match(subtitle):
        return ParsedPredicate(
            rate_definition=RATE_TARGET_CHANGE_BPS,
            threshold=Decimal(0),
            inequality=None,
            yes_axis=YES_AXIS_CHANGE,
            orientation=1,
            yes_sub_title=subtitle,
            title=str(title),
        )
    match = _CHANGE_RE.match(subtitle)
    if match:
        magnitude = Decimal(match.group("bps"))
        direction = match.group("direction").lower()
        signed = magnitude if direction == "hike" else -magnitude
        strict = bool(match.group("strict"))
        return ParsedPredicate(
            rate_definition=RATE_TARGET_CHANGE_BPS,
            threshold=signed,
            # A strict extension is an inequality; an exact stated change is a level,
            # and it is recorded with no direction rather than with a fabricated one.
            inequality=("above" if signed > 0 else "below") if strict else None,
            yes_axis=YES_AXIS_CHANGE,
            orientation=1,
            yes_sub_title=subtitle,
            title=str(title),
        )
    raise PredicateError(
        REASON_TEXT_UNREADABLE,
        f"the contract's published payout text {subtitle!r} is not one of the declared forms "
        "(a level above or below a rate, a stated rate change, or no change), so no predicate "
        "is read and the contract is not admitted to the graph",
    )


def decision_date_for(event_ticker: str, calendar: Mapping[tuple[int, int], dt.date]) -> dt.date:
    """The meeting a contract is about, resolved against the declared calendar.

    ``calendar`` maps ``(year, month)`` to the meeting date the study declares for
    it. A month the calendar does not date is unplaceable: resolving it from the
    ticker would put a contract on a meeting date the study never declared, and the
    previous-decision lookup would then be built on an invented schedule.
    """
    match = _EXPIRY_RE.match(str(event_ticker))
    if match is None:
        raise PredicateError(
            REASON_MONTH_NOT_IN_CALENDAR,
            f"event ticker {event_ticker!r} carries no 'SERIES-YYMMM' expiry this module can "
            "read, so it names no month a calendar could date",
        )
    year = 2000 + int(match.group("yy"))
    month = _MONTH_CODES.get(match.group("mon").upper())
    if month is None:
        raise PredicateError(
            REASON_MONTH_NOT_IN_CALENDAR,
            f"event ticker {event_ticker!r} names month code {match.group('mon')!r}, which is "
            "not a month this module can read",
        )
    date = calendar.get((year, month))
    if date is None:
        raise PredicateError(
            REASON_MONTH_NOT_IN_CALENDAR,
            f"the declared calendar dates no meeting in {year}-{month:02d}, so the contract "
            f"{event_ticker!r} cannot be placed on the exposure axis",
        )
    return date


def threshold_decimal(value: Any) -> Decimal:
    """A threshold as an exact decimal, refusing anything that is not one."""
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PredicateError(
            REASON_TEXT_UNREADABLE, f"threshold {value!r} is not a decimal"
        ) from exc
