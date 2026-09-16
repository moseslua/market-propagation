"""Bounded G0 coverage audit.

What this produces, per plan gate G0: for a prespecified set of historical release
dates, the attempted/acquired/eligible contract universe including **inactive**
contracts, whether each endpoint actually supports the needed filters and
pagination, the real history spacing, and an explicit record of everything that
could not be retrieved. The output is a machine-readable directory plus one fully
raw-provenanced event card.

The event cohort is **not** defined here. It comes from the study's own cohort
configuration (``configs/cohort.yaml``), whose events carry an aware
``scheduled_at``, a canonical ``family``, a ``reference_period`` and the source
URLs they were read from. :meth:`CohortAuditor.audit_cohort` requires that input
and refuses an empty cohort, so no second cohort can exist inside this module.

Design rules this module enforces, because G0 is exactly where a study is most
tempted to lie to itself:

**No post-event volume selection.** Candidates come from the series' own historic
market listing, filtered only by the contract's own lifecycle times. A contract
that existed before the release but traded nothing afterwards is still a
candidate. Its low liquidity is an *observed property*, recorded as such, never a
reason to drop it.

**A closed market is not a null.** When a post-release window shows no quotes, the
audit checks the contract's own ``close_time``. A market that had already closed
is recorded in its own ``direct_closed_pre_release`` cohort, which is the direct
resolution study's material rather than the downstream propagation cohort. It is
never recorded as an observation of no movement, and never filled forward.

**Attempted is not acquired, and acquired is not eligible.** Every event reports
how many contracts the venue returned, how many were successfully acquired, how
many the prespecified lifecycle rules admit, and how many survive the full
empirical chain. An HTTP 200, a returned market row or a returned candle is
evidence that a request succeeded, never that a contract is usable.

**Partial coverage is never reported as complete.** Every event carries explicit
coverage gates, and the audit's top-level ``complete`` is true only when every
event is ``audited``, every event-level gate is satisfied *and* every cohort-level
scientific gate is satisfied. A truncated page walk, a bounded candidate
selection, a blocked series, an interior candle hole or a missing quote side is
reported as an unsatisfied gate that blocks that claim. ``acquisition_complete``
reports the read alone, so a run that walked every page can be acquisition-complete
while its cohort is not study-eligible, and the two are never conflated.

**Requested versus observed resolution.** Every candle query records what was
requested and what came back, and whether holes were present. Candle frequency is
never presented as order-book depth.

**Discovery is bounded, not global.** Downstream series are found from the
exchange's own listing by keyword. A keyword filter cannot establish that the
returned set is the complete universe, and the audit says so rather than claiming
completeness.

**The primary cohort is the configured downstream policy exposure.** The study's
primary cohort is the contract whose payoff depends on an event later than the
release, and the release's prespecified exposure is a later US policy-rate
decision. That cohort is named by the cohort configuration's
``candidate_contract_families`` section, including the observed venue series
identifiers for both the current and the legacy series prefix. A series reached by
a keyword match alone is not a policy contract: it must carry a US Federal
Reserve settlement source, an economics category and a policy-rate or
policy-meeting title, and a foreign central-bank, foreign CPI, Fed-personnel or
Fed-communication series is recorded as excluded rather than admitted. The
prespecified series are ordered ahead of any keyword find, so a cost bound can
never truncate the primary cohort in favour of an alphabetical lookalike.

**One universe, evaluated per event.** The contract universe a release is
measured against is the same for every event in a run, so the listing pages are
fetched once per request identity and reused, and each event then applies its own
lifecycle eligibility to those records. A fetch repeated per event produces
identical bodies and no new evidence.

**A record's fields do not certify its rule vintage.** A market's ``open_time`` and
``created_time`` date the market, not the rule text a later fetch returned, so
neither is a rule-version timestamp and neither is admitted as one. Full study
eligibility therefore rests on an explicit, configured verification record: the
contract it describes, the sha256 digest of the rule text it certifies, the source
that text was read from, the method that verified it, and the instants that bound
which version was in force. A record missing any of that is a configuration fault,
and a list of bare market identifiers is not such a record however many it names.
With no verified record the fetched rule hash stays uncertified: no contract is
study-eligible however many are lifecycle-eligible, and both the rule-version and
the settlement-semantics gate stay unsatisfied. That is a statement about the
evidence supplied, not a hardcoded gate -- declaring a complete record in the
cohort configuration satisfies them with no change here. Catalog metadata existing
is not the same fact as the metadata having been verified.

The audit is bounded on every axis: events, series, contracts per event, candle
windows, pagination pages and transport attempts.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import pathlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..domain import parse_utc_time
from ..storage import RawStore
from .kalshi_rest import (
    KalshiClient,
    PartitionDecision,
    inspect_candle_spacing,
    parse_fixed_point_count,
    parse_fixed_point_dollars,
)
from .macro_releases import MacroReleaseClient
from .normalize import (
    CANDLE_SCHEMA_HISTORICAL,
    CANDLE_SCHEMA_LIVE,
    rule_hash,
    trade_direction_conflicts,
)
from .pagination import (
    PaginationResult,
    PointerResolutionError,
    RecordOrigin,
)
from .transport import TransportError, WireShapeError

#: Series considered for each release family, in descending priority. Discovered
#: contract series are appended after these, never instead of them. Keyed by the
#: canonical family name the cohort configuration uses. These are the *direct*
#: release families, whose own contracts settle on the announced statistic; the
#: primary downstream cohort is the configured policy exposure below.
FAMILY_SERIES: Mapping[str, tuple[str, ...]] = {
    "cpi": ("KXCPI", "KXCPIYOY"),
    "employment": ("KXPAYROLLS",),
}

#: Canonical cohort family to BLS archive slug. This is the single place the
#: employment family is translated into the venue's ``empsit`` slug, so the
#: translation lives at the BLS boundary rather than in the cohort vocabulary.
FAMILY_RELEASE_SLUG: Mapping[str, str] = {
    "cpi": "cpi",
    "employment": "empsit",
}

#: The configuration key naming the study's primary downstream cohort, and the
#: family key inside it that carries the US policy-rate exposure. The cohort is
#: read from the study's own configuration so this module holds no second cohort.
POLICY_COHORT_CONFIG_KEY = "candidate_contract_families"
POLICY_COHORT_SECTION = "policy_linked_downstream"
POLICY_FAMILY_KEY = "policy_rate_decision"

#: Configuration keys a policy family may use to name the venue series it covers.
#: ``observed_venue_series`` holds identifiers observed in the exchange's own
#: catalog; ``candidate_venue_series`` holds leads that have not been observed and
#: are therefore not queried as if they had been.
POLICY_SERIES_FIELDS = ("observed_venue_series", "candidate_venue_series")

#: Configuration key naming a family's independently verified rule-version
#: records. The _record_ is the evidence: the contract it describes, the sha256
#: digest of the rule text it certifies, the source that text was read from, the
#: method that verified it, the interval over which that version was in force and
#: the instant the verification was performed. A list of bare market identifiers
#: cannot carry any of that, which is why identifiers alone are refused rather than
#: read as verification.
RULE_VERSION_EVIDENCE_KEY = "verified_rule_versions"

#: Facts a verified rule-version record must state. A record missing any of them is
#: a configuration fault raised at the boundary that reads it, not a partial record
#: that silently certifies less than it appears to.
REQUIRED_RULE_VERSION_EVIDENCE_FIELDS = (
    "contract_id",
    "rule_hash",
    "source_url",
    "verified_by",
    "in_force_from",
    "observed_at",
    "settlement_semantics",
)

#: ``rule_hash`` is a sha256 digest of rule text, so the digest shape is checked
#: rather than trusted: a truncated or non-hex value would compare unequal to every
#: fetched rule text and silently certify nothing, or worse, look like an id.
_RULE_HASH_PATTERN = re.compile(r"[0-9a-f]{64}")

#: Downstream series found by keyword when the configuration names none for a
#: family. A keyword match is a candidate set, not a proven-complete universe,
#: and it is never sufficient to admit a contract into the policy cohort.
DOWNSTREAM_SERIES_QUERIES: Mapping[str, tuple[str, ...]] = {
    "cpi": ("inflation", "cpi"),
    "employment": ("payroll", "jobs", "unemployment", "employment"),
}

#: Positive evidence that a series is a US policy-rate or policy-meeting contract
#: rather than a keyword lookalike. All three must hold: the settlement source
#: must be a US Federal Reserve property, the category must be economics, and the
#: title must name a policy rate or a policy meeting. A foreign central bank
#: clears the third test and fails the first, which is why the source is checked
#: rather than the title alone.
POLICY_SETTLEMENT_HOST = "federalreserve.gov"
POLICY_CATEGORY = "economics"
_POLICY_TITLE_PATTERN = re.compile(
    r"fomc|fed funds|federal funds|policy interest rate|interest rate decision"
    r"|rate decision|fed meeting|fed rate|rate cut|rate hike"
)

#: Series excluded from the policy cohort even when they clear the positive
#: evidence. Fed personnel, Fed communications and foreign inflation contracts are
#: exposures to a different question than a policy-rate outcome: a confirmation
#: vote, a social-media mention or a foreign CPI print does not pay on the
#: Federal Reserve's own policy decision.
POLICY_EXCLUSION_PATTERNS: Mapping[str, str] = {
    "fed_personnel_or_nomination": (
        r"fed chair|fed board|fed governor|fed vice|fed nominee|fed nom|"
        r"senate.*vote for.*fed|confirmed as fed|to chair the .*fomc"
    ),
    "fed_communication_or_mention": r"fed tweet|fed mention|fed dissent|dot plot|fedwatch",
    "foreign_or_non_us_inflation": (
        r"brazil|argentina|china|cpi in china|canada|euro|eu |france|italy|japan|"
        r"uk |britain|iran|hungary|sweden|poland|czech|swiss|norway|turkey|"
        r"india|indonesia|korea|australia|mexico|israel|zealand|south africa"
    ),
}

#: Direct release families whose contracts settle on the announced statistic.
#: Kept beside the policy cohort so a direct contract is never counted as a
#: downstream policy contract, and so the separate direct-resolution study keeps
#: its own material.
DIRECT_RELEASE_SERIES: tuple[str, ...] = ("KXCPI", "KXCPIYOY", "KXPAYROLLS", "KXU3")

#: Why a CPI or employment release is an exposure to a later policy outcome. This
#: is the mechanism the primary cohort rests on, recorded with the result so the
#: relation is stated rather than assumed.
POLICY_EXPOSURE_MECHANISM = (
    "the release updates information about a later US policy-rate decision, so a "
    "contract paying on that later decision carries economic exposure to this "
    "release; that exposure is a hypothesis about later payoff, not a verified "
    "equivalence between the release and the policy contract"
)

#: Default measurement window around a release, in seconds before and after. The
#: canonical study window is 30 minutes pre and 60 minutes post
#: (``configs/event_windows.yaml``); the caller supplies the configured values and
#: these are only the defaults when it does not.
DEFAULT_BEFORE_SECONDS = 1800
DEFAULT_AFTER_SECONDS = 3600

#: Requested candle resolutions, coarse to fine. The audit reports which ones the
#: endpoint actually honours, which is the point of the exercise.
REQUESTED_INTERVALS = (60, 1)

#: Contract cohort labels. ``direct_closed_pre_release`` holds contracts whose own
#: lifecycle closed them before the release, which are the separate direct
#: resolution study rather than the primary post-release propagation cohort.
COHORT_DOWNSTREAM = "downstream"
COHORT_DIRECT_CLOSED_PRE_RELEASE = "direct_closed_pre_release"

#: Liquidity thresholds are reported, never used to drop a candidate.
THIN_VOLUME_FP = Decimal("10.00")


#: Fields a cohort event must carry to be auditable. The canonical configuration
#: (``configs/cohort.yaml``) supplies each one; a missing field is a configuration
#: fault rather than something to guess at.
REQUIRED_EVENT_FIELDS = ("event_id", "family", "reference_period", "scheduled_at")

#: Prefix of a cohort row's source URL fields, all of which are kept in the audit
#: result so a reader can re-derive the schedule rather than trust it.
_EVENT_URL_FIELDS = (
    "source_url",
    "calendar_url",
    "initial_release_url",
)


def event_cohort(
    config: Mapping[str, Any], *, events: Sequence[Mapping[str, Any]] | None = None
) -> tuple[dict[str, Any], ...]:
    """Read the study's event cohort from its own configuration mapping.

    The canonical shape is ``cohort.yaml``'s: a top-level ``events`` sequence
    whose rows carry ``event_id``, ``family``, ``reference_period``,
    ``scheduled_at`` (an aware ISO instant), and the calendar/archive URLs each
    date was read from. The URLs are retained in the audit output because the
    schedule must be re-derivable rather than trusted.

    ``events`` overrides the sequence for a bounded run; it is passed through the
    same validation, so an override cannot introduce a differently shaped row.
    """
    rows = events if events is not None else config.get("events")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ValueError(
            "cohort configuration must carry an 'events' sequence; the audit no "
            "longer supplies a default cohort of its own"
        )
    if not rows:
        raise ValueError(
            "cohort configuration carries no events; an empty cohort cannot be "
            "audited and must not be reported as a complete one"
        )
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"cohort event {index} is not a mapping: {row!r}")
        missing = [name for name in REQUIRED_EVENT_FIELDS if row.get(name) in (None, "")]
        if missing:
            raise ValueError(
                f"cohort event {index} is missing {', '.join(missing)}; every event "
                "must state its own identity, family, reference period and instant"
            )
        event_id = str(row["event_id"])
        if event_id in seen:
            raise ValueError(
                f"cohort event id {event_id!r} appears twice; two releases sharing an "
                "id cannot be separated in any later join"
            )
        seen.add(event_id)
        # The family is validated here rather than only at audit time, so a bad
        # cohort mapping fails at the boundary that reads it.
        _event_family(row)
        out.append(
            {**row, "scheduled_at": parse_utc_time(row["scheduled_at"], field_name="scheduled_at")}
        )
    return tuple(out)


def _event_family(row: Mapping[str, Any]) -> str:
    """The canonical release family a cohort row names."""
    family = str(row["family"])
    if family not in FAMILY_RELEASE_SLUG:
        raise ValueError(
            f"cohort event {row.get('event_id')!r} names family {family!r}, which has "
            f"no BLS release slug; known families are {sorted(FAMILY_RELEASE_SLUG)}"
        )
    return family


@dataclass(frozen=True, slots=True)
class PolicyCohort:
    """The configured primary downstream cohort, with where each series came from.

    ``configured_series`` are the venue series the study's own configuration names
    for the policy exposure, in the order it names them; ``unobserved_leads`` are
    leads the configuration names but that no catalog inspection has confirmed, so
    they are reported and not queried as though they were observed. The prespecified
    order is the prioritization: a cost bound truncates the tail, never the head.
    """

    family_key: str
    configured_series: tuple[str, ...]
    unobserved_leads: tuple[str, ...]
    strike_dependency: str | None
    relation_type: str | None
    verified_contract_ids: tuple[str, ...]
    configured: bool = False
    max_events_per_series: int = 60
    max_markets_per_event_listing: int = 200

    def as_dict(self) -> dict[str, Any]:
        return {
            "family_key": self.family_key,
            "configured": self.configured,
            "configured_series": list(self.configured_series),
            "unobserved_leads": list(self.unobserved_leads),
            "strike_dependency": self.strike_dependency,
            "relation_type": self.relation_type,
            "verified_contract_ids": list(self.verified_contract_ids),
            "verified_contract_count": len(self.verified_contract_ids),
            "exposure_mechanism": POLICY_EXPOSURE_MECHANISM,
            "verified_payoff_equivalence_claimed": False,
            "selection_basis": "configured_policy_cohort_series_order",
        }


@dataclass(frozen=True, slots=True)
class PolicySeriesVerdict:
    """One discovered series, with the evidence that admitted or excluded it.

    A verdict carries its own evidence so a reader can re-derive the decision
    rather than trust the label. ``source_host`` is the decisive field: a foreign
    central-bank contract names a policy-rate meeting in its title but does not
    settle on a US Federal Reserve source, and that is the difference between the
    primary cohort and a lookalike.
    """

    ticker: str
    admitted: bool
    reason: str
    title: str
    category: str
    source_host: str | None
    exclusion_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "admitted": self.admitted,
            "reason": self.reason,
            "title": self.title,
            "category": self.category,
            "source_host": self.source_host,
            "exclusion_reason": self.exclusion_reason,
        }


def policy_cohort(config: Mapping[str, Any]) -> PolicyCohort:
    """Read the study's primary policy cohort from its own configuration mapping.

    The cohort is *configured*, not hardcoded: the section, family key and series
    lists all come from the configuration, so a second cohort cannot be smuggled
    into this module.

    Only ``observed_venue_series`` is treated as queryable. A lead in
    ``candidate_venue_series`` is recorded as unobserved and left alone, because a
    ticker that no catalog inspection confirmed would otherwise be requested as
    though it had been observed.

    An absent section is reported as ``configured=False`` rather than raised on.
    A cohort-only run is a legitimate bounded run, and the audit records the absent
    primary cohort as an unsatisfied gate instead of refusing to run at all. The
    audit that *is* claiming policy coverage must configure it.
    """
    empty = PolicyCohort(
        family_key=POLICY_FAMILY_KEY,
        configured_series=(),
        unobserved_leads=(),
        strike_dependency=None,
        relation_type=None,
        verified_contract_ids=(),
        configured=False,
    )
    section = config.get(POLICY_COHORT_CONFIG_KEY)
    if not isinstance(section, Mapping):
        return empty
    families = section.get(POLICY_COHORT_SECTION)
    if not isinstance(families, Sequence) or isinstance(families, (str, bytes)):
        return empty
    chosen: Mapping[str, Any] | None = None
    for entry in families:
        if isinstance(entry, Mapping) and str(entry.get("family_key")) == POLICY_FAMILY_KEY:
            chosen = entry
            break
    if chosen is None:
        return empty

    observed: list[str] = []
    leads: list[str] = []
    for series_field in POLICY_SERIES_FIELDS:
        raw = chosen.get(series_field)
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            continue
        target = observed if series_field == "observed_venue_series" else leads
        for value in raw:
            ticker = str(value).strip()
            if ticker and ticker not in target:
                target.append(ticker)
    verified = chosen.get("verified_contract_ids")
    return PolicyCohort(
        family_key=str(chosen.get("family_key")),
        configured_series=tuple(observed),
        unobserved_leads=tuple(leads),
        strike_dependency=(
            str(chosen["strike_dependency"]) if chosen.get("strike_dependency") else None
        ),
        relation_type=(str(chosen["relation_type"]) if chosen.get("relation_type") else None),
        verified_contract_ids=(
            tuple(str(v) for v in verified)
            if isinstance(verified, Sequence) and not isinstance(verified, (str, bytes))
            else ()
        ),
        configured=True,
        max_events_per_series=_configured_bound(chosen, "max_events_per_series", 60),
        max_markets_per_event_listing=_configured_bound(
            chosen, "max_markets_per_event_listing", 200
        ),
    )


def _configured_bound(entry: Mapping[str, Any], name: str, default: int) -> int:
    """A positive integer bound from a family's ``series_selection_bounds``."""
    bounds = entry.get("series_selection_bounds")
    if not isinstance(bounds, Mapping):
        return default
    value = bounds.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return default
    return value


def classify_policy_series(item: Mapping[str, Any]) -> PolicySeriesVerdict:
    """Decide whether a catalog series is a US policy-rate contract, with evidence.

    A keyword match is not enough and a title match is not enough. The series must
    settle on a US Federal Reserve property, carry the economics category, and name
    a policy rate or policy meeting. A foreign central bank's rate-decision contract
    matches the title and fails the source test; a Fed confirmation vote or a Fed
    mention carries the Fed source and fails the title test; both are recorded as
    exclusions with the reason that decided it rather than silently dropped.
    """
    ticker, title, category, host = _series_metadata_normalized(item)
    hosts = _settlement_hosts(item)
    # The *primary* settlement source decides, not any host in the list. A series
    # whose source list is dominated by news outlets with one Federal Reserve URL
    # somewhere in it settles from the news list, and admitting it on that basis
    # would let a media-summarised contract pass as a policy-rate contract while a
    # genuinely US-sourced one is indistinguishable from it.
    primary = hosts[0] if hosts else ""
    fed_source = POLICY_SETTLEMENT_HOST in primary
    is_economics = category.strip().lower() == POLICY_CATEGORY
    title_matches = bool(_POLICY_TITLE_PATTERN.search(f"{title} {ticker}".lower()))

    haystack = f"{ticker} {title}".lower()
    for name, pattern in POLICY_EXCLUSION_PATTERNS.items():
        if re.search(pattern, haystack):
            return PolicySeriesVerdict(
                ticker=ticker,
                admitted=False,
                reason="excluded_by_pattern",
                title=title,
                category=category,
                source_host=host,
                exclusion_reason=name,
            )
    if not fed_source:
        return PolicySeriesVerdict(
            ticker=ticker,
            admitted=False,
            reason="no_us_federal_reserve_settlement_source",
            title=title,
            category=category,
            source_host=host,
            exclusion_reason="not_us_policy_source",
        )
    if not is_economics:
        return PolicySeriesVerdict(
            ticker=ticker,
            admitted=False,
            reason="category_is_not_economics",
            title=title,
            category=category,
            source_host=host,
            exclusion_reason="category_not_economics",
        )
    if not title_matches:
        return PolicySeriesVerdict(
            ticker=ticker,
            admitted=False,
            reason="title_names_no_policy_rate_or_meeting",
            title=title,
            category=category,
            source_host=host,
            exclusion_reason="title_not_policy_rate",
        )
    return PolicySeriesVerdict(
        ticker=ticker,
        admitted=True,
        reason="fed_settlement_source_with_policy_rate_title",
        title=title,
        category=category,
        source_host=host,
    )


def _settlement_hosts(item: Mapping[str, Any]) -> list[str]:
    """Hosts of a series' settlement sources, lowercased and bare."""
    hosts: list[str] = []
    sources = item.get("settlement_sources")
    if not isinstance(sources, Sequence) or isinstance(sources, (str, bytes)):
        return hosts
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        url = str(source.get("url") or "")
        host = url.split("//", 1)[-1].split("/", 1)[0].lower().strip()
        if host:
            hosts.append(host)
    return hosts


def _series_metadata_normalized(item: Mapping[str, Any]) -> tuple[str, str, str, str | None]:
    """Ticker, title, category and first settlement host of a catalog record."""
    hosts = _settlement_hosts(item)
    return (
        str(item.get("ticker") or ""),
        str(item.get("title") or ""),
        str(item.get("category") or ""),
        hosts[0] if hosts else None,
    )


def _configured_families(config: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """Every configured candidate family, in configuration order.

    Read so a run can report the families it deliberately did *not* audit. A
    configured family that is not the primary policy cohort is an exclusion to be
    named, not a silently absent row.
    """
    section = config.get(POLICY_COHORT_CONFIG_KEY)
    if not isinstance(section, Mapping):
        return ()
    out: list[Mapping[str, Any]] = []
    for value in section.values():
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            continue
        for entry in value:
            if isinstance(entry, Mapping) and entry.get("family_key"):
                out.append(entry)
    return tuple(out)


@dataclass(frozen=True, slots=True)
class RuleVersionEvidence:
    """One independently verified rule-version record.

    This is the only evidence that certifies a fetched rule hash as the version in
    force at a release; a contract's own fields cannot, because ``open_time`` and
    ``created_time`` date the market rather than the rule text a later fetch
    returned. Every field is load-bearing:

    ``rule_hash`` binds the verdict to exact rule text, so re-fetched text that
    changed stops being certified instead of inheriting an earlier verdict.
    ``in_force_from`` and ``in_force_to`` state the interval the version was live, so
    a record cannot certify a release that predates it, and an open interval is
    stated as such rather than left to mean "forever". ``source_url`` and
    ``verified_by`` name where the text was read and by what method, so a reader can
    re-derive the verdict instead of trusting it. ``observed_at`` dates the
    verification rather than the rule. ``settlement_semantics`` states what the
    verified text resolves on, which is what separates a verified rule version from a
    verified payoff equivalence.
    """

    contract_id: str
    rule_hash: str
    source_url: str
    verified_by: str
    in_force_from: dt.datetime
    in_force_to: dt.datetime | None
    observed_at: dt.datetime
    settlement_semantics: str

    def applies_to(self, *, rule_hash: str, at: dt.datetime) -> bool:
        """Whether this record certifies ``rule_hash`` as in force at ``at``."""
        if rule_hash != self.rule_hash:
            return False
        if at < self.in_force_from:
            return False
        return self.in_force_to is None or at < self.in_force_to

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "rule_hash": self.rule_hash,
            "source_url": self.source_url,
            "verified_by": self.verified_by,
            "in_force_from": self.in_force_from.isoformat(),
            "in_force_to": self.in_force_to.isoformat() if self.in_force_to else None,
            "in_force_to_is_open": self.in_force_to is None,
            "observed_at": self.observed_at.isoformat(),
            "settlement_semantics": self.settlement_semantics,
        }


def _parse_rule_version_evidence(item: Any, *, where: str) -> RuleVersionEvidence:
    """Validate one configured rule-version record at the boundary that read it.

    A malformed record is refused rather than accepted as far as it parses. A record
    missing its source, its rule hash or its validity interval has not verified
    anything, and admitting it would let an incomplete entry certify a rule version
    on the strength of the fields it happened to carry.
    """
    if not isinstance(item, Mapping):
        raise ValueError(
            f"{where} must be a mapping of rule-version evidence, got {type(item).__name__}"
        )
    missing = [
        name for name in REQUIRED_RULE_VERSION_EVIDENCE_FIELDS if item.get(name) in (None, "")
    ]
    if missing:
        raise ValueError(
            f"{where} is missing {', '.join(missing)}; a rule version is certified "
            "only by a record that names the contract, the rule hash it certifies, "
            "the source that text was read from, the method that verified it, the "
            "interval it was in force and the instant it was verified. A bare market "
            "identifier is not such a record."
        )
    contract_id = str(item["contract_id"]).strip()
    rule_digest = str(item["rule_hash"]).strip()
    if not _RULE_HASH_PATTERN.fullmatch(rule_digest):
        raise ValueError(
            f"{where}.rule_hash {rule_digest!r} is not a sha256 digest of rule text; "
            "the evidence is bound to exact rule text, so an identifier or a "
            "truncated digest cannot stand in for one"
        )
    source_url = str(item["source_url"]).strip()
    if not source_url.startswith(("http://", "https://")):
        raise ValueError(f"{where}.source_url {source_url!r} does not name a readable source")
    in_force_from = parse_utc_time(item["in_force_from"], field_name=f"{where}.in_force_from")
    in_force_to = (
        parse_utc_time(item["in_force_to"], field_name=f"{where}.in_force_to")
        if item.get("in_force_to")
        else None
    )
    if in_force_to is not None and in_force_to <= in_force_from:
        raise ValueError(
            f"{where}.in_force_to {in_force_to.isoformat()} is not after "
            f"in_force_from {in_force_from.isoformat()}; an empty interval certifies "
            "no release"
        )
    return RuleVersionEvidence(
        contract_id=contract_id,
        rule_hash=rule_digest,
        source_url=source_url,
        verified_by=str(item["verified_by"]).strip(),
        in_force_from=in_force_from,
        in_force_to=in_force_to,
        observed_at=parse_utc_time(item["observed_at"], field_name=f"{where}.observed_at"),
        settlement_semantics=str(item["settlement_semantics"]).strip(),
    )


def rule_version_evidence(config: Mapping[str, Any]) -> tuple[RuleVersionEvidence, ...]:
    """Every configured verified rule-version record, read from the study's config.

    Each configured candidate family may carry ``verified_rule_versions``. A family
    that carries only ``verified_contract_ids`` supplies no record here: an
    identifier list states which markets someone believed were verified, not what
    was verified about them, and it stays a reported fact rather than becoming proof.

    Two records for the same contract and rule hash are refused, because a later
    lookup would have to choose one and the choice would not be derivable from the
    configuration.
    """
    records: list[RuleVersionEvidence] = []
    seen: set[tuple[str, str]] = set()
    for entry in _configured_families(config):
        raw = entry.get(RULE_VERSION_EVIDENCE_KEY)
        if raw is None:
            continue
        where = f"{POLICY_COHORT_CONFIG_KEY}.{entry.get('family_key')}.{RULE_VERSION_EVIDENCE_KEY}"
        if isinstance(raw, (str, bytes)) or not isinstance(raw, Sequence):
            raise ValueError(
                f"{where} must be a sequence of rule-version records, got {type(raw).__name__}"
            )
        for index, item in enumerate(raw):
            record = _parse_rule_version_evidence(item, where=f"{where}[{index}]")
            key = (record.contract_id, record.rule_hash)
            if key in seen:
                raise ValueError(
                    f"{where}[{index}] repeats the evidence for contract "
                    f"{record.contract_id!r} at rule hash {record.rule_hash!r}; two "
                    "records for one version would make the verdict depend on "
                    "configuration order"
                )
            seen.add(key)
            records.append(record)
    return tuple(records)


def _applicable_rule_evidence(
    evidence: Sequence[RuleVersionEvidence],
    *,
    contract_id: str,
    rule_hash: str,
    at: dt.datetime,
) -> RuleVersionEvidence | None:
    """The configured record certifying this contract's rule hash at this instant."""
    for record in evidence:
        if record.contract_id == contract_id and record.applies_to(rule_hash=rule_hash, at=at):
            return record
    return None


def _study_eligibility_gates(
    audits: Sequence[EventAudit],
    policy: PolicyCohort,
    policy_series: Sequence[str],
    evidence: Sequence[RuleVersionEvidence],
) -> dict[str, Any]:
    """Lifecycle-eligible versus fully study-eligible, and what still gates the leap.

    The two counts are reported apart because they rest on different evidence.
    Lifecycle eligibility comes from the contract's own ``open_time`` and
    ``close_time``. Full study eligibility additionally requires a configured,
    verified rule-version record that names the market, matches the rule hash
    actually fetched and covers the release instant -- so a contract created before
    the release is not thereby certified to have carried that rule version, and
    neither a catalog settlement-source name nor a bare identifier list is read as
    verification.

    Both scientific gates are derived from that same decision rather than asserted
    beside it, so a gate cannot read satisfied while the count it guards is zero and
    no count can disagree with its gate. They are reported apart because they block
    different claims. One record carries the version binding, the validity interval
    and the settlement semantics together, which is what keeps the two facets from
    drifting apart.
    """
    downstream = [c for e in audits for c in e.downstream_candidates]
    lifecycle = sum(1 for c in downstream if c.eligible)
    verified = sum(1 for c in downstream if c.study_eligible)
    uncovered = [c for c in downstream if c.eligible and c.rule_evidence is None]
    records = tuple(evidence)
    # The gates guard claims about the cohort's contracts, so a run that reached no
    # lifecycle-eligible contract establishes nothing about rule versions and says
    # so, rather than reading satisfied on the strength of a vacuous universal.
    scientific = bool(records) and lifecycle > 0 and not uncovered
    covered = sorted({c.ticker for c in downstream if c.rule_evidence is not None})
    if not records:
        rule_detail = (
            "no verified rule-version record is configured for this cohort, and a "
            "market's own creation and open times date the market rather than the "
            "rule text a later fetch returned; with nothing to bind a fetched rule "
            "hash to an interval in force at the release, no lifecycle-eligible "
            "contract is promoted to verified semantics"
        )
        semantics_detail = (
            "no verified rule-version record is configured, so no contract's "
            "settlement semantics have been read from a named source by a stated "
            "method; a catalog settlement-source host names an authority the series "
            "intends to resolve from, which is not that verification"
        )
    elif not lifecycle:
        rule_detail = (
            "no lifecycle-eligible contract was reached by this run, so no rule "
            "version in force at a release is established here; this is a coverage "
            "limit rather than a verified cohort of zero"
        )
        semantics_detail = (
            "no lifecycle-eligible contract was reached by this run, so no contract's "
            "settlement semantics are established here"
        )
    elif uncovered:
        rule_detail = (
            f"{len(uncovered)} lifecycle-eligible contract(s) carry no verified "
            "rule-version record covering the release "
            f"(for example {[c.ticker for c in uncovered[:5]]}); a configured record "
            "certifies only the exact rule hash and interval it names"
        )
        semantics_detail = (
            f"{len(uncovered)} lifecycle-eligible contract(s) have no record stating "
            "their settlement semantics, so the exact statistic, threshold basis and "
            "rounding the release contract and the policy contract each use remain "
            "unverified for them"
        )
    else:
        rule_detail = (
            f"every lifecycle-eligible contract ({lifecycle}) is covered by a "
            f"configured verified rule-version record naming {covered}, so each "
            "fetched rule hash is certified as the version in force at its release"
        )
        semantics_detail = (
            f"every study-eligible contract ({verified}) carries a record naming the "
            "source its rule text was read from and the settlement semantics it "
            "establishes"
        )
    return {
        "lifecycle_eligible_downstream": lifecycle,
        "study_eligible_downstream": verified,
        "lifecycle_eligible_is_not_study_eligible": lifecycle != verified,
        "lifecycle_eligible_without_verified_rule_version": len(uncovered),
        "policy_series_configured": list(policy.configured_series),
        "policy_series_queried": list(policy_series),
        "policy_cohort_verified_contract_ids": list(policy.verified_contract_ids),
        "verified_rule_version_record_count": len(records),
        "verified_rule_version_contract_ids": covered,
        "evidence_kind": "configured_rule_version_record",
        "evidence_requirements": list(REQUIRED_RULE_VERSION_EVIDENCE_FIELDS),
        "rule_vintage_gate": {
            "satisfied": scientific,
            "gate": "rule_version_in_force_at_release_verified",
            "detail": rule_detail,
            "blocks": (
                "any claim that a candidate contract's payoff rule was the rule in "
                "force at the release",
                "any claim that a lifecycle-eligible contract is a verified study input",
            )
            if not scientific
            else (),
        },
        "source_semantics_gate": {
            "satisfied": scientific,
            "gate": "settlement_source_semantics_verified",
            "detail": semantics_detail,
            "blocks": (
                "any payoff-equivalence claim between a release contract and a policy contract",
                "any statement that a policy contract's payoff is the release outcome",
            )
            if not scientific
            else (),
        },
    }


class _UniverseCache:
    """Contract universes fetched once per request identity, then reused.

    The universe a release is measured against does not vary with the event, so
    refetching it per event returns identical bodies and adds no evidence. This
    holds the pages for the whole run, keyed by the request that produced them:
    the listing kind, the series filter and the bound. Lifecycle eligibility is
    *not* cached — each event applies its own to the same records, which is what
    keeps a shared fetch from implying a shared answer.

    Nothing is persisted across runs. A cached listing carries the cutoffs and
    bounds it was fetched under, so a later run with a moved cutoff refetches
    rather than inheriting a stale page.
    """

    def __init__(self, kalshi: KalshiClient, *, max_pages: int) -> None:
        self._kalshi = kalshi
        self._max_pages = max_pages
        self._entries: dict[tuple[str, str, int | None], PaginationResult] = {}
        self._hits = 0
        self._misses = 0

    def fetch(
        self,
        *,
        kind: str,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        max_items: int | None = None,
    ) -> PaginationResult:
        key = (kind, series_ticker or event_ticker or "", max_items)
        if key in self._entries:
            self._hits += 1
            return self._entries[key]
        self._misses += 1
        if kind == "events":
            result = self._kalshi.list_events(
                series_ticker=series_ticker,
                limit=200,
                max_pages=self._max_pages,
                max_items=max_items,
            )
        elif kind == "historical":
            result = self._kalshi.list_historical_markets(
                series_ticker=series_ticker,
                event_ticker=event_ticker,
                limit=1000,
                max_pages=self._max_pages,
                max_items=max_items,
            )
        elif kind == "live":
            result = self._kalshi.list_markets(
                series_ticker=series_ticker,
                limit=1000,
                max_pages=self._max_pages,
                max_items=max_items,
            )
        else:
            raise ValueError(f"unknown listing kind {kind!r}")
        self._entries[key] = result
        return result

    def as_dict(self) -> dict[str, Any]:
        return {
            "requests_served_from_cache": self._hits,
            "distinct_request_identities": len(self._entries),
            "cache_scope": "single_run_in_memory",
            "persisted_across_runs": False,
            "identity_includes": ["listing_kind", "series_or_event_filter", "item_bound"],
            "lifecycle_eligibility_cached": False,
            "cutoff_fetched_per_event": True,
            "cutoff_cache_scope": "not_cached_by_design",
            "cutoff_note": (
                "the partition cutoff is deliberately not cached: each event records "
                "the cutoff it reconciled against, so a cutoff that moves underneath a "
                "running audit is visible in the result instead of hidden behind a "
                "run-scoped constant. It is one small request per event, not the "
                "series-listing walk this cache exists to stop repeating"
            ),
            "note": (
                "the universe does not vary with the release event, so it is fetched "
                "once per request identity and each event applies its own lifecycle "
                "eligibility to the same records; nothing is persisted between runs"
            ),
        }


class CandidateOriginMismatch(RuntimeError):
    """A record whose archived origin could not be resolved to that record."""


@dataclass(frozen=True, slots=True)
class VerifiedOrigin:
    """A :class:`RecordOrigin` resolved against the bytes the store archived.

    Holding one of these means the page ``origin.raw_hash`` names was read, the
    origin's pointer resolved inside it, and the record found there is the record
    that was cited. The digest carried here is the one the *stored bytes* produced:
    a digest computed from a copy in hand would agree with itself whether or not
    anything was archived behind it.

    A candidate can only be built from one, and the verifier is the only producer,
    so a candidate citing an unchecked pointer does not exist.
    """

    origin: RecordOrigin
    #: Digest of the record as read back from the archived page.
    record_hash: str

    @property
    def raw_hash(self) -> str:
        """The page hash, which ``RawStore.get`` resolves to the original bytes."""
        return self.origin.raw_hash

    @property
    def pointer(self) -> str:
        """RFC 6901 pointer to this record inside the page ``raw_hash`` names."""
        return self.origin.pointer

    @property
    def page_index(self) -> int:
        return self.origin.page_index

    @property
    def record_index(self) -> int:
        return self.origin.record_index

    @property
    def items_key(self) -> str:
        return self.origin.items_key


class _CandidateProvenanceVerifier:
    """Resolves each retained record's origin against the bytes the store archived.

    The listing walk hands back records parsed from an archived page, so the origin
    it carries is a claim about those bytes. Resolving that claim is one read of the
    page, one pointer resolution inside it, and one comparison of the record found
    there against the record being cited. A candidate is only built from a verdict
    this class produced, so no candidate reaches the audit result with an unbacked
    pointer or a pointer to somebody else's record.

    Page bytes are parsed once and held for the run, and verdicts are memoized per
    origin, because the same listing page backs every event: a per-event re-parse of
    an identical page would repeat identical work for an identical answer. Holding
    the parsed pages retains this run's listings in memory, bounded by the pages the
    walk already kept in :class:`PaginationResult`.

    Failures are returned, not raised, because they are deliberately not fatal: a
    page that cannot be read or a pointer that resolves to a different record is a
    limit on the claim that record would support, and the event keeps its other
    candidates with a blocking gate naming the loss.
    """

    def __init__(self, store: RawStore) -> None:
        self._store = store
        self._pages: dict[str, Any] = {}
        self._entries: dict[tuple[str, str], tuple[VerifiedOrigin | None, str | None]] = {}
        self._reported: set[str] = set()
        self._verified = 0
        self._refusals = 0
        self._blocked: list[str] = []

    def verify(
        self, origin: RecordOrigin | None, record: Mapping[str, Any]
    ) -> tuple[VerifiedOrigin | None, CandidateOriginMismatch | None]:
        """The resolved origin for ``record``, or ``(None, why it was refused)``.

        ``origin`` is ``None`` when the listing retained more records than origins,
        which is a provenance fault in its own right: nothing archived was named for
        the record, so it has no source to cite.
        """
        if origin is None:
            return self._refuse(
                "<no page>",
                CandidateOriginMismatch(
                    f"the record digesting to {_record_hash(record)} carries no page "
                    "origin, so no archived page was named for it"
                ),
            )
        key = (origin.raw_hash, origin.pointer)
        if key not in self._entries:
            self._entries[key] = self._resolve(origin)
        verified, reason = self._entries[key]
        if verified is None:
            return self._refuse(origin.raw_hash, reason or CandidateOriginMismatch("unresolved"))
        if verified.record_hash != _record_hash(record):
            return self._refuse(
                origin.raw_hash,
                CandidateOriginMismatch(
                    f"{origin.pointer} resolves in {origin.raw_hash} to the record "
                    f"digesting to {verified.record_hash}, not the record digesting to "
                    f"{_record_hash(record)} that was cited"
                ),
            )
        self._verified += 1
        return verified, None

    def _refuse(
        self, raw_hash: str, reason: CandidateOriginMismatch
    ) -> tuple[None, CandidateOriginMismatch]:
        self._refusals += 1
        entry = f"{raw_hash}: {reason}"
        # One line per distinct failure. A page backs every event in the run and
        # every record on it, so an undeduplicated list would repeat one reason
        # once per record per event and bury the other failures in it. The count is
        # kept separately because it counts refused records, not distinct reasons.
        if entry not in self._reported:
            self._reported.add(entry)
            self._blocked.append(entry)
        return None, reason

    def _resolve(
        self, origin: RecordOrigin
    ) -> tuple[VerifiedOrigin | None, CandidateOriginMismatch | None]:
        try:
            payload = self._page(origin.raw_hash)
        except FileNotFoundError:
            return None, CandidateOriginMismatch(f"no payload is stored for {origin.raw_hash}")
        except ValueError as exc:
            # Covers a hash mismatch (``RawStore.get`` refuses corrupt bytes), a
            # payload that is not UTF-8, and a body that is not JSON: each means the
            # page cannot be read, so the record's source cannot be produced.
            return None, CandidateOriginMismatch(
                f"stored payload {origin.raw_hash} could not be read as a JSON page: {exc}"
            )
        try:
            found = origin.resolve(payload)
        except PointerResolutionError as exc:
            return None, CandidateOriginMismatch(
                f"{origin.pointer} does not resolve in {origin.raw_hash}: {exc}"
            )
        if not isinstance(found, Mapping):
            return None, CandidateOriginMismatch(
                f"{origin.pointer} resolved in {origin.raw_hash} to a "
                f"{type(found).__name__} rather than a record"
            )
        return VerifiedOrigin(origin, _record_hash(found)), None

    def _page(self, raw_hash: str) -> Any:
        if raw_hash not in self._pages:
            self._pages[raw_hash] = json.loads(self._store.get(raw_hash).decode("utf-8"))
        return self._pages[raw_hash]

    @property
    def blocked(self) -> tuple[str, ...]:
        """One reason per distinct refusal, in the order the records were seen."""
        return tuple(self._blocked)

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate": "candidate_origins_verified",
            "candidates_verified": self._verified,
            "records_refused": self._refusals,
            "distinct_refusal_reasons": len(self._blocked),
            "page_bodies_parsed": len(self._pages),
            "verdicts_memoized": len(self._entries),
            "scope": "single_run_in_memory",
            "persisted_across_runs": False,
            "verification": (
                "RawStore.get(page raw_hash), json.loads, RFC 6901 pointer resolution, "
                "then the resolved record's own digest compared against the cited record"
            ),
        }


def _is_policy_record(
    record: Mapping[str, Any],
    *,
    policy_event_series: Mapping[str, str],
    asked_series: str | None,
    policy_series: Sequence[str],
) -> bool:
    """Whether one market record belongs to the configured primary policy cohort.

    The record's own identity decides, in this order: its event ticker, its series
    prefix, then the series it was requested under. Only the last is a fallback,
    because a venue may answer a series filter more broadly than it was asked and a
    record that names a different series is not policy material just because a
    policy-shaped request returned it.
    """
    event_ticker = str(record.get("event_ticker") or "")
    if event_ticker:
        if event_ticker in policy_event_series:
            return True
        if event_ticker.split("-", 1)[0] in policy_series:
            return True
    series_field = str(record.get("series_ticker") or "")
    if series_field:
        return series_field in policy_series
    ticker = str(record.get("ticker") or "")
    if ticker:
        prefix = ticker.split("-", 1)[0]
        if prefix:
            return prefix in policy_series
    return bool(asked_series) and asked_series in policy_series


def _event_urls(row: Mapping[str, Any]) -> dict[str, Any]:
    """Source URLs carried by a cohort row, kept so the schedule is re-derivable."""
    return {name: row.get(name) for name in _EVENT_URL_FIELDS if row.get(name)}


@dataclass(frozen=True, slots=True)
class CandidateContract:
    """One eligible contract, with the eligibility facts that qualified it."""

    ticker: str
    event_ticker: str
    series_ticker: str
    open_time: dt.datetime | None
    close_time: dt.datetime | None
    resolve_time: dt.datetime | None
    status: str
    active_at_release: bool
    known_at_release: bool
    volume_fp: Decimal | None
    open_interest_fp: Decimal | None
    strike_type: str
    floor_strike: Decimal | None
    cap_strike: Decimal | None
    rule_hash: str
    rule_available_at: dt.datetime | None
    partition: str
    #: The origin this candidate was read from, already resolved against the bytes
    #: the store archived: the page hash, the pointer inside it and the digest of the
    #: record found there. It is the one field a reader needs, so the page hash,
    #: record digest, locator and items key are derived from it rather than carried
    #: beside it, where they could drift apart. It is never a digest of the record
    #: re-serialized here: that would name a payload nothing archived, and a reader
    #: following it would find nothing.
    origin: VerifiedOrigin
    cohort: str = COHORT_DOWNSTREAM
    exclusion_reasons: tuple[str, ...] = ()
    thin_liquidity: bool = False
    window_overlap: bool = True
    policy_series: bool = False
    rule_evidence: RuleVersionEvidence | None = None
    #: The venue's own creation instant, kept as a lifecycle fact about the market.
    #: It dates the market, never the rule text a later fetch returned, so it is
    #: reported here and never promoted into ``rule_available_at``.
    created_time: dt.datetime | None = None

    @property
    def raw_hash(self) -> str:
        """The archived page this candidate was read from, retrievable by hash."""
        return self.origin.raw_hash

    @property
    def record_hash(self) -> str:
        """Digest of the record as read back from that archived page.

        It identifies which entry inside the page a candidate came from. It is a
        digest of the archived bytes rather than of a re-serialization, so it can be
        recomputed by a reader who follows ``raw_hash`` and resolves the pointer.
        """
        return self.origin.record_hash

    @property
    def raw_record_pointer(self) -> str:
        """RFC 6901 pointer to this record inside the page ``raw_hash`` names.

        Resolving it against ``RawStore.get(raw_hash)`` returns this candidate's own
        record, which is what makes two candidates sharing one page distinguishable
        without either of them citing bytes that were never archived.
        """
        return self.origin.pointer

    @property
    def raw_page_index(self) -> int:
        """Which fetched page of the listing walk carried this record."""
        return self.origin.page_index

    @property
    def raw_items_key(self) -> str:
        """The page's own JSON member that held the record."""
        return self.origin.items_key

    @property
    def rule_available_at_basis(self) -> str:
        """Where ``rule_available_at`` came from, stated rather than implied.

        A contract with no verified version has no rule-availability instant at all:
        the market's creation time is a fact about the market, and reporting it as
        the instant the rule became readable is the substitution this states.
        """
        if self.rule_evidence is None:
            return "unknown_no_verified_rule_version"
        return "verified_rule_version_in_force_from"

    @property
    def eligible(self) -> bool:
        return not self.exclusion_reasons

    @property
    def closed_before_release(self) -> bool:
        """True when the contract's own lifecycle closed it before the release.

        Such a contract is direct-resolution material: its post-release book was
        closed, so it cannot contribute a post-release quote response and must not
        be counted as empirical eligibility for the propagation cohort.
        """
        return self.cohort == COHORT_DIRECT_CLOSED_PRE_RELEASE

    @property
    def study_eligible(self) -> bool:
        """Lifecycle-eligible *and* carrying verified rule-version evidence.

        Lifecycle eligibility is the weaker kind: the contract existed before the
        release and carried rule text. Full study eligibility additionally requires a
        configured verification record that names this contract, matches the rule
        hash actually fetched and covers the release instant, because the market's own
        times date the market rather than the rule version a later fetch returned.
        A contract with no such record is ``None`` on that evidence and cannot be
        study-eligible, so a cohort cannot report study-eligible contracts while the
        gates guarding the same claim are unsatisfied.
        """
        return (
            self.eligible
            and self.rule_evidence is not None
            and "rule_text_differs_between_partitions" not in self.exclusion_reasons
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "event_ticker": self.event_ticker,
            "series_ticker": self.series_ticker,
            "cohort": self.cohort,
            "policy_series": self.policy_series,
            "open_time": self.open_time.isoformat() if self.open_time else None,
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "resolve_time": self.resolve_time.isoformat() if self.resolve_time else None,
            "status": self.status,
            "active_at_release": self.active_at_release,
            "known_at_release": self.known_at_release,
            "volume_fp": str(self.volume_fp) if self.volume_fp is not None else None,
            "open_interest_fp": (
                str(self.open_interest_fp) if self.open_interest_fp is not None else None
            ),
            "strike_type": self.strike_type,
            "floor_strike": str(self.floor_strike) if self.floor_strike is not None else None,
            "cap_strike": str(self.cap_strike) if self.cap_strike is not None else None,
            "rule_hash": self.rule_hash,
            "rule_available_at": (
                self.rule_available_at.isoformat() if self.rule_available_at else None
            ),
            "rule_available_at_basis": self.rule_available_at_basis,
            "created_time": (self.created_time.isoformat() if self.created_time else None),
            "created_time_is_not_a_rule_version_timestamp": True,
            "rule_version_evidence": (
                self.rule_evidence.as_dict() if self.rule_evidence is not None else None
            ),
            "rule_version_verified": self.rule_evidence is not None,
            "partition": self.partition,
            "raw_hash": self.raw_hash,
            "record_hash": self.record_hash,
            "raw_record_pointer": self.raw_record_pointer,
            "raw_page_index": self.raw_page_index,
            "raw_items_key": self.raw_items_key,
            "origin_resolved_against_archived_bytes": True,
            "eligible": self.eligible,
            "lifecycle_eligible": self.eligible,
            "study_eligible": self.study_eligible,
            "exclusion_reasons": list(self.exclusion_reasons),
            "thin_liquidity": self.thin_liquidity,
            "window_overlap": self.window_overlap,
            "selection_basis": "series_lifecycle_only_no_post_event_volume_filter",
        }


@dataclass(frozen=True, slots=True)
class CoverageGate:
    """One empirical-coverage claim, with whether it is actually established.

    Gates exist because a request succeeding is not the same fact as a claim being
    supported. An HTTP 200, a returned market row or a returned candle says the
    endpoint answered; it says nothing about whether the returned set was
    complete, whether the pagination walk finished, whether the candles are on
    grid, or whether a quote had two sides. Each of those is a separate gate, and
    an unsatisfied gate is reported rather than absorbed into a success.

    ``blocks`` names the claims this gate would otherwise let a reader make.
    """

    name: str
    satisfied: bool
    detail: str
    blocks: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate": self.name,
            "satisfied": self.satisfied,
            "detail": self.detail,
            "blocks": list(self.blocks),
        }


@dataclass(frozen=True, slots=True)
class CandleAudit:
    """Requested versus observed candle resolution for one contract and window.

    Candle *presence* and quote *availability* are separate facts, so they are
    counted separately. A candle can carry a trade price with no two-sided quote,
    and a returned candle series that spans the window can still contain no
    executable quote at all. Neither may be read as an eligible observation just
    because the endpoint answered.
    """

    ticker: str
    interval_minutes: int
    start_ts: int
    end_ts: int
    schema_flavour: str
    candle_count: int
    spacing: Mapping[str, Any]
    raw_hash: str | None
    two_sided_quote_candles: int = 0
    one_sided_or_missing_quote_candles: int = 0
    trade_only_candles: int = 0
    blocked: Mapping[str, Any] | None = None
    error: str | None = None

    @property
    def has_two_sided_quotes(self) -> bool:
        return self.two_sided_quote_candles > 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "requested_interval_minutes": self.interval_minutes,
            "requested_spacing_seconds": self.interval_minutes * 60,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "schema_flavour": self.schema_flavour,
            "candle_count": self.candle_count,
            "two_sided_quote_candles": self.two_sided_quote_candles,
            "one_sided_or_missing_quote_candles": self.one_sided_or_missing_quote_candles,
            "trade_only_candles": self.trade_only_candles,
            "observed": dict(self.spacing),
            "raw_hash": self.raw_hash,
            "blocked": dict(self.blocked) if self.blocked else None,
            "error": self.error,
            "candle_resolution_is_not_book_depth": True,
        }


@dataclass(frozen=True, slots=True)
class EventAudit:
    """Everything established about one release event, including what failed."""

    event_id: str
    family: str
    reference_period: str
    scheduled_at: dt.datetime
    window_start: dt.datetime
    window_end: dt.datetime
    status: str
    partition: Mapping[str, Any] | None
    candidates: tuple[CandidateContract, ...]
    candle_audits: tuple[CandleAudit, ...]
    trade_counts: Mapping[str, int]
    release: Mapping[str, Any] | None
    missing_expectations: Mapping[str, Any]
    blocked: tuple[Mapping[str, Any], ...]
    errors: tuple[str, ...]
    raw_hashes: tuple[str, ...]
    latency_seconds: float
    gates: tuple[CoverageGate, ...] = ()
    cohort_size: Mapping[str, int] = field(default_factory=dict)
    source_urls: Mapping[str, Any] = field(default_factory=dict)
    out_of_window_candidates: int = 0

    @property
    def downstream_candidates(self) -> tuple[CandidateContract, ...]:
        """Contracts in the primary post-release propagation cohort."""
        return tuple(c for c in self.candidates if not c.closed_before_release)

    @property
    def direct_closed_pre_release(self) -> tuple[CandidateContract, ...]:
        """Contracts the venue closed before the release.

        These are the direct-resolution study's material. Their post-release book
        was closed, so they cannot carry a post-release quote response.
        """
        return tuple(c for c in self.candidates if c.closed_before_release)

    @property
    def eligible_candidates(self) -> tuple[CandidateContract, ...]:
        """Downstream contracts the prespecified lifecycle rules admit."""
        return tuple(c for c in self.downstream_candidates if c.eligible)

    @property
    def unsatisfied_gates(self) -> tuple[CoverageGate, ...]:
        return tuple(g for g in self.gates if not g.satisfied)

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "family": self.family,
            "reference_period": self.reference_period,
            "scheduled_at": self.scheduled_at.isoformat(),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "source_urls": dict(self.source_urls),
            "status": self.status,
            "partition": dict(self.partition) if self.partition else None,
            "cohort_size": dict(self.cohort_size),
            "candidate_count": len(self.candidates),
            "downstream_candidate_count": len(self.downstream_candidates),
            "direct_closed_pre_release_count": len(self.direct_closed_pre_release),
            "eligible_count": len(self.eligible_candidates),
            "out_of_window_candidate_count": self.out_of_window_candidates,
            "out_of_window_note": (
                "contracts whose own lifecycle closed before the measurement window "
                "are kept in the direct_closed_pre_release cohort rather than dropped; "
                "their absence from the downstream cohort is a lifecycle fact, not a "
                "data-quality choice, and liquidity played no part in it"
            )
            if self.out_of_window_candidates
            else None,
            "candidates": [c.as_dict() for c in self.candidates],
            "exclusions": [
                {"ticker": c.ticker, "reasons": list(c.exclusion_reasons)}
                for c in self.candidates
                if c.exclusion_reasons
            ],
            "coverage_gates": [g.as_dict() for g in self.gates],
            "unsatisfied_gates": [g.name for g in self.unsatisfied_gates],
            "candle_audits": [a.as_dict() for a in self.candle_audits],
            "trade_counts": dict(self.trade_counts),
            "release": dict(self.release) if self.release else None,
            "missing_expectations": dict(self.missing_expectations),
            "blocked": [dict(b) for b in self.blocked],
            "errors": list(self.errors),
            "raw_hashes": list(self.raw_hashes),
            "latency_seconds": self.latency_seconds,
        }


@dataclass(frozen=True, slots=True)
class CohortAudit:
    """The G0 result. ``complete`` requires every event to be genuinely audited.

    ``complete`` is deliberately conjunctive, over three different kinds of fact.
    An event can be ``audited`` — the cohort was reached and read — while a coverage
    gate is still unsatisfied, and in that case the audit is not complete for the
    claim the gate guards. It can also be perfectly covered while its cohort-level
    scientific gates are unsatisfied, in which case the read succeeded and the study
    cohort still does not exist. ``acquisition_complete`` reports the read alone, at
    either level, so a run that walked every page and reached every contract is never
    reported as study-complete for it: HTTP and pagination success establish
    coverage of a request, never eligibility of a cohort.
    """

    venue: str
    started_at: dt.datetime
    ended_at: dt.datetime
    events: tuple[EventAudit, ...]
    cohort_definition_hash: str
    discoverable_series: Mapping[str, tuple[str, ...]]
    requested_contracts: bool = True
    limitations: tuple[str, ...] = ()
    blocked: tuple[Mapping[str, Any], ...] = ()
    outputs: Mapping[str, str] = field(default_factory=dict)
    cohort_size: Mapping[str, int] = field(default_factory=dict)
    discovery_basis: Mapping[str, Any] = field(default_factory=dict)
    policy_cohort: Mapping[str, Any] = field(default_factory=dict)
    series_selection: Mapping[str, Any] = field(default_factory=dict)
    universe_reuse: Mapping[str, Any] = field(default_factory=dict)
    candidate_provenance: Mapping[str, Any] = field(default_factory=dict)
    study_eligibility: Mapping[str, Any] = field(default_factory=dict)

    @property
    def unsatisfied_gates(self) -> tuple[tuple[str, str], ...]:
        """``(event_id, gate)`` for every event-level gate that is not satisfied."""
        return tuple(
            (event.event_id, gate.name) for event in self.events for gate in event.unsatisfied_gates
        )

    @property
    def unsatisfied_scientific_gates(self) -> tuple[str, ...]:
        """Cohort-level gate names that are not satisfied for this run."""
        return tuple(
            str(name)
            for name in ("rule_vintage_gate", "source_semantics_gate")
            if not (self.study_eligibility.get(name) or {}).get("satisfied")
        )

    @property
    def acquisition_complete(self) -> bool:
        """True when the read finished: every event audited with no coverage gate.

        This is coverage of the requests the audit made. It is not eligibility of
        the cohort those requests found, and it never implies study completeness.
        """
        return (
            bool(self.events)
            and all(e.status == "audited" for e in self.events)
            and not self.unsatisfied_gates
        )

    @property
    def complete(self) -> bool:
        """True only when the read finished *and* the cohort's science is settled.

        The scientific gates are included rather than reported beside the verdict,
        because a run whose rule-version and settlement-semantics gates are
        unsatisfied has established no study cohort, whatever it acquired. Leaving
        them out of this decision is what let a run report a study-eligible cohort
        while printing the same gates as unsatisfied.
        """
        return self.acquisition_complete and not self.unsatisfied_scientific_gates

    @property
    def status(self) -> str:
        if not self.events:
            return "empty"
        statuses = {e.status for e in self.events}
        if statuses == {"audited"} and not self.unsatisfied_gates:
            return "complete" if self.complete else "acquisition_complete_study_blocked"
        if statuses == {"blocked"}:
            return "blocked"
        if "blocked" in statuses:
            return "partial_with_blocked_events"
        return "partial"

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate": "G0",
            "venue": self.venue,
            "status": self.status,
            "complete": self.complete,
            "acquisition_complete": self.acquisition_complete,
            "acquisition_is_not_study_eligibility": True,
            "unsatisfied_scientific_gates": list(self.unsatisfied_scientific_gates),
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "event_count": len(self.events),
            "audited_event_count": sum(1 for e in self.events if e.status == "audited"),
            "blocked_event_count": sum(1 for e in self.events if e.status == "blocked"),
            "cohort_definition_hash": self.cohort_definition_hash,
            "cohort_size": dict(self.cohort_size),
            "discoverable_series": {k: list(v) for k, v in self.discoverable_series.items()},
            "discovery_basis": dict(self.discovery_basis),
            "policy_cohort": dict(self.policy_cohort),
            "series_selection": dict(self.series_selection),
            "universe_reuse": dict(self.universe_reuse),
            "candidate_provenance": dict(self.candidate_provenance),
            "study_eligibility": dict(self.study_eligibility),
            "unsatisfied_gates": [
                {"event_id": event_id, "gate": gate} for event_id, gate in self.unsatisfied_gates
            ],
            "limitations": list(self.limitations),
            "blocked": [dict(b) for b in self.blocked],
            "outputs": dict(self.outputs),
            "events": [e.as_dict() for e in self.events],
            "no_post_event_volume_selection": True,
            "candle_resolution_is_not_book_depth": True,
            "cohort_comes_from_study_configuration": True,
        }


class CohortAuditor:
    """Runs the bounded G0 audit against public read-only endpoints.

    The event cohort is supplied by the caller from the study's own configuration.
    This class holds no default cohort: a second one would be a second source of
    truth for the study's events.
    """

    def __init__(
        self,
        store: RawStore,
        *,
        kalshi: KalshiClient | None = None,
        bls: MacroReleaseClient | None = None,
        max_contracts_per_event: int = 40,
        max_candle_contracts_per_event: int = 4,
        max_pages: int = 8,
        trade_page_limit: int = 1000,
    ) -> None:
        self._store = store
        self._kalshi = kalshi or KalshiClient(store)
        self._bls = bls or MacroReleaseClient(store)
        self._max_contracts = max_contracts_per_event
        self._max_candle_contracts = max_candle_contracts_per_event
        self._max_pages = max_pages
        self._trade_page_limit = trade_page_limit

    def close(self) -> None:
        self._kalshi.close()
        self._bls.close()

    def __enter__(self) -> CohortAuditor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def audit_cohort(
        self,
        output_dir: str | pathlib.Path,
        *,
        events: Sequence[Mapping[str, Any]] | None = None,
        config: Mapping[str, Any] | None = None,
        discover_series: bool = True,
        before_seconds: int = DEFAULT_BEFORE_SECONDS,
        after_seconds: int = DEFAULT_AFTER_SECONDS,
        candle_intervals: Sequence[int] = REQUESTED_INTERVALS,
        series_override: Sequence[str] | None = None,
    ) -> CohortAudit:
        """Run the bounded G0 audit and write its machine-readable outputs.

        The cohort must be supplied, through exactly one of two routes. Pass
        ``events`` directly with the canonical cohort shape (``event_id``,
        ``family``, ``reference_period``, an aware ``scheduled_at`` and the source
        URLs it was read from), or pass the parsed ``config`` mapping of
        ``configs/cohort.yaml`` and its ``events`` are used. An absent, empty or
        doubly-supplied cohort raises: an empty cohort has no complete state, and
        silently preferring one argument over the other would hide a caller bug
        behind the wrong cohort.

        ``before_seconds`` and ``after_seconds`` are the configured measurement
        window. The defaults match the study's canonical window, and the caller is
        expected to pass ``configs/event_windows.yaml``'s values so this module
        holds no competing window definition.

        ``series_override`` narrows the primary policy cohort to an explicitly
        configured subset, for a bounded rerun. It is a *narrowing*, not a source:
        every name in it must already be configured as an observed venue series for
        the policy family, so the override cannot introduce a series the study's own
        configuration does not carry. It requires a configured policy cohort and
        raises otherwise rather than becoming a second, caller-supplied cohort.

        The primary cohort is the configured downstream policy exposure, whose
        series come from ``candidate_contract_families``. Contract universes are
        fetched once per request identity for the whole run and each event applies
        its own lifecycle eligibility to them, because a fetch repeated per event
        returns identical bodies and adds no evidence.

        Returns the :class:`CohortAudit` and writes:

        * ``coverage.json`` — per-event status, cohort sizes, coverage gates,
          candidate eligibility, exclusions, partition cutoffs,
          requested/observed candle spacing, trade counts and blocked responses.
        * ``raw_hashes.json`` — every archived payload hash with its source.
        * ``event_card.json`` — one event card whose every value is traced to an
          archived raw hash.
        * ``series_discovery.json`` — discovered downstream series identifiers with
          the evidence that admitted or excluded each one, so a later reader can see
          where a ticker came from and that a filter bounds the set.
        """
        if before_seconds < 0 or after_seconds < 0:
            raise ValueError(
                "window bounds must not be negative; a negative bound would place "
                "the window on the wrong side of the release"
            )
        if (events is None) == (config is None):
            raise ValueError(
                "audit_cohort requires exactly one cohort source: pass "
                "events=<cohort events> or config=<parsed configs/cohort.yaml>, not "
                "both and not neither. This module defines no cohort of its own, and "
                "silently preferring one argument over the other would hide a caller "
                "bug behind the wrong cohort."
            )
        cohort = event_cohort(config or {}, events=events)
        intervals = tuple(candle_intervals)
        if not intervals:
            raise ValueError("at least one candle interval must be requested")

        policy = policy_cohort(config or {})
        policy_series = self._resolve_policy_series(policy, series_override)
        evidence = rule_version_evidence(config or {})

        started = dt.datetime.now(dt.UTC)
        out = pathlib.Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        if discover_series:
            discovered, verdicts, discovery_blocked = self._discover_series(cohort, policy)
        else:
            discovered, verdicts, discovery_blocked = {}, (), ()

        universe = _UniverseCache(self._kalshi, max_pages=self._max_pages)
        provenance = _CandidateProvenanceVerifier(self._store)
        policy_events, policy_event_blocked = self._policy_event_tickers(
            policy_series, universe, policy
        )

        audits: list[EventAudit] = []
        for spec in cohort:
            audits.append(
                self._audit_event(
                    spec,
                    discovered=discovered,
                    policy_series=policy_series,
                    policy_events=policy_events,
                    policy_market_bound=policy.max_markets_per_event_listing,
                    universe=universe,
                    provenance=provenance,
                    before_seconds=before_seconds,
                    after_seconds=after_seconds,
                    candle_intervals=intervals,
                    rule_evidence=evidence,
                )
            )

        raw_hashes = sorted({h for audit in audits for h in audit.raw_hashes})
        limitations = self._limitations(audits, discovery_blocked)
        cohort_hash = _digest_cohort(cohort)
        study_gates = _study_eligibility_gates(audits, policy, policy_series, evidence)

        result = CohortAudit(
            venue="kalshi",
            started_at=started,
            ended_at=dt.datetime.now(dt.UTC),
            events=tuple(audits),
            cohort_definition_hash=cohort_hash,
            discoverable_series={family: tuple(series) for family, series in discovered.items()},
            limitations=limitations,
            blocked=tuple(discovery_blocked) + tuple(policy_event_blocked),
            cohort_size=_cohort_size(audits),
            discovery_basis=self._discovery_basis(verdicts, policy, policy_series),
            policy_cohort=policy.as_dict(),
            series_selection={
                "policy_series_queried": list(policy_series),
                "series_override_applied": series_override is not None,
                "series_override_values": (
                    list(series_override) if series_override is not None else None
                ),
                "override_is_narrowing_only": True,
                "unobserved_leads_not_queried": list(policy.unobserved_leads),
                "excluded_families": [
                    {
                        "family_key": str(entry.get("family_key")),
                        "relation_type": entry.get("relation_type"),
                        "is_primary_cohort": bool(entry.get("is_primary_cohort")),
                        "observed_venue_series": list(entry.get("observed_venue_series") or []),
                        "reason": "family_is_not_the_primary_policy_cohort",
                    }
                    for entry in _configured_families(config or {})
                    if str(entry.get("family_key")) != POLICY_FAMILY_KEY
                ],
                "direct_release_series_kept_separate": list(DIRECT_RELEASE_SERIES),
            },
            universe_reuse=universe.as_dict(),
            candidate_provenance=provenance.as_dict(),
            study_eligibility=study_gates,
        )

        (out / "coverage.json").write_text(
            json.dumps(result.as_dict(), indent=2, sort_keys=True), encoding="utf-8"
        )
        (out / "raw_hashes.json").write_text(
            json.dumps(
                {
                    "cohort_definition_hash": cohort_hash,
                    "store_root": str(getattr(self._store, "root", "")),
                    "raw_hashes": [
                        {"raw_hash": h, "source": "kalshi_or_bls_public_get"} for h in raw_hashes
                    ],
                    "count": len(raw_hashes),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (out / "series_discovery.json").write_text(
            json.dumps(
                {
                    "keywords": {
                        family: list(terms) for family, terms in DOWNSTREAM_SERIES_QUERIES.items()
                    },
                    "matches": {family: list(series) for family, series in discovered.items()},
                    "policy_series_verdicts": [v.as_dict() for v in verdicts],
                    "policy_series_admitted": [v.ticker for v in verdicts if v.admitted],
                    "policy_series_excluded": [v.as_dict() for v in verdicts if not v.admitted],
                    "configured_policy_series": list(policy.configured_series),
                    "complete_universe_claimed": False,
                    "note": (
                        "keyword candidates over the exchange's own listing, not a "
                        "proven-complete universe; a policy series is admitted only "
                        "on a US Federal Reserve settlement source together with an "
                        "economics category and a policy-rate or policy-meeting title"
                    ),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        (out / "event_card.json").write_text(
            json.dumps(
                self.build_event_card(result, out=out),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        finals = dataclasses.replace(
            result,
            outputs={
                "coverage": str(out / "coverage.json"),
                "raw_hashes": str(out / "raw_hashes.json"),
                "event_card": str(out / "event_card.json"),
                "series_discovery": str(out / "series_discovery.json"),
            },
        )
        return finals

    def _resolve_policy_series(
        self,
        policy: PolicyCohort,
        series_override: Sequence[str] | None,
    ) -> tuple[str, ...]:
        """The policy series this run queries, in the configuration's own order.

        The order is the prioritization: a cost bound truncates the tail of this
        tuple, never the head, so the configured primary series cannot be displaced
        by an alphabetically earlier keyword find.
        """
        if series_override is None:
            return policy.configured_series
        if not policy.configured:
            raise ValueError(
                "series_override requires a configured policy cohort; configure "
                f"{POLICY_COHORT_SECTION}.{POLICY_FAMILY_KEY}.observed_venue_series "
                "rather than passing series at the call site, so the primary cohort "
                "has one source of truth"
            )
        if not policy.configured_series:
            raise ValueError(
                "series_override was supplied but the configured policy family "
                "names no observed venue series; there is nothing to narrow"
            )
        requested: list[str] = []
        for value in series_override:
            ticker = str(value).strip()
            if not ticker:
                raise ValueError("series_override must not contain empty entries")
            if ticker not in policy.configured_series:
                raise ValueError(
                    f"series_override names {ticker!r}, which the configured policy "
                    f"cohort does not carry; configured series are "
                    f"{list(policy.configured_series)}. The override narrows the "
                    "configured cohort and never introduces a series of its own."
                )
            if ticker not in requested:
                requested.append(ticker)
        if not requested:
            raise ValueError("series_override must name at least one configured series")
        return tuple(requested)

    def _policy_event_tickers(
        self,
        policy_series: Sequence[str],
        universe: _UniverseCache,
        policy: PolicyCohort,
    ) -> tuple[dict[str, tuple[str, ...]], tuple[Mapping[str, Any], ...]]:
        """Event identifiers per policy series, from the venue's own event listing.

        The event ticker is what the historical-market filter accepts, so this is
        how a series' contracts are reached without walking every settled market.
        The listing is fetched once per series for the whole run. Nested markets are
        not requested: they were observed empty for historical events, so the
        contracts come from the market filter keyed by the event ticker.

        The walk is bounded by the configured per-series bound, and a truncated walk
        is recorded as a bound rather than presented as the series' full event set.
        """
        bound = policy.max_events_per_series
        mapping: dict[str, tuple[str, ...]] = {}
        blocked: list[Mapping[str, Any]] = []
        for series_ticker in policy_series:
            # The paginator raises WireShapeError for a 200 body that is not the
            # documented shape, and neither that type nor TransportError is caught
            # by cli.main. Unhandled, one malformed listing aborts the whole run
            # before any artifact is written, so a misbehaving venue produces no
            # blocked record to inspect. Record the series instead.
            try:
                page = universe.fetch(
                    kind="events",
                    series_ticker=series_ticker,
                    max_items=bound,
                )
            except (TransportError, WireShapeError) as exc:
                blocked.append({**_blocked(exc), "policy_series": series_ticker})
                continue
            if page.blocked:
                blocked.append({**dict(page.blocked[0]), "policy_series": series_ticker})
            elif not page.complete:
                blocked.append(
                    {
                        "recorded": True,
                        "empty_result": False,
                        "url": None,
                        "status_code": None,
                        "reason": (
                            f"the event listing for {series_ticker} stopped early "
                            f"({page.stop_reason}); the series' event set is bounded, "
                            "not complete"
                        ),
                        "attempts": 0,
                        "payload_hash": None,
                        "policy_series": series_ticker,
                        "observed_at": dt.datetime.now(dt.UTC).isoformat(),
                    }
                )
            tickers: list[str] = []
            for record in page.items:
                if not isinstance(record, Mapping):
                    continue
                value = record.get("event_ticker") or record.get("ticker")
                if isinstance(value, str) and value and value not in tickers:
                    tickers.append(value)
            mapping[series_ticker] = tuple(tickers[:bound])
        return mapping, tuple(blocked)

    def _discovery_basis(
        self,
        verdicts: Sequence[PolicySeriesVerdict],
        policy: PolicyCohort,
        policy_series: Sequence[str],
    ) -> dict[str, Any]:
        """How the downstream series were found, and what bounds that finding."""
        return {
            "method": "exchange_listing_keyword_filter",
            "keyword_terms": {
                family: list(terms) for family, terms in DOWNSTREAM_SERIES_QUERIES.items()
            },
            "complete_universe_claimed": False,
            "policy_series_source": "configured_candidate_contract_families",
            "policy_series_queried": list(policy_series),
            "policy_series_query_is_a_bounded_subset": True,
            "policy_series_excluded_count": sum(1 for v in verdicts if not v.admitted),
            "policy_cohort_verified_contract_ids": list(policy.verified_contract_ids),
            "note": (
                "discovery is a keyword candidate set over the exchange's own "
                "listing; a keyword filter cannot establish that the returned "
                "set is the complete universe, and it is not used as one. The "
                "primary policy cohort is separate: it is the configured series, and "
                "a discovered series joins it only on positive evidence."
            ),
        }

    def _audit_event(
        self,
        spec: Mapping[str, Any],
        *,
        discovered: Mapping[str, Sequence[str]],
        policy_series: Sequence[str],
        policy_events: Mapping[str, Sequence[str]],
        policy_market_bound: int,
        universe: _UniverseCache,
        before_seconds: int,
        after_seconds: int,
        candle_intervals: Sequence[int],
        provenance: _CandidateProvenanceVerifier,
        rule_evidence: Sequence[RuleVersionEvidence] = (),
    ) -> EventAudit:
        started = dt.datetime.now(dt.UTC)
        family = _event_family(spec)
        event_id = str(spec["event_id"])
        reference_period = str(spec["reference_period"])
        scheduled_at = spec["scheduled_at"]
        publication_date = scheduled_at.date().isoformat()
        window_start = scheduled_at - dt.timedelta(seconds=before_seconds)
        window_end = scheduled_at + dt.timedelta(seconds=after_seconds)

        blocked: list[Mapping[str, Any]] = []
        errors: list[str] = []
        raw_hashes: list[str] = []
        candle_audits: list[CandleAudit] = []
        trade_counts: dict[str, int] = {}
        candidates: list[CandidateContract] = []
        attempted = 0
        acquired = 0
        pages_incomplete: list[str] = []
        series_blocked: list[str] = []
        provenance_blocked: list[str] = []

        # 1. Partition reconciliation against the *current* cutoff. The cutoff
        #    moves, so the answer is recorded with the cutoff it was computed from.
        partition: PartitionDecision | None = None
        try:
            partition = self._kalshi.resolve_partition(window_start, window_end)
            raw_hashes.append(partition.cutoff.raw_hash)
        except (TransportError, WireShapeError) as exc:
            blocked.append(_blocked(exc))
            errors.append(f"cutoff: {exc}")

        # 2. Contract universe, fetched once per request identity for the whole run
        #    and reused here. The universe does not vary with the release event, so
        #    a per-event refetch returns identical bodies; only the lifecycle
        #    eligibility applied to these records is per event.
        #
        #    The universe is the union of three documented queries, because no
        #    single one returns it:
        #      * the series filter returns the series' markets live-side
        #      * the same filter against the archived listing returns the inactive
        #        ones, including the retired legacy prefix
        #      * a policy event's own markets come from the event-ticker filter, so
        #        a series' contracts are reachable without a global history walk
        #    No documented filter accepts a time window, so the window is applied to
        #    each candidate's own lifecycle times below rather than to the request.
        #    The walk is bounded, so the returned set is a bounded candidate set
        #    rather than a proven-complete universe, and that limit is gated below.
        series_list = tuple(
            dict.fromkeys(tuple(FAMILY_SERIES.get(family, ())) + tuple(discovered.get(family, ())))
        )
        universe_queries: list[tuple[str, dict[str, Any]]] = []
        for series_ticker in series_list:
            universe_queries.append(("historical", {"series_ticker": series_ticker}))
            universe_queries.append(("live", {"series_ticker": series_ticker}))
        # The configured primary policy series are reached through their own event
        # tickers, which the venue's event listing supplied. A series-filtered
        # listing for the same series would return the same markets a second time,
        # so it is used only as the fallback when the event walk found nothing for
        # that series: it is a safety net, not a duplicate request.
        # Every event ticker that belongs to the primary policy cohort, mapped back
        # to the series that listed it, so a record reached by event filter is
        # recognized as a policy contract regardless of its ticker prefix.
        policy_event_series: dict[str, str] = {}
        for series_ticker in policy_series:
            tickers = policy_events.get(series_ticker, ())
            if tickers:
                for event_ticker in tickers:
                    policy_event_series[str(event_ticker)] = series_ticker
                    # One event's own market set is bounded by the configured bound,
                    # so the truncation is attributable to a stated number.
                    universe_queries.append(
                        (
                            "historical",
                            {
                                "event_ticker": event_ticker,
                                "max_items": policy_market_bound,
                            },
                        )
                    )
            else:
                universe_queries.append(("historical", {"series_ticker": series_ticker}))
                universe_queries.append(("live", {"series_ticker": series_ticker}))

        for kind, extra in universe_queries:
            label = (
                f"{kind} markets "
                f"{extra.get('series_ticker') or extra.get('event_ticker') or 'unfiltered'}"
            )
            try:
                result = universe.fetch(kind=kind, **extra)
            except (TransportError, WireShapeError) as exc:
                blocked.append(_blocked(exc))
                errors.append(f"{label}: {exc}")
                series_blocked.append(label)
                continue
            raw_hashes.extend(result.raw_hashes)
            blocked.extend(result.blocked)
            # A truncated page walk is a coverage limit, not a result: the venue
            # returned a bounded set of what exists, so nothing here may be
            # treated as the complete universe.
            if not result.complete:
                pages_incomplete.append(f"{label}: {result.stop_reason}")
            attempted += len(result.items)
            for record_index, record in enumerate(result.items):
                if not isinstance(record, Mapping):
                    continue
                # ``items`` is deduplicated, so the origin list is positionally
                # aligned with it rather than with the raw page records. Resolving
                # the origin against the archived bytes is a precondition of the
                # candidate: a record whose source cannot be produced is refused
                # here and named in the provenance gate, never reported with an
                # empty or invented one.
                verified, provenance_error = provenance.verify(result.origins[record_index], record)
                if provenance_error is not None:
                    provenance_blocked.append(str(provenance_error))
                    continue
                candidate = self._evaluate_candidate(
                    record,
                    series_ticker=extra.get("series_ticker"),
                    scheduled_at=scheduled_at,
                    window_start=window_start,
                    window_end=window_end,
                    partition=kind,
                    origin=verified,
                    rule_evidence=rule_evidence,
                    # Membership is decided from the record's own identity, not from
                    # the request that returned it. A series filter is a request the
                    # venue may answer more broadly than asked, and trusting it would
                    # label an unrelated contract as policy material on the strength
                    # of the query rather than the record.
                    policy_scope=_is_policy_record(
                        record,
                        policy_event_series=policy_event_series,
                        asked_series=extra.get("series_ticker"),
                        policy_series=policy_series,
                    ),
                )
                if candidate is not None:
                    acquired += 1
                    candidates.append(candidate)

        deduped = _dedupe_candidates(candidates)
        selected = _select_candidates(deduped, limit=self._max_contracts)
        out_of_window_count = sum(1 for c in deduped if not c.window_overlap)
        bounded_out = len(selected) < len([c for c in deduped if c.window_overlap])
        candidates = selected
        downstream = [c for c in candidates if not c.closed_before_release]
        direct_closed = [c for c in candidates if c.closed_before_release]

        # 3. Real history resolution per candidate. Bounded to a few contracts,
        #    recorded for every one attempted.
        for candidate in candidates[: self._max_candle_contracts]:
            for interval in candle_intervals:
                candle_audits.append(
                    self._audit_candles(
                        candidate,
                        window_start=window_start,
                        window_end=window_end,
                        interval_minutes=interval,
                        blocked=blocked,
                        errors=errors,
                        raw_hashes=raw_hashes,
                    )
                )

        # 4. Trade retrieval, kept per event so a count is comparable across a
        #    pagination boundary.
        trades_incomplete: list[str] = []
        for candidate in candidates[: self._max_candle_contracts]:
            try:
                trades = self._kalshi.get_historical_trades(
                    ticker=candidate.ticker,
                    min_ts=int(window_start.timestamp()),
                    max_ts=int(window_end.timestamp()),
                    limit=self._trade_page_limit,
                    max_pages=self._max_pages,
                )
            except (TransportError, WireShapeError) as exc:
                blocked.append(_blocked(exc))
                errors.append(f"trades {candidate.ticker}: {exc}")
                trades_incomplete.append(f"{candidate.ticker}: {exc}")
                continue
            raw_hashes.extend(trades.raw_hashes)
            blocked.extend(trades.blocked)
            if not trades.complete:
                trades_incomplete.append(f"{candidate.ticker}: {trades.stop_reason}")
            trade_counts[candidate.ticker] = len(trades.items)
            trade_counts[f"{candidate.ticker}::pages"] = len(trades.pages)
            trade_counts[f"{candidate.ticker}::repeated_ids"] = trades.repeated_ids_across_pages
            # Direction-field disagreement is a data-quality observation.
            conflicts = sum(
                1
                for record in trades.items
                if isinstance(record, Mapping) and trade_direction_conflicts(record)["conflict"]
            )
            if conflicts:
                trade_counts[f"{candidate.ticker}::direction_conflicts"] = conflicts

        # 5. Archived initial release. The cohort's family is translated to the
        #    BLS archive slug here, at the boundary, so the cohort vocabulary stays
        #    canonical.
        release_payload: dict[str, Any] | None = None
        published = dt.date.fromisoformat(publication_date)
        try:
            release, release_blocked = self._bls.get_initial_release(
                FAMILY_RELEASE_SLUG[family],
                published,
                scheduled_at=scheduled_at,
                event_id=event_id,
            )
        except (TransportError, WireShapeError) as exc:
            release, release_blocked = None, _blocked(exc)
        if release_blocked:
            blocked.append(release_blocked)
            errors.append(f"release {family} {publication_date}: blocked")
        if release is not None:
            raw_hashes.append(release.provenance.raw_hash)
            release_payload = release.as_dict()

        # 6. Expectations are never invented. The response records the route that
        #    was not available rather than a plausible number.
        missing_expectations = {
            "status": "unavailable",
            "route_attempted": "none",
            "reason": (
                "no licensed point-in-time consensus is available to this project, "
                "no forecast archive was prespecified, and no pre-release "
                "market-implied distribution has been captured; a surprise slope is "
                "therefore not estimable for this event"
            ),
            "vendor_consensus_assumed_free": False,
            "revised_series_substituted": False,
            "midpoint_substituted_for_bucket": False,
        }

        release_embargo = (release_payload or {}).get("schedule_agreement")
        gates = _event_gates(
            attempted=attempted,
            acquired=acquired,
            downstream=downstream,
            direct_closed=direct_closed,
            pages_incomplete=pages_incomplete,
            series_blocked=series_blocked,
            bounded_out=bounded_out,
            candle_audits=candle_audits,
            release_payload=release_payload,
            release_embargo_agreement=release_embargo,
            release_blocked=release_blocked is not None,
            discovery_terms=tuple(discovered.get(family, ())),
            policy_series=policy_series,
            policy_candidates=[c for c in candidates if c.policy_series],
            provenance_blocked=provenance_blocked,
        )

        # A read that reached the venue and returned candidates is ``audited``.
        # Unsatisfied coverage gates do not turn that into a failure of the read;
        # they block the empirical claims instead, and ``CohortAudit.complete`` is
        # conjunctive over both.
        status = (
            "blocked"
            if (not candidates and blocked)
            else ("audited" if candidates else "no_candidates")
        )
        if status == "audited" and errors:
            status = "audited_with_errors"

        return EventAudit(
            event_id=event_id,
            family=family,
            reference_period=reference_period,
            scheduled_at=scheduled_at,
            window_start=window_start,
            window_end=window_end,
            status=status,
            partition=partition.as_dict() if partition else None,
            candidates=tuple(candidates),
            candle_audits=tuple(candle_audits),
            trade_counts=trade_counts,
            release=release_payload,
            missing_expectations=missing_expectations,
            blocked=tuple(blocked),
            errors=tuple(errors),
            raw_hashes=tuple(dict.fromkeys(raw_hashes)),
            latency_seconds=(dt.datetime.now(dt.UTC) - started).total_seconds(),
            gates=gates,
            cohort_size={
                "attempted_records": attempted,
                "acquired_candidates": acquired,
                "deduped_candidates": len(deduped),
                "downstream_candidates": len(downstream),
                "direct_closed_pre_release": len(direct_closed),
                "eligible_downstream": len([c for c in downstream if c.eligible]),
                "policy_series_candidates": len([c for c in candidates if c.policy_series]),
                "policy_series_lifecycle_eligible": len(
                    [c for c in candidates if c.policy_series and c.eligible]
                ),
                "policy_series_study_eligible": len(
                    [c for c in candidates if c.policy_series and c.study_eligible]
                ),
                "direct_release_candidates": len(
                    [c for c in candidates if c.series_ticker in DIRECT_RELEASE_SERIES]
                ),
                "selected_for_deep_audit": len(candidates),
            },
            source_urls=_event_urls(spec),
            out_of_window_candidates=out_of_window_count,
        )

    def _evaluate_candidate(
        self,
        record: Mapping[str, Any],
        *,
        series_ticker: str | None,
        scheduled_at: dt.datetime,
        window_start: dt.datetime,
        window_end: dt.datetime,
        partition: str,
        origin: VerifiedOrigin | None = None,
        policy_scope: bool = False,
        rule_evidence: Sequence[RuleVersionEvidence] = (),
    ) -> CandidateContract | None:
        """Apply prespecified lifecycle eligibility. Liquidity is reported only.

        A candidate is excluded only for facts about the contract itself: it did not
        exist yet at the release, it was already closed when the measurement window
        opened, or its record carries no identifiable ticker and no rule text.
        Post-event volume is never one of them, and neither is thin trading.

        ``origin`` is the resolved provenance of ``record`` and is required. Without
        it the record's own bytes are not reachable, so the candidate would cite an
        empty or invented source; it is returned as ``None`` here and refused with a
        reason by the caller, because a candidate whose source cannot be produced is
        not a candidate. The record is still parsed for eligibility, so a rule
        disagreement between partitions is never hidden by a provenance failure.

        ``window_overlap`` records whether the contract's own lifecycle intersects
        the measurement window. It is a separate reported property rather than an
        exclusion, because a contract that closed mid-window is exactly the case the
        audit must surface instead of dropping.

        Rule-version evidence is resolved here against the release instant and the
        rule hash actually fetched, so the decision the counts and gates read is the
        same object attached to the candidate. A record that names this market but a
        different rule hash, or that takes effect after the release, resolves to
        ``None`` rather than to a near miss.
        """
        if origin is None:
            return None
        ticker = record.get("ticker")
        if not isinstance(ticker, str) or not ticker:
            return None
        rules_primary = record.get("rules_primary")
        rules_secondary = record.get("rules_secondary")
        hashes = rule_hash(
            rules_primary if isinstance(rules_primary, str) else None,
            rules_secondary if isinstance(rules_secondary, str) else None,
        )
        verified_rule_version = _applicable_rule_evidence(
            rule_evidence, contract_id=ticker, rule_hash=hashes, at=scheduled_at
        )

        open_time = _parse_time(record.get("open_time"))
        close_time = _parse_time(record.get("close_time"))
        resolve_time = _parse_time(record.get("settlement_ts"))
        # The venue's own creation instant is kept as a lifecycle fact. It dates the
        # market, so it is reported beside the rule hash rather than read as the
        # instant the fetched rule text became readable.
        created_time = _parse_time(record.get("created_time"))

        exclusions: list[str] = []
        active_at_release = True
        if open_time is not None and open_time > scheduled_at:
            exclusions.append("created_after_release")
            active_at_release = False
        closed_before_release = close_time is not None and close_time < scheduled_at
        if closed_before_release:
            exclusions.append("closed_before_release")
            active_at_release = False
        if not rules_primary and not rules_secondary:
            exclusions.append("no_rule_text")

        opens_by_window_start = open_time is None or open_time <= window_start
        closes_after_window_end = close_time is None or close_time >= window_end
        window_overlap = opens_by_window_start and closes_after_window_end

        volume = parse_fixed_point_count(record.get("volume_fp"), "volume_fp")
        open_interest = parse_fixed_point_count(record.get("open_interest_fp"), "open_interest_fp")

        return CandidateContract(
            ticker=ticker,
            event_ticker=str(record.get("event_ticker") or ""),
            series_ticker=series_ticker
            or _series_of(ticker, record.get("series_ticker"), record.get("event_ticker")),
            open_time=open_time,
            close_time=close_time,
            resolve_time=resolve_time,
            status=str(record.get("status") or ""),
            active_at_release=active_at_release,
            known_at_release=open_time is not None and open_time <= scheduled_at,
            volume_fp=volume,
            open_interest_fp=open_interest,
            strike_type=str(record.get("strike_type") or ""),
            floor_strike=parse_fixed_point_dollars(record.get("floor_strike"), "floor_strike"),
            cap_strike=parse_fixed_point_dollars(record.get("cap_strike"), "cap_strike"),
            rule_hash=hashes,
            # Rule availability is a fact about the rule text, not the market: with no
            # verified version there is no instant at which this rule was certified
            # readable, so it stays null rather than borrowing the market's open time.
            rule_available_at=(
                verified_rule_version.in_force_from if verified_rule_version is not None else None
            ),
            partition=partition,
            # The archived page is the source of record. The origin handed here was
            # already resolved against those bytes by the run's verifier, so the page
            # hash is one ``RawStore.get`` retrieves, the pointer resolves inside it,
            # and the record digest is the one those bytes produced.
            origin=origin,
            # A pre-release close is a lifecycle fact about the venue's own
            # contract, so it separates the direct-resolution material from the
            # downstream propagation cohort rather than being dropped.
            cohort=(
                COHORT_DIRECT_CLOSED_PRE_RELEASE if closed_before_release else COHORT_DOWNSTREAM
            ),
            exclusion_reasons=tuple(exclusions),
            thin_liquidity=bool(volume is not None and volume < THIN_VOLUME_FP and not exclusions),
            window_overlap=window_overlap,
            policy_series=policy_scope,
            rule_evidence=verified_rule_version,
            created_time=created_time,
        )

    def _audit_candles(
        self,
        candidate: CandidateContract,
        *,
        window_start: dt.datetime,
        window_end: dt.datetime,
        interval_minutes: int,
        blocked: list[Mapping[str, Any]],
        errors: list[str],
        raw_hashes: list[str],
    ) -> CandleAudit:
        """Query one candle resolution and record requested versus observed."""
        start_ts = int(window_start.timestamp())
        end_ts = int(window_end.timestamp())
        # The historical endpoint serves archived markets, the live one only
        # within the cutoff window. Try the partition the candidate came from
        # first, then the other, and record which served it.
        attempts: list[tuple[str, str | None]] = (
            [("historical", None), ("live", candidate.series_ticker)]
            if candidate.partition == "historical"
            else [("live", candidate.series_ticker), ("historical", None)]
        )
        last_block: Mapping[str, Any] | None = None
        last_error: str | None = None
        for flavour_name, series in attempts:
            flavour = (
                CANDLE_SCHEMA_HISTORICAL if flavour_name == "historical" else CANDLE_SCHEMA_LIVE
            )
            try:
                if flavour_name == "historical" or not series:
                    candles, raw_hash = self._kalshi.get_historical_candles(
                        candidate.ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=interval_minutes,
                    )
                else:
                    candles, raw_hash = self._kalshi.get_live_candles(
                        series,
                        candidate.ticker,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        period_interval=interval_minutes,
                    )
            except TransportError as exc:
                last_block = exc.as_blocked_record()
                last_error = str(exc)
                continue
            except WireShapeError as exc:
                last_error = str(exc)
                continue

            spacing = inspect_candle_spacing(
                candles,
                interval_minutes=interval_minutes,
                start_ts=start_ts,
                end_ts=end_ts,
            )
            raw_hashes.append(raw_hash)
            two_sided, one_sided, trade_only = _candle_quote_counts(candles, schema_flavour=flavour)
            return CandleAudit(
                ticker=candidate.ticker,
                interval_minutes=interval_minutes,
                start_ts=start_ts,
                end_ts=end_ts,
                schema_flavour=flavour,
                candle_count=spacing.candle_count,
                spacing=spacing.as_dict(),
                raw_hash=raw_hash,
                two_sided_quote_candles=two_sided,
                one_sided_or_missing_quote_candles=one_sided,
                trade_only_candles=trade_only,
            )

        if last_block:
            blocked.append(last_block)
        if last_error:
            errors.append(f"candles {candidate.ticker}@{interval_minutes}m: {last_error}")
        return CandleAudit(
            ticker=candidate.ticker,
            interval_minutes=interval_minutes,
            start_ts=start_ts,
            end_ts=end_ts,
            schema_flavour="unavailable",
            candle_count=0,
            spacing={
                "candle_count": 0,
                "note": (
                    "no candle data retrieved; this is an access outcome, not an "
                    "observation that no quotes existed"
                ),
                "usable_for_replay": False,
            },
            raw_hash=None,
            blocked=last_block,
            error=last_error,
        )

    def _discover_series(
        self,
        cohort: Sequence[Mapping[str, Any]],
        policy: PolicyCohort | None,
    ) -> tuple[
        dict[str, list[str]],
        tuple[PolicySeriesVerdict, ...],
        tuple[Mapping[str, Any], ...],
    ]:
        """Find downstream series identifiers from the exchange's own listing.

        Two results are returned because they are two different kinds of evidence.
        The family-keyed keyword matches are the bounded candidate set for the
        release families. The policy verdicts are the catalog's own series classified
        against positive evidence for a US policy-rate contract — a Federal Reserve
        settlement source, the economics category and a policy-rate or
        policy-meeting title — so a foreign central bank or a Fed-personnel series is
        recorded as an exclusion with its reason rather than admitted as a policy
        lookalike.

        A ticker is never guessed. If discovery fails, the audit records the block
        and proceeds with the configured series, which is a narrower result rather
        than a wrong one.
        """
        block_ref: list[Mapping[str, Any]] = []
        considered: dict[str, list[str]] = {}
        try:
            listing, _hashes = self._kalshi.discover_series()
        except (TransportError, WireShapeError) as exc:
            return {}, (), (_blocked(exc),)

        families = {str(spec["family"]) for spec in cohort}
        keyword_terms = [term for terms in DOWNSTREAM_SERIES_QUERIES.values() for term in terms]
        for family in sorted(families):
            terms = DOWNSTREAM_SERIES_QUERIES.get(family, ())
            matched: list[str] = []
            for item in listing:
                if not isinstance(item, Mapping):
                    continue
                ticker = item.get("ticker")
                if not isinstance(ticker, str) or not ticker:
                    continue
                haystack = " ".join(
                    str(item.get(key, "")) for key in ("ticker", "title", "category")
                ).lower()
                if any(term in haystack for term in terms):
                    matched.append(ticker)
            # Discovery is informational; it never overrides a prespecified series.
            considered[family] = sorted(set(matched))

        # The policy verdicts are bounded to series that a policy-relevant keyword
        # put in scope, so the exclusions a reader needs are reported without
        # dumping the whole catalog.
        verdicts: list[PolicySeriesVerdict] = []
        seen: set[str] = set()
        for item in listing:
            if not isinstance(item, Mapping):
                continue
            ticker = item.get("ticker")
            if not isinstance(ticker, str) or not ticker:
                continue
            haystack = " ".join(str(item.get(key, "")) for key in ("ticker", "title")).lower()
            # Scope, not admission. A series is classified when its own text
            # suggests a policy rate or meeting, or when a Fed/release keyword put it
            # in view, so that a foreign central bank and a foreign inflation series
            # are recorded as exclusions with their reason instead of never being
            # seen. Admission is decided by classify_policy_series.
            if not (
                "fed" in haystack
                or "fomc" in haystack
                or _POLICY_TITLE_PATTERN.search(haystack)
                or any(term in haystack for term in keyword_terms)
            ):
                continue
            if ticker in seen:
                continue
            seen.add(ticker)
            verdicts.append(classify_policy_series(item))
        return considered, tuple(verdicts), tuple(block_ref)

    def build_event_card(self, audit: CohortAudit, *, out: pathlib.Path) -> dict[str, Any]:
        """One fully raw-provenanced event card for the first audited event.

        Every field either comes from an archived payload and names its hash, or
        is explicitly ``null`` with a reason. Nothing is carried over from a
        plausible-looking default.
        """
        audited = [e for e in audit.events if e.status.startswith("audited")]
        chosen = audited[0] if audited else None
        if chosen is None:
            return {
                "status": "no_audited_event",
                "reason": (
                    "no event completed with an eligible contract set; an event card "
                    "cannot be produced from an access failure"
                ),
                "cohort_definition_hash": audit.cohort_definition_hash,
                "raw_hashes": [],
            }

        eligible = chosen.eligible_candidates
        primary = eligible[0] if eligible else None
        card: dict[str, Any] = {
            "status": "audited",
            "gate": "G0",
            "event_id": chosen.event_id,
            "family": chosen.family,
            "reference_period": chosen.reference_period,
            "scheduled_at": chosen.scheduled_at.isoformat(),
            "scheduled_at_basis": "the cohort configuration's own aware instant",
            "source_urls": dict(chosen.source_urls),
            "window_seconds_before": int(
                (chosen.scheduled_at - chosen.window_start).total_seconds()
            ),
            "window_seconds_after": int((chosen.window_end - chosen.scheduled_at).total_seconds()),
            "window_start": chosen.window_start.isoformat(),
            "window_end": chosen.window_end.isoformat(),
            "release": chosen.release,
            # ``True`` states that the values in ``release`` are the first release
            # and that revisions are held apart from them. With no release payload
            # there is nothing to assert, and the ``release_payload_archived`` gate
            # below already blocks exactly this claim, so the two must agree.
            "release_values_are_first_release": (True if chosen.release is not None else None),
            "revisions_kept_separate": (True if chosen.release is not None else None),
            "consensus_available": False,
            "consensus_note": chosen.missing_expectations["reason"],
            "partition": chosen.partition,
            "cohort_size": dict(chosen.cohort_size),
            "eligible_contract_count": len(eligible),
            "direct_closed_pre_release_count": len(chosen.direct_closed_pre_release),
            "excluded_contract_count": len(chosen.downstream_candidates) - len(eligible),
            "coverage_gates": [g.as_dict() for g in chosen.gates],
            "unsatisfied_gate_names": [g.name for g in chosen.unsatisfied_gates],
            "primary_contract": primary.as_dict() if primary else None,
            "rule_meaning": _rule_meaning(primary) if primary else None,
            "candle_resolution_by_contract": [
                {
                    "ticker": a.ticker,
                    "requested_interval_minutes": a.interval_minutes,
                    "candle_count": a.candle_count,
                    "holes_present": a.spacing.get("holes_present"),
                    "usable_for_replay": a.spacing.get("usable_for_replay"),
                    "raw_hash": a.raw_hash,
                }
                for a in chosen.candle_audits
            ],
            "trade_counts": chosen.trade_counts,
            "what_the_feeds_showed": _what_feeds_showed(chosen),
            "when_the_system_could_know": _knowability(chosen),
            "claims_established_only_where_gates_satisfied": True,
            "claims_supported": [
                "the candidate contract set the venue listed at audit time, with "
                "attempted, acquired and eligible sizes reported separately",
                *(
                    [
                        "the archived initial release values and the exact payload hash "
                        "they came from"
                    ]
                    if chosen.release is not None
                    else []
                ),
                "the requested and actually observed candle resolution for the contracts attempted",
            ],
            "claims_blocked_by_unsatisfied_gates": {
                gate.name: list(gate.blocks) for gate in chosen.unsatisfied_gates
            },
            "claims_not_supported": [
                "any surprise or expectation-relative response: no point-in-time "
                "expectation source is available to this project",
                "any order-book depth or tick-level reconstruction from candles",
                "any statement about messages that were not observed",
                "causal attribution of any price movement to this release",
            ],
            "raw_provenance": {
                "event_raw_hashes": list(chosen.raw_hashes),
                "release_raw_hash": (chosen.release.get("raw_hash") if chosen.release else None),
                "cohort_definition_hash": audit.cohort_definition_hash,
                "card_path": str(out / "event_card.json"),
            },
            "blocked": [dict(b) for b in chosen.blocked],
            "errors": list(chosen.errors),
        }
        policy = dict(audit.policy_cohort)
        policy_candidates = [c for c in chosen.candidates if c.policy_series]
        card["primary_downstream_cohort"] = {
            **policy,
            "event_policy_contract_count": len(policy_candidates),
            "event_policy_contracts_study_eligible": len(
                [c for c in policy_candidates if c.study_eligible]
            ),
            "event_policy_contracts": [
                {
                    "ticker": c.ticker,
                    "series_ticker": c.series_ticker,
                    "eligible": c.eligible,
                    "study_eligible": c.study_eligible,
                    "window_overlap": c.window_overlap,
                }
                for c in policy_candidates
            ],
            "exposure_relation_is_hypothesis": True,
        }
        card["series_selection"] = dict(audit.series_selection)
        card["study_eligibility"] = dict(audit.study_eligibility)
        return card

    def _limitations(
        self,
        audits: Sequence[EventAudit],
        discovery_blocked: Sequence[Mapping[str, Any]],
    ) -> tuple[str, ...]:
        notes: list[str] = [
            "Kalshi documents historical candle intervals of 1, 60 and 1440 minutes "
            "only; no finer resolution was requested and none is available",
            "historical candles are candle-frequency observations and are not "
            "evidence of complete historical order-book depth",
            "the live/historical cutoff moves, so a window recorded here may be "
            "served by a different endpoint on a later run",
            "no point-in-time expectation source is available, so no surprise slope "
            "is estimable from this audit",
            "the public order-book snapshot carries no sequence number, so it cannot "
            "close a sequence gap",
        ]
        if discovery_blocked:
            notes.append(
                "series discovery did not complete; only the prespecified series "
                "were used, so downstream coverage is narrower than intended"
            )
        if any(e.status == "blocked" for e in audits):
            notes.append(
                "at least one event produced no contract universe; it is recorded as "
                "blocked and is not counted as audited"
            )
        if any(e.errors for e in audits):
            notes.append(
                "some endpoint calls failed; every failure is listed per event with "
                "its reason and any archived failing payload"
            )
        policy_configured = any(c.policy_series for e in audits for c in e.candidates)
        if not policy_configured:
            notes.append(
                "no contract from the configured primary policy cohort was acquired; "
                "the downstream policy coverage claim is bounded by that, and the "
                "event is not evidence that policy contracts do not exist"
            )
        notes.append(
            "a market's recorded times date the market, not the version of its rule "
            "text that a later fetch returned, so no rule becomes study-eligible "
            "without a configured verification record naming that contract, matching "
            "the fetched rule hash and covering the release instant; "
            "lifecycle-eligible contracts are reported separately from study-eligible "
            "ones, and the venue's own creation time is kept as a lifecycle fact "
            "rather than read as a rule publication time"
        )
        notes.append(
            "the primary policy cohort is reached through each series' own event "
            "tickers, so a series whose event listing was truncated is covered to the "
            "bound its listing reported rather than to its full history"
        )
        if any(c.thin_liquidity for e in audits for c in e.candidates):
            notes.append(
                "some eligible contracts are thin; thinness is recorded as an "
                "observed property and was not used to drop a candidate"
            )
        return tuple(notes)


def _candle_quote_counts(
    candles: Iterable[Mapping[str, Any]], *, schema_flavour: str
) -> tuple[int, int, int]:
    """Count candles by quote availability, keyed off the endpoint's leaf names.

    Returns ``(two_sided, one_sided_or_missing, trade_only)``. The two candle
    endpoints share top-level key names with different leaf names, so the flavour
    selects the leaf suffix rather than being guessed at.

    This exists because a returned candle is not a usable quote: a period can
    carry a trade price with no bid or ask at all, and a bid without an ask is not
    an executable two-sided quote. Counting them separately keeps a successful
    request from being read as an eligible observation.
    """
    legacy = schema_flavour == CANDLE_SCHEMA_HISTORICAL
    suffix = "" if legacy else "_dollars"
    two_sided = one_sided = trade_only = 0
    for candle in candles:
        if not isinstance(candle, Mapping):
            continue
        bid = candle.get("yes_bid")
        ask = candle.get("yes_ask")
        bid_close = bid.get(f"close{suffix}") if isinstance(bid, Mapping) else None
        ask_close = ask.get(f"close{suffix}") if isinstance(ask, Mapping) else None
        has_bid = bid_close is not None
        has_ask = ask_close is not None
        if has_bid and has_ask:
            two_sided += 1
            continue
        one_sided += 1
        price = candle.get("price")
        trade_close = price.get(f"close{suffix}") if isinstance(price, Mapping) else None
        if trade_close is not None and not has_bid and not has_ask:
            trade_only += 1
    return two_sided, one_sided, trade_only


def _cohort_size(audits: Sequence[EventAudit]) -> dict[str, int]:
    """Totals per coverage stage across the cohort, summed from the events."""
    keys = (
        "attempted_records",
        "acquired_candidates",
        "deduped_candidates",
        "downstream_candidates",
        "direct_closed_pre_release",
        "eligible_downstream",
        "policy_series_candidates",
        "policy_series_lifecycle_eligible",
        "policy_series_study_eligible",
        "direct_release_candidates",
        "selected_for_deep_audit",
    )
    return {key: sum(int(e.cohort_size.get(key, 0)) for e in audits) for key in keys}


def _candidate_provenance_gate(blocked: Sequence[str]) -> CoverageGate:
    """Whether every candidate's archived origin was resolved before reporting.

    A candidate is only evidence if the page it names is retrievable, the locator
    inside that page resolves to a record, and that record is the one cited. A
    candidate whose source was missing, truncated in the page's JSON, unresolvable
    or a pointer to a different record is not a candidate at all, so such records
    are never reported: the gate exists to name the loss at the event level rather
    than let a silently smaller candidate set look like a smaller cohort.
    """
    if blocked:
        shown = "; ".join(blocked[:3])
        detail = (
            f"{len(blocked)} record(s) that lifecycle eligibility would have admitted "
            f"were not reported because their archived origin could not be resolved "
            f"({shown})"
        )
    else:
        detail = (
            "every reported candidate's page was retrieved from the raw store and its "
            "JSON pointer resolved to the record it cites"
        )
    return CoverageGate(
        name="candidate_origins_verified",
        satisfied=not blocked,
        detail=detail,
        blocks=(
            "any statement that the reported candidate set is the set the listing returned",
            "any count of attempted, acquired or eligible contracts for this event",
        )
        if blocked
        else (),
    )


def _event_gates(
    *,
    attempted: int,
    acquired: int,
    downstream: Sequence[CandidateContract],
    direct_closed: Sequence[CandidateContract],
    pages_incomplete: Sequence[str],
    series_blocked: Sequence[str],
    bounded_out: bool,
    candle_audits: Sequence[CandleAudit],
    release_payload: Mapping[str, Any] | None,
    release_embargo_agreement: Any,
    release_blocked: bool,
    discovery_terms: Sequence[str],
    policy_series: Sequence[str] = (),
    policy_candidates: Sequence[CandidateContract] = (),
    provenance_blocked: Sequence[str] = (),
) -> tuple[CoverageGate, ...]:
    """The empirical-coverage gates for one event, each with what it blocks.

    Every gate names a distinct fact that a request succeeding does not establish:
    that the page walk finished, that the series were reachable, that candidate
    selection did not truncate, that a candle series is on grid and spans the
    window, that quotes had two sides, and that the release payload was read. An
    unsatisfied gate blocks the claims listed in its ``blocks`` field, which is
    why the audit result reports them by name rather than folding them into a
    status.
    """
    gates: list[CoverageGate] = []

    gates.append(_candidate_provenance_gate(provenance_blocked))

    gates.append(
        CoverageGate(
            name="contract_universe_attempted",
            satisfied=attempted > 0,
            detail=(
                f"the venue returned {attempted} market record(s) across the queried "
                "listings; this counts attempts, not usable contracts"
            ),
            blocks=("any statement about the contract universe",),
        )
    )
    gates.append(
        CoverageGate(
            name="contract_universe_acquired",
            satisfied=acquired > 0,
            detail=(
                f"{acquired} record(s) normalized into candidates; this is acquisition, "
                "not empirical eligibility"
            ),
            blocks=("any statement about an eligible contract set",),
        )
    )

    incomplete_pages = bool(pages_incomplete)
    gates.append(
        CoverageGate(
            name="listing_pagination_complete",
            satisfied=not incomplete_pages,
            detail=(
                "every listing walk ran to a documented end"
                if not incomplete_pages
                else "listing walks stopped early: " + "; ".join(pages_incomplete)
            ),
            blocks=(
                "a complete contract universe for this event",
                "any coverage count presented as the full universe",
            ),
        )
    )
    gates.append(
        CoverageGate(
            name="prespecified_series_reachable",
            satisfied=not series_blocked,
            detail=(
                "every prespecified series listing was reachable"
                if not series_blocked
                else "unreachable series listings: " + "; ".join(series_blocked)
            ),
            blocks=("coverage of the families behind the unreachable series",),
        )
    )
    gates.append(
        CoverageGate(
            name="candidate_selection_unbounded",
            satisfied=not bounded_out,
            detail=(
                "every surviving candidate was carried into the deep audit"
                if not bounded_out
                else "the candidate bound truncated the deep audit set; the boundary "
                "is a cost bound, not a property of the contracts"
            ),
            blocks=(
                "any claim that the deepest-audited contracts are representative of "
                "the whole candidate set",
            ),
        )
    )
    gates.append(
        CoverageGate(
            name="downstream_post_release_cohort_nonempty",
            satisfied=bool(downstream),
            detail=(
                f"{len(downstream)} candidate(s) were still tradeable across the "
                "post-release window"
            ),
            blocks=("any post-release quote response measurement for this event",),
        )
    )
    # The invariant is that no pre-release-closed contract is counted as
    # downstream empirical eligibility. With no such contract the check is
    # vacuously satisfied, which is correct: there was nothing to separate.
    violated = sorted({c.ticker for c in direct_closed} & {c.ticker for c in downstream})
    gates.append(
        CoverageGate(
            name="direct_closed_contracts_separated",
            satisfied=not violated,
            detail=(
                f"{len(direct_closed)} candidate(s) closed before the release and are "
                "held in the direct_closed_pre_release cohort, excluded from "
                "downstream eligibility"
                if direct_closed
                else "no candidate carried a pre-release close, so no direct-contract "
                "separation was needed for this event"
            )
            + (f"; contradicted for {violated}, which appear in both cohorts" if violated else ""),
            blocks=(
                "any downstream eligibility count that would absorb a direct-resolution contract",
            )
            if violated
            else (),
        )
    )

    # Candle resolution gates. A returned candle is a successful request; whether
    # it is usable is a separate question that the spacing measurement answers.
    attempted_candles = [a for a in candle_audits if a.candle_count > 0]
    holes = [a for a in candle_audits if a.spacing.get("holes_present")]
    off_window = [a for a in candle_audits if not a.spacing.get("covers_window")]
    gates.append(
        CoverageGate(
            name="candle_series_on_grid_without_interior_holes",
            satisfied=bool(attempted_candles) and not holes,
            detail=(
                "every returned candle series is on grid with no interior hole"
                if not holes
                else "interior candle holes in: "
                + ", ".join(f"{a.ticker}@{a.interval_minutes}m" for a in holes)
                + "; a hole breaks reconstruction until a fresh snapshot or a "
                "coarser interval"
            ),
            blocks=(
                "quote reconstruction across the affected window",
                "any claim that the candles are a complete order-book history",
            ),
        )
    )
    gates.append(
        CoverageGate(
            name="candle_series_spans_requested_window",
            satisfied=bool(attempted_candles) and not off_window,
            detail=(
                "every returned candle series spans the requested window"
                if not off_window
                else "series that do not span the window: "
                + ", ".join(f"{a.ticker}@{a.interval_minutes}m" for a in off_window)
                + "; absence before the first or after the last returned period is "
                "not evidence that the market was quiet"
            ),
            blocks=("any statement about quote activity outside the returned range",),
        )
    )

    gates.append(
        CoverageGate(
            name="candle_quotes_two_sided",
            satisfied=any(a.has_two_sided_quotes for a in attempted_candles),
            detail=(
                "at least one returned candle series carries two-sided quotes: "
                + ", ".join(
                    f"{a.ticker}@{a.interval_minutes}m={a.two_sided_quote_candles}"
                    for a in attempted_candles
                    if a.has_two_sided_quotes
                )
                if any(a.has_two_sided_quotes for a in attempted_candles)
                else "no returned candle carries a two-sided quote; a trade price "
                "without a bid and ask is not an executable quote, so this blocks "
                "any quote-based response measurement"
            ),
            blocks=(
                "any quote-based response or spread measurement for this event",
                "any statement that a returned candle is a usable observation",
            ),
        )
    )

    gates.append(
        CoverageGate(
            name="release_payload_archived",
            satisfied=release_payload is not None and not release_blocked,
            detail=(
                "the archived initial release payload was retrieved and parsed"
                if release_payload is not None
                else "the initial release payload could not be retrieved; the failure "
                "is recorded in blocked rather than replaced with an empty release"
            ),
            blocks=(
                "any statement about first-release values for this event",
                "any surprise-relative statement for this event",
            ),
        )
    )
    # ``release_embargo_agreement`` is a closed categorical string, not a boolean:
    # ``unverified`` means no comparison was available and ``differs_from_calendar``
    # means the payload contradicted the calendar. Both are truthy, so testing
    # truthiness passed this gate in exactly the two cases it exists to block.
    embargo_agrees = release_embargo_agreement == "agrees_with_calendar"
    gates.append(
        CoverageGate(
            name="release_time_basis_named",
            satisfied=embargo_agrees,
            detail=(
                "the payload's own embargo line was compared with the calendar "
                f"(agreement: {release_embargo_agreement})"
                if embargo_agrees
                else "no agreeing embargo comparison is available, so the scheduled "
                "instant is a schedule rather than an observed publication time "
                f"(agreement: {release_embargo_agreement!r})"
            ),
            blocks=("any claim that the scheduled instant is an observed publication time",),
        )
    )
    gates.append(
        CoverageGate(
            name="discovery_basis_is_bounded",
            satisfied=True,
            detail=(
                "downstream series came from a keyword filter over the exchange's own "
                f"listing (terms: {list(discovery_terms)}); a keyword filter bounds the "
                "candidate set and cannot establish the complete universe"
            ),
            blocks=("any claim of a globally complete downstream universe",),
        )
    )
    # The primary cohort gate. With no configured policy series there is nothing to
    # cover, so it is vacuously satisfied; with them configured, a read that reached
    # none is a coverage limit rather than a policy result. A policy contract that
    # was acquired but is not lifecycle-eligible does not satisfy it either, because
    # the claim being gated is about the cohort the releases are measured against.
    if not policy_series:
        policy_detail = (
            "no policy series were configured for this run, so the primary "
            "downstream cohort is outside its scope; a policy coverage claim needs "
            f"{POLICY_COHORT_SECTION}.{POLICY_FAMILY_KEY}.observed_venue_series"
        )
    elif policy_candidates:
        policy_detail = (
            f"{len(policy_candidates)} contract(s) from the configured policy series "
            f"{list(policy_series)} were acquired"
        )
    else:
        policy_detail = (
            f"the configured policy series {list(policy_series)} were queried and "
            "returned no candidate contract for this event; the primary downstream "
            "cohort is therefore unreached here rather than empty"
        )
    gates.append(
        CoverageGate(
            name="configured_policy_cohort_reached",
            satisfied=bool(policy_candidates) if policy_series else True,
            detail=policy_detail,
            blocks=(
                "any statement about the primary downstream policy cohort for this release",
                "any propagation claim resting on a policy contract",
            )
            if policy_series
            else (),
        )
    )
    fed_series = sorted({c.series_ticker for c in policy_candidates})
    gates.append(
        CoverageGate(
            name="policy_contracts_are_not_direct_release_contracts",
            satisfied=not (fed_series and set(fed_series) & set(DIRECT_RELEASE_SERIES)),
            detail=(
                "policy cohort contracts carry series "
                f"{fed_series or ['none']}, disjoint from the direct release series "
                f"{list(DIRECT_RELEASE_SERIES)}"
            ),
            blocks=("any downstream policy count that would absorb a direct-release contract",),
        )
    )
    return tuple(gates)


def _blocked(exc: Exception) -> Mapping[str, Any]:
    if isinstance(exc, TransportError):
        return exc.as_blocked_record()
    return {
        "recorded": True,
        "empty_result": False,
        "url": None,
        "status_code": None,
        "reason": f"{type(exc).__name__}: {exc}",
        "attempts": 0,
        "payload_hash": None,
        "observed_at": dt.datetime.now(dt.UTC).isoformat(),
    }


def _series_of(ticker: str, prefix: str = "", event_ticker: Any = None) -> str:
    """Recover a series ticker without guessing at its shape.

    Tries, in order: the documented ``series_ticker`` field, an event ticker's
    documented ``SERIES-EVENT`` prefix, then the ticker's own leading segment.
    Only the last is a heuristic, and it is only reached when the venue supplied
    neither of the identifying fields.
    """
    if prefix:
        return prefix
    event = str(event_ticker or "")
    if "-" in event:
        return event.split("-", 1)[0]
    return ticker.split("-", 1)[0]


def _select_candidates(
    candidates: Sequence[CandidateContract], *, limit: int
) -> list[CandidateContract]:
    """Bound the *deep-audit* set, preferring contracts tradeable post-release.

    ``window_overlap`` is decided per candidate against its own ``open_time`` and
    ``close_time`` while the record is parsed, so this needs no window bounds of its
    own.

    This chooses which contracts get the bounded candle and trade queries; it does
    not decide membership of the cohort. A contract the venue closed before the
    release is a direct-resolution contract whose post-release book was closed, so
    a post-release quote query against it would return an access-shaped absence
    rather than an observation. It is therefore ordered behind the contracts that
    were still tradeable, and it stays in the returned event's candidate set under
    its own cohort label rather than being dropped. Its count is reported, so the
    separation is visible instead of silent. Liquidity never participates in this
    choice.

    The configured primary policy series rank ahead of everything else. A cost
    bound must truncate the tail of the candidate set, never the primary cohort, so
    the ordering starts from ``policy_series`` and only then reaches the alphabetic
    tiebreak; sorting by ticker first would let an alphabetically earlier lookalike
    displace the cohort the audit exists to measure.
    """
    ranked = sorted(
        candidates,
        key=lambda c: (
            # The configured primary cohort first, then downstream window-spanning
            # contracts, which are the only ones that can carry a post-release
            # response.
            not c.policy_series,
            c.closed_before_release,
            not c.window_overlap,
            c.ticker,
        ),
    )
    return ranked[:limit]


def _parse_time(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = dt.datetime.fromtimestamp(float(value), tz=dt.UTC)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = dt.datetime.fromisoformat(text)
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _record_hash(record: Mapping[str, Any]) -> str:
    """Digest of the market record as received.

    This identifies which entry in an archived page a candidate came from. It is
    not a retrievable payload: re-serializing a record does not reproduce the
    bytes the venue sent, so it is reported as ``record_hash`` beside the page
    hash rather than as the source of record.
    """
    payload = json.dumps(record, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _dedupe_candidates(
    candidates: Iterable[CandidateContract],
) -> list[CandidateContract]:
    """Keep one entry per ticker, preferring the historical-partition record.

    The same market can appear in both partitions when a window straddles the
    cutoff. The archived (historical) record is the preferred vintage, and when
    the two records disagree on rule text the surviving record carries an explicit
    ``rule_text_differs_between_partitions`` exclusion.

    The comparison happens whichever order the two arrive in. Skipping it whenever
    the historical record happened to come last would let a genuine rule change
    between vintages vanish, which is the one thing this check exists to prevent.
    """
    by_ticker: dict[str, CandidateContract] = {}
    for candidate in candidates:
        existing = by_ticker.get(candidate.ticker)
        if existing is None:
            by_ticker[candidate.ticker] = candidate
            continue
        preferred, other = (
            (candidate, existing) if candidate.partition == "historical" else (existing, candidate)
        )
        if preferred.rule_hash != other.rule_hash:
            by_ticker[candidate.ticker] = dataclasses.replace(
                preferred,
                exclusion_reasons=tuple(
                    dict.fromkeys(
                        (
                            *preferred.exclusion_reasons,
                            "rule_text_differs_between_partitions",
                        )
                    )
                ),
            )
        else:
            by_ticker[candidate.ticker] = preferred
    return sorted(by_ticker.values(), key=lambda c: c.ticker)


def _digest_cohort(cohort: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(list(cohort), sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _rule_meaning(candidate: CandidateContract) -> dict[str, Any]:
    """State what the contract paid on, from its own recorded terms."""
    operator = {
        "greater": ">",
        "greater_or_equal": ">=",
        "less": "<",
        "less_or_equal": "<=",
    }.get(candidate.strike_type, candidate.strike_type or "unknown")
    threshold = candidate.floor_strike
    if threshold is None:
        meaning = "no numeric strike recorded; the rule text is the authority"
    else:
        meaning = (
            f"YES pays 1 if the reference statistic is {operator} {threshold} "
            f"in {candidate.strike_type or 'the stated'} terms"
        )
    return {
        "series_ticker": candidate.series_ticker,
        "strike_type": candidate.strike_type,
        "operator": operator,
        "threshold": str(threshold) if threshold is not None else None,
        "cap_strike": str(candidate.cap_strike) if candidate.cap_strike else None,
        "rule_hash": candidate.rule_hash,
        "rule_available_at": (
            candidate.rule_available_at.isoformat() if candidate.rule_available_at else None
        ),
        "meaning": meaning,
        "rule_text_is_the_authority": True,
    }


def _what_feeds_showed(event: EventAudit) -> dict[str, Any]:
    """Summarize what the observed feeds contained, and what they could not."""
    candles = event.candle_audits
    with_holes = [a for a in candles if a.spacing.get("holes_present")]
    usable = [a for a in candles if a.spacing.get("usable_for_replay")]
    return {
        "candle_queries": len(candles),
        "candle_queries_with_holes": len(with_holes),
        "candle_queries_on_grid": len(usable),
        "trade_counts": dict(event.trade_counts),
        "distinct_resolutions_requested": sorted({a.interval_minutes for a in candles}),
        "observed_resolutions": sorted(
            {a.spacing.get("requested_interval_minutes") for a in candles if a.candle_count}
        ),
        "quotes_available_at": "candle close only; no intra-candle quote timestamps",
        "book_depth": "not available from candles; no sizes or levels are published",
        "note": (
            "a candle with no trade still carries bid and ask levels, so quote "
            "presence and trade presence are independent facts here"
        ),
    }


def _knowability(event: EventAudit) -> dict[str, Any]:
    """State the time precision attached to what this audit observed.

    The distinction this carries is the whole point of the field group: the
    payload's embargo line is the release's *own claim about its schedule*, read
    out of a document fetched later. It is evidence about when the material was
    scheduled to become public, not an observation of when it did, and it is
    labelled that way rather than reported as an observed publication time.
    """
    release = event.release or {}
    return {
        "release_scheduled_at": event.scheduled_at.isoformat(),
        "release_scheduled_precision": "minute, from the study cohort configuration",
        "release_embargo_line_agreement": release.get("schedule_agreement", "unverified"),
        "release_embargo_evidence_kind": (
            "source_claim_about_schedule: the archived payload's own embargo line, "
            "read from a document fetched after the event"
        ),
        "release_embargo_timestamp": release.get("embargo_time_from_payload"),
        "release_embargo_is_not_observed_publication": True,
        "release_observed_publication_at": None,
        "release_observed_publication_unavailable_reason": (
            "a later fetch cannot establish when a payload first became public, and "
            "no capture observed this release at its publication instant"
        ),
        "release_availability_upper_bound_basis": (
            "the scheduled instant, admitted only as a documented schedule when the "
            "payload's own embargo line agrees with it; never as an observed "
            "publication time"
        ),
        "candle_usable_time": (
            "response receipt time, which is when this system could first have read the payload"
        ),
        "candle_source_time": "candle period end, as published by the venue",
        "trade_source_time": "venue trade execution timestamp",
        "trade_receipt_time": "response receipt time, recorded per payload",
        "monotonic_ns_available": True,
        "monotonic_precision_claim": (
            "int monotonic_ns only; sub-second source precision is not claimed"
        ),
        "unsatisfied_coverage_gates": [g.name for g in event.unsatisfied_gates],
    }


#: Version of the external coverage contract, recorded on every report so a grid
#: can be told apart from one built under different evidence rules.
EXTERNAL_COVERAGE_VERSION = "external_coverage_v1"

#: The blocked literal the CLI tests for. A gate is blocked or it passes; there is
#: no third state, because an undetermined gate must not read as a satisfied one.
GATE_BLOCKED = "blocked"
GATE_PASS = "pass"

#: Reason vocabulary for the external coverage grid. Each names a distinct way a
#: candidate can fail to become an observation.
REASON_RULE_EVIDENCE_MISSING = "rule_evidence_missing"
REASON_RULE_VERSION_UNKNOWN = "rule_version_unknown"
REASON_RULE_SEMANTICS_UNVERIFIED = "settlement_semantics_unverified"
REASON_CONTRACT_CLOSED_BEFORE_RELEASE = "contract_closed_before_release"
REASON_NO_WINDOW_OVERLAP = "no_observation_window_overlap"
REASON_LIFECYCLE_INELIGIBLE = "lifecycle_ineligible"
REASON_SUBSTRING_SERIES_MATCH_REFUSED = "substring_series_match_refused"
REASON_TRADE_COUNTS_NOT_JOINED = "trade_counts_not_joined_for_pair"
REASON_CLOCK_UNIDENTIFIABLE = "availability_unidentifiable"

#: The series identifier is the leading run of capital letters in a ticker. Kalshi
#: event identifiers embed hexadecimal suffixes, so a substring match on a series
#: name returns unrelated markets: ``%FED%`` matches
#: ``KXMVENFLSINGLEGAME-S2025FED4B0DA5B1``, a Monday Night Football market. Series
#: identity is therefore read from the front of the ticker and nowhere else.
_SERIES_PREFIX = re.compile(r"^[A-Z]+")


def series_of(ticker: str) -> str | None:
    """The series identifier a ticker belongs to, or ``None`` when it has none."""
    if not isinstance(ticker, str):
        return None
    match = _SERIES_PREFIX.match(ticker.strip())
    return match.group(0) if match else None


def is_exact_series(ticker: str, series: str) -> bool:
    """Whether ``ticker`` belongs to ``series`` by exact prefix identity.

    A substring test is deliberately not offered: it is the mistake this function
    exists to make impossible.
    """
    if not isinstance(series, str) or not series:
        return False
    found = series_of(ticker)
    return found is not None and found == series


@dataclass(frozen=True, slots=True)
class ExternalCandidatePair:
    """One candidate contract for one release, before any observation is measured.

    The fields are exactly the ones the bounded public audit publishes per
    candidate, so a pair can be rebuilt from that artifact rather than
    re-derived, and so the provenance of the candidate universe travels with it.
    """

    event_id: str
    family: str
    ticker: str
    series_ticker: str | None = None
    lifecycle_eligible: bool | None = None
    rule_version_verified: bool = False
    rule_version_evidence: Mapping[str, Any] | None = None
    rule_hash: str | None = None
    close_time: str | None = None
    window_overlap: bool | None = None
    selection_basis: str | None = None
    cohort: str | None = None
    pre_window_trades: int | None = None
    baseline_window_trades: int | None = None
    release_window_trades: int | None = None
    endpoint_window_trades: int | None = None
    baseline_age_seconds: float | None = None
    endpoint_age_seconds: float | None = None
    available_outcome_axes: tuple[str, ...] = ()
    size_quality: str | None = None
    source_time_precision: str | None = None

    @classmethod
    def from_candidate(
        cls, event_id: str, family: str, candidate: Mapping[str, Any]
    ) -> ExternalCandidatePair:
        """Rebuild one pair from a candidate row of the bounded audit's artifact."""
        return cls(
            event_id=event_id,
            family=family,
            ticker=str(candidate.get("ticker") or ""),
            series_ticker=_optional_str(candidate.get("series_ticker")),
            lifecycle_eligible=_optional_bool(candidate.get("lifecycle_eligible")),
            rule_version_verified=bool(candidate.get("rule_version_verified") or False),
            rule_version_evidence=candidate.get("rule_version_evidence"),
            rule_hash=_optional_str(candidate.get("rule_hash")),
            close_time=_optional_str(candidate.get("close_time")),
            window_overlap=_optional_bool(candidate.get("window_overlap")),
            selection_basis=_optional_str(candidate.get("selection_basis")),
            cohort=_optional_str(candidate.get("cohort")),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in (
                "event_id",
                "family",
                "ticker",
                "series_ticker",
                "lifecycle_eligible",
                "rule_version_verified",
                "rule_hash",
                "close_time",
                "window_overlap",
                "selection_basis",
                "cohort",
            )
        }


@dataclass(frozen=True, slots=True)
class ExternalContractCoverage:
    """Everything the coverage grid establishes about one event and contract pair.

    A missing measurement is ``None``, never zero. An unmeasured window count and a
    window that genuinely held no trade are different facts, and a grid that
    reported both as ``0`` would erase the distinction the plan exists to keep.
    """

    event_id: str
    family: str
    ticker: str
    venue: str
    lifecycle_eligible: bool
    rule_verified: bool
    baseline_observed: bool | None
    endpoint_observed: bool | None
    pre_window_trades: int | None
    baseline_window_trades: int | None
    release_window_trades: int | None
    endpoint_window_trades: int | None
    baseline_age_seconds: float | None
    endpoint_age_seconds: float | None
    available_outcome_axes: tuple[str, ...]
    size_quality: str | None
    source_time_precision: str | None
    valid_horizons: tuple[int, ...]
    exclusion_reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "family": self.family,
            "ticker": self.ticker,
            "venue": self.venue,
            "lifecycle_eligible": self.lifecycle_eligible,
            "rule_verified": self.rule_verified,
            "baseline_observed": self.baseline_observed,
            "endpoint_observed": self.endpoint_observed,
            "pre_window_trades": self.pre_window_trades,
            "baseline_window_trades": self.baseline_window_trades,
            "release_window_trades": self.release_window_trades,
            "endpoint_window_trades": self.endpoint_window_trades,
            "baseline_age_seconds": self.baseline_age_seconds,
            "endpoint_age_seconds": self.endpoint_age_seconds,
            "available_outcome_axes": list(self.available_outcome_axes),
            "size_quality": self.size_quality,
            "source_time_precision": self.source_time_precision,
            "valid_horizons": list(self.valid_horizons),
            "exclusion_reasons": list(self.exclusion_reasons),
        }


@dataclass(frozen=True, slots=True)
class ExternalCoverageReport:
    """The external G0 grid: which candidates exist and why any is not an observation."""

    release_dataset: str
    events: tuple[Mapping[str, Any], ...]
    pairs: tuple[ExternalContractCoverage, ...]
    counts: Mapping[str, Any]
    blocked: tuple[Mapping[str, Any], ...]
    gates: Mapping[str, Any]
    gate_g0: str
    capabilities: Mapping[str, Any]
    flags: tuple[str, ...]
    #: What the caller actually read to build this grid. Recorded rather than
    #: assumed, so a grid built without rule evidence says so in the artifact.
    inputs_read: Mapping[str, Any] = field(default_factory=dict)
    coverage_version: str = EXTERNAL_COVERAGE_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "gate": "G0",
            "coverage_version": self.coverage_version,
            "release_dataset": self.release_dataset,
            "gate_g0": self.gate_g0,
            "gates": dict(self.gates),
            "event_count": len(self.events),
            "pair_count": len(self.pairs),
            "counts": dict(self.counts),
            "blocked": [dict(entry) for entry in self.blocked],
            "blocked_count": len(self.blocked),
            "capabilities": dict(self.capabilities),
            "flags": list(self.flags),
            "inputs_read": dict(self.inputs_read),
            "events": [dict(event) for event in self.events],
            "pairs": [pair.as_dict() for pair in self.pairs],
            "missing_cells_retained": True,
            "unmeasured_is_not_zero": True,
            "candidate_universe_is_pre_event": True,
            "counts_are_not_empirical_results": True,
        }


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "yes"):
            return True
        if lowered in ("false", "no"):
            return False
    return None


#: The evidence a rule verification must bind.
#:
#: This is the study's own required set, taken from
#: :data:`REQUIRED_RULE_VERSION_EVIDENCE_FIELDS`, not a second one. A record that
#: satisfies the bounded audit's contract has to satisfy this check too, or coverage
#: would refuse exactly the evidence the audit accepts and no rule could ever be
#: verified through this path.
RULE_EVIDENCE_BINDINGS: tuple[tuple[str, str], ...] = (
    ("contract_id", "the contract identity the rule belongs to"),
    ("rule_hash", "the raw rule document's hash"),
    ("source_url", "where the rule text was read from"),
    ("verified_by", "who verified it, and by what method"),
    ("in_force_from", "the start of the interval the rule was in force"),
    ("observed_at", "when the verification was performed, not when the rule was written"),
    ("settlement_semantics", "what the verified text resolves on"),
)

#: Bound when present, but not required. An open interval is stated as open rather
#: than demanded, so a rule version still in force is not refused for lacking an end.
RULE_EVIDENCE_OPTIONAL_BINDINGS: tuple[tuple[str, str], ...] = (
    ("in_force_to", "the end of the interval, null when the version is still in force"),
)

#: Field aliases accepted for each binding. Different vintages and registries name
#: the same fact differently, and refusing a documented alias would block evidence
#: that exists. The canonical name is always first.
_RULE_EVIDENCE_ALIASES: Mapping[str, tuple[str, ...]] = {
    "contract_id": ("contract_id", "ticker"),
    "rule_hash": ("rule_hash", "raw_rule_hash", "document_hash"),
    "source_url": ("source_url", "source", "url", "rule_source"),
    "verified_by": ("verified_by", "verifier", "verified_by_method"),
    "in_force_from": ("in_force_from", "effective_from", "rule_effective_from"),
    "observed_at": ("observed_at", "rule_available_at", "observed_at_utc"),
    "settlement_semantics": ("settlement_semantics", "settlement", "payout_rule"),
    "in_force_to": ("in_force_to", "effective_to", "rule_effective_to"),
}


def validate_rule_evidence(
    evidence: Mapping[str, Any] | None,
    *,
    ticker: str,
    release_family: str,
) -> dict[str, Any]:
    """Whether one contract's rule evidence is complete enough to admit a row.

    The plan requires rule verification to bind contract identity, the raw rule hash,
    the source, the observer, the in-force interval and the settlement semantics.
    Missing evidence is not waived by a trade-history join, so an incomplete record
    returns ``verified: False`` with every unbound field named rather than being
    accepted on the strength of the contracts that traded.

    The required set is the bounded audit's own, so a record this check accepts is one
    that audit accepts too.
    """
    contract = _optional_str(ticker)
    family = _optional_str(release_family)
    if contract is None:
        raise ValueError("validate_rule_evidence requires a non-empty ticker")
    if family is None:
        raise ValueError("validate_rule_evidence requires a non-empty release_family")

    record = evidence if isinstance(evidence, Mapping) else {}
    bound: dict[str, Any] = {}
    missing: list[str] = []
    for name, _ in RULE_EVIDENCE_BINDINGS:
        found = None
        for alias in _RULE_EVIDENCE_ALIASES.get(name, (name,)):
            if record.get(alias) is not None:
                found = record.get(alias)
                break
        if found is None:
            missing.append(name)
        else:
            bound[name] = found
    for name, _ in RULE_EVIDENCE_OPTIONAL_BINDINGS:
        for alias in _RULE_EVIDENCE_ALIASES.get(name, (name,)):
            if record.get(alias) is not None:
                bound[name] = record.get(alias)
                break

    # A bound contract identity must be this contract. Evidence that names a
    # different ticker is not evidence about this one.
    if "contract_id" in bound and _optional_str(bound["contract_id"]) != contract:
        return {
            "verified": False,
            "ticker": contract,
            "release_family": family,
            "missing": ["contract_id"],
            "bound": bound,
            "reason": "rule_evidence_names_a_different_contract",
            "note": (
                "the evidence binds another contract, so it establishes nothing about this one"
            ),
        }

    # A rule hash that is not a digest of rule text compares unequal to every fetched
    # text, so accepting it would certify nothing while looking like evidence.
    digest = _optional_str(bound.get("rule_hash"))
    if digest is not None and not _RULE_HASH_PATTERN.fullmatch(digest):
        return {
            "verified": False,
            "ticker": contract,
            "release_family": family,
            "missing": ["rule_hash"],
            "bound": bound,
            "reason": "rule_hash_is_not_a_digest_of_rule_text",
            "note": (
                "the hash is not a sha256 digest, so it cannot bind a verdict to exact "
                "rule text and would certify nothing"
            ),
        }

    verified = not missing
    return {
        "verified": verified,
        "ticker": contract,
        "release_family": family,
        "missing": missing,
        "bound": bound,
        "reason": None if verified else REASON_RULE_EVIDENCE_MISSING,
        "missing_bindings": [name for name, _ in RULE_EVIDENCE_BINDINGS if name in missing],
        "bindings": dict(RULE_EVIDENCE_BINDINGS),
        "optional_bindings": dict(RULE_EVIDENCE_OPTIONAL_BINDINGS),
        "note": (
            "every required binding is present"
            if verified
            else "a rule that cannot bind all of these establishes nothing about the contract"
        ),
    }


def _coverage_for_pair(
    pair: ExternalCandidatePair,
    *,
    settings: Any,
    rule: Mapping[str, Any],
) -> ExternalContractCoverage:
    """One pair's grid cell, with every unmeasured field left null."""
    reasons: list[str] = []
    lifecycle_eligible = pair.lifecycle_eligible is True

    if not lifecycle_eligible:
        reasons.append(REASON_LIFECYCLE_INELIGIBLE)
    if pair.window_overlap is False:
        reasons.append(REASON_NO_WINDOW_OVERLAP)
    if pair.close_time is not None and pair.lifecycle_eligible is not True:
        reasons.append(REASON_CONTRACT_CLOSED_BEFORE_RELEASE)

    rule_verified = bool(rule.get("verified"))
    if not rule_verified:
        missing = set(rule.get("missing") or ())
        if rule.get("reason") == "rule_evidence_names_a_different_contract":
            reasons.append(REASON_RULE_EVIDENCE_MISSING)
        elif "rule_hash" in missing or not pair.rule_version_verified:
            reasons.append(REASON_RULE_VERSION_UNKNOWN)
        elif "settlement_semantics" in missing:
            reasons.append(REASON_RULE_SEMANTICS_UNVERIFIED)
        else:
            reasons.append(REASON_RULE_EVIDENCE_MISSING)

    counted = any(
        count is not None
        for count in (
            pair.baseline_window_trades,
            pair.endpoint_window_trades,
            pair.release_window_trades,
        )
    )
    if counted:
        baseline_observed: bool | None = bool(pair.baseline_window_trades)
        endpoint_observed: bool | None = bool(pair.endpoint_window_trades)
    else:
        baseline_observed = None
        endpoint_observed = None
        reasons.append(REASON_TRADE_COUNTS_NOT_JOINED)

    horizons = tuple(int(h) for h in (getattr(settings, "horizons_seconds", ()) or ()))
    # A horizon is valid for a pair only when a row could exist for it, which
    # needs the pair admitted at all. Nothing here widens a horizon to rescue a
    # pair with no observation.
    valid_horizons = horizons if (lifecycle_eligible and rule_verified) else ()

    return ExternalContractCoverage(
        event_id=pair.event_id,
        family=pair.family,
        ticker=pair.ticker,
        venue="kalshi",
        lifecycle_eligible=lifecycle_eligible,
        rule_verified=rule_verified,
        baseline_observed=baseline_observed,
        endpoint_observed=endpoint_observed,
        pre_window_trades=pair.pre_window_trades,
        baseline_window_trades=pair.baseline_window_trades,
        release_window_trades=pair.release_window_trades,
        endpoint_window_trades=pair.endpoint_window_trades,
        baseline_age_seconds=pair.baseline_age_seconds,
        endpoint_age_seconds=pair.endpoint_age_seconds,
        available_outcome_axes=tuple(pair.available_outcome_axes),
        size_quality=pair.size_quality,
        source_time_precision=pair.source_time_precision,
        valid_horizons=valid_horizons,
        exclusion_reasons=tuple(dict.fromkeys(reasons)),
    )


def _coverage_counts(
    pairs: Sequence[ExternalContractCoverage],
    *,
    events: Sequence[Mapping[str, Any]],
    settings: Any,
) -> dict[str, Any]:
    """Pair-class counts, with unmeasured classes reported as null and explained.

    Counting an unmeasured class as ``0`` would report "no pair was observed" when
    the truth is "no window count was joined", so every count whose input is
    missing is ``None`` and carries its reason.
    """
    by_event: dict[str, dict[str, Any]] = {}
    for pair in pairs:
        block = by_event.setdefault(
            pair.event_id,
            {
                "family": pair.family,
                "candidate_pairs": 0,
                "lifecycle_eligible_pairs": 0,
                "rule_verified_pairs": 0,
                "blocked_rule_candidates": 0,
                "baseline_observed_pairs": 0,
                "endpoint_observed_pairs": 0,
                "baseline_measurement_complete": True,
                "endpoint_measurement_complete": True,
                "exclusion_reason_counts": {},
                "tickers": [],
            },
        )
        block["candidate_pairs"] += 1
        block["tickers"].append(pair.ticker)
        if pair.lifecycle_eligible:
            block["lifecycle_eligible_pairs"] += 1
        if pair.rule_verified:
            block["rule_verified_pairs"] += 1
        else:
            block["blocked_rule_candidates"] += 1
        if pair.baseline_observed is None:
            block["baseline_measurement_complete"] = False
        elif pair.baseline_observed:
            block["baseline_observed_pairs"] += 1
        if pair.endpoint_observed is None:
            block["endpoint_measurement_complete"] = False
        elif pair.endpoint_observed:
            block["endpoint_observed_pairs"] += 1
        for reason in pair.exclusion_reasons:
            counts = block["exclusion_reason_counts"]
            counts[reason] = counts.get(reason, 0) + 1

    for block in by_event.values():
        if not block["baseline_measurement_complete"]:
            block["baseline_observed_pairs"] = None
        if not block["endpoint_measurement_complete"]:
            block["endpoint_observed_pairs"] = None

    overall = {
        "candidate_pairs": len(pairs),
        "lifecycle_eligible_pairs": sum(1 for p in pairs if p.lifecycle_eligible),
        "rule_verified_pairs": sum(1 for p in pairs if p.rule_verified),
        "blocked_rule_candidates": sum(1 for p in pairs if not p.rule_verified),
        "baseline_observed_pairs": (
            sum(1 for p in pairs if p.baseline_observed)
            if all(p.baseline_observed is not None for p in pairs)
            else None
        ),
        "endpoint_observed_pairs": (
            sum(1 for p in pairs if p.endpoint_observed)
            if all(p.endpoint_observed is not None for p in pairs)
            else None
        ),
        "distinct_release_clusters": len({p.event_id for p in pairs}),
        "release_clusters": sorted({p.event_id for p in pairs}),
        "release_count": len(events),
        "horizons_seconds": list(getattr(settings, "horizons_seconds", ()) or ()),
        "primary_horizon_seconds": getattr(settings, "primary_horizon_seconds", None),
        "preselected_denominator": "candidate_pairs_in_declared_observation_window",
    }
    unmeasured = {
        name: REASON_TRADE_COUNTS_NOT_JOINED
        for name in ("baseline_observed_pairs", "endpoint_observed_pairs")
        if overall[name] is None
    }
    overall["unmeasured"] = unmeasured
    return {"overall": overall, "by_event": by_event}


def build_external_coverage(
    *,
    releases: Sequence[Mapping[str, Any]],
    pairs: Sequence[ExternalCandidatePair | Mapping[str, Any]],
    settings: Any,
    events: Sequence[Mapping[str, Any]] | None = None,
    policy_series: Sequence[str] | None = None,
    config: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
) -> ExternalCoverageReport:
    """Build the external release/rule coverage grid for the configured cohort.

    The grid keeps every candidate, including the zero-activity ones and the ones a
    missing rule version blocks, because a contract that existed before the release
    and then traded nothing is an observed property rather than a reason to drop a
    row. Post-event activity never selects the candidate universe: the universe is
    whatever the pre-event artifact already named.
    """
    blocking = config.get("blocking") if isinstance(config, Mapping) else None
    blocking = blocking if isinstance(blocking, Mapping) else {}
    release_dataset = str(blocking.get("release_dataset") or "")

    normalised: list[ExternalCandidatePair] = []
    refused_series: list[dict[str, Any]] = []
    for entry in pairs:
        pair = (
            entry
            if isinstance(entry, ExternalCandidatePair)
            else ExternalCandidatePair(
                event_id=str(entry.get("event_id") or ""),
                family=str(entry.get("family") or ""),
                ticker=str(entry.get("ticker") or ""),
                series_ticker=_optional_str(entry.get("series_ticker")),
                lifecycle_eligible=_optional_bool(entry.get("lifecycle_eligible")),
                rule_version_verified=bool(entry.get("rule_version_verified") or False),
                rule_version_evidence=entry.get("rule_version_evidence"),
                rule_hash=_optional_str(entry.get("rule_hash")),
                close_time=_optional_str(entry.get("close_time")),
                window_overlap=_optional_bool(entry.get("window_overlap")),
                selection_basis=_optional_str(entry.get("selection_basis")),
                cohort=_optional_str(entry.get("cohort")),
            )
        )
        if policy_series:
            # Exact series identity only. A candidate whose series is not one the
            # study configured is refused rather than reinterpreted, and the
            # refusal is recorded so the excluded universe stays visible.
            matched = [s for s in policy_series if is_exact_series(pair.ticker, s)]
            if not matched:
                refused_series.append(
                    {
                        "ticker": pair.ticker,
                        "series": series_of(pair.ticker),
                        "event_id": pair.event_id,
                        "reason": REASON_SUBSTRING_SERIES_MATCH_REFUSED,
                    }
                )
                continue
        normalised.append(pair)

    cells: list[ExternalContractCoverage] = []
    blocked: list[dict[str, Any]] = []
    for pair in normalised:
        rule = validate_rule_evidence(
            pair.rule_version_evidence,
            ticker=pair.ticker,
            release_family=pair.family,
        )
        cell = _coverage_for_pair(pair, settings=settings, rule=rule)
        cells.append(cell)
        for reason in cell.exclusion_reasons:
            if reason in (REASON_TRADE_COUNTS_NOT_JOINED,):
                continue
            blocked.append(
                {
                    "scope": "pair",
                    "event_id": cell.event_id,
                    "ticker": cell.ticker,
                    "reason": reason,
                }
            )

    counts = _coverage_counts(cells, events=releases, settings=settings)
    if refused_series:
        counts["overall"]["series_refused_by_exact_identity"] = len(refused_series)
        counts["refused_series"] = refused_series

    eligibility = None
    if isinstance(config, Mapping):
        eligibility = config.get("study_eligibility")
    eligibility = eligibility if isinstance(eligibility, Mapping) else {}

    def _gate(name: str) -> dict[str, Any]:
        if name in eligibility:
            entry = eligibility.get(name)
            if isinstance(entry, Mapping):
                satisfied = bool(entry.get("satisfied"))
                return {
                    "satisfied": satisfied,
                    "source": "bounded_audit_artifact",
                    "detail": dict(entry),
                    "reason": None if satisfied else str(entry.get("reason") or name),
                }
        return {
            "satisfied": False,
            "source": "not_established",
            "detail": None,
            "reason": f"{name}_not_established_by_any_artifact",
        }

    rule_gate = {
        "satisfied": counts["overall"]["rule_verified_pairs"] > 0,
        "source": "external_coverage_grid",
        "detail": {
            "rule_verified_pairs": counts["overall"]["rule_verified_pairs"],
            "blocked_rule_candidates": counts["overall"]["blocked_rule_candidates"],
        },
        "reason": (
            None
            if counts["overall"]["rule_verified_pairs"] > 0
            else "no candidate carries a rule version verified against its own rule document"
        ),
    }
    cohort_gate = _gate("rule_vintage_gate")
    frequency_gate = {
        "satisfied": bool(getattr(settings, "horizons_seconds", ())),
        "source": "pipeline_configuration",
        "detail": {"horizons_seconds": list(getattr(settings, "horizons_seconds", ()) or ())},
        "reason": None,
    }
    gates = {
        "rule_gate": rule_gate,
        "rule_vintage_gate": cohort_gate,
        "source_semantics_gate": _gate("source_semantics_gate"),
        "supported_frequency_gate": frequency_gate,
    }
    unsatisfied = [name for name, gate in gates.items() if not gate["satisfied"]]
    for name in unsatisfied:
        if name == "rule_gate":
            continue
        blocked.append(
            {
                "scope": "cohort",
                "event_id": None,
                "ticker": None,
                "reason": str(gates[name]["reason"] or name),
                "gate": name,
            }
        )
    gate_g0 = GATE_PASS if not unsatisfied else GATE_BLOCKED

    capabilities = {
        "historical_trades": True,
        "historical_quotes": False,
        "receipt_clock": False,
        "rule_vintage_verified": bool(counts["overall"]["rule_verified_pairs"]),
        "initial_release_verified": bool(releases),
        "expectation_verified": False,
        "economic_size_verified": False,
    }
    flags: list[str] = [
        "candidate_universe_came_from_the_pre_event_artifact",
        "missing_cells_retained_in_the_grid",
        "unmeasured_pair_classes_reported_as_null",
        "series_identity_matched_exactly_not_by_substring",
    ]
    if counts["overall"]["baseline_observed_pairs"] is None:
        flags.append("pair_level_window_counts_not_joined")
    if not releases:
        flags.append("no_releases_supplied")
    if refused_series:
        flags.append("some_candidates_refused_by_series_identity")

    return ExternalCoverageReport(
        release_dataset=release_dataset,
        events=tuple(dict(event) for event in releases),
        pairs=tuple(cells),
        counts=counts,
        blocked=tuple(blocked),
        gates=gates,
        gate_g0=gate_g0,
        capabilities=capabilities,
        flags=tuple(flags),
        inputs_read=dict(inputs) if isinstance(inputs, Mapping) else {},
    )


def _none_if_missing(value: Any) -> Any:
    """A stored null as ``None``, never as a pandas or numpy missing marker.

    ``pandas.NaT`` is an instance of ``datetime``, so a NaT that reaches the JSON
    serializer is treated as an instant and fails in ``astimezone`` rather than
    being written as a null. Mapping every missing marker to ``None`` here keeps a
    missing value missing all the way to the artifact.
    """
    if value is None:
        return None
    if type(value).__name__ in ("NaTType", "NAType"):
        return None
    if isinstance(value, float) and value != value:
        return None
    return value


def _records(frame: Any) -> list[dict[str, Any]]:
    """Rows of a sealed frame with every missing marker mapped to ``None``."""
    return [
        {str(key): _none_if_missing(value) for key, value in record.items()}
        for record in frame.to_dict("records")
    ]


def _read_json_if_present(path: pathlib.Path) -> Any:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _repo_relative(candidate: Any, *, config_path: pathlib.Path) -> pathlib.Path | None:
    """Resolve a configured path against the checkout, not the working directory."""
    text = _optional_str(candidate)
    if text is None:
        return None
    raw = pathlib.Path(text).expanduser()
    if raw.is_absolute():
        return raw
    for root in (config_path.resolve().parent.parent, pathlib.Path.cwd()):
        if (root / raw).exists():
            return root / raw
    return config_path.resolve().parent.parent / raw


def load_coverage_inputs(
    config_path: str | pathlib.Path = "configs/external_history_v1.yaml",
    *,
    release_dataset: str | pathlib.Path | None = None,
    max_contracts: int | None = None,
    audit_coverage_path: str | pathlib.Path | None = None,
    rule_evidence_path: str | pathlib.Path | None = None,
) -> dict[str, Any]:
    """Load everything the external coverage grid is built from.

    The candidate and rule semantics live here rather than in the command handler,
    so the CLI stays a thin forwarder and a second caller cannot assemble a
    different candidate universe from the same configuration.

    Returns exactly the keys the caller consumes: ``releases``, ``pairs``,
    ``settings``, ``config`` and ``inputs``. ``inputs`` records what was actually
    read, so a grid built without rule evidence or without a candidate artifact
    says so instead of looking like a grid with nothing to report.
    """
    import yaml

    from .. import trade_panel as panel_module
    from ..storage import read_parquet as _read_sealed

    path = pathlib.Path(config_path)
    if not path.is_file():
        raise FileNotFoundError(f"pipeline configuration not found: {path}")
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(config, Mapping):
        raise ValueError(f"{path} does not parse to a configuration mapping")
    inputs_block = config.get("inputs") or {}
    if not isinstance(inputs_block, Mapping):
        raise ValueError(f"{path} declares a non-mapping inputs block")

    dataset = release_dataset or inputs_block.get("release_dataset")
    dataset_path = _repo_relative(dataset, config_path=path)
    if dataset_path is None or not dataset_path.is_file():
        raise FileNotFoundError(
            f"sealed release dataset not found: {dataset!r}; coverage is built from the "
            "archived releases and never from a substitute cohort"
        )
    frame = _read_sealed(dataset_path)
    releases = _records(frame)

    evidence_path = rule_evidence_path or inputs_block.get("rule_evidence_source")
    resolved_evidence = _repo_relative(evidence_path, config_path=path)
    rule_evidence = _read_json_if_present(resolved_evidence) if resolved_evidence else None

    coverage_source = audit_coverage_path or inputs_block.get("audit_coverage")
    resolved_coverage = _repo_relative(coverage_source, config_path=path)
    audit_coverage = _read_json_if_present(resolved_coverage) if resolved_coverage else None

    configured_series = tuple(
        str(name) for name in (config.get("policy_series") or ()) if isinstance(name, str)
    )
    cap = (
        max_contracts
        if isinstance(max_contracts, int) and not isinstance(max_contracts, bool)
        else None
    )
    if cap is None:
        extraction = config.get("extraction") or {}
        cap = extraction.get("max_contracts_per_event") if isinstance(extraction, Mapping) else None
    cap = cap if isinstance(cap, int) and not isinstance(cap, bool) else None

    pairs: list[ExternalCandidatePair] = []
    events_seen = 0
    capped_events: list[str] = []
    audit_events = (
        (audit_coverage or {}).get("events") if isinstance(audit_coverage, Mapping) else None
    )
    for event in audit_events or ():
        if not isinstance(event, Mapping):
            continue
        events_seen += 1
        event_id = str(event.get("event_id") or "")
        family = str(event.get("family") or "")
        candidates = [c for c in (event.get("candidates") or ()) if isinstance(c, Mapping)]
        if configured_series:
            candidates = [
                c
                for c in candidates
                if any(is_exact_series(str(c.get("ticker") or ""), s) for s in configured_series)
            ]
        if cap is not None and len(candidates) > cap:
            capped_events.append(event_id)
            candidates = candidates[:cap]
        for candidate in candidates:
            pair = ExternalCandidatePair.from_candidate(event_id, family, candidate)
            # The rule registry's own record for this contract, when it holds one.
            # The registry currently publishes no eligible market ids, so this is
            # normally absent and every rule stays unverified.
            if pair.rule_version_evidence is None and isinstance(rule_evidence, Mapping):
                pair = dataclasses.replace(
                    pair,
                    rule_version_evidence=rule_evidence.get(pair.ticker)
                    if isinstance(rule_evidence.get(pair.ticker), Mapping)
                    else pair.rule_version_evidence,
                )
            pairs.append(pair)

    settings = panel_module.load_panel_settings(path)

    return {
        "releases": releases,
        "pairs": pairs,
        "settings": settings,
        "config": {**dict(config), "blocking": {"release_dataset": str(dataset_path)}},
        "inputs": {
            "config_path": str(path),
            "release_dataset": str(dataset_path),
            "release_count": len(releases),
            "rule_evidence_path": str(resolved_evidence) if resolved_evidence else None,
            "rule_evidence_present": rule_evidence is not None,
            "rule_evidence_records_eligible_market_ids": (
                bool(rule_evidence.get("this_file_records_eligible_market_ids"))
                if isinstance(rule_evidence, Mapping)
                else None
            ),
            "audit_coverage_path": str(resolved_coverage) if resolved_coverage else None,
            "audit_coverage_present": audit_coverage is not None,
            "audited_events_read": events_seen,
            "policy_series_configured": list(configured_series),
            "max_contracts": cap,
            "max_contracts_applied": cap,
            "events_capped_by_max_contracts": capped_events,
            "candidate_pairs": len(pairs),
            "selection_basis": "configured_series_identity_over_pre_event_audit_candidates",
            "post_event_activity_used_for_selection": False,
        },
    }


__all__ = [
    "COHORT_DIRECT_CLOSED_PRE_RELEASE",
    "COHORT_DOWNSTREAM",
    "DEFAULT_AFTER_SECONDS",
    "DEFAULT_BEFORE_SECONDS",
    "DIRECT_RELEASE_SERIES",
    "DOWNSTREAM_SERIES_QUERIES",
    "EXTERNAL_COVERAGE_VERSION",
    "FAMILY_RELEASE_SLUG",
    "FAMILY_SERIES",
    "GATE_BLOCKED",
    "GATE_PASS",
    "POLICY_COHORT_CONFIG_KEY",
    "POLICY_COHORT_SECTION",
    "POLICY_EXPOSURE_MECHANISM",
    "POLICY_FAMILY_KEY",
    "REQUESTED_INTERVALS",
    "RULE_EVIDENCE_BINDINGS",
    "RULE_EVIDENCE_OPTIONAL_BINDINGS",
    "RULE_VERSION_EVIDENCE_KEY",
    "THIN_VOLUME_FP",
    "CandidateContract",
    "CandidateOriginMismatch",
    "CandleAudit",
    "CohortAudit",
    "CohortAuditor",
    "CoverageGate",
    "EventAudit",
    "ExternalCandidatePair",
    "ExternalContractCoverage",
    "ExternalCoverageReport",
    "PolicyCohort",
    "PolicySeriesVerdict",
    "RuleVersionEvidence",
    "VerifiedOrigin",
    "build_external_coverage",
    "classify_policy_series",
    "event_cohort",
    "is_exact_series",
    "load_coverage_inputs",
    "policy_cohort",
    "rule_version_evidence",
    "series_of",
    "validate_rule_evidence",
]
