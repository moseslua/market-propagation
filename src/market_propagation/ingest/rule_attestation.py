"""Capture of public rule text, and attestation of a per-contract rule vintage.

The whole propagation programme is blocked on one input. Every other quantity the
graph needs is measured; what is missing is a per-contract record stating which
version of a contract's rule text was in force over an interval, which is why
``reports/study_execution_status.md`` reports ``rule_vintage_unverified`` for every
one of the 785 receiver-side decisions. This module produces that record, and it
produces nothing else.

The consumer already exists and is imported rather than restated:
:class:`market_propagation.ingest.audit.RuleVersionEvidence`, whose
:meth:`~market_propagation.ingest.audit.RuleVersionEvidence.applies_to` is the
in-force test the graph asks. A second definition of that record would be a second
answer to the same question, so the requirement this module is built to satisfy is
checked against the class's own fields when the configuration is loaded: a
configuration that named a different field set would fail at the boundary instead
of producing records the consumer cannot read.

Four rules decide what a report from this module can mean, and each is enforced
structurally rather than left to a caller's discipline.

**Captured bytes are archived before anything reads them, and the digest addresses
those bytes.** Capture goes through
:class:`market_propagation.ingest.transport.HttpTransport` and
:class:`market_propagation.storage.RawStore`, the same GET-only, retrying,
archive-first pair the rest of the ingest package uses. No second HTTP client
exists here. A rule hash that does not resolve to archived bytes is a refusal, so a
record cannot cite a digest nothing holds.

**An interval bound comes only from a source whose own dating bounds it.** This is
the crux, and it is the reason the gate is still blocked rather than closed. A
contract's ``open_time``, ``close_time``, ``created_time``, ``updated_time``,
``settlement_ts`` and the instant a capture was taken are each refused as an
interval bound, **each under its own named reason**, so a report says which
inadmissible bound was offered instead of quietly ignoring it. The venue was
measured rewriting settled records: all eleven ``FED-25MAY`` markets closed
2025-05-07 and carry ``updated_time`` 2026-02-19T08:48:16Z, about nine and a half
months later, with no revision history exposed. A venue's current rule text
therefore cannot attest its own past version, and text read today yields no
interval at all rather than an interval guessed from the fetch claiming it.

**A capture that cannot bound an interval yields a refusal, never a default.** No
bound is imputed from the epoch, from ``now``, or from a sibling contract's dates.
An interval whose end does not follow its start is refused with the consumer's own
empty-interval rule in mind rather than clamped, and two dated sources that state
different intervals are refused because the choice between them would not be
derivable from the configuration.

**An unattested contract is a named refusal, not a zero.** A contract holding no
capture at all is reported as ``no_capture_holds_this_contract`` with a count of
one, because an unmeasured quantity and a measured absence are different states and
the report must not let the first read as the second.

What this module does **not** do is widen the evidence standard. The two dated
routes that were actually located are declared in the configuration — an archived
snapshot of the rule page, and a regulatory self-certification filing that names
the contract — and both were measured to cover none of the declared contracts, so a
run over this repository attests nothing and says so. That absence is the finding,
and it is a coverage result rather than a defect in the standard.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields
from html import unescape
from pathlib import Path
from typing import Any

import yaml

from ..domain import parse_utc_time
from ..storage import RawStore, _atomic_write_bytes, hash_bytes
from .audit import RuleVersionEvidence
from .transport import HttpTransport, ResponseEnvelope

__all__ = [
    "ATTESTATION_REPORT_NAME",
    "ATTESTATION_REPORT_VERSION",
    "BOUND_BASES",
    "BOUND_BASIS_OBSERVATION_INSTANT",
    "BOUND_BASIS_STATED_INTERVAL",
    "CONFIG_PATH",
    "REFUSALS",
    "REFUSED_BOUND_REASONS",
    "RULE_CAPTURE_VERSION",
    "SOURCE_KIND_LIVE_RULE_TEXT",
    "AttestationRefusal",
    "AttestationSettings",
    "ContractAttestation",
    "DatedSourceKind",
    "RefusedBoundField",
    "RuleAttestationReport",
    "RuleAttestor",
    "RuleCapture",
    "RuleCaptureStore",
    "load_attestation_settings",
    "rule_text_digest",
    "visible_text",
]

#: Path to the configuration this module reads its evidence standard from,
#: relative to the repository root.
CONFIG_PATH = "configs/rule_attestation_v1.yaml"

#: Version of this configuration file that this module can read.
DECLARED_CONFIG_VERSION = "rule_attestation_v1"

#: Version stamped into a capture record and into a report, so a reader can tell
#: which capture scheme and which evidence standard produced what it is reading.
RULE_CAPTURE_VERSION = "rule_attestation_v1"
ATTESTATION_REPORT_VERSION = "rule_attestation_v1"

#: File written under an output directory by :func:`RuleAttestationReport.write`.
ATTESTATION_REPORT_NAME = "rule_attestation_report.json"

#: How a bound is read from an admissible dated source.
#:
#: ``stated_effective_interval`` means the document states the interval itself.
#: ``observation_instant`` means the document is a dated observation of the live
#: rule page, so the text it carries was live at that instant and only at that
#: instant.
BOUND_BASIS_STATED_INTERVAL = "stated_effective_interval"
BOUND_BASIS_OBSERVATION_INSTANT = "observation_instant"

#: The permitted bound bases. A configuration naming another one is refused at the
#: boundary that read it rather than silently creating a third route.
BOUND_BASES: tuple[str, ...] = (BOUND_BASIS_STATED_INTERVAL, BOUND_BASIS_OBSERVATION_INSTANT)

#: The declared kind of a dated observation of the venue's live market listing.
SOURCE_KIND_LIVE_RULE_TEXT = "dated_observation_of_the_live_rule_text"

#: Refusal codes. Each names one distinct way a contract fails to reach an attested
#: rule vintage, so a report states which one applied rather than reporting a rate.
REFUSAL_CAPTURE_STORE_ABSENT = "capture_store_is_absent"
REFUSAL_NO_CAPTURES_HELD = "no_capture_holds_this_contract"
REFUSAL_CAPTURE_RECORD_MALFORMED = "capture_record_is_malformed"
REFUSAL_CAPTURE_EVIDENCE_MISSING = "capture_bytes_are_not_archived"
REFUSAL_CAPTURE_DIGEST_MISMATCH = "capture_bytes_do_not_match_the_recorded_digest"
REFUSAL_CAPTURE_CONTRACT_MISMATCH = "capture_is_indexed_under_a_different_contract"
REFUSAL_RULE_TEXT_ABSENT = "capture_carries_no_rule_text"
REFUSAL_INADMISSIBLE_SOURCE_KIND = "capture_kind_is_not_an_admissible_dated_source"
REFUSAL_SOURCE_NAMES_NO_CONTRACT = "dated_source_does_not_name_the_contract"
REFUSAL_SETTLEMENT_SEMANTICS_UNSTATED = "capture_states_no_settlement_semantics"
REFUSAL_UNDECLARED_BOUND_FIELD = "offered_bound_field_is_not_declared"
REFUSAL_NO_BOUNDING_SOURCE = "no_admissible_dated_source_bounds_this_interval"
REFUSAL_AMBIGUOUS_STATED_INTERVALS = "two_dated_sources_state_different_intervals"
REFUSAL_EMPTY_INTERVAL = "stated_interval_is_empty"
REFUSAL_RULE_HASH_NOT_CAPTURED = "no_capture_carries_the_requested_rule_hash"

#: The reason each field a caller may offer as an interval bound is refused, and the
#: single place those reasons are spelled. ``configs/rule_attestation_v1.yaml``
#: declares the same mapping, and the loader refuses a configuration whose mapping
#: differs, so an edit to one cannot silently change what the other reports.
#:
#: These are separate codes rather than one ``inadmissible_bound`` because the
#: states are different: ``open_time`` is when the *previous* meeting resolved,
#: ``updated_time`` dates the venue's most recent rewrite of a settled record, and
#: a capture instant dates the fetch. A report that collapsed them could not say
#: which inadmissible route was attempted.
REFUSED_BOUND_REASONS: Mapping[str, str] = {
    "open_time": "recorded_open_time_is_not_a_rule_bound",
    "close_time": "recorded_close_time_is_not_a_rule_bound",
    "created_time": "recorded_created_time_is_not_a_rule_bound",
    "updated_time": "recorded_updated_time_is_not_a_rule_bound",
    "settlement_ts": "recorded_settlement_time_is_not_a_rule_bound",
    "capture_time": "current_rule_text_is_not_historical_rule_evidence",
}

#: The refusals a caller can branch on, in the order this module applies them.
REFUSALS: tuple[str, ...] = (
    REFUSAL_CAPTURE_STORE_ABSENT,
    REFUSAL_NO_CAPTURES_HELD,
    REFUSAL_CAPTURE_RECORD_MALFORMED,
    REFUSAL_CAPTURE_EVIDENCE_MISSING,
    REFUSAL_CAPTURE_DIGEST_MISMATCH,
    REFUSAL_CAPTURE_CONTRACT_MISMATCH,
    REFUSAL_RULE_TEXT_ABSENT,
    REFUSAL_INADMISSIBLE_SOURCE_KIND,
    REFUSAL_SOURCE_NAMES_NO_CONTRACT,
    REFUSAL_SETTLEMENT_SEMANTICS_UNSTATED,
    *REFUSED_BOUND_REASONS.values(),
    REFUSAL_UNDECLARED_BOUND_FIELD,
    REFUSAL_RULE_HASH_NOT_CAPTURED,
    REFUSAL_AMBIGUOUS_STATED_INTERVALS,
    REFUSAL_EMPTY_INTERVAL,
    REFUSAL_NO_BOUNDING_SOURCE,
)

#: A contract identifier is used as a directory name, so it is restricted to the
#: characters a venue ticker actually carries and refused otherwise. Sanitizing
#: instead would make two distinct identifiers collide on one directory, which is
#: how one contract's captured rule text ends up certifying another's.
_SAFE_CONTRACT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

_HASH_RE = re.compile(r"^[0-9a-f]{64}$")

#: The body of a ``<script>`` or ``<style>`` element, and any remaining tag. Both
#: are removed when a capture's text is read from an HTML page, and both are
#: declared lossy below rather than presented as the page's own text.
_SCRIPT_OR_STYLE = re.compile(r"(?is)<(script|style)\b[^>]*>.*?</\1\s*>")
_TAG = re.compile(r"(?s)<[^>]*>")
_INLINE_SPACE = re.compile(r"[ \t\r\f\v]+")

#: Why a field the evidence standard does not declare may not serve as a bound.
_UNDECLARED_FIELD_WHY = (
    "the evidence standard does not declare this field as a bound, and an undeclared "
    "field is refused rather than read as the declared field it resembles"
)


def _require_mapping(value: Any, *, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be a mapping, got {type(value).__name__}")
    return value


def _require_sequence(value: Any, *, where: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{where} must be a sequence, got {type(value).__name__}")
    return value


def _require_text(value: Any, *, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where} must be a non-empty string, got {value!r}")
    return value.strip()


def _optional_instant(value: Any, *, where: str) -> dt.datetime | None:
    if value is None or value == "":
        return None
    return parse_utc_time(value, field_name=where)


def visible_text(body: bytes, *, content_type: str = "") -> str | None:
    """The readable text of a captured body, or ``None`` when it carries none.

    This is a **lossy** reduction and is declared as one: markup, script and style
    bodies and inline whitespace runs are removed, and the result is not the page.
    It exists because a capture record has to carry the rule text a reader can
    check, and the bytes it was read from are archived separately and are what the
    digest addresses. A body that reduces to no text at all — a client-rendered
    page shell, a JSON error envelope, an empty fetch — returns ``None``, which is
    a capture carrying no rule text rather than an empty rule.
    """
    text = bytes(body).decode("utf-8", errors="replace")
    if "html" not in content_type.lower():
        return text if text.strip() else None
    stripped = _INLINE_SPACE.sub(" ", unescape(_TAG.sub("", _SCRIPT_OR_STYLE.sub(" ", text))))
    lines = [line.strip() for line in stripped.splitlines()]
    kept = "\n".join(line for line in lines if line)
    return kept or None


def rule_text_digest(rule_text: str) -> str:
    """The sha256 of the rule text one capture records.

    The observation run is grouped on *this*, not on the digest of the archived bytes.
    A captured live listing page carries fields that move between fetches — volume,
    last price, open interest — while the rule text inside it does not, so grouping on
    the payload would close a run on every re-fetch of an unchanged rule text and a
    version that never changed would be certified one observation at a time and never
    left open. The ``rule_hash`` a record publishes remains the digest of the archived
    bytes, which is the binding the consumer requires and the payload a reader can
    re-verify; only the grouping reads the text.
    """
    return hash_bytes(str(rule_text).encode("utf-8"))


@dataclass(frozen=True, slots=True)
class DatedSourceKind:
    """One declared category of dated source, and what it is allowed to bound.

    ``bound_basis`` states where a bound is read from in this kind of source, and it
    is read from the configuration rather than from the capture, so a capture cannot
    promote itself into a stronger route than the one it was taken under.
    ``must_name_the_contract`` states whether a document of this kind bounds an
    individual contract's text at all: a regulatory filing that names a product
    template and no market cannot, which is exactly the limit that was measured on
    the one filing read in full.
    """

    source_id: str
    bound_basis: str
    must_name_the_contract: bool
    why: str


@dataclass(frozen=True, slots=True)
class RefusedBoundField:
    """One field that may be offered as an interval bound, with the reason it is not."""

    field: str
    reason: str
    why: str


@dataclass(frozen=True, slots=True)
class AttestationSettings:
    """The declared evidence standard, read from the configuration.

    ``required_record_fields`` is held here rather than in the configuration alone
    because the configuration's list is checked against the consumer's own fields
    when this is built. The record this module produces is the record
    :class:`~market_propagation.ingest.audit.RuleVersionEvidence` consumes, and a
    requirement that drifted from that class would otherwise be discovered only as a
    ``TypeError`` at the far end of the pipeline.
    """

    config_version: str
    capture_root: Path
    raw_subdirectory: str
    index_subdirectory: str
    required_record_fields: tuple[str, ...]
    admissible_sources: Mapping[str, DatedSourceKind]
    inadmissible_sources: Mapping[str, str]
    #: The refused-bound vocabulary, keyed by field, each carrying the evidence
    #: standard's own statement of why it may not bound an interval. The statement
    #: travels with the reason so a report quotes the declared rule rather than a
    #: second telling of it kept in code.
    refused_fields: Mapping[str, RefusedBoundField]
    verification_methods: Mapping[str, str]
    open_interval_semantics: str
    rule_hash_binds: str
    venue_rule_text_cannot_attest_its_own_past_version: bool

    def source_kind(self, source_id: str) -> DatedSourceKind | None:
        """The declared kind of a capture's source, or ``None`` when undeclared."""
        return self.admissible_sources.get(str(source_id))

    def is_declared_source(self, source_id: str) -> bool:
        """Whether the evidence standard names this source kind at all."""
        name = str(source_id)
        return name in self.admissible_sources or name in self.inadmissible_sources

    def refused_field(self, field_name: str) -> RefusedBoundField:
        """The declared refusal for a field offered as an interval bound.

        A field the evidence standard does not declare is refused as undeclared
        rather than interpreted, so an offered ``expiration_time`` cannot be read as a
        bound by resemblance to a declared one.
        """
        name = str(field_name)
        declared = self.refused_fields.get(name)
        if declared is not None:
            return declared
        return RefusedBoundField(
            field=name, reason=REFUSAL_UNDECLARED_BOUND_FIELD, why=_UNDECLARED_FIELD_WHY
        )

    def verification_method(self, bound_basis: str) -> str:
        """The ``verified_by`` method for an interval read under ``bound_basis``."""
        try:
            return self.verification_methods[bound_basis]
        except KeyError as exc:
            raise ValueError(
                f"the evidence standard declares no verification method for bound basis "
                f"{bound_basis!r}; a record whose interval was read one way may not claim a "
                "method it was not built by"
            ) from exc


def load_attestation_settings(config_path: str | Path = CONFIG_PATH) -> AttestationSettings:
    """Read the declared evidence standard from its configuration file.

    Every mismatch here is a configuration fault raised at the boundary that read it
    rather than a partial standard applied silently: an unexpected
    ``config_version``, a ``required_record_fields`` list that disagrees with the
    consumer's own fields, a bound basis outside :data:`BOUND_BASES`, or a
    refused-field mapping that disagrees with :data:`REFUSED_BOUND_REASONS`.
    """
    path = Path(config_path)
    if not path.exists():
        raise ValueError(
            f"no rule-attestation configuration at {path}; the evidence standard is declared "
            "in a configuration file and this module keeps no fallback copy of it"
        )
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(
            f"rule-attestation configuration at {path} is not valid YAML: {exc}"
        ) from exc
    config = _require_mapping(payload, where=str(path))

    version = _require_text(config.get("config_version"), where=f"{path}: config_version")
    if version != DECLARED_CONFIG_VERSION:
        raise ValueError(
            f"{path} declares config_version {version!r}, and this module reads only "
            f"{DECLARED_CONFIG_VERSION!r}"
        )

    capture = _require_mapping(config.get("capture"), where=f"{path}: capture")
    index_subdirectory = _require_text(
        capture.get("index_subdirectory"), where=f"{path}: capture.index_subdirectory"
    )
    raw_subdirectory = _require_text(
        capture.get("raw_subdirectory"), where=f"{path}: capture.raw_subdirectory"
    )
    capture_root = Path(_require_text(capture.get("root"), where=f"{path}: capture.root"))
    if index_subdirectory == raw_subdirectory:
        raise ValueError(
            f"{path}: the capture index and the raw archive both use "
            f"{index_subdirectory!r}; two different stores under one directory collide"
        )

    record = _require_mapping(config.get("record"), where=f"{path}: record")
    declared_fields = tuple(
        _require_text(name, where=f"{path}: record.required_record_fields")
        for name in _require_sequence(
            record.get("required_record_fields"), where=f"{path}: record.required_record_fields"
        )
    )
    consumer_fields = tuple(field.name for field in fields(RuleVersionEvidence))
    if sorted(declared_fields) != sorted(consumer_fields):
        missing = sorted(set(consumer_fields) - set(declared_fields))
        extra = sorted(set(declared_fields) - set(consumer_fields))
        raise ValueError(
            f"{path}: record.required_record_fields disagrees with "
            f"{RuleVersionEvidence.__module__}.{RuleVersionEvidence.__qualname__}: missing "
            f"{missing}, not a field of the consumer {extra}. The record this pipeline "
            "produces is the record the graph consumes, so the two cannot state "
            "different requirements."
        )
    produced = _require_text(record.get("produced_type"), where=f"{path}: record.produced_type")
    if not produced.endswith(RuleVersionEvidence.__qualname__):
        raise ValueError(
            f"{path}: record.produced_type is {produced!r}, and this module produces "
            f"{RuleVersionEvidence.__module__}.{RuleVersionEvidence.__qualname__}"
        )

    dated = _require_mapping(config.get("dated_sources"), where=f"{path}: dated_sources")
    admissible: dict[str, DatedSourceKind] = {}
    for index, entry in enumerate(
        _require_sequence(dated.get("admissible"), where=f"{path}: dated_sources.admissible")
    ):
        where = f"{path}: dated_sources.admissible[{index}]"
        item = _require_mapping(entry, where=where)
        kind = DatedSourceKind(
            source_id=_require_text(item.get("id"), where=f"{where}.id"),
            bound_basis=_require_text(item.get("bound_basis"), where=f"{where}.bound_basis"),
            must_name_the_contract=bool(item.get("must_name_the_contract")),
            why=str(item.get("why", "")),
        )
        if kind.bound_basis not in BOUND_BASES:
            raise ValueError(
                f"{where}.bound_basis is {kind.bound_basis!r}, and the declared bases are "
                f"{list(BOUND_BASES)}; an undeclared basis is a third route with no stated "
                "rule for reading a bound from it"
            )
        if kind.source_id in admissible:
            raise ValueError(f"{where}.id repeats the declared source kind {kind.source_id!r}")
        admissible[kind.source_id] = kind
    inadmissible: dict[str, str] = {}
    for index, entry in enumerate(
        _require_sequence(dated.get("inadmissible"), where=f"{path}: dated_sources.inadmissible")
    ):
        where = f"{path}: dated_sources.inadmissible[{index}]"
        item = _require_mapping(entry, where=where)
        source_id = _require_text(item.get("id"), where=f"{where}.id")
        if source_id in admissible:
            raise ValueError(
                f"{where}.id {source_id!r} is declared both admissible and inadmissible"
            )
        inadmissible[source_id] = str(item.get("why", ""))
    if not dated.get("venue_rule_text_cannot_attest_its_own_past_version"):
        raise ValueError(
            f"{path}: dated_sources.venue_rule_text_cannot_attest_its_own_past_version must be "
            "declared. A venue's current rule text cannot attest its own past version, and a "
            "standard that omitted this would admit exactly the inference that is measured "
            "false: the venue rewrites settled records months after settlement and exposes no "
            "revision history."
        )

    declared_refusals: dict[str, RefusedBoundField] = {}
    for index, entry in enumerate(
        _require_sequence(config.get("refused_fields"), where=f"{path}: refused_fields")
    ):
        where = f"{path}: refused_fields[{index}]"
        item = _require_mapping(entry, where=where)
        refused = RefusedBoundField(
            field=_require_text(item.get("field"), where=f"{where}.field"),
            reason=_require_text(item.get("reason"), where=f"{where}.reason"),
            why=_require_text(item.get("why"), where=f"{where}.why"),
        )
        if refused.reason not in REFUSALS:
            raise ValueError(
                f"{where}.reason {refused.reason!r} is not a declared refusal code; a refusal a "
                "caller cannot branch on is a dropped bound rather than a reported one"
            )
        if refused.field in declared_refusals:
            raise ValueError(f"{where}.field repeats the refused field {refused.field!r}")
        declared_refusals[refused.field] = refused
    if {name: item.reason for name, item in declared_refusals.items()} != dict(
        REFUSED_BOUND_REASONS
    ):
        raise ValueError(
            f"{path}: the refused-field mapping "
            f"{sorted((name, item.reason) for name, item in declared_refusals.items())} disagrees "
            f"with the refusal vocabulary this module applies "
            f"{sorted(REFUSED_BOUND_REASONS.items())}; a bound refused under one standard and "
            "admitted under the other cannot be reported honestly"
        )

    methods_block = _require_mapping(
        config.get("verification_methods"), where=f"{path}: verification_methods"
    )
    methods = {
        basis: _require_text(
            methods_block.get(basis), where=f"{path}: verification_methods.{basis}"
        )
        for basis in BOUND_BASES
    }

    interval = _require_mapping(
        config.get("interval_construction"), where=f"{path}: interval_construction"
    )
    if not interval.get("interval_is_half_open"):
        raise ValueError(
            f"{path}: interval_construction.interval_is_half_open must be declared. The "
            "consumer's in-force test is half-open, so an interval read as closed would "
            "certify a release at the instant its version was superseded."
        )
    return AttestationSettings(
        config_version=version,
        capture_root=capture_root,
        raw_subdirectory=raw_subdirectory,
        index_subdirectory=index_subdirectory,
        required_record_fields=declared_fields,
        admissible_sources=admissible,
        inadmissible_sources=inadmissible,
        refused_fields=declared_refusals,
        verification_methods=methods,
        open_interval_semantics=_require_text(
            record.get("open_interval_semantics"), where=f"{path}: record.open_interval_semantics"
        ),
        rule_hash_binds=_require_text(
            record.get("rule_hash_binds"), where=f"{path}: record.rule_hash_binds"
        ),
        venue_rule_text_cannot_attest_its_own_past_version=True,
    )


@dataclass(frozen=True, slots=True)
class RuleCapture:
    """One archived capture of a contract's rule text.

    ``raw_hash`` addresses the exact bytes in the repository's content-addressed
    archive, and ``rule_text`` is the readable text reduced from those bytes.
    ``bound_basis`` is the declared basis of the kind the capture was taken under
    rather than a field the capture chose, so a capture cannot claim a stronger
    route than the one it was taken under.

    ``offered_bound_fields`` records the values a caller proposed as an interval
    bound — a venue ``open_time``, an ``updated_time``, the capture instant — with
    the value it proposed. They are kept rather than dropped because the refusal is
    the finding: a run that offered ``updated_time`` and was refused for it reports
    which bound it tried, and a capture record that silently discarded the offer
    would make that refusal unattributable.
    """

    capture_id: str
    contract_id: str
    source_url: str
    source_kind: str
    bound_basis: str
    captured_at: dt.datetime
    raw_hash: str
    raw_size: int
    rule_text: str
    settlement_semantics: str | None = None
    #: The instant the *source* records the rule text as live: an archive's own
    #: snapshot time, a filing's notification date. This is the only kind of instant
    #: that can bound an interval under the ``observation_instant`` basis.
    source_observed_at: dt.datetime | None = None
    #: The interval the document states for itself, read under the
    #: ``stated_effective_interval`` basis.
    stated_in_force_from: dt.datetime | None = None
    stated_in_force_to: dt.datetime | None = None
    source_names_contract: bool = False
    offered_bound_fields: tuple[tuple[str, str], ...] = ()
    note: str | None = None

    def __post_init__(self) -> None:
        for name in ("capture_id", "contract_id", "source_url", "source_kind", "bound_basis"):
            object.__setattr__(
                self, name, _require_text(getattr(self, name), where=f"RuleCapture.{name}")
            )
        if not _SAFE_CONTRACT_ID.match(self.contract_id):
            raise ValueError(
                f"RuleCapture.contract_id {self.contract_id!r} is not a plain contract token; "
                "a contract identifier names a capture directory and is refused rather than "
                "sanitized, because two identifiers reduced to one name would collide"
            )
        if not _HASH_RE.match(self.raw_hash):
            raise ValueError(
                f"RuleCapture.raw_hash {self.raw_hash!r} is not a sha256 digest, so it cannot "
                "address archived bytes"
            )
        object.__setattr__(
            self,
            "captured_at",
            parse_utc_time(self.captured_at, field_name="RuleCapture.captured_at"),
        )
        for name in ("stated_in_force_from", "stated_in_force_to", "source_observed_at"):
            object.__setattr__(
                self,
                name,
                _optional_instant(getattr(self, name), where=f"RuleCapture.{name}"),
            )
        if self.bound_basis not in BOUND_BASES:
            raise ValueError(
                f"RuleCapture.bound_basis must be one of {list(BOUND_BASES)}, got "
                f"{self.bound_basis!r}"
            )
        if isinstance(self.raw_size, bool) or not isinstance(self.raw_size, int):
            raise TypeError(f"RuleCapture.raw_size must be an int, got {self.raw_size!r}")
        object.__setattr__(self, "offered_bound_fields", tuple(sorted(self.offered_bound_fields)))

    @property
    def bounding_instant(self) -> dt.datetime | None:
        """The instant this capture's *source* records, or ``None`` when it records none.

        This is the whole of the bounding test, and it is deliberately narrower than
        "the capture has a timestamp". Every capture carries ``captured_at``, the
        instant this run archived it, and a run's own fetch instant bounds nothing: a
        rule text fetched today cannot attest its own past version. A capture bounds an
        interval only when the source it was read from records an instant at which the
        text it carries was live — an archive's own snapshot time, or the effective
        date the document states for itself. The instant read depends on the declared
        basis of the source kind, so a document that states an interval is never read
        by its fetch time and vice versa.
        """
        if self.bound_basis == BOUND_BASIS_STATED_INTERVAL:
            return self.stated_in_force_from
        return self.source_observed_at

    def as_dict(self) -> dict[str, Any]:
        return {
            "capture_version": RULE_CAPTURE_VERSION,
            "capture_id": self.capture_id,
            "contract_id": self.contract_id,
            "source_url": self.source_url,
            "source_kind": self.source_kind,
            "bound_basis": self.bound_basis,
            "captured_at": self.captured_at.isoformat(),
            "raw_hash": self.raw_hash,
            "raw_size": self.raw_size,
            "rule_text": self.rule_text,
            "settlement_semantics": self.settlement_semantics,
            "source_observed_at": (
                self.source_observed_at.isoformat() if self.source_observed_at else None
            ),
            "stated_in_force_from": (
                self.stated_in_force_from.isoformat() if self.stated_in_force_from else None
            ),
            "stated_in_force_to": (
                self.stated_in_force_to.isoformat() if self.stated_in_force_to else None
            ),
            "source_names_contract": self.source_names_contract,
            "offered_bound_fields": dict(self.offered_bound_fields),
            "note": self.note,
        }


def _capture_from_dict(document: Mapping[str, Any], *, where: str) -> RuleCapture:
    """Read one capture record, refusing a malformed one rather than reading part of it."""
    for name in (
        "capture_id",
        "contract_id",
        "source_url",
        "source_kind",
        "bound_basis",
        "captured_at",
        "raw_hash",
        "raw_size",
    ):
        if document.get(name) in (None, ""):
            raise ValueError(f"{where} is missing {name}")
    # ``rule_text`` is deliberately absent from that list. A capture of a
    # client-rendered page shell carries no readable rule text, and that is a state
    # worth recording: the attestation refuses it as ``capture_carries_no_rule_text``
    # rather than the loader pretending the capture was never taken.
    if document.get("rule_text") is None:
        raise ValueError(
            f"{where} is missing rule_text; an empty string states a capture that carries none"
        )
    offered = document.get("offered_bound_fields") or {}
    if not isinstance(offered, Mapping):
        raise ValueError(f"{where}.offered_bound_fields must be a mapping")
    note = document.get("note")
    return RuleCapture(
        capture_id=str(document["capture_id"]),
        contract_id=str(document["contract_id"]),
        source_url=str(document["source_url"]),
        source_kind=str(document["source_kind"]),
        bound_basis=str(document["bound_basis"]),
        captured_at=parse_utc_time(document["captured_at"], field_name=f"{where}.captured_at"),
        raw_hash=str(document["raw_hash"]),
        raw_size=int(document["raw_size"]),
        rule_text=str(document["rule_text"]),
        settlement_semantics=(
            str(document["settlement_semantics"]) if document.get("settlement_semantics") else None
        ),
        source_observed_at=document.get("source_observed_at"),
        stated_in_force_from=document.get("stated_in_force_from"),
        stated_in_force_to=document.get("stated_in_force_to"),
        source_names_contract=bool(document.get("source_names_contract")),
        offered_bound_fields=tuple((str(k), str(v)) for k, v in offered.items()),
        note=str(note) if note else None,
    )


class RuleCaptureStore:
    """A contract-keyed capture store over the repository's archive and transport.

    Layout under the configured root::

        raw/<hash[:2]>/<hash>.bin            the exact bytes, content-addressed
        captures/<contract_id>/<capture_id>.json   one immutable capture record

    The raw archive is a :class:`~market_propagation.storage.RawStore`, and a live
    capture is fetched by :class:`~market_propagation.ingest.transport.HttpTransport`
    built over that same archive, so the body the capture record cites is the body
    the transport archived, with the transport's own receipt beside it.

    Capture records are immutable. Writing the same capture twice returns the record
    already on disk; two records for one identity with different content are refused
    rather than overwritten, because a capture that changed after the fact would
    certify a version that was never observed.
    """

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        settings: AttestationSettings | None = None,
    ) -> None:
        self._settings = settings or load_attestation_settings()
        self._root = Path(root) if root is not None else self._settings.capture_root

    @property
    def settings(self) -> AttestationSettings:
        return self._settings

    @property
    def root(self) -> Path:
        return self._root

    @property
    def captures_root(self) -> Path:
        return self._root / self._settings.index_subdirectory

    @property
    def raw_store(self) -> RawStore:
        """The content-addressed archive every capture's bytes live in."""
        return RawStore(self._root / self._settings.raw_subdirectory)

    def contract_directory(self, contract_id: str) -> Path:
        """The directory holding one contract's capture records."""
        if not _SAFE_CONTRACT_ID.match(str(contract_id)):
            raise ValueError(
                f"contract id {contract_id!r} is not a plain contract token; a capture "
                "directory is named from it"
            )
        return self.captures_root / str(contract_id)

    @staticmethod
    def capture_id(
        *,
        contract_id: str,
        source_url: str,
        captured_at: dt.datetime,
        source_observed_at: dt.datetime | None = None,
    ) -> str:
        """A deterministic identifier for one capture occurrence.

        Derived from the contract, the source, the fetch instant and the instant the
        source itself records, but never from the bytes: two captures with identical
        bytes taken at different instants are two occurrences, and keying on content
        would merge them into one. The source's own recorded instant is part of the
        identity because two observations of one archive URL taken in a single batch
        share a fetch instant while recording different instants, and those are two
        distinct observations of two distinct versions.
        """
        observed = source_observed_at.isoformat() if source_observed_at else ""
        material = f"{contract_id}\x1f{source_url}\x1f{captured_at.isoformat()}\x1f{observed}"
        return hashlib.blake2b(material.encode("utf-8"), digest_size=16).hexdigest()

    def put(
        self,
        payload: bytes,
        *,
        contract_id: str,
        source_url: str,
        source_kind: str,
        captured_at: dt.datetime,
        content_type: str = "",
        rule_text: str | None = None,
        settlement_semantics: str | None = None,
        source_observed_at: Any = None,
        stated_in_force_from: Any = None,
        stated_in_force_to: Any = None,
        source_names_contract: bool = False,
        offered_bound_fields: Mapping[str, Any] | None = None,
        note: str | None = None,
        archived_by: str | None = None,
    ) -> RuleCapture:
        """Archive ``payload`` and write one capture record for it.

        ``source_kind`` must be a kind the evidence standard declares, admissible or
        not. Capturing under an undeclared kind is refused here as a programming
        fault, while capturing under a declared *inadmissible* kind is allowed and
        refused later at attestation, because a current-state page capture is a
        legitimate thing to hold and an illegitimate thing to derive an interval
        from. ``rule_text`` defaults to :func:`visible_text` of the archived bytes.

        ``archived_by`` names the source the bytes were already archived under, for
        the case where a body was fetched through :class:`HttpTransport` and archived
        by it before this call. The blob is content-addressed, so the same bytes are
        the same stored payload; skipping the second archive keeps one occurrence
        from being recorded as two.
        """
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise TypeError(f"put expects bytes, got {type(payload).__name__}")
        directory = self.contract_directory(contract_id)
        if not self._settings.is_declared_source(source_kind):
            raise ValueError(
                f"source kind {source_kind!r} is not declared by the evidence standard in "
                f"{CONFIG_PATH}; a capture taken under an undeclared kind has no stated rule "
                "for reading a bound from it"
            )
        kind = self._settings.source_kind(source_kind)
        moment = parse_utc_time(captured_at, field_name="RuleCaptureStore.put.captured_at")
        observed = _optional_instant(
            source_observed_at, where="RuleCaptureStore.put.source_observed_at"
        )
        body = bytes(payload)
        text = rule_text if rule_text is not None else visible_text(body, content_type=content_type)
        offered = {str(name): str(value) for name, value in (offered_bound_fields or {}).items()}

        if archived_by is None:
            self.raw_store.put(
                body,
                source=f"rule_capture:{source_kind}",
                received_time=moment,
                metadata={
                    "contract_id": str(contract_id),
                    "source_url": str(source_url),
                    "source_kind": str(source_kind),
                    "bound_basis": kind.bound_basis if kind else "undeclared",
                    "captured_at": moment.isoformat(),
                    "credentials": "none",
                    "offered_bound_fields": offered,
                },
            )
        capture = RuleCapture(
            capture_id=self.capture_id(
                contract_id=str(contract_id),
                source_url=str(source_url),
                captured_at=moment,
                source_observed_at=observed,
            ),
            contract_id=str(contract_id),
            source_url=str(source_url),
            source_kind=str(source_kind),
            bound_basis=kind.bound_basis if kind else BOUND_BASIS_OBSERVATION_INSTANT,
            captured_at=moment,
            raw_hash=hash_bytes(body),
            raw_size=len(body),
            rule_text=text or "",
            settlement_semantics=str(settlement_semantics) if settlement_semantics else None,
            source_observed_at=observed,
            stated_in_force_from=stated_in_force_from,
            stated_in_force_to=stated_in_force_to,
            source_names_contract=bool(source_names_contract),
            offered_bound_fields=tuple(offered.items()),
            note=str(note) if note else None,
        )
        self._write_record(capture, directory=directory)
        return capture

    def capture(
        self,
        envelope: ResponseEnvelope,
        *,
        contract_id: str,
        source_kind: str,
        **kwargs: Any,
    ) -> RuleCapture:
        """Record one already-fetched response, under the archive entry that holds it.

        The bytes and the digest are the response's, and the payload is already stored:
        :class:`HttpTransport` archives every body it receives before returning, so this
        writes the capture record and leaves the single occurrence the transport
        recorded as the one occurrence it is.
        """
        return self.put(
            envelope.body,
            contract_id=contract_id,
            source_url=envelope.url,
            source_kind=source_kind,
            captured_at=envelope.received_time,
            content_type=envelope.content_type,
            archived_by=envelope.provenance.source,
            **kwargs,
        )

    def fetch(
        self,
        url: str,
        *,
        contract_id: str,
        source_kind: str,
        transport: HttpTransport,
        **kwargs: Any,
    ) -> RuleCapture:
        """GET ``url`` through the shared transport and record the response.

        ``transport`` is supplied rather than constructed here so one paced, retrying
        GET-only client is shared across a run; its archive is the archive this store
        reads. No request is issued by this module directly.
        """
        envelope = transport.get(url, source=f"rule_capture:{source_kind}")
        return self.capture(envelope, contract_id=contract_id, source_kind=source_kind, **kwargs)

    def _write_record(self, capture: RuleCapture, *, directory: Path) -> None:
        payload = (json.dumps(capture.as_dict(), indent=2, sort_keys=True) + "\n").encode("utf-8")
        path = directory / f"{capture.capture_id}.json"
        try:
            _atomic_write_bytes(path, payload)
        except FileExistsError as exc:
            raise FileExistsError(
                f"capture {capture.capture_id} for {capture.contract_id} is already recorded and "
                "the new record differs; a capture that changed after the fact would certify a "
                "version that was never observed"
            ) from exc

    def load(self, path: str | Path) -> RuleCapture:
        """Read one capture record from a file."""
        target = Path(path)
        try:
            document = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"capture record {target} could not be read: {exc}") from exc
        return _capture_from_dict(_require_mapping(document, where=str(target)), where=str(target))

    def captures(self, contract_id: str | None = None) -> tuple[RuleCapture, ...]:
        """Every capture held, optionally for one contract, deterministically ordered."""
        if contract_id is not None:
            directories = [self.contract_directory(contract_id)]
        elif self.captures_root.exists():
            directories = sorted(self.captures_root.glob("*"))
        else:
            directories = []
        out: list[RuleCapture] = []
        for directory in directories:
            if not directory.is_dir():
                continue
            for path in sorted(directory.glob("*.json")):
                out.append(self.load(path))
        out.sort(key=lambda capture: (capture.contract_id, capture.captured_at, capture.capture_id))
        return tuple(out)

    def contract_ids(self) -> tuple[str, ...]:
        """Every contract holding at least one capture, sorted."""
        return tuple(sorted({capture.contract_id for capture in self.captures()}))

    def verify(self, capture: RuleCapture) -> None:
        """Re-read a capture's bytes and check them against its own record.

        Raises :class:`FileNotFoundError` when the archive holds no such payload and
        :class:`ValueError` when the stored bytes hash to something else or the stored
        text is not the text that was recorded. Both are refusals at the caller: a
        capture whose bytes are gone or changed certifies nothing.
        """
        body = self.raw_store.get(capture.raw_hash)
        actual = hash_bytes(body)
        if actual != capture.raw_hash:
            raise ValueError(
                f"capture {capture.capture_id} cites {capture.raw_hash} and the stored bytes "
                f"hash to {actual}"
            )
        if len(body) != capture.raw_size:
            raise ValueError(
                f"capture {capture.capture_id} records {capture.raw_size} bytes and the stored "
                f"payload is {len(body)}"
            )
        stored_text = body.decode("utf-8", errors="replace")
        if capture.rule_text and capture.rule_text not in stored_text:
            raise ValueError(
                f"capture {capture.capture_id} records rule text that does not occur in the "
                "bytes it cites"
            )


@dataclass(frozen=True, slots=True)
class AttestationRefusal:
    """One named reason a contract did not reach an attested rule vintage.

    ``field`` is set when the refusal is about a field a caller offered as an interval
    bound, so a report can be read as "this run offered ``updated_time`` and was
    refused for it" rather than as a bare count.
    """

    contract_id: str
    reason: str
    detail: str
    field: str | None = None
    capture_id: str | None = None

    def __post_init__(self) -> None:
        if self.reason not in REFUSALS:
            raise ValueError(
                f"{self.reason!r} is not a declared refusal code; a refusal no caller can "
                "branch on is a dropped bound rather than a reported one"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "reason": self.reason,
            "detail": self.detail,
            "field": self.field,
            "capture_id": self.capture_id,
        }


@dataclass(frozen=True, slots=True)
class ContractAttestation:
    """One contract's attestation: at most one record, and every refusal that applied.

    ``records_attested`` is zero or one by construction. A contract that reaches no
    record carries refusals instead of an empty interval, and a contract holding no
    capture at all carries ``no_capture_holds_this_contract``: an unattested contract
    is a named refusal rather than a zero, because an unmeasured quantity and a
    measured absence are different facts.
    """

    contract_id: str
    captures_held: int
    records_attested: int
    refusals: tuple[AttestationRefusal, ...]
    evidence: tuple[RuleVersionEvidence, ...] = ()

    @property
    def attested(self) -> bool:
        return self.records_attested > 0

    @property
    def refusals_by_reason(self) -> dict[str, int]:
        """Refusal counts keyed by reason code, sorted by code."""
        return dict(sorted(Counter(r.reason for r in self.refusals).items()))

    @property
    def primary_reason(self) -> str | None:
        """The first refusal in the order this module applies them, or ``None``."""
        return self.refusals[0].reason if self.refusals else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "captures_held": self.captures_held,
            "records_attested": self.records_attested,
            "attested": self.attested,
            "primary_reason": self.primary_reason,
            "refusals_by_reason": self.refusals_by_reason,
            "refusals": [refusal.as_dict() for refusal in self.refusals],
            "evidence": [record.as_dict() for record in self.evidence],
        }


@dataclass(frozen=True, slots=True)
class RuleAttestationReport:
    """Per-contract verification: captures held, records attested, refusals by reason."""

    config_version: str
    required_record_fields: tuple[str, ...]
    contracts: tuple[ContractAttestation, ...]

    @property
    def refusals_by_reason(self) -> dict[str, int]:
        counts: Counter[str] = Counter()
        for contract in self.contracts:
            counts.update(contract.refusals_by_reason)
        return dict(sorted(counts.items()))

    @property
    def unattested_contract_ids(self) -> tuple[str, ...]:
        return tuple(contract.contract_id for contract in self.contracts if not contract.attested)

    def totals(self) -> dict[str, int]:
        return {
            "contracts_examined": len(self.contracts),
            "contracts_attested": sum(1 for c in self.contracts if c.attested),
            "contracts_unattested": len(self.unattested_contract_ids),
            "captures_held": sum(c.captures_held for c in self.contracts),
            "records_attested": sum(c.records_attested for c in self.contracts),
            "refusals": sum(len(c.refusals) for c in self.contracts),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "report_version": ATTESTATION_REPORT_VERSION,
            "produced_by": "market_propagation.ingest.rule_attestation",
            "config_version": self.config_version,
            "record_type": "market_propagation.ingest.audit.RuleVersionEvidence",
            "required_record_fields": list(self.required_record_fields),
            "totals": self.totals(),
            "refusals_by_reason": self.refusals_by_reason,
            "unattested_contract_ids": list(self.unattested_contract_ids),
            "contracts": [contract.as_dict() for contract in self.contracts],
        }

    def write(self, output_dir: str | Path, *, name: str = ATTESTATION_REPORT_NAME) -> Path:
        """Write the report under ``output_dir`` and return the path written."""
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        path.write_text(
            json.dumps(self.as_dict(), indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        return path


@dataclass(frozen=True, slots=True)
class _CaptureVerdict:
    """One capture as the attester reads it.

    ``refusals`` are about the capture itself: they mean it cannot bound an interval.
    ``bound_refusals`` are about fields a caller offered as a bound. They are kept
    apart because offering ``open_time`` is not a defect in an otherwise admissible
    capture — it is a bound that was declined, and it is reported by name either way.
    Collapsing the two would let one inadmissible field disqualify a capture whose own
    dating does bound the interval, which would refuse evidence that exists.
    """

    capture: RuleCapture
    refusals: tuple[AttestationRefusal, ...]
    bound_refusals: tuple[AttestationRefusal, ...] = ()

    @property
    def admissible(self) -> bool:
        return not self.refusals

    @property
    def all_refusals(self) -> tuple[AttestationRefusal, ...]:
        return self.refusals + self.bound_refusals


class RuleAttestor:
    """Turns archived captures into :class:`RuleVersionEvidence`, or into refusals.

    The attestation itself is short and the refusals are where the work is, because
    the honest answer on this archive is a refusal for every contract. Three
    properties are load-bearing.

    *No bound is ever derived from a refused field.* The wording of a refusal names
    the field; no code path reads ``open_time``, ``close_time``, ``created_time``,
    ``updated_time``, ``settlement_ts`` or the capture instant as an interval bound.
    ``capture_time`` appears in the refused-field vocabulary, and the instant a
    capture was taken is used as an observation instant only through a source kind
    the standard declares admissible under the ``observation_instant`` basis — which
    is a statement about the document being a dated observation, not about when the
    fetch happened to run.

    *A stated interval is decisive.* When two dated sources state different
    intervals, or when a stated interval does not follow, the contract is refused
    rather than resolved by a tie-break this module would have invented.

    *An observation run closes only on another observation.* A run of observations
    carrying one digest is certified from its first observation, and is closed by the
    earliest later observation of a different digest. It is never closed by a wall
    clock, and an unclosed run is written with ``in_force_to`` as ``null`` — which
    the consumer states as open rather than as forever.
    """

    def __init__(self, store: RuleCaptureStore) -> None:
        self._store = store

    @property
    def store(self) -> RuleCaptureStore:
        return self._store

    def refused_bound_field(self, contract_id: str, field: str) -> AttestationRefusal:
        """The refusal for one field offered as an interval bound.

        Every declared field has its own reason, so an offered ``updated_time`` is
        refused as a rewrite stamp and an offered ``open_time`` as the previous
        meeting's resolution instant. A field the standard does not declare is
        refused as undeclared rather than read as the declared field it resembles.

        The statement of why travels with the reason from the configuration, so a
        report quotes the declared rule rather than a second telling of it kept here.
        """
        declared = self._store.settings.refused_field(field)
        return AttestationRefusal(
            contract_id=contract_id,
            reason=declared.reason,
            field=declared.field,
            detail=(
                f"{declared.field!r} was offered as an in-force bound for {contract_id} and is "
                f"refused: {declared.why}"
            ),
        )

    def _verdict(self, capture: RuleCapture, *, contract_id: str) -> _CaptureVerdict:
        """One capture judged against the evidence standard."""
        refusals: list[AttestationRefusal] = []

        def refuse(reason: str, detail: str) -> None:
            refusals.append(
                AttestationRefusal(
                    contract_id=contract_id,
                    reason=reason,
                    detail=detail,
                    capture_id=capture.capture_id,
                )
            )

        if capture.contract_id != contract_id:
            refuse(
                REFUSAL_CAPTURE_CONTRACT_MISMATCH,
                f"capture {capture.capture_id} is recorded for {capture.contract_id} and was "
                f"offered against {contract_id}",
            )
        try:
            self._store.verify(capture)
        except FileNotFoundError:
            refuse(
                REFUSAL_CAPTURE_EVIDENCE_MISSING,
                f"the archive holds no payload {capture.raw_hash} for capture "
                f"{capture.capture_id}, so nothing behind the record can be re-read",
            )
        except ValueError as exc:
            refuse(REFUSAL_CAPTURE_DIGEST_MISMATCH, str(exc))

        settings = self._store.settings
        kind = settings.source_kind(capture.source_kind)
        if kind is None:
            stated = settings.inadmissible_sources.get(capture.source_kind, "undeclared")
            refuse(
                REFUSAL_INADMISSIBLE_SOURCE_KIND,
                f"capture {capture.capture_id} was taken under source kind "
                f"{capture.source_kind!r}, which the evidence standard does not admit as a "
                f"dated source: {stated}",
            )
        elif kind.must_name_the_contract and not capture.source_names_contract:
            refuse(
                REFUSAL_SOURCE_NAMES_NO_CONTRACT,
                f"capture {capture.capture_id} is a {kind.source_id} that names no individual "
                "contract, so its dating bounds a product template rather than this contract's "
                "rule text",
            )
        if not capture.rule_text.strip():
            refuse(
                REFUSAL_RULE_TEXT_ABSENT,
                f"capture {capture.capture_id} carries no rule text; a page shell or an error "
                "envelope attests nothing",
            )
        if not (capture.settlement_semantics or "").strip():
            refuse(
                REFUSAL_SETTLEMENT_SEMANTICS_UNSTATED,
                f"capture {capture.capture_id} states no settlement semantics, and the record "
                "must say what the verified text resolves on",
            )
        bound_refusals: list[AttestationRefusal] = []
        for field_name, value in capture.offered_bound_fields:
            refusal = self.refused_bound_field(contract_id, field_name)
            bound_refusals.append(
                AttestationRefusal(
                    contract_id=refusal.contract_id,
                    reason=refusal.reason,
                    field=refusal.field,
                    capture_id=capture.capture_id,
                    detail=f"{refusal.detail} (offered value {value})",
                )
            )
        return _CaptureVerdict(
            capture=capture,
            refusals=tuple(refusals),
            bound_refusals=tuple(bound_refusals),
        )

    def attest(
        self, contract_id: str, *, wanted_rule_hash: str | None = None
    ) -> ContractAttestation:
        """The attested record for one contract, or the refusals that stopped it.

        ``wanted_rule_hash`` selects which captured version to certify when more than
        one digest has been observed, because the graph asks which version was in
        force at an instant and the answer for a specific version is a different
        question from the answer for the first one observed. When it is not given, the
        earliest observed version is certified, which is the version whose interval can
        be closed by observation.
        """
        captures = self._store.captures(contract_id)
        if not captures:
            return ContractAttestation(
                contract_id=contract_id,
                captures_held=0,
                records_attested=0,
                refusals=(
                    AttestationRefusal(
                        contract_id=contract_id,
                        reason=REFUSAL_NO_CAPTURES_HELD,
                        detail=(
                            f"no capture holds {contract_id}: the store records no rule capture "
                            "for this contract, which is an unattested vintage rather than a "
                            "zero-length interval"
                        ),
                    ),
                ),
            )

        verdicts = tuple(self._verdict(capture, contract_id=contract_id) for capture in captures)
        refusals = [refusal for verdict in verdicts for refusal in verdict.all_refusals]
        admissible = [verdict.capture for verdict in verdicts if verdict.admissible]
        bounding = [capture for capture in admissible if capture.bounding_instant is not None]

        if not bounding:
            # One refusal per contract rather than one per capture: the finding is that
            # this contract has no dated source bounding its rule text, and the detail
            # names every capture that failed to supply one and why, so a reader can
            # tell "no dated source was found" apart from "the dated source was refused".
            unbounded = [
                f"{capture.capture_id} fetched {capture.captured_at.isoformat()} records no "
                f"{capture.bound_basis} instant"
                for capture in admissible
            ]
            refused = [
                f"{verdict.capture.capture_id} ({verdict.refusals[0].reason})"
                for verdict in verdicts
                if not verdict.admissible
            ]
            refusals.append(
                AttestationRefusal(
                    contract_id=contract_id,
                    reason=REFUSAL_NO_BOUNDING_SOURCE,
                    detail=(
                        f"{contract_id} holds {len(captures)} capture(s) and none carries a dated "
                        "source that bounds the interval its rule text was in force. No bound is "
                        "imputed from an offered field, from a capture instant or from another "
                        f"contract's dates, and the run's own fetch instants above are not "
                        f"bounds. Captures refused: {refused or 'none'}. Captures whose source "
                        f"records no bound: {unbounded or 'none'}"
                    ),
                )
            )
            return ContractAttestation(
                contract_id=contract_id,
                captures_held=len(captures),
                records_attested=0,
                refusals=tuple(refusals),
            )

        stated = [
            capture for capture in bounding if capture.bound_basis == BOUND_BASIS_STATED_INTERVAL
        ]
        # A document that states its own effective interval is decisive over one that
        # is merely observed to be live at an instant, because the stated interval is
        # the source's own claim about its span rather than an inference from when it
        # happened to be read.
        outcome = (
            self._from_stated_intervals(contract_id, stated)
            if stated
            else self._from_observations(contract_id, bounding, wanted_rule_hash=wanted_rule_hash)
        )

        if outcome.refusals:
            refusals.extend(outcome.refusals)
            return ContractAttestation(
                contract_id=contract_id,
                captures_held=len(captures),
                records_attested=0,
                refusals=tuple(refusals),
            )
        assert outcome.evidence is not None  # narrowed by the refusal check above
        return ContractAttestation(
            contract_id=contract_id,
            captures_held=len(captures),
            records_attested=1,
            refusals=tuple(refusals),
            evidence=(outcome.evidence,),
        )

    def _from_stated_intervals(self, contract_id: str, stated: Sequence[RuleCapture]) -> _Outcome:
        """Build the record from documents that state their own in-force interval."""
        settings = self._store.settings
        spans = {(capture.stated_in_force_from, capture.stated_in_force_to) for capture in stated}
        if len(spans) > 1:
            shown = sorted(
                (opening.isoformat(), closing.isoformat() if closing else None)
                for opening, closing in spans
                if opening is not None
            )
            return _Outcome(
                refusals=(
                    AttestationRefusal(
                        contract_id=contract_id,
                        reason=REFUSAL_AMBIGUOUS_STATED_INTERVALS,
                        detail=(
                            f"{len(stated)} dated sources state {len(spans)} different in-force "
                            f"intervals for {contract_id}: {shown}. The choice between them "
                            "would not be derivable from the configuration, so none is taken"
                        ),
                    ),
                )
            )
        opening, closing = next(iter(spans))
        assert opening is not None  # stated captures always carry an opening bound
        if closing is not None and closing <= opening:
            return _Outcome(
                refusals=(
                    AttestationRefusal(
                        contract_id=contract_id,
                        reason=REFUSAL_EMPTY_INTERVAL,
                        detail=(
                            f"the in-force interval stated for {contract_id} is "
                            f"[{opening.isoformat()}, {closing.isoformat()}), which contains no "
                            "instant; an empty interval certifies no release and is refused "
                            "rather than clamped"
                        ),
                    ),
                )
            )
        source = max(stated, key=lambda capture: capture.captured_at)
        return _Outcome(
            evidence=RuleVersionEvidence(
                contract_id=contract_id,
                rule_hash=source.raw_hash,
                source_url=source.source_url,
                verified_by=settings.verification_method(BOUND_BASIS_STATED_INTERVAL),
                in_force_from=opening,
                in_force_to=closing,
                observed_at=source.captured_at,
                settlement_semantics=str(source.settlement_semantics),
            )
        )

    def _from_observations(
        self,
        contract_id: str,
        observational: Sequence[RuleCapture],
        *,
        wanted_rule_hash: str | None,
    ) -> _Outcome:
        """Build the record from dated observations of the live rule page.

        The run certified is the consecutive span of observations carrying one
        rule-text digest. Its opening bound is the earliest *source-recorded* instant
        at which that text was observed live and its closing bound is the earliest
        later source-recorded instant at which a different text was, so both bounds
        come from the sources and never from when this run happened to fetch them.
        When no contradicting observation exists the interval is left open, and the
        record's ``observed_at`` dates the verification itself, which is the capture's
        own recorded instant.
        """
        settings = self._store.settings
        ordered = sorted(
            observational,
            key=lambda capture: (capture.bounding_instant, capture.capture_id),
        )
        observed = {capture.raw_hash: capture for capture in ordered}
        if wanted_rule_hash is not None and wanted_rule_hash not in observed:
            return _Outcome(
                refusals=(
                    AttestationRefusal(
                        contract_id=contract_id,
                        reason=REFUSAL_RULE_HASH_NOT_CAPTURED,
                        detail=(
                            f"no capture held for {contract_id} carries rule hash "
                            f"{wanted_rule_hash}; the observed digests are "
                            f"{sorted(observed)}"
                        ),
                    ),
                )
            )
        target_hash = wanted_rule_hash or ordered[0].raw_hash
        # The run is grouped on the text the captures record rather than on the bytes
        # they cite. Grouping on the payload would close the run on the next fetch of
        # a listing whose live fields moved while its rule text did not, so the text is
        # what decides whether an observation confirms or contradicts. The published
        # ``rule_hash`` stays the archived payload's digest.
        target_text = rule_text_digest(observed[target_hash].rule_text)
        run = [capture for capture in ordered if rule_text_digest(capture.rule_text) == target_text]
        opening_capture = run[0]
        opening = opening_capture.bounding_instant
        assert opening is not None  # every capture here came from the bounding set
        contradicting = [
            capture.bounding_instant
            for capture in ordered
            if rule_text_digest(capture.rule_text) != target_text
            and capture.bounding_instant is not None
            and capture.bounding_instant > opening
        ]
        closing = min(contradicting) if contradicting else None
        if closing is not None and closing <= opening:
            return _Outcome(
                refusals=(
                    AttestationRefusal(
                        contract_id=contract_id,
                        reason=REFUSAL_EMPTY_INTERVAL,
                        detail=(
                            f"the run of observations carrying {target_hash} for {contract_id} "
                            f"would close at {closing.isoformat()}, which does not follow its "
                            f"opening at {opening.isoformat()}"
                        ),
                    ),
                )
            )
        last_confirmation = max(run, key=lambda capture: capture.captured_at)
        return _Outcome(
            evidence=RuleVersionEvidence(
                contract_id=contract_id,
                rule_hash=target_hash,
                source_url=opening_capture.source_url,
                verified_by=settings.verification_method(BOUND_BASIS_OBSERVATION_INSTANT),
                in_force_from=opening,
                in_force_to=closing,
                observed_at=last_confirmation.captured_at,
                settlement_semantics=str(opening_capture.settlement_semantics),
            )
        )

    def report(self, contract_ids: Iterable[str] | None = None) -> RuleAttestationReport:
        """Attest each contract and collect the per-contract verification.

        ``contract_ids`` defaults to every contract the store holds a capture for. A
        caller that hands in contracts the store holds nothing for gets a refusal per
        contract rather than a skipped row, because a contract left out of a coverage
        table reads as one that passed.
        """
        if contract_ids is None:
            wanted = list(self._store.contract_ids())
        else:
            wanted = [str(contract_id) for contract_id in contract_ids]
        return RuleAttestationReport(
            config_version=self._store.settings.config_version,
            required_record_fields=self._store.settings.required_record_fields,
            contracts=tuple(self.attest(contract_id) for contract_id in wanted),
        )


@dataclass(frozen=True, slots=True)
class _Outcome:
    """Either one built record or the refusals that stopped it, never both."""

    evidence: RuleVersionEvidence | None = None
    refusals: tuple[AttestationRefusal, ...] = ()

    def __post_init__(self) -> None:
        if (self.evidence is None) == (not self.refusals):
            raise ValueError(
                "an attestation outcome is a record or a non-empty refusal list, and not both "
                "and not neither"
            )
