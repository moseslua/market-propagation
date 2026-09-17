"""Acquisition and holding of Polymarket's own market metadata.

The second venue's cleaned local layer carries trade and lifecycle data plus a
``market_slug`` and nothing that states what a contract pays on, so no parser can
be declared for it and every cross-venue pair is refused for want of one. The
venue does state the predicate, verbatim, in its own live metadata: a market's
``question`` and ``description`` say which rate the claim resolves on and by how
much, and the parent event names the meeting. This module fetches that metadata
and holds one immutable record per contract, so the predicate layer reads a
venue's own statement rather than a slug this repository would have to interpret.

Three properties are load-bearing, and each is a way this pipeline could quietly
manufacture evidence.

**A held record is the venue's bytes, cited.** The page is archived through the
repository's existing content-addressed :class:`~market_propagation.storage.RawStore`
before any field is read from it, and the record carries that page's ``raw_hash``
and the exact URL it came from. :meth:`PolymarketMetadataStore.verify` re-reads the
archived bytes and refuses a record that no longer matches them or whose own stated
text does not occur in the page it cites.

**The instant is the serving system's, never this run's.** A record's
``source_observed_at`` is read from the response's own ``Date`` header. A response
that states no instant yields ``null``: an unobserved instant is a recorded
absence, and this run's clock would be a fabricated one.

**One route answers.** ``GET /public-search?q=<query>`` was measured on 2026-09-17
to return the venue's own metadata for closed markets, including a market whose
``conditionId`` matches a ``condition_id`` in the cleaned local candidate layer.
The documented by-id routes (``/markets?condition_ids=``, ``/markets?slug=``) were
measured returning ``[]`` for those same closed markets, so this module offers no
route to them: a lookup that answers empty for a market that exists would be read
downstream as a market that does not exist.

The query list is a declared cohort decision, not a convenience. Each declared
query carries the venue's own naming for one slice of the family, so the sweep
universe is readable in ``configs/matching_v1.yaml`` beside the selection pattern
that produced the candidates, rather than assembled here out of the slugs that
happened to need looking up.

Read-only and fail-closed, like every module under ``ingest``: GET only, no
credentials, and a page that refuses access is reported as a blocked query with its
own reason rather than as a page that carried no markets.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ..domain import parse_utc_time
from ..storage import RawStore, _atomic_write_bytes
from .transport import HttpTransport, ResponseEnvelope, RetryPolicy, TransportError

__all__ = [
    "MATCH_CONFIG_PATH",
    "METADATA_BLOCK",
    "REASON_NO_RECORD_HELD",
    "REASON_RECORD_BOUND_REACHED",
    "RECORD_FIELDS",
    "RECORD_SUBJECT_FIELDS",
    "RECORD_VERSION",
    "SECOND_VENUE_ID",
    "SKIP_REASONS",
    "AcquisitionSummary",
    "DeclaredQuery",
    "MarketLookup",
    "MetadataAcquisitionSettings",
    "PolymarketMarketRecord",
    "PolymarketMetadataStore",
    "capture",
    "load_metadata_acquisition_settings",
    "text_as_stored",
]

#: The pipeline configuration this module reads its acquisition plan from. It is the
#: matching configuration rather than a file of this module's own, because the second
#: venue is declared there beside the selection pattern that produced the candidates:
#: one file answers "which contracts" and "where the venue's own text for them is".
MATCH_CONFIG_PATH = "configs/matching_v1.yaml"

#: The declared venue id this module acquires metadata for.
SECOND_VENUE_ID = "polymarket"

#: The block, inside that venue's entry, that declares the acquisition.
METADATA_BLOCK = "metadata_acquisition"

#: Version stamped into every held record, so a reader can tell which record scheme
#: wrote what it is reading.
RECORD_VERSION = "polymarket_market_metadata_v1"

#: The exact keys of a held record. This list is the contract the predicate layer
#: reads, so it is stated once here and asserted against every record written and
#: against the declaration in ``configs/matching_v1.yaml``.
RECORD_FIELDS: tuple[str, ...] = (
    "record_version",
    "condition_id",
    "slug",
    "event_slug",
    "event_title",
    "question",
    "description",
    "group_item_title",
    "group_item_threshold",
    "outcomes",
    "end_date",
    "closed_time",
    "source_url",
    "source_observed_at",
    "raw_hash",
)

#: The fields that state what the venue says about the contract, as opposed to which
#: page said it. Two records for one contract disagreeing on any of these are a
#: restatement rather than a second observation of one statement, and the write is
#: refused; the citation fields are deliberately outside the comparison, because the
#: same statement read from two pages carries two citations and is still one
#: statement.
RECORD_SUBJECT_FIELDS: tuple[str, ...] = tuple(
    name for name in RECORD_FIELDS if name not in ("source_url", "source_observed_at", "raw_hash")
)

#: The reason the accessor gives when the store holds no record for a contract. It is
#: a named reason rather than ``None`` so a caller reports "the venue's metadata holds
#: no record for this contract" instead of "no data".
REASON_NO_RECORD_HELD = "the_venue_metadata_holds_no_record_for_this_contract"

#: The reasons a market on a page yields no record. Each names the field that was
#: absent or in a form this module does not read, so a page that half-parsed is
#: reported per market instead of being dropped as a page.
REASON_NO_CONDITION_ID = "the_venue_metadata_names_no_condition_id_for_this_market"
REASON_CONDITION_ID_NOT_A_MARKET_KEY = (
    "the_venue_metadata_states_a_condition_id_that_is_not_a_market_key"
)
REASON_NO_QUESTION = "the_venue_metadata_states_no_question_for_this_contract"
REASON_NO_DESCRIPTION = "the_venue_metadata_states_no_description_for_this_contract"
REASON_NO_MARKET_SLUG = "the_venue_metadata_states_no_market_slug_for_this_contract"
REASON_NO_PARENT_EVENT = "the_venue_metadata_states_no_parent_event_for_this_market"
REASON_OUTCOMES_NOT_A_STATED_STRING = (
    "the_venue_metadata_states_its_outcomes_in_a_form_this_record_does_not_carry"
)
REASON_RECORD_BOUND_REACHED = "the_sweeps_declared_record_bound_was_reached"

#: The refusal codes a market read can yield, in the order this module applies them.
SKIP_REASONS: tuple[str, ...] = (
    REASON_NO_CONDITION_ID,
    REASON_CONDITION_ID_NOT_A_MARKET_KEY,
    REASON_NO_MARKET_SLUG,
    REASON_NO_QUESTION,
    REASON_NO_DESCRIPTION,
    REASON_NO_PARENT_EVENT,
    REASON_OUTCOMES_NOT_A_STATED_STRING,
    REASON_RECORD_BOUND_REACHED,
)

#: A contract key is used as a file name and as the join to the cleaned local layer,
#: so it is restricted to the lowercase 32-byte hex the venue states and refused
#: otherwise. Sanitizing instead would make two distinct contracts collide on one
#: record, which is how one contract's metadata ends up read for another.
_CONDITION_ID_RE = re.compile(r"^0x[0-9a-f]{64}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")

#: What a receipt label may contain. The label names the request for a checkpoint
#: audit; it is never an occurrence identity (the transport mints those), so a query
#: is reduced to a plain token rather than used verbatim.
_LABEL_CHARS = re.compile(r"[^a-z0-9]+")

#: Default request timeout, matching the other acquisition paths in this package.
DEFAULT_TIMEOUT_SECONDS = 30.0


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


def _optional_text(value: Any, *, where: str) -> str | None:
    """The field as the venue states it, or ``None`` when it states none.

    An absent field is recorded as ``null`` rather than as an empty string: a
    ``groupItemTitle`` the venue did not publish and one it published as empty are
    different states, and a downstream reader cannot tell them apart once both are
    the empty string.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string or null, got {type(value).__name__}")
    return value


def _required_int(value: Any, *, where: str, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{where} must be an int of at least {minimum}, got {value!r}")
    return value


def _query_label(query: str) -> str:
    """A receipt label for one declared query, reduced to a plain token."""
    token = _LABEL_CHARS.sub("-", str(query).strip().lower()).strip("-")
    if not token:
        raise ValueError(f"declared query {query!r} reduces to an empty receipt label")
    return token[:60]


def text_as_stored(page_text: str, value: str) -> str:
    """``value`` in some exact form the archived page carries it.

    A payload's text is not byte-for-byte the parsed string. The venue was measured
    sending raw UTF-8 with control characters escaped (``"a\\nb“Yes”"``): its newlines
    arrive as ``\\n`` while its typographic quotes arrive as themselves. Other encoders
    escape everything, so a text is accepted in three forms and the first that occurs
    in the page is returned:

    1. the text as the parser read it, for a page that carries it verbatim;
    2. the venue's own measured convention — control characters escaped, non-ASCII
       left raw;
    3. the fully escaped form, for a payload whose encoder escapes non-ASCII too.

    A text in none of those forms means the payload and the record disagree about the
    page, which is refused rather than recorded as a match on a near-form.
    """
    candidates = (
        value,
        json.dumps(value, ensure_ascii=False)[1:-1],
        json.dumps(value, ensure_ascii=True)[1:-1],
    )
    for candidate in candidates:
        if candidate in page_text:
            return candidate
    raise ValueError(
        "the text this record states does not occur in the page it cites, in the form the "
        "parser read it or in either JSON-escaped form; the archived payload and the record "
        "would disagree about the same contract"
    )


@dataclass(frozen=True, slots=True)
class DeclaredQuery:
    """One search query the sweep issues, and why it is in the universe.

    The ``why`` travels with the query so the declared list reads as a statement
    about the venue's own vocabulary rather than as a list of favourites: a reader
    has to be able to see which slice of the family each query is there to reach.
    """

    query: str
    why: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "query", _require_text(self.query, where="DeclaredQuery.query"))
        object.__setattr__(self, "why", _require_text(self.why, where="DeclaredQuery.why"))

    def as_dict(self) -> dict[str, Any]:
        return {"query": self.query, "why": self.why}


@dataclass(frozen=True, slots=True)
class MetadataAcquisitionSettings:
    """The declared acquisition plan, read from ``configs/matching_v1.yaml``.

    Every field is required rather than defaulted, because a configuration that
    omitted the query list or the page bound would leave this module to supply its
    own, and a run would then report a universe it never read.
    """

    config_version: str
    venue: str
    host: str
    search_path: str
    store_root: Path
    page_parameter: str
    page_size: int
    max_pages_per_query: int
    max_records_per_run: int
    record_version: str
    note: str
    queries: tuple[DeclaredQuery, ...]
    writes_captured_data_into_the_repository: bool

    @property
    def search_url(self) -> str:
        """The one route this module reads, composed from the declared host and path."""
        return f"{self.host.rstrip('/')}/{self.search_path.lstrip('/')}"

    @property
    def declared_queries(self) -> tuple[str, ...]:
        return tuple(declared.query for declared in self.queries)

    def as_dict(self) -> dict[str, Any]:
        return {
            "config_version": self.config_version,
            "venue": self.venue,
            "host": self.host,
            "search_path": self.search_path,
            "store_root": str(self.store_root),
            "page_parameter": self.page_parameter,
            "page_size": self.page_size,
            "max_pages_per_query": self.max_pages_per_query,
            "max_records_per_run": self.max_records_per_run,
            "record_version": self.record_version,
            "queries": [declared.as_dict() for declared in self.queries],
            "writes_captured_data_into_the_repository": (
                self.writes_captured_data_into_the_repository
            ),
        }


def load_metadata_acquisition_settings(
    config_path: str | Path = MATCH_CONFIG_PATH,
) -> MetadataAcquisitionSettings:
    """Read the declared acquisition plan from the matching configuration.

    Every mismatch is a configuration fault raised at the boundary that read it: a
    missing block, an empty query list, a query with no stated reason, a page or
    record bound that is not a positive count, or a record version this module does
    not write. The record version is checked here rather than trusted, because a
    configuration that named a different scheme would otherwise be discovered only
    by a reader of the held records.
    """
    path = Path(config_path)
    if not path.exists():
        raise ValueError(
            f"no matching configuration at {path}; the second venue's metadata acquisition is "
            "declared beside the selection that produced its candidates, and this module keeps "
            "no fallback plan of its own"
        )
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"matching configuration at {path} is not valid YAML: {exc}") from exc
    config = _require_mapping(payload, where=str(path))
    version = _require_text(config.get("config_version"), where=f"{path}: config_version")
    venues = _require_sequence(config.get("venues"), where=f"{path}: venues")

    block: Mapping[str, Any] | None = None
    for entry in venues:
        venue = _require_mapping(entry, where=f"{path}: venues[]")
        if str(venue.get("id") or "") != SECOND_VENUE_ID:
            continue
        declared = venue.get(METADATA_BLOCK)
        if declared is None:
            raise ValueError(
                f"{path}: the {SECOND_VENUE_ID!r} venue entry carries no {METADATA_BLOCK} block; "
                "the acquisition universe is a declared decision and this module does not assume "
                "one"
            )
        block = _require_mapping(
            declared, where=f"{path}: venues[{SECOND_VENUE_ID}].{METADATA_BLOCK}"
        )
    if block is None:
        raise ValueError(f"{path}: no venue entry declares the id {SECOND_VENUE_ID!r}")

    where = f"{path}: venues[{SECOND_VENUE_ID}].{METADATA_BLOCK}"
    host = _require_text(block.get("host"), where=f"{where}.host")
    if not host.startswith("https://") or "?" in host:
        raise ValueError(
            f"{where}.host is {host!r}; a declared source must be an https origin with no query "
            "of its own, because the query is the search this module issues"
        )
    record_version = _require_text(block.get("record_version"), where=f"{where}.record_version")
    if record_version != RECORD_VERSION:
        raise ValueError(
            f"{where}.record_version is {record_version!r}, and this module writes "
            f"{RECORD_VERSION!r}; a reader of the held records would otherwise have to know which "
            "of the two names the bytes on disk carry"
        )
    if block.get("writes_captured_data_into_the_repository") is not False:
        raise ValueError(
            f"{where}.writes_captured_data_into_the_repository must be declared false; the store "
            "holds bytes fetched from a public venue and is not a repository source"
        )
    queries: list[DeclaredQuery] = []
    for index, entry in enumerate(
        _require_sequence(block.get("queries"), where=f"{where}.queries")
    ):
        item = _require_mapping(entry, where=f"{where}.queries[{index}]")
        queries.append(
            DeclaredQuery(
                query=_require_text(item.get("query"), where=f"{where}.queries[{index}].query"),
                why=_require_text(item.get("why"), where=f"{where}.queries[{index}].why"),
            )
        )
    if not queries:
        raise ValueError(
            f"{where}.queries is empty; an acquisition with no declared query reads nothing and "
            "would report that as a successful run"
        )
    names = [declared.query for declared in queries]
    if len(set(names)) != len(names):
        raise ValueError(f"{where}.queries declares the same query twice: {names}")

    return MetadataAcquisitionSettings(
        config_version=version,
        venue=SECOND_VENUE_ID,
        host=host,
        search_path=_require_text(block.get("search_path"), where=f"{where}.search_path"),
        store_root=Path(_require_text(block.get("store_root"), where=f"{where}.store_root")),
        page_parameter=_require_text(block.get("page_parameter"), where=f"{where}.page_parameter"),
        page_size=_required_int(block.get("page_size"), where=f"{where}.page_size"),
        max_pages_per_query=_required_int(
            block.get("max_pages_per_query"), where=f"{where}.max_pages_per_query"
        ),
        max_records_per_run=_required_int(
            block.get("max_records_per_run"), where=f"{where}.max_records_per_run"
        ),
        record_version=record_version,
        note=_require_text(block.get("note"), where=f"{where}.note"),
        queries=tuple(queries),
        writes_captured_data_into_the_repository=False,
    )


@dataclass(frozen=True, slots=True)
class PolymarketMarketRecord:
    """One contract's metadata as the venue's own page states it.

    ``question`` and ``description`` are the fields the predicate layer reads: they
    state the rate the claim resolves on and the change it resolves to, verbatim.
    ``slug`` is the market's own slug, which is an identifier here and never a
    component of the predicate. ``group_item_title`` and ``group_item_threshold``
    are carried as the venue states them (the latter is a string on the wire) and are
    ``None`` when it states none.

    ``source_observed_at`` is the instant the serving system states, never the
    instant this run fetched, and ``raw_hash`` addresses the page the record cites in
    the content-addressed archive.
    """

    record_version: str
    condition_id: str
    slug: str
    event_slug: str
    event_title: str
    question: str
    description: str
    group_item_title: str | None
    group_item_threshold: str | None
    outcomes: str | None
    end_date: str | None
    closed_time: str | None
    source_url: str
    source_observed_at: dt.datetime | None
    raw_hash: str

    def __post_init__(self) -> None:
        if self.record_version != RECORD_VERSION:
            raise ValueError(
                f"PolymarketMarketRecord.record_version must be {RECORD_VERSION!r}, got "
                f"{self.record_version!r}"
            )
        if not _CONDITION_ID_RE.match(self.condition_id):
            raise ValueError(
                f"PolymarketMarketRecord.condition_id {self.condition_id!r} is not the lowercase "
                "32-byte hex the venue states; two identifiers reduced to one file name would "
                "collide on one record"
            )
        for name in ("slug", "event_slug", "event_title", "question", "description", "source_url"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"PolymarketMarketRecord.{name} must be the non-empty text the venue states, "
                    f"got {value!r}"
                )
        for name in (
            "group_item_title",
            "group_item_threshold",
            "outcomes",
            "end_date",
            "closed_time",
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"PolymarketMarketRecord.{name} must be a string or None")
        if not _HASH_RE.match(self.raw_hash):
            raise ValueError(
                f"PolymarketMarketRecord.raw_hash {self.raw_hash!r} is not a sha256 digest, so it "
                "cannot address archived bytes"
            )
        if self.source_observed_at is not None:
            object.__setattr__(
                self,
                "source_observed_at",
                parse_utc_time(self.source_observed_at, field_name="source_observed_at"),
            )

    @property
    def subject(self) -> dict[str, Any]:
        """What the venue states about the contract, without the citation fields."""
        return {name: getattr(self, name) for name in RECORD_SUBJECT_FIELDS}

    @property
    def states_an_instant(self) -> bool:
        """Whether the page this record cites stated when it answered."""
        return self.source_observed_at is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "record_version": self.record_version,
            "condition_id": self.condition_id,
            "slug": self.slug,
            "event_slug": self.event_slug,
            "event_title": self.event_title,
            "question": self.question,
            "description": self.description,
            "group_item_title": self.group_item_title,
            "group_item_threshold": self.group_item_threshold,
            "outcomes": self.outcomes,
            "end_date": self.end_date,
            "closed_time": self.closed_time,
            "source_url": self.source_url,
            "source_observed_at": (
                self.source_observed_at.isoformat() if self.source_observed_at else None
            ),
            "raw_hash": self.raw_hash,
        }


def _record_from_dict(document: Mapping[str, Any], *, where: str) -> PolymarketMarketRecord:
    """Read one held record, refusing a malformed one rather than reading part of it.

    An absent key is a malformed record, and so is a key the contract does not
    declare: a record carrying a field no reader was told about is a record written
    by a different scheme, which is refused here rather than read as though the
    declared fields were all of it.
    """
    missing = [name for name in RECORD_FIELDS if name not in document]
    if missing:
        raise ValueError(f"{where} is missing {missing} of the declared record fields")
    undeclared = sorted(set(document) - set(RECORD_FIELDS))
    if undeclared:
        raise ValueError(
            f"{where} carries fields the declared record does not: {undeclared}; a reader of this "
            "contract cannot be handed a record that is a different shape"
        )
    if document["question"] is None or document["description"] is None:
        raise ValueError(
            f"{where} states a null question or description; a null is an absence, and a market "
            "whose question is absent is a market this record cannot be written for"
        )
    return PolymarketMarketRecord(
        record_version=str(document["record_version"]),
        condition_id=str(document["condition_id"]),
        slug=str(document["slug"]),
        event_slug=str(document["event_slug"]),
        event_title=str(document["event_title"]),
        question=str(document["question"]),
        description=str(document["description"]),
        group_item_title=document["group_item_title"],
        group_item_threshold=document["group_item_threshold"],
        outcomes=document["outcomes"],
        end_date=document["end_date"],
        closed_time=document["closed_time"],
        source_url=str(document["source_url"]),
        source_observed_at=document["source_observed_at"],
        raw_hash=str(document["raw_hash"]),
    )


@dataclass(frozen=True, slots=True)
class MarketLookup:
    """The record held for one contract, or the named reason none is held.

    Exactly one of ``record`` and ``reason`` is set. A lookup that returned a bare
    ``None`` would make "this contract's metadata was never acquired" and "this
    contract has no metadata" the same answer, and only one of those is true.
    """

    condition_id: str
    record: PolymarketMarketRecord | None
    reason: str | None

    def __post_init__(self) -> None:
        if (self.record is None) == (self.reason is None):
            raise ValueError(
                "MarketLookup carries exactly one of a record and a reason; a lookup with both or "
                "neither cannot be read as an answer"
            )
        if self.reason is not None and self.reason != REASON_NO_RECORD_HELD:
            raise ValueError(
                f"MarketLookup.reason {self.reason!r} is not a declared reason; a caller cannot "
                "branch on a code this module does not state"
            )

    @property
    def held(self) -> bool:
        return self.record is not None

    def as_dict(self) -> dict[str, Any]:
        return {
            "condition_id": self.condition_id,
            "held": self.held,
            "reason": self.reason,
            "record": self.record.as_dict() if self.record is not None else None,
        }


class PolymarketMetadataStore:
    """A contract-keyed store of the venue's own market metadata.

    Layout under ``root``::

        raw/blobs/<hash[:2]>/<hash>.bin     the exact page bytes, content-addressed
        markets/<condition_id>.json         one immutable record per contract

    The raw archive is the repository's existing
    :class:`~market_propagation.storage.RawStore`, and pages are fetched through
    :class:`~market_propagation.ingest.transport.HttpTransport` built over that same
    archive, so the page a record cites is the page the transport archived.

    A record is immutable and there is one per contract. Holding the same statement
    twice returns the record already on disk; a page that restates the contract's own
    fields differently is refused rather than overwriting, because a record that
    changed after the fact would have a reader trust a restatement it never cited.
    The citation fields are outside that comparison: the same statement read from two
    pages carries two citations and is still one statement, so the first sighting's
    citation is kept rather than replaced by a later one.
    """

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        settings: MetadataAcquisitionSettings | None = None,
    ) -> None:
        self._settings = settings or load_metadata_acquisition_settings()
        self._root = Path(root) if root is not None else self._settings.store_root

    @property
    def settings(self) -> MetadataAcquisitionSettings:
        return self._settings

    @property
    def root(self) -> Path:
        return self._root

    @property
    def markets_root(self) -> Path:
        return self._root / "markets"

    @property
    def raw_store(self) -> RawStore:
        """The content-addressed archive every held record's page bytes live in."""
        return RawStore(self._root / "raw")

    def market_path(self, condition_id: str) -> Path:
        """The file holding one contract's record.

        An identifier that is not a plain contract key is refused rather than
        sanitized, because a sanitized name is how two contracts come to share one
        record.
        """
        key = str(condition_id)
        if not _CONDITION_ID_RE.match(key):
            raise ValueError(
                f"condition id {key!r} is not the lowercase 32-byte hex this store keys on; a "
                "record file is named from it"
            )
        return self.markets_root / f"{key}.json"

    def load(self, path: str | Path) -> PolymarketMarketRecord:
        """Read one held record from a file."""
        target = Path(path)
        try:
            document = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"held record {target} could not be read: {exc}") from exc
        return _record_from_dict(_require_mapping(document, where=str(target)), where=str(target))

    def write(self, record: PolymarketMarketRecord) -> PolymarketMarketRecord:
        """Hold ``record``, returning the record now held for its contract.

        The return value is the held record rather than the argument because a
        re-sighting of one statement returns the record already on disk, citation and
        all, and a caller that counted its own argument would count two records where
        the store holds one.
        """
        path = self.market_path(record.condition_id)
        if path.exists():
            held = self.load(path)
            if held.subject != record.subject:
                raise FileExistsError(
                    f"a metadata record for {record.condition_id} is already held and the venue's "
                    "own stated fields differ; a record that changed after the fact would have a "
                    "reader trust a restatement it never cited"
                )
            return held
        payload = (json.dumps(record.as_dict(), indent=2, sort_keys=True) + "\n").encode("utf-8")
        try:
            _atomic_write_bytes(path, payload)
        except FileExistsError as exc:
            raise FileExistsError(
                f"a metadata record for {record.condition_id} appeared while this one was being "
                "written, with different content"
            ) from exc
        return record

    def records(self) -> tuple[PolymarketMarketRecord, ...]:
        """Every record held, deterministically ordered by contract key."""
        return self.records_for(self.condition_ids())

    def records_for(self, condition_ids: Sequence[str]) -> tuple[PolymarketMarketRecord, ...]:
        """The records held for the named contracts, in the order named."""
        out: list[PolymarketMarketRecord] = []
        for condition_id in condition_ids:
            path = self.market_path(condition_id)
            if path.exists():
                out.append(self.load(path))
        return tuple(out)

    def condition_ids(self) -> tuple[str, ...]:
        """Every contract holding a record, sorted."""
        if not self.markets_root.exists():
            return ()
        return tuple(sorted(path.stem for path in self.markets_root.glob("*.json")))

    def held(self, condition_id: str) -> MarketLookup:
        """The record held for one contract, or the reason none is held.

        The identifier is validated first, so an identifier that is not a contract
        key raises while an identifier that is a contract key and is simply not held
        answers with the named reason. Those are different facts and a caller has to
        be able to tell them apart.
        """
        path = self.market_path(condition_id)
        if not path.exists():
            return MarketLookup(
                condition_id=str(condition_id), record=None, reason=REASON_NO_RECORD_HELD
            )
        return MarketLookup(condition_id=str(condition_id), record=self.load(path), reason=None)

    def counts(self) -> dict[str, Any]:
        """What the store holds, as counts rather than as a listing.

        ``distinct_condition_ids`` and ``markets_held`` coincide by construction,
        because the store keys one record per contract; they are both reported
        because a store that ever held two records under one contract would show the
        difference here rather than in a reader's arithmetic.
        """
        records = self.records()
        keys = {record.condition_id for record in records}
        with_instant = sum(1 for record in records if record.states_an_instant)
        return {
            "record_version": RECORD_VERSION,
            "markets_held": len(records),
            "markets_held_with_a_stated_instant": with_instant,
            "markets_held_without_a_stated_instant": len(records) - with_instant,
            "distinct_condition_ids": len(keys),
        }

    def verify(self, record: PolymarketMarketRecord) -> None:
        """Re-read a record's page and check it against the record.

        Raises :class:`FileNotFoundError` when the archive holds no such page and
        :class:`ValueError` when the stored bytes hash to something else, or when the
        question and description the record states do not occur in the page it cites.
        Both are refusals at the caller: a record whose page is gone or changed, or
        which cites a page that does not carry the text it quotes, is a record that
        establishes nothing.
        """
        body = self.raw_store.get(record.raw_hash)
        page = body.decode("utf-8", errors="replace")
        text_as_stored(page, record.question)
        text_as_stored(page, record.description)


@dataclass(frozen=True, slots=True)
class AcquisitionSummary:
    """What one sweep did, counted rather than described.

    ``markets_held`` counts the contracts this run held into the store; the same
    contract sighted twice under two queries is one held contract and two sightings,
    because the store keys on the contract. ``distinct_condition_ids`` counts the
    contracts the sweep's pages named at all, which is the number to read beside
    ``markets_held``: a difference between them is a market this run saw and could
    not hold, and each of those is named in ``skipped``.
    """

    config_version: str
    venue: str
    host: str
    search_path: str
    store_root: str
    queries: tuple[str, ...]
    requests_made: int
    pages_archived: int
    markets_sighted: int
    markets_held: int
    markets_held_with_a_stated_instant: int
    distinct_condition_ids: int
    records_bound_reached: bool
    page_bounded_queries: tuple[str, ...]
    blocked: tuple[dict[str, Any], ...]
    skipped: tuple[dict[str, Any], ...]

    @property
    def blocked_queries(self) -> tuple[str, ...]:
        """The queries a page of which refused access, in the order they were read."""
        return tuple(str(item["query"]) for item in self.blocked)

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary_version": RECORD_VERSION,
            "config_version": self.config_version,
            "venue": self.venue,
            "host": self.host,
            "search_path": self.search_path,
            "store_root": self.store_root,
            "queries": list(self.queries),
            "requests_made": self.requests_made,
            "pages_archived": self.pages_archived,
            "markets_sighted": self.markets_sighted,
            "markets_held": self.markets_held,
            "markets_held_with_a_stated_instant": self.markets_held_with_a_stated_instant,
            "distinct_condition_ids": self.distinct_condition_ids,
            "records_bound_reached": self.records_bound_reached,
            "page_bounded_queries": list(self.page_bounded_queries),
            "blocked_queries": list(self.blocked_queries),
            "blocked": [dict(item) for item in self.blocked],
            "skipped": [dict(item) for item in self.skipped],
        }


@dataclass(frozen=True, slots=True)
class _MarketRead:
    """Either one record built from a market, or the named reason it yielded none."""

    record: PolymarketMarketRecord | None
    reason: str | None
    condition_id: str | None
    detail: str

    @property
    def readable(self) -> bool:
        return self.record is not None


def _read_market(
    market: Mapping[str, Any],
    *,
    event: Mapping[str, Any],
    envelope: ResponseEnvelope,
) -> _MarketRead:
    """Read one market on a page as the record contract states it, or refuse it by name.

    Nothing is defaulted: a market whose question, description, slug or parent event
    the page does not state yields no record, because a record with an empty question
    would be a fabricated statement about a contract rather than a missing one.
    """
    raw_key = market.get("conditionId")
    if not isinstance(raw_key, str) or not raw_key.strip():
        return _MarketRead(None, REASON_NO_CONDITION_ID, None, f"conditionId={raw_key!r}")
    condition_id = raw_key.strip().lower()
    if not _CONDITION_ID_RE.match(condition_id):
        return _MarketRead(
            None, REASON_CONDITION_ID_NOT_A_MARKET_KEY, None, f"conditionId={raw_key!r}"
        )

    def refuse(reason: str, detail: str) -> _MarketRead:
        return _MarketRead(None, reason, condition_id, detail)

    slug = market.get("slug")
    if not isinstance(slug, str) or not slug.strip():
        return refuse(REASON_NO_MARKET_SLUG, f"slug={slug!r}")
    question = market.get("question")
    if not isinstance(question, str) or not question.strip():
        return refuse(REASON_NO_QUESTION, f"question={question!r}")
    description = market.get("description")
    if not isinstance(description, str) or not description.strip():
        return refuse(REASON_NO_DESCRIPTION, f"description={description!r}")
    event_slug = event.get("slug")
    event_title = event.get("title")
    if (
        not isinstance(event_slug, str)
        or not event_slug.strip()
        or not isinstance(event_title, str)
        or not event_title.strip()
    ):
        return refuse(REASON_NO_PARENT_EVENT, f"event slug={event_slug!r} title={event_title!r}")
    outcomes = market.get("outcomes")
    if outcomes is not None and not isinstance(outcomes, str):
        return refuse(REASON_OUTCOMES_NOT_A_STATED_STRING, f"outcomes={type(outcomes).__name__}")

    return _MarketRead(
        PolymarketMarketRecord(
            record_version=RECORD_VERSION,
            condition_id=condition_id,
            slug=slug,
            event_slug=event_slug,
            event_title=event_title,
            question=question,
            description=description,
            group_item_title=_optional_text(
                market.get("groupItemTitle"), where="market.groupItemTitle"
            ),
            group_item_threshold=_optional_text(
                market.get("groupItemThreshold"), where="market.groupItemThreshold"
            ),
            outcomes=outcomes,
            end_date=_optional_text(market.get("endDate"), where="market.endDate"),
            closed_time=_optional_text(market.get("closedTime"), where="market.closedTime"),
            source_url=envelope.url,
            source_observed_at=envelope.server_date,
            raw_hash=envelope.provenance.raw_hash,
        ),
        None,
        condition_id,
        "",
    )


def _page_markets(
    envelope: ResponseEnvelope, *, where: str
) -> tuple[list[tuple[Mapping[str, Any], Mapping[str, Any]]], bool]:
    """The page's nested markets with their parent events, and whether more follow.

    A body that is not the declared shape is refused by raising rather than recorded
    as an empty page: a renamed field read as an absence would report a venue with no
    markets where the venue had changed its wire format. The one exception is the
    payload the venue returns past the end of a result set, which was measured
    carrying a ``pagination`` block and nothing else; that ends the sweep without
    inventing a market. The exception is drawn as narrowly as it can be: it applies
    only to a payload whose keys are all pagination, so a payload carrying a renamed
    field beside its pagination still raises rather than ending the sweep.
    """
    payload = envelope.json()
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"the search response at {where} is not a JSON object; the wire shape changed and "
            "this module refuses rather than reading an empty page"
        )
    pagination = payload.get("pagination")
    has_more = bool(pagination.get("hasMore")) if isinstance(pagination, Mapping) else False
    events = payload.get("events")
    if events is None:
        extra = sorted(set(payload) - {"pagination"})
        if has_more or extra:
            raise ValueError(
                f"the search response at {where} carries no 'events' list and states "
                f"{'more results follow' if has_more else f'the undeclared keys {extra}'}; the "
                "wire shape changed and this module refuses rather than recording an empty success"
            )
        return [], False
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes)):
        raise ValueError(
            f"the search response at {where} carries 'events' that is not a list; the wire shape "
            "changed and this module refuses rather than reading an empty page"
        )

    pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for index, event in enumerate(events):
        if not isinstance(event, Mapping):
            raise ValueError(f"{where}: events[{index}] is not an object")
        markets = event.get("markets")
        if not isinstance(markets, Sequence) or isinstance(markets, (str, bytes)):
            raise ValueError(
                f"{where}: events[{index}] carries no 'markets' list; the wire shape changed and "
                "this module refuses rather than reading an empty event"
            )
        for market in markets:
            if not isinstance(market, Mapping):
                raise ValueError(f"{where}: events[{index}].markets[] is not an object")
            pairs.append((event, market))
    return pairs, has_more


def _sweep(
    store: PolymarketMetadataStore,
    settings: MetadataAcquisitionSettings,
    transport: HttpTransport,
) -> AcquisitionSummary:
    """Read every declared query to its end, or to the declared bound, holding what it finds."""
    requests_made = 0
    pages_archived = 0
    markets_sighted = 0
    held_ids: list[str] = []
    seen_ids: list[str] = []
    bounded_queries: list[str] = []
    blocked: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    records_bound_reached = False

    for declared in settings.queries:
        page = 1
        more_offered = True
        while page <= settings.max_pages_per_query:
            try:
                envelope = transport.get(
                    settings.search_url,
                    params={
                        "q": declared.query,
                        settings.page_parameter: page,
                        "limit_per_type": settings.page_size,
                    },
                    source=f"polymarket_market_metadata:{RECORD_VERSION}",
                    record_id=f"public-search-{_query_label(declared.query)}-{page:05d}",
                )
            except TransportError as error:
                requests_made += 1
                if error.payload_hash is not None:
                    pages_archived += 1
                blocked.append(
                    {
                        "query": declared.query,
                        "page": page,
                        "reason": error.reason,
                        "detail": str(error),
                        "status_code": error.status_code,
                        "payload_hash": error.payload_hash,
                    }
                )
                break
            requests_made += 1
            pages_archived += 1
            pairs, more_offered = _page_markets(
                envelope, where=f"query {declared.query!r} page {page}"
            )
            for event, market in pairs:
                read = _read_market(market, event=event, envelope=envelope)
                if read.condition_id is not None and read.condition_id not in seen_ids:
                    seen_ids.append(read.condition_id)
                if not read.readable or read.record is None:
                    skipped.append(
                        {
                            "query": declared.query,
                            "page": page,
                            "condition_id": read.condition_id,
                            "reason": read.reason,
                            "detail": read.detail,
                        }
                    )
                    continue
                if (
                    read.record.condition_id not in held_ids
                    and len(held_ids) >= settings.max_records_per_run
                ):
                    # The bound is checked per market, not per page, so it can never be
                    # overshot by the markets a page happens to carry. The markets a
                    # bounded sweep did not hold are named rather than silently absent.
                    records_bound_reached = True
                    skipped.append(
                        {
                            "query": declared.query,
                            "page": page,
                            "condition_id": read.record.condition_id,
                            "reason": REASON_RECORD_BOUND_REACHED,
                            "detail": f"records held={len(held_ids)}",
                        }
                    )
                    continue
                held = store.write(read.record)
                markets_sighted += 1
                if held.condition_id not in held_ids:
                    held_ids.append(held.condition_id)
            if not more_offered or records_bound_reached:
                break
            page += 1
        if not records_bound_reached and more_offered and page > settings.max_pages_per_query:
            bounded_queries.append(declared.query)
        if records_bound_reached:
            break

    records = store.records_for(held_ids)
    with_instant = sum(1 for record in records if record.states_an_instant)
    return AcquisitionSummary(
        config_version=settings.config_version,
        venue=settings.venue,
        host=settings.host,
        search_path=settings.search_path,
        store_root=str(store.root),
        queries=settings.declared_queries,
        requests_made=requests_made,
        pages_archived=pages_archived,
        markets_sighted=markets_sighted,
        markets_held=len(held_ids),
        markets_held_with_a_stated_instant=with_instant,
        distinct_condition_ids=len(seen_ids),
        records_bound_reached=records_bound_reached,
        page_bounded_queries=tuple(bounded_queries),
        blocked=tuple(blocked),
        skipped=tuple(skipped),
    )


def capture(
    store_root: str | Path | None = None,
    *,
    config_path: str | Path = MATCH_CONFIG_PATH,
    transport: HttpTransport | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> AcquisitionSummary:
    """Sweep the declared queries and hold one record per contract the pages state.

    ``transport`` is supplied by a caller that already owns a paced client, which is
    how the tests drive this offline; when it is omitted one is built over the
    store's own archive and closed here.

    A page that refuses access is recorded in the summary as a blocked query and the
    sweep continues with the next declared query, because an access failure on one
    slice of the family is not a statement about the others. A page whose bytes are
    not the declared wire shape is refused by raising: an upstream rename must not
    read as a venue with no markets.
    """
    settings = load_metadata_acquisition_settings(config_path)
    store = PolymarketMetadataStore(store_root, settings=settings)
    owned = transport is None
    if transport is None:
        # Imported here rather than at module scope so reading the plan stays a light
        # operation, matching the rule-capture command's own lazy import of the pacing
        # floor this module must pace at.
        from ..operations import REQUEST_PACING_SECONDS

        transport = HttpTransport(
            store.raw_store,
            timeout_seconds=timeout_seconds,
            policy=RetryPolicy(min_interval_seconds=REQUEST_PACING_SECONDS),
        )
    try:
        return _sweep(store, settings, transport)
    finally:
        if owned:
            transport.close()
