"""Bounded cursor pagination with checkpoints and loop detection.

The public sources in scope share one shape: a list of items plus an opaque
``cursor`` that yields the next page. Two observed behaviours make a naive loop
unsafe.

* An invalid cursor is **not** always an error. Kalshi's historical trades
  endpoint was observed returning HTTP 200 and the *first page again* for
  ``cursor=NOTACURSOR``. A loop that only counted pages would spin until the
  bound and then report a duplicate-laden success.
* Cursors are opaque and unhashed, so a repeat cursor is not by itself proof of
  a loop. A repeated cursor that also repeats items is.

Both are handled by hashing cursor tokens and tracking the identity of the first
item on each page, then refusing to continue once a cursor or a leading item
repeats. No cursor is ever unreachable: the paginator stops and reports
``loop_detected`` so the caller records a partial, honest result.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .transport import (
    HttpTransport,
    ResponseEnvelope,
    TransportError,
    WireShapeError,
)


@dataclass(frozen=True, slots=True)
class Page:
    """One fetched page plus the provenance needed to audit it."""

    index: int
    items: tuple[Any, ...]
    cursor_in: str | None
    cursor_out: str | None
    raw_hash: str
    url: str
    status_code: int
    received_time: Any
    record_id: str
    attempts: int = 1


@dataclass(frozen=True, slots=True)
class PaginationCheckpoint:
    """Resumable state. Written by the caller so a crash loses at most one page."""

    url: str
    params: tuple[tuple[str, str], ...]
    cursor: str | None
    items_seen: int
    pages_fetched: int
    complete: bool
    stop_reason: str
    #: The request parameter the walk carried its cursor in. Recorded here rather
    #: than assumed by the resumer: the member name is a parameter of
    #: :func:`paginate`, so a resume that wrote the cursor under a different key
    #: would be ignored by the venue and the walk would restart at page one.
    cursor_key: str = "cursor"
    last_raw_hash: str | None = None
    observed_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "params": dict(self.params),
            "cursor": self.cursor,
            "cursor_key": self.cursor_key,
            "items_seen": self.items_seen,
            "pages_fetched": self.pages_fetched,
            "complete": self.complete,
            "stop_reason": self.stop_reason,
            "last_raw_hash": self.last_raw_hash,
            "observed_at": self.observed_at,
        }


def escape_json_pointer_token(token: str) -> str:
    """One RFC 6901 reference token, escaped (``~`` then ``/``, in that order)."""
    return token.replace("~", "~0").replace("/", "~1")


class PointerResolutionError(ValueError):
    """An RFC 6901 pointer that addresses nothing in the payload it was read against."""


def resolve_json_pointer(payload: Any, pointer: str) -> Any:
    """Resolve an RFC 6901 pointer against a parsed JSON payload.

    ``""`` addresses the whole document. ``-`` is refused: it names the position
    *after* the last element of an array, so no element can be read from it.
    """
    if pointer == "":
        return payload
    if not pointer.startswith("/"):
        raise PointerResolutionError(f"pointer must start with '/': {pointer!r}")
    current = payload
    for raw_token in pointer[1:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            if token not in current:
                raise PointerResolutionError(
                    f"{token!r} is not a member of the object at {pointer!r}"
                )
            current = current[token]
            continue
        if isinstance(current, list):
            if not (token.isascii() and token.isdigit()):
                raise PointerResolutionError(f"{token!r} is not an array index in {pointer!r}")
            index = int(token)
            if index >= len(current):
                raise PointerResolutionError(
                    f"{pointer!r} addresses index {index} of a {len(current)}-element array"
                )
            current = current[index]
            continue
        raise PointerResolutionError(
            f"nothing resolves inside the {type(current).__name__} at {pointer!r}"
        )
    return current


@dataclass(frozen=True, slots=True)
class RecordOrigin:
    """Where one retained record came from: its backed page and its position there.

    A paginated listing is retrieved once and then read many times, so a caller
    that ignores this and re-derives an identifier from the record in hand has
    nothing archived behind it. Carrying the page hash and the record's own index
    inside that page is what lets a record be traced to the original bytes rather
    than to a re-serialization of itself.

    The index alone does not address anything in those bytes, so the JSON member
    that held the list is carried too and :attr:`pointer` combines the two into an
    RFC 6901 pointer. The pointer is a locator rather than a digest precisely
    because it has to stay resolvable by reading the payload: resolving it against
    ``RawStore.get(raw_hash)`` returns this record, where a digest of the record
    would name bytes nothing archived.
    """

    raw_hash: str
    page_index: int
    record_index: int
    #: The member of the archived page that held the list this record was read
    #: from, spelled as the venue spelled it (``markets``, ``trades``, ``events``).
    items_key: str

    @property
    def pointer(self) -> str:
        """RFC 6901 pointer to this record inside the page ``raw_hash`` names."""
        return f"/{escape_json_pointer_token(self.items_key)}/{self.record_index}"

    def resolve(self, payload: Any) -> Any:
        """The record this origin addresses inside an already-parsed ``payload``."""
        return resolve_json_pointer(payload, self.pointer)

    def as_dict(self) -> dict[str, Any]:
        return {
            "raw_hash": self.raw_hash,
            "page_index": self.page_index,
            "record_index": self.record_index,
            "items_key": self.items_key,
            "record_pointer": self.pointer,
        }


@dataclass(frozen=True, slots=True)
class PaginationResult:
    """Every item retrieved, deduplicated by identity, plus the honest stop reason."""

    items: tuple[Any, ...]
    pages: tuple[Page, ...]
    complete: bool
    stop_reason: str
    checkpoint: PaginationCheckpoint
    #: One origin per retained item, positionally aligned with ``items``. Every
    #: entry names a page hash that the transport actually archived and the JSON
    #: member that held the record, so a caller can retrieve the original bytes
    #: behind a record and resolve it there instead of trusting an identifier
    #: derived from it. Required rather than defaulted: an empty tuple would let a
    #: caller read ``items`` with nothing archived behind them, which is the
    #: unbacked record this type exists to make impossible.
    origins: tuple[RecordOrigin, ...]
    blocked: tuple[dict[str, Any], ...] = ()
    #: Observed per-page counts, before deduplication, so a caller can verify
    #: count consistency across a pagination boundary rather than trusting the
    #: concatenation.
    page_counts: tuple[int, ...] = ()
    #: Records retained that carried an identifier. A record with no usable id
    #: is kept but contributes no identity here, so this is not ``len(items)``.
    distinct_ids_across_pages: int = 0
    #: Repeats of an identity whose first delivery was on an *earlier* page.
    repeated_ids_across_pages: int = 0
    #: Repeats of an identity already delivered on the *same* page.
    dups_within_pages: int = 0
    #: Records the ``max_items`` cap excluded after their page had already been
    #: counted. A cap that cuts inside a page leaves the observed page sizes larger
    #: than the records the dedup loop was allowed to consider, so without this the
    #: retained-plus-dropped total cannot account for what the pages carried. Zero
    #: whenever the cap did not bind.
    excluded_by_max_items: int = 0

    def __post_init__(self) -> None:
        if len(self.origins) != len(self.items):
            raise ValueError(
                f"{len(self.items)} retained item(s) but {len(self.origins)} "
                "origin(s); every item must name the page and position it was read "
                "from, so the two are positionally aligned"
            )

    @property
    def raw_hashes(self) -> tuple[str, ...]:
        return tuple(page.raw_hash for page in self.pages)

    @property
    def dups_total(self) -> int:
        """Every redundant delivery dropped, within a page or across pages."""
        return self.dups_within_pages + self.repeated_ids_across_pages

    def as_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "stop_reason": self.stop_reason,
            "item_count": len(self.items),
            "page_counts": list(self.page_counts),
            "pages_fetched": len(self.pages),
            "dups_total": self.dups_total,
            "dups_within_pages": self.dups_within_pages,
            "excluded_by_max_items": self.excluded_by_max_items,
            "distinct_ids_across_pages": self.distinct_ids_across_pages,
            "repeated_ids_across_pages": self.repeated_ids_across_pages,
            "raw_hashes": list(self.raw_hashes),
            "checkpoint": self.checkpoint.as_dict(),
            "blocked": list(self.blocked),
        }


def _identity(item: Any, keys: Sequence[Callable[[Any], Any]]) -> str | None:
    """The item's own identifier, or ``None`` when the feed supplied none.

    Keys are tried in order and the first non-``None`` value wins. A content
    digest is deliberately not the fallback: two byte-identical records with no
    identifier are two occurrences, and collapsing them by content would delete
    the repeat the caller is measuring.
    """
    for key in keys:
        try:
            value = key(item)
        except (KeyError, TypeError, AttributeError):
            continue
        if value is None:
            continue
        return str(value)
    return None


def _cursor_key(cursor: str) -> str:
    return hashlib.sha256(cursor.encode("utf-8")).hexdigest()[:16]


def paginate(
    transport: HttpTransport,
    url: str,
    *,
    items_key: str,
    cursor_key: str = "cursor",
    source: str,
    params: Mapping[str, Any] | None = None,
    max_pages: int = 50,
    max_items: int | None = None,
    identity_keys: Sequence[Callable[[Any], Any]] = (),
    record_prefix: str = "page",
    start_cursor: str | None = None,
) -> PaginationResult:
    """Retrieve up to ``max_pages`` pages, deduplicating by occurrence identity.

    ``identity_keys`` are tried in order and the first non-``None`` value wins.
    There is no content-digest fallback: without a usable identifier a record
    carries no identity to repeat *under*, so two identical payloads stay two
    occurrences. A record whose identity was already delivered is a duplicate
    and is dropped, whether it arrived earlier on the same page
    (``dups_within_pages``) or on an earlier page
    (``repeated_ids_across_pages``). Two records with distinct ``trade_id``
    values are therefore both kept even when time, price and size are identical,
    which the public trade feeds were observed producing.

    ``limit`` in ``params`` is honored as sent. Consistency across a boundary is
    reported through ``page_counts``, ``distinct_ids_across_pages`` and
    ``repeated_ids_across_pages`` rather than by silently dropping repeats.
    """
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")

    base_params = dict(params or {})
    pages: list[Page] = []
    seen_hashes: set[str] = set()
    seen_cursors: set[str] = set()
    blocked: list[dict[str, Any]] = []
    occurrences = 0
    cursor = start_cursor
    stop_reason = "exhausted"
    complete = False

    for index in range(max_pages):
        request_params = dict(base_params)
        request_params[cursor_key] = cursor
        try:
            envelope = transport.get(
                url,
                params=request_params,
                source=source,
                record_id=f"{record_prefix}-{index:05d}",
            )
        except TransportError as exc:
            blocked.append(exc.as_blocked_record())
            stop_reason = "blocked"
            break

        payload = _require_mapping(envelope, url)
        items = _require_items(payload, url, items_key)
        raw_hash = envelope.provenance.raw_hash
        next_cursor = payload.get(cursor_key) or None

        # A page whose body repeats exactly is a loop even if the cursor string
        # differs in spelling.
        body_repeat = raw_hash in seen_hashes
        seen_hashes.add(raw_hash)

        occurrences += len(items)
        pages.append(
            Page(
                index=index,
                items=tuple(items),
                cursor_in=cursor,
                cursor_out=next_cursor,
                raw_hash=raw_hash,
                url=envelope.url,
                status_code=envelope.status_code,
                received_time=envelope.received_time,
                record_id=f"{record_prefix}-{index:05d}",
                attempts=envelope.attempts,
            )
        )

        if max_items is not None and occurrences >= max_items:
            stop_reason = "max_items"
            break
        if body_repeat:
            stop_reason = "repeated_page_body"
            break
        if not items:
            # An empty page with no cursor is a genuinely exhausted result set and
            # is complete. An empty page that still advertises a cursor is not: the
            # server is asserting more data exists behind a page it returned empty,
            # which is an anomaly rather than an end.
            if next_cursor is None:
                stop_reason = "exhausted"
                complete = True
            else:
                stop_reason = "empty_page_with_cursor"
            break
        if next_cursor is None:
            stop_reason = "exhausted"
            complete = True
            break

        key = _cursor_key(next_cursor)
        if key in seen_cursors:
            stop_reason = "cursor_loop"
            break
        seen_cursors.add(key)
        if start_cursor is not None:
            seen_cursors.add(_cursor_key(start_cursor))
        cursor = next_cursor
    else:
        stop_reason = "max_pages"

    # Identity is resolved per page so every redundant delivery is attributed to
    # the boundary it actually crossed: a repeat of an identity first delivered
    # on an earlier page is an across-page repeat, while a repeat inside its own
    # page is not. ``max_items`` still bounds the raw occurrences considered, so
    # the cap keeps meaning occurrences retrieved rather than records kept.
    deduped: list[Any] = []
    origins: list[RecordOrigin] = []
    seen_ids: set[str] = set()
    dups_within_pages = 0
    repeated_ids_across_pages = 0
    remaining = max_items
    for page in pages:
        if remaining is not None and remaining <= 0:
            break
        page_items = page.items if remaining is None else page.items[:remaining]
        if remaining is not None:
            remaining -= len(page_items)
        page_ids: set[str] = set()
        for record_index, item in enumerate(page_items):
            identity = _identity(item, identity_keys)
            if identity is None:
                # No usable identifier, so this record has no identity to repeat
                # under. It is kept, and it is never matched against another: two
                # identical payloads without an id are two occurrences.
                deduped.append(item)
                origins.append(
                    RecordOrigin(
                        raw_hash=page.raw_hash,
                        page_index=page.index,
                        record_index=record_index,
                        items_key=items_key,
                    )
                )
                continue
            if identity in page_ids:
                dups_within_pages += 1
                continue
            page_ids.add(identity)
            if identity in seen_ids:
                repeated_ids_across_pages += 1
                continue
            seen_ids.add(identity)
            deduped.append(item)
            # The retained record keeps the page it was read from and its own
            # position inside that page's list, so the bytes behind it stay reachable
            # and the pointer resolves: page hash plus the member name plus the index.
            origins.append(
                RecordOrigin(
                    raw_hash=page.raw_hash,
                    page_index=page.index,
                    record_index=record_index,
                    items_key=items_key,
                )
            )

    checkpoint = PaginationCheckpoint(
        url=url,
        params=tuple((str(k), str(v)) for k, v in base_params.items() if v is not None),
        cursor=cursor if not complete else None,
        items_seen=len(deduped),
        pages_fetched=len(pages),
        complete=complete,
        stop_reason=stop_reason,
        cursor_key=cursor_key,
        last_raw_hash=pages[-1].raw_hash if pages else None,
    )

    return PaginationResult(
        items=tuple(deduped),
        pages=tuple(pages),
        complete=complete,
        stop_reason=stop_reason,
        checkpoint=checkpoint,
        blocked=tuple(blocked),
        # Derived from the pages actually retained, so the total a caller compares
        # against cannot drift from the pages it was told about.
        page_counts=tuple(len(page.items) for page in pages),
        # The pages were counted before the cap was applied, so the records the cap
        # kept out are named here rather than left as an unexplained gap between the
        # page sizes and the retained-plus-dropped total.
        excluded_by_max_items=(
            sum(len(page.items) for page in pages)
            - len(deduped)
            - dups_within_pages
            - repeated_ids_across_pages
        ),
        distinct_ids_across_pages=len(seen_ids),
        repeated_ids_across_pages=repeated_ids_across_pages,
        dups_within_pages=dups_within_pages,
        origins=tuple(origins),
    )


def cursor_sets_consistent(result: PaginationResult) -> bool:
    """True when every page carried the same page size except a final short page.

    The documented pages are fixed-size until the data runs out, so a full page
    followed by a longer one indicates the boundary moved rather than the data
    ending. A single page is trivially consistent.
    """
    counts = list(result.page_counts)
    if len(counts) <= 1:
        return True
    return all(count == counts[0] for count in counts[:-1]) and counts[-1] <= counts[0]


def checkpoint_params(checkpoint: PaginationCheckpoint) -> dict[str, Any]:
    """Rebuild request parameters from a checkpoint for a resume.

    The cursor goes back under the parameter name the walk actually used, which
    the checkpoint recorded, so a feed whose cursor member is not ``cursor``
    resumes where it stopped instead of silently restarting from page one.
    """
    params: dict[str, Any] = dict(checkpoint.params)
    if checkpoint.cursor:
        params[checkpoint.cursor_key] = checkpoint.cursor
    return params


def _require_mapping(envelope: ResponseEnvelope, url: str) -> dict[str, Any]:
    payload = envelope.json()
    if not isinstance(payload, dict):
        raise WireShapeError(f"expected a JSON object from {url}, got {type(payload).__name__}")
    return payload


def _require_items(payload: Mapping[str, Any], url: str, items_key: str) -> list[Any]:
    if items_key not in payload:
        raise WireShapeError(
            f"response from {url} has no {items_key!r} key (present keys: {sorted(payload)})"
        )
    items = payload[items_key]
    if items is None:
        return []
    if not isinstance(items, list):
        raise WireShapeError(f"{items_key!r} from {url} is {type(items).__name__}, expected a list")
    return items
