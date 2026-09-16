"""Order-book reconstruction and dual-order replay.

One :class:`BookState` reconstructs every book in the input stream. It is
mutable and caller-owned: :func:`apply_book_event` updates the state in place and
returns it. Separate collectors never share a state.

Identity and scoping
--------------------
A book is identified by ``(venue, contract_id, connection_id,
sequence_scope)``. Two connections feeding the same market are two books, and
their levels are never mixed.

A sequence cursor, by contrast, belongs to
``(venue, connection_id, sequence_scope)``. One documented stream can carry
messages for several contracts, so an interleaved A/B stream on one scope is
normal and must not look like a gap. When a real gap appears, every book in that
scope is invalid until its own snapshot restores it.

Validity
--------
A quote is emitted for every event, including invalid ones. That is deliberate:
as-of selection cannot then resurrect an older valid book across a gap,
disconnect or closure, because the invalid quote is there at that time. While a
book is invalid, deltas do not modify its levels. They are counted as deferred,
and the book keeps reporting the reason it cannot be trusted.

Three times stay distinct: ``last_price_change`` (last level change),
``last_verified`` (last message seen on a live stream) and ``last_trade``. An
unchanged standing quote is not stale, so freshness is measured against
``last_verified``.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .domain import (
    UTC,
    BookEvent,
    BookKind,
    BookOperation,
    BookSide,
    Provenance,
    Quote,
    QuoteValidity,
    Trade,
    market_key,
)

__all__ = [
    "ORDER_ALIASES",
    "ORDER_SOURCE",
    "ORDER_USABLE",
    "BookKey",
    "BookState",
    "ReplayResult",
    "apply_book_event",
    "canonicalize_events",
    "compare_replay_orders",
    "replay",
    "resolve_order",
]

ORDER_SOURCE = "source"
ORDER_USABLE = "usable"

ORDER_ALIASES: dict[str, str] = {
    "source": ORDER_SOURCE,
    "source_time": ORDER_SOURCE,
    "source_order": ORDER_SOURCE,
    "usable": ORDER_USABLE,
    "usable_time": ORDER_USABLE,
    "usable_order": ORDER_USABLE,
}

_MAX_REPORTED_INVERSIONS = 50


def resolve_order(order: str) -> str:
    """Map an order spelling to ``'source'`` or ``'usable'``."""
    if not isinstance(order, str):
        raise TypeError(f"order must be a str, got {type(order).__name__}")
    try:
        return ORDER_ALIASES[order.strip().lower()]
    except KeyError:
        raise ValueError(
            f"unknown replay order {order!r}; expected one of {sorted(set(ORDER_ALIASES))}"
        ) from None


@dataclass(frozen=True, slots=True)
class BookKey:
    """Identity of one reconstructed book."""

    venue: str
    contract_id: str
    connection_id: str = ""
    sequence_scope: str = ""

    @property
    def market_key(self) -> str:
        return market_key(self.venue, self.contract_id)

    @property
    def scope_key(self) -> tuple[str, str, str]:
        return (self.venue, self.connection_id, self.sequence_scope)

    @classmethod
    def of(cls, event: BookEvent) -> BookKey:
        return cls(event.venue, event.contract_id, event.connection_id, event.sequence_scope)


@dataclass(slots=True)
class _Scope:
    """Sequence cursor for one documented stream.

    The cursor is per scope, not per book and not global. ``generation``
    increments each time the stream becomes untrustworthy (gap, reconnect), and
    a book is trusted only when its last snapshot carries the current
    generation.
    """

    venue: str
    connection_id: str
    sequence_scope: str
    last_sequence: int | None = None
    generation: int = 0
    disconnected: bool = False
    gap_open: bool = False
    event_count: int = 0

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.venue, self.connection_id, self.sequence_scope)


@dataclass(slots=True)
class _Book:
    """Mutable levels and lifecycle for one :class:`BookKey`."""

    key: BookKey
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    sequence: int | None = None
    snapshot_generation: int | None = None
    halted: bool = False
    closed: bool = False
    pending: QuoteValidity | None = QuoteValidity.AWAITING_SNAPSHOT
    last_price_change: dt.datetime | None = None
    last_verified: dt.datetime | None = None
    last_trade: dt.datetime | None = None
    deferred_deltas: int = 0
    applied_events: int = 0
    rejected_events: int = 0
    provenance: Provenance | None = None

    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    def is_crossed(self) -> bool:
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid >= ask

    def validity(self, scope: _Scope) -> QuoteValidity:
        if self.closed:
            return QuoteValidity.CLOSED
        if scope.disconnected:
            return QuoteValidity.DISCONNECTED
        if self.snapshot_generation != scope.generation:
            if scope.gap_open or self.snapshot_generation is not None:
                return QuoteValidity.GAP
            return self.pending or QuoteValidity.AWAITING_SNAPSHOT
        if self.pending is not None:
            return self.pending
        if self.halted:
            return QuoteValidity.HALTED
        if self.is_crossed():
            return QuoteValidity.CROSSED
        return QuoteValidity.VALID


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Reconstructed book state after one event.

    Addressable by ``(venue, contract_id)`` or by ``market_key``, with both
    sides' levels kept as sorted ``(price, size)`` tuples, so a consumer can see
    depth rather than only the top of book. ``validity`` is the state's own
    vocabulary; a valid state with no levels is an observed empty book, not a
    missing one.
    """

    market_key: str
    venue: str
    contract_id: str
    connection_id: str
    sequence_scope: str
    order_time: dt.datetime | None
    validity: QuoteValidity
    bid: Decimal | None
    ask: Decimal | None
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    last_price_change: dt.datetime | None
    last_verified: dt.datetime | None
    last_trade: dt.datetime | None
    deferred_deltas: int = 0
    quote: Quote | None = None

    @property
    def valid(self) -> bool:
        return self.validity is QuoteValidity.VALID

    @property
    def midpoint(self) -> Decimal | None:
        if not self.valid or self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> Decimal | None:
        if not self.valid or self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid

    @property
    def depth(self) -> Decimal | None:
        """Observed top-of-book size, or ``None`` when neither side is observed."""
        sizes = [size for size in (self.best_bid_size, self.best_ask_size) if size is not None]
        if not sizes:
            return None
        return sum(sizes, Decimal(0))

    @property
    def best_bid_size(self) -> Decimal | None:
        if not self.bids:
            return None
        return max(self.bids)[1]

    @property
    def best_ask_size(self) -> Decimal | None:
        if not self.asks:
            return None
        return min(self.asks)[1]


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Outcome of one replay fold.

    ``quotes`` is one quote per event that produced or revised a book, in the
    fold's order. Every quote carries ``replay_order`` naming the fold that
    produced it, so a panel row can never be mistaken for the other fold.
    ``gaps`` records every detected discontinuity, and ``coverage`` summarises
    what the fold actually saw.
    """

    order: str
    quotes: tuple[Quote, ...]
    gaps: tuple[Mapping[str, Any], ...]
    coverage: Mapping[str, Any]

    @property
    def replay_order(self) -> str:
        return self.order

    @property
    def valid_quotes(self) -> tuple[Quote, ...]:
        return tuple(quote for quote in self.quotes if quote.valid)

    def quotes_for(self, venue: str, contract_id: str) -> tuple[Quote, ...]:
        key = market_key(venue, contract_id)
        return tuple(quote for quote in self.quotes if quote.market_key == key)


def _occurrence_key(record: BookEvent | Trade) -> tuple[str, str, str, str, str]:
    """Deduplication identity: record class plus venue plus occurrence identity.

    One delivered occurrence may normalize into one book event and one trade, so
    the record class is part of the key. Two records sharing this key are the
    same occurrence delivered twice, not two occurrences.
    """
    provenance = record.provenance
    return (type(record).__name__, record.venue, *provenance.occurrence_key)


def _content_key(event: BookEvent) -> tuple[Any, ...]:
    return (
        event.kind,
        event.connection_id,
        event.sequence_scope,
        event.sequence,
        event.operation,
        event.side,
        event.price,
        event.size,
        event.bids,
        event.asks,
    )


def _order_time(event: BookEvent | Trade, order: str) -> dt.datetime | None:
    return event.clock.source_time if order == ORDER_SOURCE else event.clock.usable_time


def _sort_key(event: BookEvent | Trade, order: str) -> tuple:
    primary = _order_time(event, order)
    primary_missing = 1 if primary is None else 0
    primary_value = 0.0 if primary is None else primary.timestamp()
    fallback = event.clock.usable_time if order == ORDER_SOURCE else event.clock.source_time
    fallback_missing = 1 if fallback is None else 0
    fallback_value = 0.0 if fallback is None else fallback.timestamp()
    return (
        primary_missing,
        primary_value,
        fallback_missing,
        fallback_value,
        type(event).__name__,
        event.venue,
        event.contract_id,
        event.provenance.source,
        event.provenance.record_id,
        event.provenance.raw_hash,
    )


def canonicalize_events(
    events: Iterable[BookEvent | Trade], *, order: str
) -> tuple[list[BookEvent | Trade], list[Mapping[str, Any]]]:
    """Deduplicate and deterministically order a raw event stream.

    Returns the ordered stream and a list of issues found while canonicalizing.
    Deduplication is by occurrence identity, never by payload content: two
    genuinely repeated identical messages carrying different occurrence
    identities both survive, and a single occurrence delivered twice is dropped
    once. Events with no time on the requested axis are kept and reported, then
    sorted last.
    """
    resolved = resolve_order(order)
    seen: dict[tuple[str, str, str, str, str], tuple[Any, ...] | None] = {}
    issues: list[Mapping[str, Any]] = []
    unique: list[BookEvent | Trade] = []
    for record in events:
        if isinstance(record, BookEvent):
            content: tuple[Any, ...] | None = _content_key(record)
        elif isinstance(record, Trade):
            content = None
        else:
            raise TypeError(
                f"replay expects BookEvent or Trade records, got {type(record).__name__}"
            )
        key = _occurrence_key(record)
        if key in seen:
            prior = seen[key]
            if prior is not None and content is not None and prior != content:
                issues.append(
                    {
                        "kind": "occurrence_content_conflict",
                        "occurrence": list(key),
                        "detail": "one occurrence identity carried two different payloads",
                    }
                )
            issues.append(
                {
                    "kind": "duplicate_occurrence",
                    "occurrence": list(key),
                    "venue": record.venue,
                    "contract_id": record.contract_id,
                }
            )
            continue
        seen[key] = content
        unique.append(record)

    ordered = sorted(unique, key=lambda item: _sort_key(item, resolved))
    for record in ordered:
        if _order_time(record, resolved) is None:
            issues.append(
                {
                    "kind": "missing_order_time",
                    "order": resolved,
                    "venue": record.venue,
                    "contract_id": record.contract_id,
                    "occurrence": list(_occurrence_key(record)),
                }
            )
    return ordered, issues


class BookState:
    """Mutable reconstruction state over many books and streams.

    :param order: ``'source'`` for economic event studies over source time, or
        ``'usable'`` for information-feasible ordering over usable time.
    """

    __slots__ = (
        "books",
        "gaps",
        "issues",
        "latest",
        "order",
        "processed",
        "quotes",
        "scopes",
        "states",
        "trades",
    )

    def __init__(self, order: str = ORDER_USABLE) -> None:
        self.order = resolve_order(order)
        self.scopes: dict[tuple[str, str, str], _Scope] = {}
        self.books: dict[BookKey, _Book] = {}
        self.quotes: list[Quote] = []
        self.states: list[BookSnapshot] = []
        self.latest: dict[BookKey, Quote] = {}
        self.trades: list[Trade] = []
        self.issues: list[Mapping[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.processed = 0

    def scope(self, event: BookEvent) -> _Scope:
        key = event.scope_key
        scope = self.scopes.get(key)
        if scope is None:
            scope = _Scope(*key)
            self.scopes[key] = scope
        return scope

    def book(self, event: BookEvent) -> _Book:
        key = BookKey.of(event)
        book = self.books.get(key)
        if book is None:
            book = _Book(key=key)
            book.snapshot_generation = None
            self.books[key] = book
        return book

    def books_for(self, venue: str, contract_id: str) -> tuple[_Book, ...]:
        key = market_key(venue, contract_id)
        return tuple(book for book_key, book in self.books.items() if book_key.market_key == key)

    def quote_series(self, venue: str, contract_id: str) -> tuple[Quote, ...]:
        """Every quote emitted for one market, in reconstruction order.

        This is the book's as-of history. Its latest entry is the state the book
        ended in, including an invalid one, which is what stops a gap or closure
        from being read as an older valid quote.
        """
        return tuple(
            quote
            for quote in self.quotes
            if quote.venue == venue and quote.contract_id == contract_id
        )

    def state_series(self, venue: str, contract_id: str) -> tuple[BookSnapshot, ...]:
        """Reconstructed state after each emitted event, for one market."""
        key = market_key(venue, contract_id)
        return tuple(state for state in self.states if state.market_key == key)

    @property
    def snapshots(self) -> tuple[BookSnapshot, ...]:
        """Reconstructed book state after every emitted event, in order."""
        return tuple(self.states)

    def flatten(self) -> list[Mapping[str, Any]]:
        """State and quote for every emitted event, in the fold's order.

        Ordered by the fold's own order time, with the occurrence identity as the
        tie-break, so the sequence is a function of the fold rather than of the
        order in which callers happened to append.
        """
        paired = sorted(
            zip(self.states, self.quotes, strict=True),
            key=lambda pair: (
                pair[0].order_time is None,
                pair[0].order_time or dt.datetime.min.replace(tzinfo=UTC),
                pair[0].venue,
                pair[0].contract_id,
                pair[1].provenance.source,
                pair[1].provenance.record_id,
                pair[1].provenance.raw_hash,
            ),
        )
        return [
            {
                "market_key": state.market_key,
                "venue": state.venue,
                "contract_id": state.contract_id,
                "connection_id": state.connection_id,
                "sequence_scope": state.sequence_scope,
                "order_time": state.order_time,
                "validity": state.validity.value,
                "bid": state.bid,
                "ask": state.ask,
                "bids": state.bids,
                "asks": state.asks,
                "last_price_change": state.last_price_change,
                "last_verified": state.last_verified,
                "last_trade": state.last_trade,
                "deferred_deltas": state.deferred_deltas,
                "quote": quote,
            }
            for state, quote in paired
        ]

    def valid_market_keys(self) -> tuple[str, ...]:
        """Market keys with at least one currently valid book, sorted."""
        keys = {
            book.key.market_key
            for book in self.books.values()
            if book.validity(self.scopes[book.key.scope_key]) is QuoteValidity.VALID
        }
        return tuple(sorted(keys))

    def _invalidate_scope(
        self, scope: _Scope, *, reason: QuoteValidity, gap: Mapping[str, Any]
    ) -> None:
        """Mark every book on a scope invalid and record the gap once."""
        gap_record = dict(gap)
        affected = sorted(
            book.key.market_key for book in self.books.values() if book.key.scope_key == scope.key
        )
        gap_record["affected_markets"] = affected
        gap_record["recovered_markets"] = []
        self.gaps.append(gap_record)
        for book in self.books.values():
            if book.key.scope_key != scope.key:
                continue
            if book.snapshot_generation != scope.generation:
                book.pending = reason

    def _scope_gap(
        self,
        scope: _Scope,
        *,
        kind: str,
        previous: int | None,
        observed: int | None,
        detail: str,
        at: dt.datetime | None,
    ) -> None:
        missing = 0
        if previous is not None and observed is not None and observed > previous:
            missing = observed - previous - 1
        scope.generation += 1
        scope.gap_open = True
        self._invalidate_scope(
            scope,
            reason=QuoteValidity.GAP,
            gap={
                "kind": kind,
                "scope": list(scope.key),
                "previous_sequence": previous,
                "observed_sequence": observed,
                "missing_count": missing,
                "detail": detail,
                "detected_at": at,
                "recovered_at": None,
            },
        )
        self.issues.append(
            {
                "kind": kind,
                "scope": list(scope.key),
                "previous_sequence": previous,
                "observed_sequence": observed,
                "missing_count": missing,
            }
        )

    def _apply_sequence(self, event: BookEvent, book: _Book) -> bool:
        """Return whether the event may modify levels, advancing cursors as needed."""
        scope = self.scope(event)
        at = event.clock.usable_time or event.clock.source_time
        sequence = event.sequence

        if sequence is None:
            if event.kind is BookKind.SNAPSHOT:
                # A snapshot always establishes the book. It is the only message
                # that may, which is why an unscoped stream stays reconstructible.
                return True
            if book.snapshot_generation != scope.generation:
                book.pending = (
                    QuoteValidity.DISCONNECTED
                    if scope.disconnected
                    else (QuoteValidity.GAP if scope.gap_open else QuoteValidity.AWAITING_SNAPSHOT)
                )
            return book.snapshot_generation == scope.generation

        if event.kind is BookKind.SNAPSHOT:
            # A snapshot is authoritative: it re-establishes the book whatever
            # the cursor says, because it is the only message that can restore a
            # reconstruction after a gap or a reconnect. An observable sequence
            # jump is still reported.
            if scope.last_sequence is not None and sequence > scope.last_sequence + 1:
                self._scope_gap(
                    scope,
                    kind="sequence_gap",
                    previous=scope.last_sequence,
                    observed=sequence,
                    detail="messages are missing before this snapshot",
                    at=at,
                )
            if scope.last_sequence is None or sequence > scope.last_sequence:
                scope.last_sequence = sequence
            book.sequence = sequence
            return True

        if scope.last_sequence is None:
            self._scope_gap(
                scope,
                kind="sequence_baseline_missing",
                previous=None,
                observed=sequence,
                detail=(
                    "first message on this stream carries a sequence without a preceding "
                    "snapshot; the stream start is unverified"
                ),
                at=at,
            )
            scope.last_sequence = sequence
            return False

        if sequence > scope.last_sequence:
            if sequence > scope.last_sequence + 1:
                self._scope_gap(
                    scope,
                    kind="sequence_gap",
                    previous=scope.last_sequence,
                    observed=sequence,
                    detail=(
                        "messages are missing from this stream; rebuild requires a fresh snapshot"
                    ),
                    at=at,
                )
            scope.last_sequence = sequence
            book.sequence = sequence
            return True

        if sequence == scope.last_sequence:
            if book.sequence is not None and book.sequence >= sequence:
                self.issues.append(
                    {
                        "kind": "duplicate_sequence",
                        "scope": list(scope.key),
                        "sequence": sequence,
                        "market_key": book.key.market_key,
                    }
                )
                return False
            book.sequence = sequence
            return True

        self.issues.append(
            {
                "kind": "stale_sequence",
                "scope": list(scope.key),
                "sequence": sequence,
                "last_sequence": scope.last_sequence,
                "market_key": book.key.market_key,
                "detail": "a message older than this book's cursor cannot advance its levels",
            }
        )
        return False

    def _emit(
        self,
        book: _Book,
        event: BookEvent,
        scope: _Scope,
        *,
        verified_at: dt.datetime | None,
    ) -> Quote:
        if verified_at is not None:
            book.last_verified = verified_at
        validity = book.validity(scope)
        book.provenance = event.provenance
        quote = Quote(
            venue=book.key.venue,
            contract_id=book.key.contract_id,
            clock=event.clock,
            provenance=event.provenance,
            bid=book.best_bid(),
            ask=book.best_ask(),
            bid_size=book.bids.get(book.best_bid()) if book.bids else None,
            ask_size=book.asks.get(book.best_ask()) if book.asks else None,
            validity=validity,
            last_price_change=book.last_price_change,
            last_verified=book.last_verified,
            last_trade=book.last_trade,
            replay_order=self.order,
        )
        series = self.quotes
        series.append(quote)
        self.latest[book.key] = quote
        self.states.append(
            BookSnapshot(
                market_key=book.key.market_key,
                venue=book.key.venue,
                contract_id=book.key.contract_id,
                connection_id=book.key.connection_id,
                sequence_scope=book.key.sequence_scope,
                order_time=quote.order_time,
                validity=validity,
                bid=quote.bid,
                ask=quote.ask,
                bids=tuple(sorted(book.bids.items())),
                asks=tuple(sorted(book.asks.items())),
                last_price_change=book.last_price_change,
                last_verified=book.last_verified,
                last_trade=book.last_trade,
                deferred_deltas=book.deferred_deltas,
                quote=quote,
            )
        )
        return quote

    def apply(self, event: BookEvent) -> Quote | None:
        """Apply one book event and return the quote it produced, if any."""
        scope = self.scope(event)
        scope.event_count += 1
        book = self.book(event)
        at = event.clock.usable_time or event.clock.source_time
        self.processed += 1

        if event.kind is BookKind.DISCONNECT:
            scope.disconnected = True
            scope.generation += 1
            book.pending = QuoteValidity.DISCONNECTED
            self._invalidate_scope(
                scope,
                reason=QuoteValidity.DISCONNECTED,
                gap={
                    "kind": "disconnect",
                    "scope": list(scope.key),
                    "previous_sequence": scope.last_sequence,
                    "observed_sequence": None,
                    "missing_count": 0,
                    "detail": "the connection feeding this stream dropped",
                    "detected_at": at,
                    "recovered_at": None,
                },
            )
            # Emitting an invalid quote here is what stops as-of selection from
            # reading the last pre-disconnect book as if the market were live.
            return self._emit(book, event, scope, verified_at=None)

        if event.kind is BookKind.CLOSE:
            if event.sequence is not None:
                self._apply_sequence(event, book)
            book.closed = True
            return self._emit(book, event, scope, verified_at=at)

        if event.kind is BookKind.HALT:
            if event.sequence is not None:
                self._apply_sequence(event, book)
            book.halted = True
            return self._emit(book, event, scope, verified_at=at)

        if event.kind is BookKind.SNAPSHOT:
            allowed = self._apply_sequence(event, book)
            if not allowed:
                book.rejected_events += 1
                return self._emit(book, event, scope, verified_at=None)
            previous_bid, previous_ask = book.best_bid(), book.best_ask()
            book.bids = dict(event.bids)
            book.asks = dict(event.asks)
            book.deferred_deltas = 0
            book.snapshot_generation = scope.generation
            book.pending = None
            book.halted = False
            scope.disconnected = False
            scope.gap_open = False
            book.applied_events += 1
            if previous_bid != book.best_bid() or previous_ask != book.best_ask():
                if book.last_price_change is None or at is None:
                    book.last_price_change = at if at is not None else book.last_price_change
                elif at > book.last_price_change:
                    book.last_price_change = at
            return self._emit(book, event, scope, verified_at=at)

        allowed = self._apply_sequence(event, book)
        if not allowed:
            book.rejected_events += 1
            book.deferred_deltas += 1
            return self._emit(book, event, scope, verified_at=None)

        levels = book.bids if event.side is BookSide.BID else book.asks
        if event.operation is BookOperation.DELETE:
            changed = levels.pop(event.price, None) is not None
        elif event.operation is BookOperation.REPLACE:
            previous = levels.get(event.price)
            changed = previous != event.size
            levels[event.price] = event.size  # type: ignore[assignment]
        else:
            previous = levels.get(event.price, Decimal(0))
            updated = previous + event.size  # type: ignore[operator]
            if updated <= 0:
                levels.pop(event.price, None)
            else:
                levels[event.price] = updated
            changed = updated != previous
        book.applied_events += 1
        if changed:
            book.last_price_change = at
        return self._emit(book, event, scope, verified_at=at)

    def record_trade(self, trade: Trade) -> None:
        """Record a trade print.

        A trade never sets a bid or ask. It contributes only ``last_trade``, so a
        print alone cannot turn into a quote.
        """
        at = trade.clock.usable_time or trade.clock.source_time
        self.trades.append(trade)
        for book in self.books_for(trade.venue, trade.contract_id):
            if book.last_trade is None or (at is not None and at > book.last_trade):
                book.last_trade = at
        self.processed += 1


def apply_book_event(state: BookState, event: BookEvent) -> BookState:
    """Apply one event to a caller-owned state and return that same state."""
    if not isinstance(state, BookState):
        raise TypeError(f"state must be a BookState, got {type(state).__name__}")
    if not isinstance(event, BookEvent):
        raise TypeError(f"event must be a BookEvent, got {type(event).__name__}")
    state.apply(event)
    return state


def _scope_summary(state: BookState) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for scope in sorted(state.scopes.values(), key=lambda s: s.key):
        books = [book for book in state.books.values() if book.key.scope_key == scope.key]
        out.append(
            {
                "scope": list(scope.key),
                "connection_id": scope.connection_id,
                "sequence_scope": scope.sequence_scope,
                "last_sequence": scope.last_sequence,
                "generation": scope.generation,
                "disconnected": scope.disconnected,
                "gap_open": scope.gap_open,
                "events": scope.event_count,
                "contracts": sorted({book.key.contract_id for book in books}),
                "valid_markets": sorted(
                    book.key.market_key
                    for book in books
                    if book.validity(scope) is QuoteValidity.VALID
                ),
            }
        )
    return out


def _book_summary(state: BookState) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for key in sorted(
        state.books, key=lambda k: (k.venue, k.contract_id, k.connection_id, k.sequence_scope)
    ):
        book = state.books[key]
        scope = state.scopes[key.scope_key]
        quote = state.latest.get(key)
        valid = book.validity(scope) is QuoteValidity.VALID
        out.append(
            {
                "market_key": key.market_key,
                "venue": key.venue,
                "contract_id": key.contract_id,
                "connection_id": key.connection_id,
                "sequence_scope": key.sequence_scope,
                "validity": book.validity(scope).value,
                "bid": book.best_bid() if valid else None,
                "ask": book.best_ask() if valid else None,
                "bid_levels": len(book.bids),
                "ask_levels": len(book.asks),
                "last_sequence": book.sequence,
                "snapshot_generation": book.snapshot_generation,
                "scope_generation": scope.generation,
                "deferred_deltas": book.deferred_deltas,
                "applied_events": book.applied_events,
                "rejected_events": book.rejected_events,
                "last_price_change": book.last_price_change,
                "last_verified": book.last_verified,
                "last_trade": book.last_trade,
                "quote": quote,
            }
        )
    return out


def _build_result(
    state: BookState,
    ordered: Sequence[BookEvent | Trade],
    canonicalization_issues: Sequence[Mapping[str, Any]],
) -> ReplayResult:
    valid = state.valid_market_keys()
    all_known = sorted({book.key.market_key for book in state.books.values()})
    order_times = [
        time
        for time in (_order_time(record, state.order) for record in ordered)
        if time is not None
    ]
    source_times = [
        record.clock.source_time for record in ordered if record.clock.source_time is not None
    ]
    gaps = [dict(gap) for gap in state.gaps]
    lasting = {summary["market_key"]: summary for summary in _book_summary(state)}
    for gap in gaps:
        recovered = [
            key
            for key in gap["affected_markets"]
            if lasting.get(key, {}).get("validity") == QuoteValidity.VALID.value
            and lasting[key]["snapshot_generation"] == lasting[key]["scope_generation"]
        ]
        gap["recovered_markets"] = sorted(recovered)
        if recovered:
            # Recovery is the first moment the book was valid again *and still
            # valid at the end of this fold*. An interim snapshot that a later
            # event invalidated is not a recovery, and a fold that never saw the
            # restoring snapshot must not claim one.
            for snapshot in state.snapshots:
                if snapshot.market_key not in recovered or not snapshot.valid:
                    continue
                if lasting[snapshot.market_key]["validity"] != QuoteValidity.VALID.value:
                    continue
                if snapshot.order_time is None:
                    continue
                detected = gap["detected_at"]
                if detected is not None and snapshot.order_time < detected:
                    continue
                if gap["recovered_at"] is None or snapshot.order_time < gap["recovered_at"]:
                    gap["recovered_at"] = snapshot.order_time

    coverage = {
        "order": state.order,
        "processed_records": len(ordered),
        "quote_count": len(state.quotes),
        "trade_count": len(state.trades),
        "book_count": len(state.books),
        "scope_count": len(state.scopes),
        "sources": sorted({record.provenance.source for record in ordered}),
        "venues": sorted({record.venue for record in ordered}),
        "first_order_time": min(order_times) if order_times else None,
        "last_order_time": max(order_times) if order_times else None,
        "first_source_time": min(source_times) if source_times else None,
        "last_source_time": max(source_times) if source_times else None,
        "gap_count": len(gaps),
        "open_gap_count": sum(
            1
            for gap in gaps
            if gap["kind"] in ("sequence_gap", "sequence_baseline_missing", "disconnect")
            and not gap["recovered_markets"]
        ),
        "all_markets_valid": bool(all_known) and set(valid) == set(all_known),
        "valid_market_keys": list(valid),
        "issued_market_keys": all_known,
        "books": _book_summary(state),
        "scopes": _scope_summary(state),
        "issues": [dict(issue) for issue in canonicalization_issues]
        + [dict(issue) for issue in state.issues],
    }
    emitted = sorted(
        state.quotes,
        key=lambda quote: (
            quote.order_time is None,
            quote.order_time or dt.datetime.min.replace(tzinfo=UTC),
            quote.venue,
            quote.contract_id,
            quote.provenance.source,
            quote.provenance.record_id,
            quote.provenance.raw_hash,
        ),
    )
    return ReplayResult(
        order=state.order,
        quotes=tuple(emitted),
        gaps=tuple(gaps),
        coverage=coverage,
    )


def replay(events: Iterable[BookEvent | Trade], *, order: str) -> ReplayResult:
    """Replay a stream into books under one ordering.

    ``order='source'`` orders by source time, for economic event studies.
    ``order='usable'`` orders by usable time, for information-feasible
    prediction. The two folds are independent; neither is a sorted copy of the
    other.
    """
    resolved = resolve_order(order)
    ordered, issues = canonicalize_events(events, order=resolved)
    state = BookState(resolved)
    for record in ordered:
        if isinstance(record, BookEvent):
            state.apply(record)
        else:
            state.record_trade(record)
    return _build_result(state, ordered, issues)


def _per_occurrence(
    result: ReplayResult,
) -> tuple[dict[tuple[str, str, str], Quote], dict[tuple[str, str, str], int]]:
    """Occurrence identity to quote, plus its position in this fold."""
    positions: dict[tuple[str, str, str], Quote] = {}
    for quote in result.quotes:
        positions[quote.occurrence_key] = quote
    ranks = {key: position for position, key in enumerate(positions)}
    return positions, ranks


def _final_state(result: ReplayResult) -> dict[str, Mapping[str, Any]]:
    state: dict[str, Mapping[str, Any]] = {}
    for summary in result.coverage["books"]:
        state[summary["market_key"]] = {
            "validity": summary["validity"],
            "bid": summary["bid"],
            "ask": summary["ask"],
            "snapshot_generation": summary["snapshot_generation"],
            "scope_generation": summary["scope_generation"],
        }
    return state


def compare_replay_orders(events: Iterable[BookEvent | Trade]) -> dict[str, Any]:
    """Replay the same stream under both orders and report every disagreement.

    Two independent folds are executed. Sorting one fold's output is not an
    equivalent computation: a fold's own as-of state depends on the order in
    which its book was built, so the comparison must be between two builds.

    Reported: inversions between the two orderings, per-occurrence state
    disagreements for markets seen in both folds, final-state disagreements, and
    coverage differences.
    """
    materialized = list(events)
    source_result = replay(materialized, order=ORDER_SOURCE)
    usable_result = replay(materialized, order=ORDER_USABLE)

    source_positions, source_ranks = _per_occurrence(source_result)
    usable_positions, usable_ranks = _per_occurrence(usable_result)

    shared = sorted(set(source_ranks) & set(usable_ranks))
    inversions: list[Mapping[str, Any]] = []
    inversion_count = 0
    for index, left in enumerate(shared):
        for right in shared[index + 1 :]:
            source_sign = source_ranks[left] - source_ranks[right]
            usable_sign = usable_ranks[left] - usable_ranks[right]
            if (source_sign > 0) != (usable_sign > 0):
                inversion_count += 1
                if len(inversions) < _MAX_REPORTED_INVERSIONS:
                    inversions.append(
                        {
                            "left": list(left),
                            "right": list(right),
                            "source_order": "left_first" if source_sign < 0 else "right_first",
                            "usable_order": "left_first" if usable_sign < 0 else "right_first",
                        }
                    )

    state_disagreements: list[Mapping[str, Any]] = []
    for occurrence in shared:
        left_quote = source_positions[occurrence]
        right_quote = usable_positions[occurrence]
        if (
            left_quote.validity is not right_quote.validity
            or left_quote.bid != right_quote.bid
            or left_quote.ask != right_quote.ask
        ):
            state_disagreements.append(
                {
                    "occurrence": list(occurrence),
                    "market_key": left_quote.market_key,
                    "source": {
                        "validity": left_quote.validity.value,
                        "bid": left_quote.bid,
                        "ask": left_quote.ask,
                    },
                    "usable": {
                        "validity": right_quote.validity.value,
                        "bid": right_quote.bid,
                        "ask": right_quote.ask,
                    },
                }
            )

    source_final = _final_state(source_result)
    usable_final = _final_state(usable_result)
    final_disagreements: list[Mapping[str, Any]] = []
    for market in sorted(set(source_final) | set(usable_final)):
        left = source_final.get(market)
        right = usable_final.get(market)
        if left != right:
            final_disagreements.append({"market_key": market, "source": left, "usable": right})

    gap_kinds = {
        "source": sorted(gap["kind"] for gap in source_result.gaps),
        "usable": sorted(gap["kind"] for gap in usable_result.gaps),
    }

    return {
        "source_order": ORDER_SOURCE,
        "usable_order": ORDER_USABLE,
        "shared_occurrences": len(shared),
        "source_only_occurrences": sorted(set(source_ranks) - set(usable_ranks)),
        "usable_only_occurrences": sorted(set(usable_ranks) - set(source_ranks)),
        "inversion_count": inversion_count,
        "inversions": inversions,
        "inversions_truncated": inversion_count > len(inversions),
        "state_disagreement_count": len(state_disagreements),
        "state_disagreements": state_disagreements,
        "final_state_disagreement_count": len(final_disagreements),
        "final_state_disagreements": final_disagreements,
        "gaps": gap_kinds,
        "coverage": {
            "source": {
                "quote_count": source_result.coverage["quote_count"],
                "trade_count": source_result.coverage["trade_count"],
                "gap_count": source_result.coverage["gap_count"],
                "all_markets_valid": source_result.coverage["all_markets_valid"],
            },
            "usable": {
                "quote_count": usable_result.coverage["quote_count"],
                "trade_count": usable_result.coverage["trade_count"],
                "gap_count": usable_result.coverage["gap_count"],
                "all_markets_valid": usable_result.coverage["all_markets_valid"],
            },
        },
        "agreement": not (
            inversion_count
            or state_disagreements
            or final_disagreements
            or gap_kinds["source"] != gap_kinds["usable"]
            or source_result.coverage["quote_count"] != usable_result.coverage["quote_count"]
        ),
    }
