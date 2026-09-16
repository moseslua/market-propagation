"""Frozen domain records, identity keys, and boundary validation.

This module owns the stable types that every other layer consumes: parsed and
normalized records on one side, point-in-time records on the other. It holds no
I/O and no policy beyond what the frozen contract states.

Two usability rules apply to every bound in this module.

* ``None`` means *missing*. It never means zero, and it never means "empty".
* A validity or reason code states why a record is not usable. Records are not
  silently dropped; they carry the reason they cannot be used.

Time discipline
---------------
Physical times are timezone-aware :class:`datetime.datetime`. ``monotonic_ns``
is an ``int`` and only ever the value a monotonic clock actually returned; it is
not a nanosecond claim about a source. Source time never becomes usable time by
default: :attr:`Clock.usable_time` is derived from
:attr:`Clock.availability`, and a record without receipt information has an
unknown availability interval.

Identity keys
-------------
``record_id`` on :class:`Provenance` is occurrence identity for a covered
occurrence. For an occurrence the feed did not identify, it is a generated
identifier that stays distinct for repeated identical payloads; it is never the
payload content hash. Source identifiers are venue-local, so every identity key
in this package is venue-qualified. A market key is::

    market_key(venue, contract_id) == f"{venue}|{contract_id}"

Use :func:`market_key` and :func:`split_market_key` rather than formatting the
string yourself.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import ClassVar
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

__all__ = [
    "HISTORICAL_PRICE_PRECISION",
    "HISTORICAL_RAW_PRICE_UNITS",
    "HISTORICAL_SIZE_QUALITIES",
    "MARKET_KEY_SEPARATOR",
    "UTC",
    "Availability",
    "BookKind",
    "BookOperation",
    "BookSide",
    "Clock",
    "Contract",
    "ContractRelation",
    "Expectation",
    "HistoricalTrade",
    "Operator",
    "Provenance",
    "Quote",
    "QuoteValidity",
    "RelationKind",
    "Release",
    "Resolution",
    "Rounding",
    "Trade",
    "classify_local_time",
    "market_key",
    "parse_decimal",
    "parse_local_time",
    "parse_utc_time",
    "split_market_key",
]

UTC = dt.UTC

MARKET_KEY_SEPARATOR = "|"

_MAX_DECIMAL_EXPONENT_DIGITS = 6


def market_key(venue: str, contract_id: str) -> str:
    """Venue-qualified market identity used by feature, coverage and quote maps."""
    if not venue or not contract_id:
        raise ValueError("market_key requires a non-empty venue and contract_id")
    if MARKET_KEY_SEPARATOR in venue or MARKET_KEY_SEPARATOR in contract_id:
        raise ValueError(
            f"venue and contract_id must not contain {MARKET_KEY_SEPARATOR!r}: "
            f"{venue!r}, {contract_id!r}"
        )
    return f"{venue}{MARKET_KEY_SEPARATOR}{contract_id}"


def split_market_key(key: str) -> tuple[str, str]:
    """Inverse of :func:`market_key`; returns ``(venue, contract_id)``."""
    venue, separator, contract_id = key.partition(MARKET_KEY_SEPARATOR)
    if not separator or not venue or not contract_id:
        raise ValueError(f"not a market key: {key!r}")
    return venue, contract_id


def parse_decimal(value: object, *, field_name: str) -> Decimal:
    """Convert one ingested value to an exact :class:`~decimal.Decimal`.

    Accepts ``Decimal``, ``int``, ``str`` and ``float``. A float is converted
    through ``repr`` so the intended decimal survives a JSON round trip;
    ingestion that wants exactness from JSON should still pass
    ``json.loads(text, parse_float=Decimal)``. Bools, non-finite values and
    strings such as ``"nan"`` are rejected: :class:`Decimal` accepts those
    spellings, and admitting them would put a non-number into a price.
    """
    if isinstance(value, bool):
        raise ValueError(f"{field_name}: bool is not a decimal value: {value!r}")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name}: empty string is not a decimal value")
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?", text):
            raise ValueError(f"{field_name}: not a decimal literal: {value!r}")
        result = Decimal(text)
    elif isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError(f"{field_name}: non-finite float is not a decimal value")
        result = Decimal(repr(value))
    else:
        raise TypeError(
            f"{field_name}: expected Decimal, int, float or str, got {type(value).__name__}"
        )
    if not result.is_finite():
        raise ValueError(f"{field_name}: non-finite decimal is not a price or quantity")
    digits = -result.as_tuple().exponent
    if digits > _MAX_DECIMAL_EXPONENT_DIGITS * 6:
        raise ValueError(f"{field_name}: implausible decimal scale in {value!r}")
    return result


def parse_utc_time(value: object, *, field_name: str) -> dt.datetime:
    """Parse an aware instant from a datetime or ISO-8601 string.

    A naive datetime and a string without an offset are rejected. There is no
    assumed default zone: which zone a wall-clock string belongs to is a
    documented property of its source, supplied through :func:`parse_local_time`.
    """
    if isinstance(value, dt.datetime):
        moment = value
    elif isinstance(value, str):
        text = value.strip()
        try:
            moment = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name}: not an ISO-8601 timestamp: {value!r}") from exc
    else:
        raise TypeError(f"{field_name}: expected datetime or str, got {type(value).__name__}")
    if moment.tzinfo is None or moment.tzinfo.utcoffset(moment) is None:
        raise ValueError(
            f"{field_name}: naive timestamp {value!r}; an explicit UTC offset is required"
        )
    return moment.astimezone(UTC)


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(f"unknown IANA timezone {name!r}") from exc


def classify_local_time(value: str, timezone: str) -> str:
    """Classify a naive local wall-clock string as ``unique``, ``ambiguous`` or ``nonexistent``.

    ``ambiguous`` is the fall-back hour, where one wall clock reading happens
    twice. ``nonexistent`` is the spring-forward gap, where the reading never
    happens. Both need a documented decision; neither may be resolved by
    attaching a fixed UTC offset.
    """
    zone = _zone(timezone)
    text = value.strip()
    try:
        naive = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"not an ISO-8601 local timestamp: {value!r}") from exc
    if naive.tzinfo is not None:
        raise ValueError(
            f"local timestamp {value!r} already carries an offset; pass the wall-clock form"
        )
    first = naive.replace(tzinfo=zone, fold=0)
    second = naive.replace(tzinfo=zone, fold=1)
    if first.utcoffset() != second.utcoffset():
        if first.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != naive:
            return "nonexistent"
        return "ambiguous"
    if first.astimezone(UTC).astimezone(zone).replace(tzinfo=None) != naive:
        return "nonexistent"
    return "unique"


def parse_local_time(
    value: str | dt.datetime, timezone: str, *, fold: int | None = None
) -> dt.datetime:
    """Resolve a documented local time to its unambiguous UTC instant.

    An already-aware datetime is treated as an instant and normalized to UTC.
    A naive wall-clock string is attached to ``timezone`` after checking it
    exists and is unique:

    * a nonexistent spring-forward reading is rejected;
    * an ambiguous fall-back reading is rejected unless ``fold`` (0 for the
      first pass, 1 for the second) is given explicitly.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None:
            if fold is not None:
                raise ValueError("fold applies only to a naive local wall-clock time")
            return value.astimezone(UTC)
        naive = value
        text = naive.isoformat()
    elif isinstance(value, str):
        text = value.strip()
    else:
        raise TypeError(f"expected datetime or str, got {type(value).__name__}")

    kind = classify_local_time(text, timezone)
    if kind == "nonexistent":
        raise ValueError(
            f"local time {value!r} does not exist in {timezone} (spring-forward gap); "
            "the source must document how it resolves this reading"
        )
    if kind == "ambiguous" and fold is None:
        raise ValueError(
            f"local time {value!r} is ambiguous in {timezone} (fall-back hour); "
            "pass fold=0 for the first pass or fold=1 for the second"
        )
    if fold is not None and fold not in (0, 1):
        raise ValueError(f"fold must be 0 or 1, got {fold!r}")
    naive = dt.datetime.fromisoformat(text)
    aware = naive.replace(tzinfo=_zone(timezone), fold=0 if fold is None else fold)
    return aware.astimezone(UTC)


def _aware(value: object, *, field_name: str) -> dt.datetime:
    return parse_utc_time(value, field_name=field_name)


def _optional_aware(value: object | None, *, field_name: str) -> dt.datetime | None:
    if value is None:
        return None
    return parse_utc_time(value, field_name=field_name)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name}: expected str, got {type(value).__name__}")
    if not value.strip():
        raise ValueError(f"{field_name}: must not be empty")
    return value


def _optional_text(value: object | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _optional_decimal(value: object | None, *, field_name: str) -> Decimal | None:
    if value is None:
        return None
    return parse_decimal(value, field_name=field_name)


def _coerce_enum(enum_cls: type[StrEnum], value: object, *, field_name: str):
    if isinstance(value, enum_cls):
        return value
    if isinstance(value, str):
        try:
            return enum_cls(value)
        except ValueError as exc:
            allowed = ", ".join(repr(member.value) for member in enum_cls)
            raise ValueError(f"{field_name}: {value!r} is not one of {allowed}") from exc
    raise TypeError(
        f"{field_name}: expected {enum_cls.__name__} or str, got {type(value).__name__}"
    )


def _optional_int(value: object | None, *, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name}: expected int or None, got {type(value).__name__}")
    if value < 0:
        raise ValueError(f"{field_name}: must not be negative, got {value}")
    return value


@dataclass(frozen=True, slots=True)
class Availability:
    """Interval during which a record was usable, with the basis for that claim.

    ``lower`` and ``upper`` are present together or absent together. Absence is
    explicit: an unknown interval is ``(None, None)``, never a zero-width
    interval. ``quality`` describes the clock behind the interval; ``basis``
    names the documented policy that produced it, so an interval can be traced
    to a statement rather than an assumption.
    """

    lower: dt.datetime | None
    upper: dt.datetime | None
    quality: str
    basis: str

    QUALITIES: ClassVar[tuple[str, ...]] = ("clock_synced", "clock_unsynced", "unknown")

    def __post_init__(self) -> None:
        if (self.lower is None) != (self.upper is None):
            raise ValueError(
                "Availability bounds must be present together or absent together: "
                f"lower={self.lower!r}, upper={self.upper!r}"
            )
        _require_text(self.quality, field_name="Availability.quality")
        _require_text(self.basis, field_name="Availability.basis")
        if self.lower is None:
            return
        lower = _aware(self.lower, field_name="Availability.lower")
        upper = _aware(self.upper, field_name="Availability.upper")
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)
        if lower > upper:
            raise ValueError(f"Availability lower {lower} is after upper {upper}")

    @classmethod
    def captured(
        cls,
        received_time: dt.datetime,
        uncertainty_seconds: float = 0,
        *,
        quality: str = "unknown",
        basis: str = "receipt_window",
    ) -> Availability:
        """Availability derived from a real receipt and a recorded window width.

        ``uncertainty_seconds`` is the width of the receipt window this process
        actually recorded. It is not a claim about the accuracy of any clock, so
        it does not license a ``clock_synced`` quality on its own: the offset
        between this process's clock and a source clock is measured only when a
        caller states the evidence for it through ``quality`` and ``basis``. The
        default is ``unknown`` because reading a receipt instant establishes that
        the record was observed on the local time axis, never that the clock
        behind that axis was synchronized to UTC.
        """
        received = _aware(received_time, field_name="Availability.captured.received_time")
        if isinstance(uncertainty_seconds, bool) or not isinstance(
            uncertainty_seconds, (int, float)
        ):
            raise TypeError("Availability.captured.uncertainty_seconds must be a number")
        if uncertainty_seconds < 0:
            raise ValueError(
                f"Availability.captured.uncertainty_seconds must not be negative: {uncertainty_seconds}"
            )
        width = dt.timedelta(seconds=float(uncertainty_seconds))
        return cls(received - width, received, quality, basis)

    @classmethod
    def unknown(
        cls,
        basis: str = "historical_without_receipt",
        *,
        quality: str = "unknown",
    ) -> Availability:
        """Explicit unknown availability, with the reason it is unknown."""
        return cls(None, None, quality, basis)

    @property
    def is_known(self) -> bool:
        return self.upper is not None

    @property
    def width_seconds(self) -> float | None:
        """Documented uncertainty width in seconds, or ``None`` when unknown."""
        if self.lower is None or self.upper is None:
            return None
        return (self.upper - self.lower).total_seconds()


@dataclass(frozen=True, slots=True)
class Clock:
    """Source, receipt and availability times for one observation.

    ``usable_time`` is derived and never a constructor field. A historical
    record with no receipt has an unknown availability interval, so its source
    time cannot leak into a point-in-time feature.
    """

    source_time: dt.datetime | None
    received_time: dt.datetime | None
    availability: Availability
    monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_time", _optional_aware(self.source_time, field_name="Clock.source_time")
        )
        object.__setattr__(
            self,
            "received_time",
            _optional_aware(self.received_time, field_name="Clock.received_time"),
        )
        if not isinstance(self.availability, Availability):
            raise TypeError("Clock.availability must be an Availability")
        object.__setattr__(
            self,
            "monotonic_ns",
            _optional_int(self.monotonic_ns, field_name="Clock.monotonic_ns"),
        )
        if self.availability.is_known and self.received_time is None:
            raise ValueError(
                "Clock: availability bounds require a receipt time; "
                "construct historical clocks with Clock.historical"
            )

    @classmethod
    def captured(
        cls,
        source_time: dt.datetime | None,
        received_time: dt.datetime,
        monotonic_ns: int | None = None,
        uncertainty_seconds: float = 0,
        *,
        quality: str = "unknown",
        basis: str = "receipt_window",
    ) -> Clock:
        """Clock for a prospectively observed record.

        ``monotonic_ns`` is recorded only when the collector actually read a
        monotonic clock; it is never derived from a wall clock.

        The receipt instant and the monotonic reading are genuine observations on
        the collector's own time axis, and they keep the record usable there. They
        do not certify that the axis was synchronized to UTC, that the source
        published at the receipt instant, or how long a feed took to arrive.
        ``quality`` therefore defaults to ``unknown``: a caller that has measured
        synchronization evidence, or that is constructing a known synthetic
        process, states ``clock_synced`` and the bound it measured.
        """
        return cls(
            source_time,
            received_time,
            Availability.captured(received_time, uncertainty_seconds, quality=quality, basis=basis),
            monotonic_ns,
        )

    @classmethod
    def historical(
        cls,
        source_time: dt.datetime | None,
        received_time: dt.datetime | None = None,
        *,
        uncertainty_seconds: float | None = None,
        quality: str | None = None,
        basis: str | None = None,
    ) -> Clock:
        """Clock for a record whose receipt time is historical.

        Without a receipt the availability interval stays unknown. With one, the
        interval is derived from it, but the caller must supply the documented
        uncertainty and a named basis before those bounds are treated as a
        latency claim.
        """
        if received_time is None:
            return cls(
                source_time,
                None,
                Availability.unknown(
                    basis or "historical_without_receipt", quality=quality or "unknown"
                ),
            )
        if uncertainty_seconds is None:
            raise ValueError(
                "Clock.historical with a receipt time requires uncertainty_seconds; "
                "a documented bound cannot be assumed"
            )
        return cls(
            source_time,
            received_time,
            Availability.captured(
                received_time,
                uncertainty_seconds,
                quality=quality or "clock_unsynced",
                basis=basis or "historical_documented_bound",
            ),
        )

    @property
    def usable_time(self) -> dt.datetime | None:
        """Latest time at which this record was certainly usable, or ``None``."""
        return self.availability.upper

    @property
    def timing_uncertainty_seconds(self) -> float | None:
        return self.availability.width_seconds


@dataclass(frozen=True, slots=True)
class Provenance:
    """Link from a normalized record back to its stored payload.

    ``raw_hash`` addresses the stored bytes. ``record_id`` is occurrence
    identity: stable for a covered occurrence, generated and mutually distinct
    for repeated identical payloads the feed did not identify. It is never the
    content hash of the payload.
    """

    raw_hash: str
    record_id: str
    source: str
    schema_version: str = "1"

    def __post_init__(self) -> None:
        _require_text(self.raw_hash, field_name="Provenance.raw_hash")
        _require_text(self.record_id, field_name="Provenance.record_id")
        _require_text(self.source, field_name="Provenance.source")
        _require_text(self.schema_version, field_name="Provenance.schema_version")
        if any(ch.isspace() for ch in self.raw_hash):
            raise ValueError(f"Provenance.raw_hash must not contain whitespace: {self.raw_hash!r}")

    @property
    def occurrence_key(self) -> tuple[str, str, str]:
        """Venue-independent occurrence identity: ``(source, record_id, raw_hash)``."""
        return (self.source, self.record_id, self.raw_hash)


class BookKind(StrEnum):
    SNAPSHOT = "snapshot"
    DELTA = "delta"
    DISCONNECT = "disconnect"
    HALT = "halt"
    CLOSE = "close"


class BookOperation(StrEnum):
    """How a delta changes one price level.

    ``replace`` sets the absolute size at a level; ``increment`` adds a signed
    change; ``delete`` removes the level. A depth decline is never inferred to
    be a cancellation.
    """

    REPLACE = "replace"
    INCREMENT = "increment"
    DELETE = "delete"


class BookSide(StrEnum):
    BID = "bid"
    ASK = "ask"


class QuoteValidity(StrEnum):
    """Closed validity vocabulary for a reconstructed book.

    ``valid`` means the reconstruction is live and trustworthy. It does not mean
    two-sided: an observed empty book is ``valid`` with both prices ``None``.
    ``missing`` means no snapshot has ever established the book.
    """

    VALID = "valid"
    AWAITING_SNAPSHOT = "awaiting_snapshot"
    GAP = "gap"
    DISCONNECTED = "disconnected"
    HALTED = "halted"
    CLOSED = "closed"
    CROSSED = "crossed"
    MISSING = "missing"

    @property
    def explanation(self) -> str | None:
        return {
            QuoteValidity.VALID: None,
            QuoteValidity.AWAITING_SNAPSHOT: "no snapshot has established this book yet",
            QuoteValidity.GAP: "sequence gap detected; a fresh snapshot is required",
            QuoteValidity.DISCONNECTED: "the connection carrying this book has not been re-established",
            QuoteValidity.HALTED: "the venue halted trading in this market",
            QuoteValidity.CLOSED: "the market is closed and its book is no longer updated",
            QuoteValidity.CROSSED: "best bid is at or above best ask",
            QuoteValidity.MISSING: "no book state has been observed for this market",
        }[self]


def _levels(levels: object, *, field_name: str) -> tuple[tuple[Decimal, Decimal], ...]:
    if levels is None:
        return ()
    if isinstance(levels, (str, bytes)) or not isinstance(levels, Iterable):
        raise TypeError(f"{field_name}: expected a sequence of (price, size) pairs")
    out: list[tuple[Decimal, Decimal]] = []
    seen: set[Decimal] = set()
    for item in levels:
        if isinstance(item, (str, bytes)) or not isinstance(item, Sequence) or len(item) != 2:
            raise TypeError(f"{field_name}: each level must be a (price, size) pair, got {item!r}")
        price = parse_decimal(item[0], field_name=f"{field_name}.price")
        size = parse_decimal(item[1], field_name=f"{field_name}.size")
        if price <= 0:
            raise ValueError(f"{field_name}: level price must be positive, got {price}")
        if size <= 0:
            raise ValueError(f"{field_name}: snapshot level size must be positive, got {size}")
        if price in seen:
            raise ValueError(f"{field_name}: duplicate price level {price}")
        seen.add(price)
        out.append((price, size))
    return tuple(out)


@dataclass(frozen=True, slots=True)
class BookEvent:
    """One order-book message: a snapshot, a delta, or a lifecycle message.

    ``kind`` is one of ``snapshot``, ``delta``, ``disconnect``, ``halt``,
    ``close``. A delta carries ``side``, ``price``, ``size`` and ``operation``
    (``replace``, ``increment`` or ``delete``). ``bids`` and ``asks`` belong to
    snapshots only.

    ``sequence``, when present, is a position in the venue's documented stream
    identified by ``sequence_scope``; the same scope may carry several
    contracts. Sequence gaps are never inferred across unrelated scopes, so a
    sequence without a scope is rejected.
    """

    venue: str
    contract_id: str
    kind: str
    clock: Clock
    provenance: Provenance
    connection_id: str = ""
    sequence_scope: str = ""
    sequence: int | None = None
    bids: tuple[tuple[Decimal, Decimal], ...] = ()
    asks: tuple[tuple[Decimal, Decimal], ...] = ()
    side: str | None = None
    price: Decimal | None = None
    size: Decimal | None = None
    operation: str = "replace"

    def __post_init__(self) -> None:
        _require_text(self.venue, field_name="BookEvent.venue")
        _require_text(self.contract_id, field_name="BookEvent.contract_id")
        kind = _coerce_enum(BookKind, self.kind, field_name="BookEvent.kind")
        object.__setattr__(self, "kind", kind)
        if not isinstance(self.clock, Clock):
            raise TypeError("BookEvent.clock must be a Clock")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("BookEvent.provenance must be a Provenance")
        _require_text(self.provenance.source, field_name="BookEvent.provenance.source")
        if not isinstance(self.connection_id, str) or not isinstance(self.sequence_scope, str):
            raise TypeError("BookEvent.connection_id and sequence_scope must be str")
        sequence = _optional_int(self.sequence, field_name="BookEvent.sequence")
        object.__setattr__(self, "sequence", sequence)
        if sequence is not None and not self.sequence_scope:
            raise ValueError(
                "BookEvent with a sequence requires sequence_scope: sequence numbers are "
                "positions in one documented stream, not a global counter"
            )
        bids = _levels(self.bids, field_name="BookEvent.bids")
        asks = _levels(self.asks, field_name="BookEvent.asks")
        object.__setattr__(self, "bids", bids)
        object.__setattr__(self, "asks", asks)
        operation = _coerce_enum(BookOperation, self.operation, field_name="BookEvent.operation")
        object.__setattr__(self, "operation", operation)

        if kind is BookKind.SNAPSHOT:
            if self.side is not None or self.price is not None or self.size is not None:
                raise ValueError("BookEvent snapshot must not carry side, price or size")
            return
        if kind is BookKind.DELTA:
            if self.bids or self.asks:
                raise ValueError("BookEvent delta must not carry whole-book levels")
            side = _coerce_enum(BookSide, self.side, field_name="BookEvent.side")
            object.__setattr__(self, "side", side)
            if self.price is None:
                raise ValueError("BookEvent delta requires a price")
            price = parse_decimal(self.price, field_name="BookEvent.price")
            if price <= 0:
                raise ValueError(f"BookEvent delta price must be positive, got {price}")
            object.__setattr__(self, "price", price)
            if operation is BookOperation.DELETE:
                if self.size is not None:
                    raise ValueError("BookEvent delete must not carry a size")
                return
            if self.size is None:
                raise ValueError(f"BookEvent {operation} delta requires a size")
            size = parse_decimal(self.size, field_name="BookEvent.size")
            if operation is BookOperation.REPLACE and size < 0:
                raise ValueError(
                    "BookEvent replace size must not be negative; use operation='delete' "
                    f"or an explicit increment, got {size}"
                )
            object.__setattr__(self, "size", size)
            return
        if (
            self.bids
            or self.asks
            or self.side is not None
            or self.price is not None
            or self.size is not None
        ):
            raise ValueError(f"BookEvent {kind} must not carry book levels, side, price or size")

    @property
    def market_key(self) -> str:
        return market_key(self.venue, self.contract_id)

    @property
    def scope_key(self) -> tuple[str, str, str]:
        return (self.venue, self.connection_id, self.sequence_scope)

    @property
    def occurrence_key(self) -> tuple[str, str, str]:
        return self.provenance.occurrence_key


@dataclass(frozen=True, slots=True)
class Quote:
    """A reconstructed top of book at one point in time.

    ``valid`` and ``reason`` are derived. ``midpoint`` and ``spread`` return
    ``None`` for an invalid or one-sided book. ``last_price_change``,
    ``last_verified`` and ``last_trade`` are three distinct facts: an unchanged
    standing quote is not stale, so freshness is measured against
    ``last_verified``.
    """

    venue: str
    contract_id: str
    clock: Clock
    provenance: Provenance
    bid: Decimal | None
    ask: Decimal | None
    bid_size: Decimal | None
    ask_size: Decimal | None
    validity: QuoteValidity
    last_price_change: dt.datetime | None
    last_verified: dt.datetime | None
    last_trade: dt.datetime | None
    replay_order: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.venue, field_name="Quote.venue")
        _require_text(self.contract_id, field_name="Quote.contract_id")
        if not isinstance(self.clock, Clock):
            raise TypeError("Quote.clock must be a Clock")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("Quote.provenance must be a Provenance")
        object.__setattr__(
            self,
            "validity",
            _coerce_enum(QuoteValidity, self.validity, field_name="Quote.validity"),
        )
        for name in ("bid", "ask", "bid_size", "ask_size"):
            value = getattr(self, name)
            if value is None:
                continue
            parsed = parse_decimal(value, field_name=f"Quote.{name}")
            if parsed < 0:
                raise ValueError(f"Quote.{name} must not be negative, got {parsed}")
            object.__setattr__(self, name, parsed)
        for name in ("last_price_change", "last_verified", "last_trade"):
            object.__setattr__(
                self, name, _optional_aware(getattr(self, name), field_name=f"Quote.{name}")
            )
        if self.replay_order is not None:
            _require_text(self.replay_order, field_name="Quote.replay_order")

    @property
    def valid(self) -> bool:
        return self.validity is QuoteValidity.VALID

    @property
    def reason(self) -> str | None:
        """Validity code when invalid, else ``None``.

        The human-readable form is :attr:`QuoteValidity.explanation`.
        """
        return None if self.valid else self.validity.value

    @property
    def has_both_sides(self) -> bool:
        return self.bid is not None and self.ask is not None

    @property
    def midpoint(self) -> Decimal | None:
        if not self.valid or not self.has_both_sides:
            return None
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> Decimal | None:
        if not self.valid or not self.has_both_sides:
            return None
        return self.ask - self.bid

    @property
    def depth(self) -> Decimal | None:
        """Observed top-of-book size, or ``None`` when neither side is observed."""
        sizes = [size for size in (self.bid_size, self.ask_size) if size is not None]
        if not sizes:
            return None
        return sum(sizes, Decimal(0))

    @property
    def market_key(self) -> str:
        return market_key(self.venue, self.contract_id)

    @property
    def order_time(self) -> dt.datetime | None:
        """The time this quote is ordered by in the fold that produced it.

        ``'source'`` folds order by source time, for economic event studies.
        ``'usable'`` folds order by usable time, for information-feasible
        prediction. A quote is never read on the other fold's axis, which is why
        the two orderings are genuinely different computations.
        """
        if self.replay_order == "source":
            return self.clock.source_time
        return self.clock.usable_time

    @property
    def source_time(self) -> dt.datetime | None:
        return self.clock.source_time

    @property
    def occurrence_key(self) -> tuple[str, str, str]:
        return self.provenance.occurrence_key


@dataclass(frozen=True, slots=True)
class Trade:
    """One public trade print.

    A trade price is not a quote and never becomes one: it carries no bid or ask
    and contributes only ``last_trade`` to a book.
    """

    venue: str
    contract_id: str
    trade_id: str | None
    price: Decimal
    size: Decimal
    clock: Clock
    provenance: Provenance
    aggressor: str | None = None
    is_block: bool | None = None

    def __post_init__(self) -> None:
        _require_text(self.venue, field_name="Trade.venue")
        _require_text(self.contract_id, field_name="Trade.contract_id")
        if self.trade_id is not None:
            _require_text(self.trade_id, field_name="Trade.trade_id")
        if not isinstance(self.clock, Clock):
            raise TypeError("Trade.clock must be a Clock")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("Trade.provenance must be a Provenance")
        price = parse_decimal(self.price, field_name="Trade.price")
        if price < 0:
            raise ValueError(f"Trade.price must not be negative, got {price}")
        object.__setattr__(self, "price", price)
        size = parse_decimal(self.size, field_name="Trade.size")
        if size < 0:
            raise ValueError(f"Trade.size must not be negative, got {size}")
        object.__setattr__(self, "size", size)
        if self.aggressor is not None:
            _require_text(self.aggressor, field_name="Trade.aggressor")
        if self.is_block is not None and not isinstance(self.is_block, bool):
            raise TypeError("Trade.is_block must be bool or None")

    @property
    def market_key(self) -> str:
        return market_key(self.venue, self.contract_id)


#: Units of the venue's own stored price column (``HistoricalTrade.raw_price``).
#: Kalshi archives integer cents; the cleaned Polymarket layers archive float64
#: dollars. ``price`` is always the declared payout axis in dollars per share,
#: so a venue's raw units are retained separately instead of being overwritten.
HISTORICAL_RAW_PRICE_UNITS: tuple[str, ...] = ("cents", "dollars")

#: How exactly the stored price is known. Archive layers differ: Kalshi cents are
#: exact integers, Polymarket prices are float64 and cannot be made exact.
HISTORICAL_PRICE_PRECISION: tuple[str, ...] = (
    "exact_integer_cents",
    "float64_source_precision",
)

#: Whether a row's quantity is usable. ``unavailable`` is a real state, not zero.
HISTORICAL_SIZE_QUALITIES: tuple[str, ...] = (
    "verified_source_quantity",
    "unavailable_in_cleaned_layer",
    "ambiguous_join",
    "zero_price_row",
)


@dataclass(frozen=True, slots=True)
class HistoricalTrade:
    """One trade print read from an external historical archive.

    This is the bounded external-path record. It exists beside
    :class:`Trade` rather than replacing it because the two carry different
    guarantees. ``Trade`` requires a verified non-null ``size``, which is the
    contract ``trades`` storage and quantity-weighted flow depend on. An
    archive row cannot always honour that: the cleaned Polymarket layers omit
    token quantity, and a raw fill whose join is ambiguous has no defensible
    quantity at all.

    So ``size`` is nullable here and ``size_quality`` says why. Unknown size
    stays null through storage round trips and is excluded from weighted flow.
    It is never replaced with zero, because zero is a quantity.

    ``price`` is the declared contract/token payout axis. ``event_price`` is a
    projection onto the event axis, and ``event_axis`` documents how it was
    obtained. Both are retained so a projection never changes price semantics
    invisibly.

    No historical row receives a fabricated receipt time: ``clock`` is built
    through :meth:`Clock.historical`, so ``usable_time`` stays ``None`` unless a
    caller has receipt evidence and a documented bound.
    """

    venue: str
    contract_id: str
    price: Decimal
    raw_price_units: str
    price_precision: str
    size_quality: str
    clock: Clock
    provenance: Provenance
    token_id: str | None = None
    outcome_seq: int | None = None
    trade_id: str | None = None
    raw_price: Decimal | None = None
    secondary_price: Decimal | None = None
    event_price: Decimal | None = None
    event_axis: str | None = None
    direction: str | None = None
    event_direction: int | None = None
    size: Decimal | None = None
    flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.venue, field_name="HistoricalTrade.venue")
        _require_text(self.contract_id, field_name="HistoricalTrade.contract_id")
        if not isinstance(self.clock, Clock):
            raise TypeError("HistoricalTrade.clock must be a Clock")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("HistoricalTrade.provenance must be a Provenance")
        if self.raw_price_units not in HISTORICAL_RAW_PRICE_UNITS:
            raise ValueError(
                f"HistoricalTrade.raw_price_units must be one of "
                f"{HISTORICAL_RAW_PRICE_UNITS}, got {self.raw_price_units!r}"
            )
        if self.price_precision not in HISTORICAL_PRICE_PRECISION:
            raise ValueError(
                f"HistoricalTrade.price_precision must be one of "
                f"{HISTORICAL_PRICE_PRECISION}, got {self.price_precision!r}"
            )
        if self.size_quality not in HISTORICAL_SIZE_QUALITIES:
            raise ValueError(
                f"HistoricalTrade.size_quality must be one of "
                f"{HISTORICAL_SIZE_QUALITIES}, got {self.size_quality!r}"
            )
        price = parse_decimal(self.price, field_name="HistoricalTrade.price")
        if price < 0:
            raise ValueError(f"HistoricalTrade.price must not be negative, got {price}")
        object.__setattr__(self, "price", price)
        for name in ("raw_price", "secondary_price", "event_price"):
            value = getattr(self, name)
            if value is None:
                continue
            parsed = parse_decimal(value, field_name=f"HistoricalTrade.{name}")
            if parsed < 0:
                raise ValueError(f"HistoricalTrade.{name} must not be negative, got {parsed}")
            object.__setattr__(self, name, parsed)
        if self.size is None:
            if self.size_quality == "verified_source_quantity":
                raise ValueError(
                    "HistoricalTrade.size_quality says the quantity is verified but size "
                    "is null; an unknown size is not a verified one"
                )
        else:
            if self.size_quality != "verified_source_quantity":
                raise ValueError(
                    f"HistoricalTrade.size is set while size_quality is "
                    f"{self.size_quality!r}; a quantity is only carried when it is verified"
                )
            size = parse_decimal(self.size, field_name="HistoricalTrade.size")
            if size < 0:
                raise ValueError(f"HistoricalTrade.size must not be negative, got {size}")
            object.__setattr__(self, "size", size)
        if self.outcome_seq is not None and (
            isinstance(self.outcome_seq, bool) or not isinstance(self.outcome_seq, int)
        ):
            raise TypeError("HistoricalTrade.outcome_seq must be an int or None")
        if self.event_direction is not None and (
            isinstance(self.event_direction, bool)
            or not isinstance(self.event_direction, int)
            or self.event_direction not in (-1, 0, 1)
        ):
            raise ValueError("HistoricalTrade.event_direction must be -1, 0, 1 or None")
        if self.event_price is not None and self.event_axis is None:
            raise ValueError(
                "HistoricalTrade.event_price requires an event_axis naming the mapping"
            )
        for name in ("token_id", "trade_id", "event_axis", "direction"):
            value = getattr(self, name)
            if value is not None:
                _require_text(value, field_name=f"HistoricalTrade.{name}")
        flags = tuple(self.flags)
        for flag in flags:
            _require_text(flag, field_name="HistoricalTrade.flags entry")
        object.__setattr__(self, "flags", flags)

    @property
    def market_key(self) -> str:
        return market_key(self.venue, self.contract_id)

    @property
    def size_is_verified(self) -> bool:
        """Whether this row may enter quantity-weighted flow."""
        return self.size is not None and self.size_quality == "verified_source_quantity"

    @property
    def has_event_axis(self) -> bool:
        """Whether a documented event-axis projection exists for this row."""
        return self.event_price is not None and self.event_axis is not None


@dataclass(frozen=True, slots=True)
class Release:
    """One economic release, at its scheduled and observed publication times.

    ``values`` holds the first-release values; ``revisions`` holds values
    published later, and are separate newly released information rather than
    corrections to the initial record. ``clock`` carries the observed
    publication time.
    """

    event_id: str
    family: str
    scheduled_at: dt.datetime
    reference_period: str
    values: dict[str, Decimal]
    clock: Clock
    provenance: Provenance
    revisions: dict[str, Decimal] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text(self.event_id, field_name="Release.event_id")
        _require_text(self.family, field_name="Release.family")
        _require_text(self.reference_period, field_name="Release.reference_period")
        object.__setattr__(
            self, "scheduled_at", _aware(self.scheduled_at, field_name="Release.scheduled_at")
        )
        if not isinstance(self.clock, Clock):
            raise TypeError("Release.clock must be a Clock")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("Release.provenance must be a Provenance")
        object.__setattr__(
            self, "values", _decimal_mapping(self.values, field_name="Release.values")
        )
        object.__setattr__(
            self, "revisions", _decimal_mapping(self.revisions, field_name="Release.revisions")
        )
        if not self.values:
            raise ValueError("Release.values must not be empty; a release publishes something")

    @property
    def observed_at(self) -> dt.datetime | None:
        """Observed publication time, when the record carries one."""
        return self.clock.source_time


def _decimal_mapping(values: object, *, field_name: str) -> dict[str, Decimal]:
    if not isinstance(values, Mapping):
        raise TypeError(f"{field_name}: expected a mapping of name to decimal value")
    out: dict[str, Decimal] = {}
    for key, value in values.items():
        name = _require_text(key, field_name=f"{field_name} key")
        if name in out:
            raise ValueError(f"{field_name}: duplicate field name {name!r}")
        out[name] = parse_decimal(value, field_name=f"{field_name}[{name!r}]")
    return out


@dataclass(frozen=True, slots=True)
class Expectation:
    """A point-in-time expectation for one release statistic.

    ``source_kind`` names the admissible route that produced the value:
    a licensed point-in-time consensus, a forecast archived before the event, or
    a documented pre-release market-implied distribution. The market-implied
    route is endogenous to the market and cannot validate it.
    """

    event_id: str
    statistic: str
    value: Decimal
    source_kind: str
    clock: Clock
    provenance: Provenance
    revision_status: str = "initial"

    SOURCE_KINDS: ClassVar[tuple[str, ...]] = (
        "licensed_consensus",
        "archived_forecast",
        "market_implied",
    )
    MARKET_IMPLIED: ClassVar[str] = "market_implied"

    def __post_init__(self) -> None:
        _require_text(self.event_id, field_name="Expectation.event_id")
        _require_text(self.statistic, field_name="Expectation.statistic")
        _require_text(self.source_kind, field_name="Expectation.source_kind")
        _require_text(self.revision_status, field_name="Expectation.revision_status")
        if not isinstance(self.clock, Clock):
            raise TypeError("Expectation.clock must be a Clock")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("Expectation.provenance must be a Provenance")
        object.__setattr__(self, "value", parse_decimal(self.value, field_name="Expectation.value"))

    @property
    def is_market_implied(self) -> bool:
        return self.source_kind == self.MARKET_IMPLIED


@dataclass(frozen=True, slots=True)
class Resolution:
    """Realized payout and the time it became known.

    ``known_at`` is the time the payout became known to the study; it is what
    governs label availability. ``resolved_at`` is the venue's settlement time
    and does not make a label available. ``exceptional`` marks a payout outside
    the binary scoring contract, such as a void or partial payout.
    """

    contract_id: str
    payout: Decimal
    known_at: dt.datetime | None
    resolved_at: dt.datetime | None
    rule_hash: str
    provenance: Provenance
    exceptional: bool = False

    def __post_init__(self) -> None:
        _require_text(self.contract_id, field_name="Resolution.contract_id")
        _require_text(self.rule_hash, field_name="Resolution.rule_hash")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("Resolution.provenance must be a Provenance")
        object.__setattr__(
            self, "payout", parse_decimal(self.payout, field_name="Resolution.payout")
        )
        object.__setattr__(
            self, "known_at", _optional_aware(self.known_at, field_name="Resolution.known_at")
        )
        object.__setattr__(
            self,
            "resolved_at",
            _optional_aware(self.resolved_at, field_name="Resolution.resolved_at"),
        )
        if not isinstance(self.exceptional, bool):
            raise TypeError("Resolution.exceptional must be bool")

    @property
    def is_binary(self) -> bool:
        """Whether this payout scores as a binary claim (exactly 0 or 1, not exceptional)."""
        return not self.exceptional and self.payout in (Decimal(0), Decimal(1))

    @property
    def binary_payout(self) -> Decimal | None:
        """Payout when the contract scored as a binary claim, else ``None``.

        A fractional payout and an exceptional payout both return ``None``: a
        half payout is a different claim, not a binary resolution.
        """
        return self.payout if self.is_binary else None

    def require_binary_payout(self) -> Decimal:
        """The payout as a binary outcome, refusing anything that is not one."""
        if self.exceptional:
            raise ValueError(
                f"resolution for {self.contract_id!r} is exceptional (payout {self.payout}); "
                "binary scoring excludes it"
            )
        if self.payout not in (Decimal(0), Decimal(1)):
            raise ValueError(
                f"resolution for {self.contract_id!r} has fractional payout {self.payout}; "
                "it is not a binary outcome"
            )
        return self.payout


class Operator(StrEnum):
    """Payoff comparison operator.

    ``above`` and ``below`` are strict; ``at_least`` and ``at_most`` are not.
    ``range`` uses ``lower`` and ``upper``; ``equal`` and ``binary`` use
    ``threshold`` where the venue defines one.
    """

    ABOVE = "above"
    AT_LEAST = "at_least"
    BELOW = "below"
    AT_MOST = "at_most"
    EQUAL = "equal"
    RANGE = "range"
    BINARY = "binary"

    @property
    def is_strict(self) -> bool | None:
        """``True``/``False`` for a threshold operator, ``None`` when it does not apply."""
        return {
            Operator.ABOVE: True,
            Operator.BELOW: True,
            Operator.AT_LEAST: False,
            Operator.AT_MOST: False,
        }.get(self)

    @property
    def needs_threshold(self) -> bool:
        return self in (
            Operator.ABOVE,
            Operator.AT_LEAST,
            Operator.BELOW,
            Operator.AT_MOST,
            Operator.EQUAL,
        )

    @property
    def orientation_sign(self) -> Decimal:
        """Sign of the payoff in the threshold, ``0`` where no single sign exists."""
        return {
            Operator.ABOVE: Decimal(1),
            Operator.AT_LEAST: Decimal(1),
            Operator.BELOW: Decimal(-1),
            Operator.AT_MOST: Decimal(-1),
        }.get(self, Decimal(0))


class Rounding(StrEnum):
    NONE = "none"
    UP = "up"
    DOWN = "down"
    NEAREST = "nearest"


class RelationKind(StrEnum):
    """The three relation types, kept distinct and never mixed.

    ``logical`` is a payoff identity, ``exposure`` is a stated economic
    mechanism, ``predictive`` is a training-only lag relationship. An
    implication is not a transmission mechanism, and a correlation is not a
    payoff identity.
    """

    LOGICAL = "logical"
    EXPOSURE = "exposure"
    PREDICTIVE = "predictive"


@dataclass(frozen=True, slots=True)
class Contract:
    """Payoff definition and lifecycle for one tradeable claim.

    Every field that participates in rule matching is compared by
    :attr:`MATCH_FIELDS`. Identity fields (``venue``, ``contract_id``,
    ``event_id``) are excluded from matching because two venues listing the same
    claim necessarily disagree on them; they are differences to report, not
    reasons to reject a match.

    A field named in :attr:`UNKNOWN_WHEN_ABSENT` may be ``None``, which states
    that the source record does not publish that fact at all. A null is then a
    statement of absence rather than a value, and never a placeholder string
    that a matcher could read as agreement.
    """

    venue: str
    contract_id: str
    event_id: str
    family: str | None
    reference_period: str | None
    source: str | None
    units: str | None
    operator: Operator
    threshold: Decimal | None
    lower: Decimal | None
    upper: Decimal | None
    rounding: Rounding | None
    vintage: str | None
    timezone: str
    deadline: dt.datetime | None
    settlement: str
    currency: str
    exceptional_policy: str | None
    open_time: dt.datetime | None
    close_time: dt.datetime | None
    resolve_time: dt.datetime | None
    rule_hash: str
    provenance: Provenance
    rule_available_at: dt.datetime | None = None

    MATCH_FIELDS: ClassVar[tuple[str, ...]] = (
        "family",
        "reference_period",
        "source",
        "units",
        "operator",
        "threshold",
        "lower",
        "upper",
        "rounding",
        "vintage",
        "timezone",
        "deadline",
        "settlement",
        "currency",
        "exceptional_policy",
        "rule_hash",
    )
    IDENTITY_FIELDS: ClassVar[tuple[str, ...]] = ("venue", "contract_id", "event_id")
    #: Matching fields a source may genuinely not publish. ``None`` on one of
    #: these states that the fact is absent from the record, so a matcher sees
    #: an unknown rather than a placeholder string it could read as agreement.
    UNKNOWN_WHEN_ABSENT: ClassVar[tuple[str, ...]] = (
        "family",
        "reference_period",
        "source",
        "units",
        "rounding",
        "vintage",
        "exceptional_policy",
    )

    def __post_init__(self) -> None:
        for name in (
            "venue",
            "contract_id",
            "event_id",
            "settlement",
            "currency",
            "rule_hash",
        ):
            _require_text(getattr(self, name), field_name=f"Contract.{name}")
        for name in (
            "family",
            "reference_period",
            "source",
            "units",
            "vintage",
            "exceptional_policy",
        ):
            object.__setattr__(
                self, name, _optional_text(getattr(self, name), field_name=f"Contract.{name}")
            )
        _zone(_require_text(self.timezone, field_name="Contract.timezone"))
        operator = _coerce_enum(Operator, self.operator, field_name="Contract.operator")
        object.__setattr__(self, "operator", operator)
        if self.rounding is not None:
            object.__setattr__(
                self,
                "rounding",
                _coerce_enum(Rounding, self.rounding, field_name="Contract.rounding"),
            )
        for name in ("threshold", "lower", "upper"):
            value = getattr(self, name)
            if value is None:
                continue
            parsed = parse_decimal(value, field_name=f"Contract.{name}")
            object.__setattr__(self, name, parsed)
        for name in ("deadline", "open_time", "close_time", "resolve_time", "rule_available_at"):
            object.__setattr__(
                self, name, _optional_aware(getattr(self, name), field_name=f"Contract.{name}")
            )
        if not isinstance(self.provenance, Provenance):
            raise TypeError("Contract.provenance must be a Provenance")

        if operator.needs_threshold and self.threshold is None:
            raise ValueError(f"Contract with operator {operator.value!r} requires a threshold")
        if operator is Operator.RANGE:
            if self.lower is None or self.upper is None:
                raise ValueError("Contract with operator 'range' requires lower and upper")
            if self.lower >= self.upper:
                raise ValueError(
                    f"Contract range lower {self.lower} must be below upper {self.upper}"
                )
            if self.threshold is not None:
                raise ValueError("Contract with operator 'range' must not also carry a threshold")
        elif self.lower is not None or self.upper is not None:
            raise ValueError(
                f"Contract with operator {operator.value!r} must not carry lower/upper bounds"
            )
        if (
            self.open_time is not None
            and self.close_time is not None
            and self.open_time > self.close_time
        ):
            raise ValueError(
                f"Contract.open_time {self.open_time} is after close_time {self.close_time}"
            )

    @property
    def market_key(self) -> str:
        return market_key(self.venue, self.contract_id)

    @property
    def orientation_sign(self) -> Decimal:
        return self.operator.orientation_sign

    def is_open_at(self, at: dt.datetime) -> bool:
        """Whether the market is inside its documented trading window at ``at``."""
        moment = _aware(at, field_name="Contract.is_open_at(at)")
        if self.open_time is not None and moment < self.open_time:
            return False
        return not (self.close_time is not None and moment >= self.close_time)

    def match_values(self) -> dict[str, object]:
        """Field values that must agree for a rule match, in :attr:`MATCH_FIELDS` order."""
        return {name: getattr(self, name) for name in self.MATCH_FIELDS}


@dataclass(frozen=True, slots=True)
class ContractRelation:
    """One typed edge in the payoff and exposure graph.

    ``left`` and ``right`` are ``(venue, contract_id)`` pairs. ``weight`` exists
    only for ``predictive`` edges and is fitted on training data; it is not a
    causal intervention coefficient.
    """

    kind: RelationKind
    left: tuple[str, str]
    right: tuple[str, str]
    basis: str
    rule_hash: str
    provenance: Provenance
    weight: Decimal | None = None

    def __post_init__(self) -> None:
        kind = _coerce_enum(RelationKind, self.kind, field_name="ContractRelation.kind")
        object.__setattr__(self, "kind", kind)
        for name in ("left", "right"):
            pair = getattr(self, name)
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise TypeError(f"ContractRelation.{name} must be a (venue, contract_id) pair")
            venue, contract_id = pair
            _require_text(venue, field_name=f"ContractRelation.{name}[0]")
            _require_text(contract_id, field_name=f"ContractRelation.{name}[1]")
            object.__setattr__(self, name, (venue, contract_id))
        _require_text(self.basis, field_name="ContractRelation.basis")
        _require_text(self.rule_hash, field_name="ContractRelation.rule_hash")
        if not isinstance(self.provenance, Provenance):
            raise TypeError("ContractRelation.provenance must be a Provenance")
        if self.left == self.right:
            raise ValueError("ContractRelation must connect two distinct markets")
        if self.weight is not None:
            if kind is not RelationKind.PREDICTIVE:
                raise ValueError(
                    f"ContractRelation weight applies only to predictive edges, not {kind.value!r}"
                )
            object.__setattr__(
                self, "weight", parse_decimal(self.weight, field_name="ContractRelation.weight")
            )
