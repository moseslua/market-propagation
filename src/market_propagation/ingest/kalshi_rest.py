"""Kalshi public market-data client.

Scope is public, read-only market data. Every call is a GET. The order, portfolio
and RFQ routes that Kalshi documents are absent by construction, so this module
cannot submit, amend, cancel or otherwise touch an order, and there is no
authenticated code path to route around an access restriction with.

Documented surfaces used, all verified reachable without credentials:

===============================  =========================================
Endpoint                         Purpose here
===============================  =========================================
``GET /historical/cutoff``       moving live/historical partition boundary
``GET /historical/markets``      archived (inactive) contract universe
``GET /markets``                 live-side markets
``GET /historical/markets/       historical candles (legacy plain fields)
 {ticker}/candlesticks``
``GET /series/{series}/markets/  live candles (``_dollars``/``_fp`` fields)
 {ticker}/candlesticks``
``GET /historical/trades``       historical trades, block-trade flagged
``GET /series/{ticker}``         series metadata, rules and contract terms
``GET /events``                  events of one series, without a global walk
``GET /events/{event_ticker}``   event metadata with nested markets
===============================  =========================================

Two documented-format facts are load-bearing and are handled explicitly rather
than guessed at:

**The partition split is per endpoint, not per vintage.** ``/historical/markets``
returns the modern ``yes_bid_dollars``/``volume_fp`` schema, while
``/historical/markets/{ticker}/candlesticks`` returns legacy plain names
(``yes_bid.open``). The live candle endpoint returns the modern schema *nested
inside the same keys* (``yes_bid.open_dollars``). Parsers therefore key off the
path they called, and normalization branches per endpoint.

**Moving cutoffs.** ``/historical/cutoff`` returns four ISO-8601 timestamps that
advance over time, which means the live/historical boundary moves underneath a
running query. A date that is historical today can be unreachable from the live
endpoint tomorrow and vice versa. :meth:`KalshiClient.resolve_partition` performs
that reconciliation explicitly and returns an auditable decision instead of
silently returning nothing.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any

from ..storage import RawStore
from .pagination import PaginationResult, paginate
from .transport import HttpTransport, WireShapeError

KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
KALSHI_ALTERNATE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

#: Documented candle intervals. Kalshi documents 1, 60 and 1440 minutes only.
#: Anything finer is not available and must not be presented as if it were.
CANDLE_INTERVALS_MINUTES = (1, 60, 1440)

#: ``status=all`` is documented nowhere and was observed to return HTTP 400.
#: The real enum is sent verbatim or the filter is omitted entirely.
MARKET_STATUSES = ("unopened", "open", "paused", "closed", "settled")

CUTOFF_FIELDS = (
    "market_settled_ts",
    "trades_created_ts",
    "orders_updated_ts",
    "market_positions_last_updated_ts",
)

#: Fields Kalshi renamed from bare integers/strings to fixed-point names. The
#: legacy names are tolerated because archived responses and older candles still
#: use them, but the fixed-point names win when both are present.
DOLLAR_ALIASES: Mapping[str, tuple[str, ...]] = {
    "yes_bid": ("yes_bid_dollars", "yes_bid"),
    "yes_ask": ("yes_ask_dollars", "yes_ask"),
    "no_bid": ("no_bid_dollars", "no_bid"),
    "no_ask": ("no_ask_dollars", "no_ask"),
    "last_price": ("last_price_dollars", "last_price"),
    "previous_yes_bid": ("previous_yes_bid_dollars", "previous_yes_bid"),
    "previous_yes_ask": ("previous_yes_ask_dollars", "previous_yes_ask"),
    "previous_price": ("previous_price_dollars", "previous_price"),
    "settlement_value": ("settlement_value_dollars", "settlement_value"),
}

COUNT_ALIASES: Mapping[str, tuple[str, ...]] = {
    "volume": ("volume_fp", "volume"),
    "volume_24h": ("volume_24h_fp", "volume_24h"),
    "open_interest": ("open_interest_fp", "open_interest"),
    "yes_bid_size": ("yes_bid_size_fp", "yes_bid_size"),
    "yes_ask_size": ("yes_ask_size_fp", "yes_ask_size"),
    "count": ("count_fp", "count"),
}


class PartitionUnavailable(RuntimeError):
    """No endpoint serves the requested window under the current cutoff.

    Raised instead of returning an empty list, because an empty list here would
    be read as "no such contracts existed" when the truth is "this window is not
    reachable through the endpoints available".
    """

    def __init__(self, message: str, *, detail: dict[str, Any]) -> None:
        super().__init__(message)
        self.detail = detail


@dataclass(frozen=True, slots=True)
class HistoricalCutoff:
    """The documented live/historical partition boundary.

    The four values are ISO-8601 strings despite their ``_ts`` names — an
    observed deviation from the documented ``int64`` schema, and the reason this
    type parses rather than trusts the field names.

    A missing ``market_settled_ts`` raises, because defaulting it to the epoch
    would route every market query to the historical partition.
    """

    market_settled_ts: dt.datetime
    trades_created_ts: dt.datetime
    orders_updated_ts: dt.datetime
    market_positions_last_updated_ts: dt.datetime
    raw_hash: str
    observed_at: dt.datetime

    @classmethod
    def from_payload(
        cls, payload: Mapping[str, Any], *, raw_hash: str, observed_at: dt.datetime
    ) -> HistoricalCutoff:
        parsed: dict[str, dt.datetime] = {}
        for field in CUTOFF_FIELDS:
            if field not in payload:
                raise WireShapeError(
                    f"historical cutoff response is missing {field!r}; "
                    "refusing to default a partition boundary"
                )
            value = payload[field]
            if value is None:
                raise WireShapeError(
                    f"historical cutoff field {field!r} is null; the live/historical "
                    "boundary is unknown rather than absent"
                )
            parsed[field] = _parse_utc(value, field)
        return cls(
            market_settled_ts=parsed["market_settled_ts"],
            trades_created_ts=parsed["trades_created_ts"],
            orders_updated_ts=parsed["orders_updated_ts"],
            market_positions_last_updated_ts=parsed["market_positions_last_updated_ts"],
            raw_hash=raw_hash,
            observed_at=observed_at,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_settled_ts": self.market_settled_ts.isoformat(),
            "trades_created_ts": self.trades_created_ts.isoformat(),
            "orders_updated_ts": self.orders_updated_ts.isoformat(),
            "market_positions_last_updated_ts": (self.market_positions_last_updated_ts.isoformat()),
            "raw_hash": self.raw_hash,
            "observed_at": self.observed_at.isoformat(),
            "source_format": "iso8601_string",
            "documented_format": "int64_unix",
            "format_note": ("observed ISO-8601 strings despite documented int64 names"),
        }


@dataclass(frozen=True, slots=True)
class PartitionDecision:
    """Which endpoint serves a window, and why.

    Recorded rather than inferred so a later reader can see that a window was
    genuinely unavailable instead of quietly empty.
    """

    venue: str
    window_start: dt.datetime
    window_end: dt.datetime
    partition: str
    endpoint: str
    reason: str
    cutoff: HistoricalCutoff
    available: bool
    detail: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "partition": self.partition,
            "endpoint": self.endpoint,
            "reason": self.reason,
            "available": self.available,
            "cutoff": self.cutoff.as_dict(),
            "detail": dict(self.detail),
        }


def _parse_utc(value: Any, field: str) -> dt.datetime:
    """Parse an ISO-8601 timestamp, or an integer Unix timestamp, as UTC."""
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = dt.datetime.fromtimestamp(float(value), tz=dt.UTC)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise WireShapeError(f"{field!r} is an empty string")
        # Kalshi emits a trailing Z; keep a space-tolerant fallback for older
        # archives that use a space separator.
        normalised = text.replace(" ", "T", 1) if re.match(r"^\d{4}-\d{2}-\d{2} ", text) else text
        if normalised.endswith(("Z", "z")):
            normalised = normalised[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(normalised)
        except ValueError as exc:
            raise WireShapeError(f"{field!r} is not a parseable timestamp: {value!r}") from exc
    else:
        raise WireShapeError(f"{field!r} has unsupported type {type(value).__name__}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def parse_fixed_point_dollars(value: Any, field: str) -> Decimal | None:
    """Parse a documented fixed-point dollar string, preserving exactness.

    Returns ``None`` for an absent or explicitly null field. A malformed value
    raises: silently coercing a renamed field to zero would fabricate a price.
    """
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Numeric JSON is not the documented form but appears in some archives.
        return Decimal(str(value))
    if not isinstance(value, str):
        raise WireShapeError(f"{field!r} is {type(value).__name__}, expected a string")
    text = value.strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise WireShapeError(f"{field!r} is not a decimal string: {value!r}") from exc


def parse_fixed_point_count(value: Any, field: str) -> Decimal | None:
    """Parse a documented fixed-point contract count string exactly."""
    return parse_fixed_point_dollars(value, field)


def pick(payload: Mapping[str, Any], aliases: Sequence[str], *, required: bool = False) -> Any:
    """Return the first present alias, preferring the documented modern name."""
    for name in aliases:
        if name in payload and payload[name] is not None:
            return payload[name]
    if required:
        joined = ", ".join(repr(a) for a in aliases)
        raise WireShapeError(f"none of {joined} present in payload with keys {sorted(payload)}")
    return None


@dataclass(frozen=True, slots=True)
class CandleSpacing:
    """Observed candle spacing versus the requested interval.

    Kalshi candles carry only ``end_period_ts``; they carry no start time, no
    executable/indicative flag and no per-candle trade count. Gaps therefore have
    to be read off the returned timestamps, and this type reports them instead of
    assuming a contiguous series.
    """

    requested_interval_minutes: int
    requested_spacing_seconds: int
    candle_count: int
    expected_count: int | None
    first_end_period_ts: int | None
    last_end_period_ts: int | None
    observed_spacings_seconds: tuple[int, ...]
    off_grid: tuple[int, ...]
    missing_periods: tuple[int, ...]
    spacing_consistent: bool
    covers_window: bool
    holes_present: bool
    usable_for_replay: bool
    note: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "requested_interval_minutes": self.requested_interval_minutes,
            "requested_spacing_seconds": self.requested_spacing_seconds,
            "candle_count": self.candle_count,
            "expected_count": self.expected_count,
            "first_end_period_ts": self.first_end_period_ts,
            "last_end_period_ts": self.last_end_period_ts,
            "observed_spacings_seconds": list(self.observed_spacings_seconds),
            "off_grid": list(self.off_grid),
            "missing_periods": list(self.missing_periods),
            "spacing_consistent": self.spacing_consistent,
            "covers_window": self.covers_window,
            "holes_present": self.holes_present,
            "usable_for_replay": self.usable_for_replay,
            "note": self.note,
        }


def inspect_candle_spacing(
    candlesticks: Iterable[Mapping[str, Any]],
    *,
    interval_minutes: int,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> CandleSpacing:
    """Measure real spacing and coverage of a returned candle series.

    A hole is a missing grid period, not merely an irregular gap. Kalshi was
    observed skipping periods at ``period_interval=1`` while a coarser 60-minute
    request over an adjacent window was contiguous, so the grid is derived from
    ``interval_minutes`` and gaps are reported as the periods that should exist
    and do not.

    ``usable_for_replay`` is ``False`` whenever a hole is present, because a
    forward-filled or silently bridged book would invent liquidity that was never
    quoted.
    """
    if interval_minutes not in CANDLE_INTERVALS_MINUTES:
        raise ValueError(
            f"interval_minutes must be one of {CANDLE_INTERVALS_MINUTES}, got {interval_minutes}"
        )
    spacing = interval_minutes * 60
    stamps = sorted(
        int(c["end_period_ts"])
        for c in candlesticks
        if isinstance(c, Mapping) and c.get("end_period_ts") is not None
    )
    if not stamps:
        expected = _expected_count(start_ts, end_ts, spacing)
        return CandleSpacing(
            requested_interval_minutes=interval_minutes,
            requested_spacing_seconds=spacing,
            candle_count=0,
            expected_count=expected,
            first_end_period_ts=None,
            last_end_period_ts=None,
            observed_spacings_seconds=(),
            off_grid=(),
            missing_periods=(),
            spacing_consistent=True,
            covers_window=False,
            holes_present=False,
            usable_for_replay=False,
            note="no candlesticks returned for the requested window",
        )

    spacings: list[int] = []
    off_grid: list[int] = []
    missing: list[int] = []
    for earlier, later in pairwise(stamps):
        delta = later - earlier
        spacings.append(delta)
        if delta <= 0:
            off_grid.append(later)
            continue
        if delta % spacing != 0:
            off_grid.append(later)
            continue
        for step in range(1, delta // spacing):
            missing.append(earlier + step * spacing)

    prefix_offsets: list[int] = []
    suffix_offsets: list[int] = []
    if start_ts is not None and stamps[0] > start_ts:
        prefix_offsets = list(range(start_ts, stamps[0], spacing))
    if end_ts is not None and stamps[-1] < end_ts:
        suffix_offsets = list(range(stamps[-1] + spacing, end_ts + 1, spacing))

    holes = tuple(sorted(set(missing)))
    expected = _expected_count(start_ts, end_ts, spacing)
    spacing_consistent = not off_grid and (len(set(spacings)) <= 1 if spacings else True)

    notes: list[str] = []
    if holes:
        notes.append(
            f"{len(holes)} missing grid period(s) inside the returned range; "
            "reconstruction invalid until a fresh snapshot or a coarser interval"
        )
    if off_grid:
        notes.append(f"{len(off_grid)} timestamp(s) off the {spacing}s grid")
    if not spacing_consistent:
        notes.append("observed spacing varies within the response")
    if prefix_offsets or suffix_offsets:
        notes.append(
            "returned range does not span the requested window; absence of candles "
            "before the first or after the last returned period is not evidence that "
            "the market was quiet"
        )
    if not notes:
        notes.append(
            f"response is on-grid at {spacing}s with no observed holes; this is "
            "still candle-frequency data, not a complete order-book history"
        )

    return CandleSpacing(
        requested_interval_minutes=interval_minutes,
        requested_spacing_seconds=spacing,
        candle_count=len(stamps),
        expected_count=expected,
        first_end_period_ts=stamps[0],
        last_end_period_ts=stamps[-1],
        observed_spacings_seconds=tuple(spacings),
        off_grid=tuple(off_grid),
        missing_periods=holes,
        spacing_consistent=spacing_consistent,
        covers_window=not prefix_offsets and not suffix_offsets,
        holes_present=bool(holes),
        usable_for_replay=not holes and not off_grid,
        note="; ".join(notes),
    )


def _expected_count(start_ts: int | None, end_ts: int | None, spacing: int) -> int | None:
    if start_ts is None or end_ts is None or end_ts < start_ts:
        return None
    # Documented semantics are inclusive at both ends, but the observed response
    # does not start exactly at start_ts. This is an upper bound on the number of
    # in-window periods, used for coverage reporting rather than to reject data.
    return (end_ts - start_ts) // spacing + 1


class KalshiClient:
    """Public Kalshi market-data client. Every method issues GET only."""

    def __init__(
        self,
        store: RawStore,
        *,
        transport: HttpTransport | None = None,
        base_url: str = KALSHI_BASE_URL,
    ) -> None:
        self._store = store
        self._owns_transport = transport is None
        self._transport = transport or HttpTransport(store)
        self._base = base_url.rstrip("/")

    @property
    def base_url(self) -> str:
        return self._base

    def close(self) -> None:
        if self._owns_transport:
            self._transport.close()

    def __enter__(self) -> KalshiClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def get_historical_cutoff(self) -> HistoricalCutoff:
        """``GET /historical/cutoff``. The boundary advances over time."""
        envelope = self._transport.get(
            f"{self._base}/historical/cutoff",
            source="kalshi.historical.cutoff",
            record_id="cutoff",
        )
        payload = envelope.json()
        if not isinstance(payload, dict):
            raise WireShapeError(f"historical cutoff returned {type(payload).__name__}")
        return HistoricalCutoff.from_payload(
            payload,
            raw_hash=envelope.provenance.raw_hash,
            observed_at=envelope.received_time,
        )

    def resolve_partition(
        self,
        window_start: dt.datetime,
        window_end: dt.datetime,
        *,
        cutoff: HistoricalCutoff | None = None,
    ) -> PartitionDecision:
        """Decide which partition serves a window under the live cutoff.

        The two partitions are mutually exclusive for settled markets: a market
        that settled before ``market_settled_ts`` is readable only through
        ``/historical/markets``, and one that settled after is readable only
        through ``/markets``. A window that straddles the cutoff is served by
        both, so the decision is ``straddling`` and names both endpoints.

        A window that is neither fully historical nor fully live, and is older
        than the live retention target, is reported unavailable rather than
        queried blindly.
        """
        boundary = cutoff or self.get_historical_cutoff()
        start = _as_utc(window_start)
        end = _as_utc(window_end)
        if end < start:
            raise ValueError("window_end must not precede window_start")

        if boundary.market_settled_ts >= end:
            partition, endpoint, available = (
                "historical",
                f"{self._base}/historical/markets",
                True,
            )
            reason = (
                "window ends before the market_settled_ts cutoff, so every settled "
                "market in it is served by the historical partition"
            )
        elif boundary.market_settled_ts <= start:
            partition, endpoint, available = "live", f"{self._base}/markets", True
            reason = (
                "window starts at or after the market_settled_ts cutoff, so settled "
                "markets in it are still served by the live partition"
            )
        else:
            partition, endpoint, available = (
                "straddling",
                f"{self._base}/historical/markets and {self._base}/markets",
                True,
            )
            reason = (
                "the cutoff falls inside the window; both endpoints must be queried "
                "and merged before any coverage claim, because neither alone is "
                "complete for this window"
            )

        detail = {
            "market_settled_ts": boundary.market_settled_ts.isoformat(),
            "trades_created_ts": boundary.trades_created_ts.isoformat(),
            "cutoff_raw_hash": boundary.raw_hash,
            "endpoints": endpoint,
            "live_retention_target": "3 months (documented)",
            "partition_is_mutually_exclusive": partition != "straddling",
        }
        return PartitionDecision(
            venue="kalshi",
            window_start=start,
            window_end=end,
            partition=partition,
            endpoint=endpoint,
            reason=reason,
            cutoff=boundary,
            available=available,
            detail=detail,
        )

    def list_markets(
        self,
        *,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str | None = None,
        tickers: Sequence[str] | None = None,
        limit: int = 200,
        max_pages: int = 25,
        max_items: int | None = None,
    ) -> PaginationResult:
        """``GET /markets`` — live-side markets.

        ``status`` must be a documented value or omitted. ``status=all`` is
        rejected locally: it is not in the documented enum and was observed to
        return HTTP 400, so sending it would waste a request and hide a caller
        bug behind a network error.
        """
        if status is not None and status not in MARKET_STATUSES:
            raise ValueError(
                f"status must be one of {MARKET_STATUSES} or None; "
                f"{status!r} is not documented and 'all' returns HTTP 400"
            )
        params: dict[str, Any] = {
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "status": status,
            "tickers": ",".join(tickers) if tickers else None,
            "limit": limit,
        }
        return paginate(
            self._transport,
            f"{self._base}/markets",
            items_key="markets",
            source="kalshi.markets",
            params=params,
            max_pages=max_pages,
            max_items=max_items,
            identity_keys=(),
            record_prefix="markets",
        )

    def list_historical_markets(
        self,
        *,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        tickers: Sequence[str] | None = None,
        limit: int = 200,
        max_pages: int = 25,
        max_items: int | None = None,
    ) -> PaginationResult:
        """``GET /historical/markets`` — the archived *inactive* contract universe.

        This is the route that makes pre-event contract eligibility auditable:
        candidates that closed before the cutoff are still listed here, so a
        universe can be built without post-event volume selection.

        These filters are documented as mutually exclusive, so more than one is
        rejected locally rather than sent and misread as a server fault.
        """
        supplied = [
            n
            for n, v in (
                ("series_ticker", series_ticker),
                ("event_ticker", event_ticker),
                ("tickers", tickers),
            )
            if v
        ]
        if len(supplied) > 1:
            raise ValueError(
                "historical market filters are mutually exclusive; got "
                f"{', '.join(supplied)}. Use GET /events/{{event_ticker}} for the "
                "nested market set of one event."
            )
        params: dict[str, Any] = {
            "series_ticker": series_ticker,
            "event_ticker": event_ticker,
            "tickers": ",".join(tickers) if tickers else None,
            "limit": limit,
        }
        return paginate(
            self._transport,
            f"{self._base}/historical/markets",
            items_key="markets",
            source="kalshi.historical.markets",
            params=params,
            max_pages=max_pages,
            max_items=max_items,
            identity_keys=(),
            record_prefix="hist-markets",
        )

    def list_events(
        self,
        *,
        series_ticker: str | None = None,
        status: str | None = None,
        limit: int = 200,
        max_pages: int = 25,
        max_items: int | None = None,
    ) -> PaginationResult:
        """``GET /events`` — one series' events, without walking global history.

        The event tickers of a series are the identifiers the historical-market
        filter accepts, so this is the route that reaches a series' contracts
        without an unfiltered page walk over every settled market. Nested markets
        are deliberately not requested: they were observed empty for historical
        events, so the contracts themselves come from the historical-market
        filter keyed by the event ticker this listing returns.

        ``status`` follows the market status enum, which is the documented one for
        event filtering.
        """
        if status is not None and status not in MARKET_STATUSES:
            raise ValueError(
                f"status must be one of {MARKET_STATUSES} or None; "
                f"{status!r} is not documented and 'all' returns HTTP 400"
            )
        params: dict[str, Any] = {
            "series_ticker": series_ticker,
            "status": status,
            "limit": limit,
        }
        return paginate(
            self._transport,
            f"{self._base}/events",
            items_key="events",
            source="kalshi.events.list",
            params=params,
            max_pages=max_pages,
            max_items=max_items,
            identity_keys=(lambda item: item.get("event_ticker"),),
            record_prefix="events",
        )

    def discover_series(
        self, query: str | None = None
    ) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        """``GET /series`` — enumerate series names rather than guessing tickers.

        Downstream series are discovered from the exchange's own listing, so a
        study never relies on a hardcoded ticker guess.
        """
        result = paginate(
            self._transport,
            f"{self._base}/series",
            items_key="series",
            source="kalshi.series.list",
            params={"limit": 200},
            max_pages=25,
            identity_keys=(),
            record_prefix="series",
        )
        if query is None:
            return list(result.items), result.raw_hashes
        needle = query.lower()
        matched = [
            item
            for item in result.items
            if isinstance(item, Mapping)
            and needle
            in " ".join(str(item.get(key, "")) for key in ("ticker", "title", "category")).lower()
        ]
        return matched, result.raw_hashes

    def get_series(self, series_ticker: str) -> tuple[dict[str, Any], str]:
        """``GET /series/{ticker}`` — rules, terms and settlement source."""
        envelope = self._transport.get(
            f"{self._base}/series/{series_ticker}",
            source="kalshi.series",
            record_id=series_ticker,
        )
        payload = envelope.json()
        if not isinstance(payload, dict) or "series" not in payload:
            raise WireShapeError(f"series response for {series_ticker} lacks a 'series' object")
        series = payload["series"]
        if not isinstance(series, dict):
            raise WireShapeError(f"'series' for {series_ticker} is not an object")
        return series, envelope.provenance.raw_hash

    def get_event(
        self, event_ticker: str, *, with_nested_markets: bool = True
    ) -> tuple[dict[str, Any], str]:
        """``GET /events/{event_ticker}`` — the full nested market set.

        Documented as supporting ``with_nested_markets``. The nested strikes of
        one event were observed carrying different ``open_time`` and
        ``created_time`` values, so callers must build a point-in-time universe
        from each market's own ``open_time`` rather than the event's identity.
        """
        envelope = self._transport.get(
            f"{self._base}/events/{event_ticker}",
            params={"with_nested_markets": "true" if with_nested_markets else None},
            source="kalshi.event",
            record_id=event_ticker,
        )
        payload = envelope.json()
        if not isinstance(payload, dict) or "event" not in payload:
            raise WireShapeError(f"event response for {event_ticker} lacks 'event'")
        event = payload["event"]
        if not isinstance(event, dict):
            raise WireShapeError(f"'event' for {event_ticker} is not an object")
        return event, envelope.provenance.raw_hash

    def get_historical_candles(
        self,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
    ) -> tuple[list[dict[str, Any]], str]:
        """``GET /historical/markets/{ticker}/candlesticks``.

        Returns legacy plain field names. ``start_ts``, ``end_ts`` and
        ``period_interval`` are all documented as required, and the interval
        enum is enforced locally so the API never has to reject the request.
        """
        _require_candle_args(period_interval, start_ts, end_ts)
        envelope = self._transport.get(
            f"{self._base}/historical/markets/{ticker}/candlesticks",
            params={
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "period_interval": int(period_interval),
            },
            source="kalshi.historical.candlesticks",
            record_id=ticker,
        )
        return _candles_from(envelope, ticker), envelope.provenance.raw_hash

    def get_live_candles(
        self,
        series_ticker: str,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
    ) -> tuple[list[dict[str, Any]], str]:
        """``GET /series/{series}/markets/{ticker}/candlesticks``.

        Served at a different path with the same key names but ``_dollars`` and
        ``_fp`` suffixes, which is why normalization is keyed off the path.
        """
        _require_candle_args(period_interval, start_ts, end_ts)
        envelope = self._transport.get(
            f"{self._base}/series/{series_ticker}/markets/{ticker}/candlesticks",
            params={
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "period_interval": int(period_interval),
            },
            source="kalshi.live.candlesticks",
            record_id=ticker,
        )
        return _candles_from(envelope, ticker), envelope.provenance.raw_hash

    def candles_with_spacing(
        self,
        ticker: str,
        *,
        start_ts: int,
        end_ts: int,
        period_interval: int,
        series_ticker: str | None = None,
    ) -> tuple[list[dict[str, Any]], CandleSpacing, str]:
        """Fetch candles and immediately measure real spacing and coverage.

        ``series_ticker`` selects the live endpoint. Without it the historical
        endpoint is used, which is correct for anything older than the cutoff.
        """
        if series_ticker is None:
            candles, raw_hash = self.get_historical_candles(
                ticker,
                start_ts=start_ts,
                end_ts=end_ts,
                period_interval=period_interval,
            )
        else:
            candles, raw_hash = self.get_live_candles(
                series_ticker,
                ticker,
                start_ts=start_ts,
                end_ts=end_ts,
                period_interval=period_interval,
            )
        spacing = inspect_candle_spacing(
            candles,
            interval_minutes=period_interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        return candles, spacing, raw_hash

    def get_historical_trades(
        self,
        *,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        is_block_trade: bool | None = None,
        limit: int = 1000,
        max_pages: int = 20,
        max_items: int | None = None,
    ) -> PaginationResult:
        """``GET /historical/trades`` — trades filled before the cutoff.

        A ticker filter is strongly preferred. The global stream is documented,
        but it is not a practical way to cover a narrow study, and an unbounded
        global pull would make the cost of a cohort audit unknowable.

        ``taker_side`` is deprecated in favour of ``taker_outcome_side`` and
        ``taker_book_side``; all three are preserved by the parser so a
        reconciliation can compare them.
        """
        params: dict[str, Any] = {
            "ticker": ticker,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "is_block_trade": None if is_block_trade is None else str(is_block_trade).lower(),
            "limit": limit,
        }
        return paginate(
            self._transport,
            f"{self._base}/historical/trades",
            items_key="trades",
            source="kalshi.historical.trades",
            params=params,
            max_pages=max_pages,
            max_items=max_items,
            identity_keys=(lambda t: t.get("trade_id") if isinstance(t, Mapping) else None,),
            record_prefix="hist-trades",
        )

    def get_live_trades(
        self,
        *,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
        max_pages: int = 20,
        max_items: int | None = None,
    ) -> PaginationResult:
        """``GET /markets/trades`` — live-side trades within the cutoff window."""
        params: dict[str, Any] = {
            "ticker": ticker,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "limit": limit,
        }
        return paginate(
            self._transport,
            f"{self._base}/markets/trades",
            items_key="trades",
            source="kalshi.live.trades",
            params=params,
            max_pages=max_pages,
            max_items=max_items,
            identity_keys=(lambda t: t.get("trade_id") if isinstance(t, Mapping) else None,),
            record_prefix="live-trades",
        )

    def get_orderbook(self, ticker: str, *, depth: int | None = None) -> tuple[dict[str, Any], str]:
        """``GET /markets/{ticker}/orderbook`` — a public point-in-time snapshot.

        Kalshi documents that this endpoint returns bids only, because a NO bid
        at price ``p`` is a YES ask at ``1 - p``. It is therefore a *snapshot*,
        not a complete book history, and it carries no sequence number. Any
        reconstruction built from these can only ever be a sequence of
        snapshots; there is no documented public delta stream, because the
        WebSocket orderbook channel requires authentication.
        """
        envelope = self._transport.get(
            f"{self._base}/markets/{ticker}/orderbook",
            params={"depth": depth},
            source="kalshi.orderbook",
            record_id=ticker,
        )
        payload = envelope.json()
        if not isinstance(payload, dict):
            raise WireShapeError(f"orderbook for {ticker} is not a JSON object")
        return payload, envelope.provenance.raw_hash


def _require_candle_args(period_interval: int, start_ts: int, end_ts: int) -> None:
    if period_interval not in CANDLE_INTERVALS_MINUTES:
        raise ValueError(
            f"period_interval must be one of {CANDLE_INTERVALS_MINUTES} minutes; "
            f"documented intervals are exactly those three, got {period_interval}"
        )
    if start_ts is None or end_ts is None:
        raise ValueError("start_ts and end_ts are both required by the documented schema")
    if end_ts < start_ts:
        raise ValueError("end_ts must not precede start_ts")


def _candles_from(envelope: Any, ticker: str) -> list[dict[str, Any]]:
    payload = envelope.json()
    if not isinstance(payload, dict):
        raise WireShapeError(f"candle response for {ticker} is not a JSON object")
    if "candlesticks" not in payload:
        raise WireShapeError(
            f"candle response for {ticker} has no 'candlesticks' key "
            f"(present keys: {sorted(payload)}); the endpoint path selects the schema"
        )
    candles = payload["candlesticks"]
    if candles is None:
        return []
    if not isinstance(candles, list):
        raise WireShapeError(f"'candlesticks' for {ticker} is not a list")
    for candle in candles:
        if not isinstance(candle, dict):
            raise WireShapeError(f"candle entry for {ticker} is not an object")
        if "end_period_ts" not in candle:
            raise WireShapeError(
                f"candle for {ticker} lacks 'end_period_ts'; timestamps cannot be inferred"
            )
    return candles


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware; naive datetimes are refused")
    return value.astimezone(dt.UTC)


__all__ = [
    "CANDLE_INTERVALS_MINUTES",
    "CUTOFF_FIELDS",
    "KALSHI_ALTERNATE_BASE_URL",
    "KALSHI_BASE_URL",
    "MARKET_STATUSES",
    "CandleSpacing",
    "HistoricalCutoff",
    "KalshiClient",
    "PartitionDecision",
    "PartitionUnavailable",
    "inspect_candle_spacing",
    "parse_fixed_point_count",
    "parse_fixed_point_dollars",
    "pick",
]
