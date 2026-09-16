"""Polymarket public market-data client and bounded public subscription capture.

**Access status in this environment: unreachable, therefore fail-closed.**
Every documented Polymarket host resolved and connected as a timeout during
implementation — ``docs.polymarket.com``, ``gamma-api.polymarket.com``,
``clob.polymarket.com`` and ``polymarket.com`` all returned no response, and the
hosts are unreachable from the shell as well as from the in-process reader. The
consequence is deliberately encoded rather than worked around:

* :meth:`PolymarketPublicClient.probe` performs bounded attempts and returns an
  explicit :class:`ReachabilityReport`. Nothing here bypasses a restriction.
* Any acquisition with no successful response raises or returns a recorded
  blocked status. No method returns an empty list that could be mistaken for an
  observed absence of markets.
* The documented hosts and field names below were **not** verified against a live
  response in this environment; ``configs/endpoints.yaml`` is the durable record
  of that, and the ``docs.hash`` pin it documents is how a fetched documentation
  revision is fixed. No hash covers these hosts, because nothing here was fetched.
  They are marked ``documentation_verified=False`` so a later reader can tell an
  assumption from a fact.

Semantics that are implemented regardless of reachability, because they are
properties of the documented protocol rather than of the network:

**Snapshot plus delta.** The market channel sends a full ``book`` snapshot
followed by incremental updates. State is only usable after a snapshot, so a
capture that starts mid-stream holds ``awaiting_snapshot`` until one arrives.

**Reconnect invalidation.** A reconnect ends the validity of every level the
previous connection established, because the new connection's first message may
be a delta against a book this process no longer holds. A reconnect therefore
emits a ``disconnect`` event and forces ``awaiting_snapshot``; a stale book is
never silently continued across a connection boundary.

**Absolute versus incremental size.** The documented update carries a ``size``
for a price level. Whether that value replaces the level or increments it cannot
be settled from documentation alone, so the parse records the interpretation it
applied and the raw value, and exposes both so a replay can compare them rather
than quietly picking one.

**Polling is snapshots.** :meth:`PolymarketPublicClient.capture_polled_snapshots`
labels every observation ``snapshot`` with the poll resolution attached. It is
never described as tick-complete, and its ``tick_complete`` field is ``False``.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from itertools import pairwise
from typing import Any

from websockets.asyncio.client import connect as _connect

from ..domain import (
    Availability,
    BookEvent,
    BookSide,
    Clock,
    Provenance,
    Quote,
    QuoteValidity,
    Trade,
)
from .normalize import stable_record_id
from .transport import (
    HttpTransport,
    TransportError,
    WireShapeError,
    blocked_record,
)

POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_URL = "https://clob.polymarket.com"
POLYMARKET_DATA_URL = "https://data-api.polymarket.com"
POLYMARKET_DOCS_URL = "https://docs.polymarket.com"
POLYMARKET_MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

#: Every host the implementation attempted, kept so a reachability report names
#: what was tried instead of asserting a general outage.
POLYMARKET_HOSTS: tuple[str, ...] = (
    POLYMARKET_GAMMA_URL,
    POLYMARKET_CLOB_URL,
    POLYMARKET_DATA_URL,
    POLYMARKET_MARKET_WS,
    POLYMARKET_DOCS_URL,
    "https://polymarket.com",
)

#: The legacy price-history surface expresses ``fidelity`` in minutes. The newer
#: documented history surface describes resolution metadata and age-dependent
#: availability. Both are recorded; neither is treated as reconstructing a tick
#: feed.
LEGACY_FIDELITY_UNITS = "minutes"

#: Documented market-channel message kinds.
CHANNEL_EVENTS = ("book", "price_change", "tick_size_change", "last_trade_price", "best_bid_ask")


@dataclass(frozen=True, slots=True)
class ReachabilityReport:
    """Result of a bounded probe. ``reachable`` is only true on a real 2xx."""

    reachable: bool
    attempts: int
    observed_at: dt.datetime
    detail: Mapping[str, Any]
    blocked: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": "polymarket",
            "reachable": self.reachable,
            "attempts": self.attempts,
            "observed_at": self.observed_at.isoformat(),
            "detail": dict(self.detail),
            "blocked": dict(self.blocked) if self.blocked else None,
            "documentation_verified": False,
            "note": (
                "no Polymarket endpoint was verified in this environment; a "
                "reachable result requires an observed 2xx response"
            ),
        }


@dataclass(frozen=True, slots=True)
class HistoryPoint:
    """One documented price-history point, at the resolution actually returned."""

    contract_id: str
    observed_at: dt.datetime
    price: Decimal
    clock: Clock
    provenance: Provenance
    resolution_seconds: int | None = None
    source_index: int = 0
    timestamp_unit: str = "unix_seconds"

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "observed_at": self.observed_at.isoformat(),
            "price": str(self.price),
            "resolution_seconds": self.resolution_seconds,
            "raw_hash": self.provenance.raw_hash,
            "usable_time": self.clock.usable_time.isoformat() if self.clock.usable_time else None,
        }


def normalize_price_history(
    raw: Any,
    *,
    contract_id: str,
    clock: Clock,
    provenance: Provenance,
    resolution_seconds: int | None = None,
    timestamp_unit: str = "unix_seconds",
) -> list[HistoryPoint]:
    """Normalize a documented ``history`` array into timestamped points.

    Each point is stamped with the archival clock, because the *payload* is what
    this system observed; the historical observation time is the point's own
    source time. Availability therefore stays ``unknown`` for a historical point,
    since a later fetch cannot establish when the point first became public.
    """
    if isinstance(raw, Mapping):
        points = raw.get("history")
        if resolution_seconds is None:
            resolution_seconds = _coerce_int(raw.get("fidelity") or raw.get("resolution"))
    else:
        points = raw
    if points is None:
        raise WireShapeError("price history payload has no 'history' array")
    if not isinstance(points, list):
        raise WireShapeError("'history' is not a list")

    out: list[HistoryPoint] = []
    for index, point in enumerate(points):
        if not isinstance(point, Mapping):
            raise WireShapeError(f"history point {index} is not an object")
        stamp = point.get("t")
        price = point.get("p")
        if stamp is None or price is None:
            raise WireShapeError(
                f"history point {index} lacks a documented 't' or 'p' field; "
                "spacing cannot be inferred without both"
            )
        observed_at = _parse_timestamp(stamp, timestamp_unit)
        point_clock = Clock(
            source_time=observed_at,
            received_time=clock.received_time,
            availability=Availability.unknown(basis="historical_without_receipt"),
            monotonic_ns=getattr(clock, "monotonic_ns", None),
        )
        out.append(
            HistoryPoint(
                contract_id=contract_id,
                observed_at=observed_at,
                price=_decimal(price),
                clock=point_clock,
                provenance=provenance,
                resolution_seconds=resolution_seconds,
                source_index=index,
                timestamp_unit=timestamp_unit,
            )
        )
    return out


def inspect_history_spacing(
    points: Sequence[HistoryPoint],
    *,
    requested_fidelity_minutes: int | None = None,
) -> dict[str, Any]:
    """Report the resolution the history endpoint actually returned.

    The legacy surface takes ``fidelity`` in minutes, but the documented newer
    surface resolves age-dependently. The observed spacing is therefore measured
    instead of assumed, and a mismatch between the requested fidelity and the
    returned spacing is reported rather than smoothed over.
    """
    if not points:
        return {
            "point_count": 0,
            "observed_spacing_seconds": [],
            "requested_fidelity_minutes": requested_fidelity_minutes,
            "matches_requested": None,
            "uniform": None,
            "note": "no history points returned; spacing is unmeasurable",
        }
    ordered = sorted(points, key=lambda p: p.observed_at)
    spacings = [
        int((later.observed_at - earlier.observed_at).total_seconds())
        for earlier, later in pairwise(ordered)
    ]
    distinct = sorted(set(spacings))
    requested_seconds = requested_fidelity_minutes * 60 if requested_fidelity_minutes else None
    matches = (
        None
        if requested_seconds is None or not spacings
        else all(space == requested_seconds for space in spacings)
    )
    return {
        "point_count": len(ordered),
        "first_observed_at": ordered[0].observed_at.isoformat(),
        "last_observed_at": ordered[-1].observed_at.isoformat(),
        "observed_spacing_seconds": spacings,
        "distinct_spacing_seconds": distinct,
        "uniform": len(distinct) <= 1,
        "requested_fidelity_minutes": requested_fidelity_minutes,
        "requested_spacing_seconds": requested_seconds,
        "matches_requested": matches,
        "effective_resolution_seconds": distinct[0] if len(distinct) == 1 else None,
        "tick_complete": False,
        "note": (
            "documented history is a sampled series at the returned resolution; it "
            "does not reconstruct a tick feed even when requests are accepted faster"
        ),
    }


def normalize_book_snapshot(
    raw: Mapping[str, Any],
    *,
    contract_id: str,
    clock: Clock,
    provenance: Provenance,
    connection_id: str = "",
    sequence_scope: str = "",
    venue: str = "polymarket",
    sequence: int | None = None,
) -> list[BookEvent]:
    """Normalize a documented CLOB book response into one snapshot ``BookEvent``.

    The response body is a snapshot by definition, so it always carries
    ``operation='replace'``. The frozen snapshot has no sequence number to carry
    and no field for the ``hash`` the response publishes, so that hash is left in
    the archived payload named by ``provenance.raw_hash`` rather than attached
    here; it orders nothing, and two snapshots may share it.
    """
    bids = _levels_from(raw.get("bids"), "bids")
    asks = _levels_from(raw.get("asks"), "asks")
    return [
        BookEvent(
            venue=venue,
            contract_id=str(raw.get("asset_id") or contract_id),
            kind="snapshot",
            clock=clock,
            provenance=provenance,
            connection_id=connection_id,
            sequence_scope=sequence_scope
            or f"{venue}:{raw.get('asset_id') or contract_id}:snapshot",
            sequence=sequence,
            bids=tuple(bids),
            asks=tuple(asks),
            side=None,
            price=None,
            size=None,
            operation="replace",
        )
    ]


def _levels_from(node: Any, field_name: str) -> list[tuple[Decimal, Decimal]]:
    if node is None:
        return []
    if isinstance(node, Mapping):
        node = list(node.items())
    if not isinstance(node, list):
        raise WireShapeError(f"book side {field_name!r} is not a list")
    levels: list[tuple[Decimal, Decimal]] = []
    for entry in node:
        if isinstance(entry, Mapping):
            price = entry.get("price")
            size = entry.get("size")
        elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
            price, size = entry[0], entry[1]
        else:
            raise WireShapeError(f"malformed book level in {field_name!r}: {entry!r}")
        if price is None or size is None:
            continue
        quantity = _decimal(size)
        # A level with zero size is a removal, not a resting order.
        if quantity == 0:
            continue
        levels.append((_decimal(price), quantity))
    levels.sort(key=lambda level: level[0], reverse=field_name == "bids")
    return levels


class PolymarketUnreachable(RuntimeError):
    """No Polymarket endpoint answered. Raised instead of returning empty data."""

    def __init__(self, message: str, *, blocked: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.blocked = dict(blocked)


class PolymarketPublicClient:
    """Read-only Polymarket REST client.

    Every call is a GET against a documented public host, and every failure
    surfaces as :class:`PolymarketUnreachable` carrying the recorded blocked
    status. The class refuses to convert unreachability into an empty dataset.
    """

    def __init__(
        self,
        store: Any,
        *,
        transport: HttpTransport | None = None,
        gamma_url: str = POLYMARKET_GAMMA_URL,
        clob_url: str = POLYMARKET_CLOB_URL,
        data_url: str = POLYMARKET_DATA_URL,
    ) -> None:
        self._store = store
        self._owns_transport = transport is None
        self._transport = transport or HttpTransport(store)
        self._gamma = gamma_url.rstrip("/")
        self._clob = clob_url.rstrip("/")
        self._data = data_url.rstrip("/")

    def close(self) -> None:
        if self._owns_transport:
            self._transport.close()

    def __enter__(self) -> PolymarketPublicClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def probe(self, *, max_attempts: int = 2) -> ReachabilityReport:
        """Bounded reachability check against the documented public hosts.

        Attempts stop at the first success. A failure leaves an explicit blocked
        record, and the report never claims reachability it did not observe.
        """
        observed = dt.datetime.now(dt.UTC)
        attempts = 0
        last_blocked: dict[str, Any] | None = None
        details: dict[str, Any] = {"hosts_attempted": list(POLYMARKET_HOSTS)}
        for url in (f"{self._gamma}/markets?limit=1", f"{self._clob}/book?token_id=0"):
            for _ in range(max_attempts):
                attempts += 1
                try:
                    envelope = self._transport.get(
                        url,
                        source="polymarket.probe",
                        record_id="probe",
                    )
                except TransportError as exc:
                    last_blocked = exc.as_blocked_record()
                    continue
                details["probe_url"] = url
                details["probe_raw_hash"] = envelope.provenance.raw_hash
                return ReachabilityReport(
                    reachable=True,
                    attempts=attempts,
                    observed_at=observed,
                    detail=details,
                )
        details["last_error"] = (last_blocked or {}).get("reason")
        return ReachabilityReport(
            reachable=False,
            attempts=attempts,
            observed_at=observed,
            detail=details,
            blocked=last_blocked,
        )

    def require_reachable(self, *, max_attempts: int = 2) -> ReachabilityReport:
        """Probe and raise unless a real response was observed."""
        report = self.probe(max_attempts=max_attempts)
        if not report.reachable:
            raise PolymarketUnreachable(
                "no documented Polymarket endpoint answered; refusing to treat an "
                "unreachable source as an empty dataset",
                blocked=report.blocked
                or blocked_record(
                    url=report.detail.get("hosts_attempted", ["polymarket"])[0],
                    status_code=None,
                    reason="unreachable",
                    attempts=report.attempts,
                    payload_hash=None,
                    observed_at=report.observed_at,
                ),
            )
        return report

    def list_markets(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        active: bool | None = None,
        closed: bool | None = None,
        max_pages: int = 10,
    ) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        """``GET /markets`` on the documented Gamma surface.

        Paginated by ``limit``/``offset``. A page that returns fewer items than
        ``limit`` ends the walk; the loop is bounded regardless.
        """
        items: list[dict[str, Any]] = []
        hashes: list[str] = []
        for page in range(max_pages):
            params: dict[str, Any] = {
                "limit": limit,
                "offset": offset + page * limit,
                "active": None if active is None else str(active).lower(),
                "closed": None if closed is None else str(closed).lower(),
            }
            envelope = self._get(
                f"{self._gamma}/markets",
                params=params,
                source="polymarket.gamma.markets",
                record_id=f"markets-{page:05d}",
                error=PolymarketUnreachable,
            )
            hashes.append(envelope.provenance.raw_hash)
            payload = envelope.json()
            page_items = payload if isinstance(payload, list) else payload.get("data")
            if not isinstance(page_items, list):
                raise WireShapeError(
                    "Gamma markets response is neither a list nor carries a 'data' list"
                )
            items.extend(item for item in page_items if isinstance(item, dict))
            if len(page_items) < limit:
                break
        return items, tuple(hashes)

    def get_book(self, token_id: str) -> tuple[dict[str, Any], str]:
        """``GET /book`` on the documented CLOB surface."""
        envelope = self._get(
            f"{self._clob}/book",
            params={"token_id": token_id},
            source="polymarket.clob.book",
            record_id=token_id,
            error=PolymarketUnreachable,
        )
        payload = envelope.json()
        if not isinstance(payload, dict):
            raise WireShapeError(f"book response for {token_id} is not an object")
        return payload, envelope.provenance.raw_hash

    def get_midpoint(self, token_id: str) -> tuple[dict[str, Any], str]:
        """``GET /midpoint`` on the documented CLOB surface."""
        envelope = self._get(
            f"{self._clob}/midpoint",
            params={"token_id": token_id},
            source="polymarket.clob.midpoint",
            record_id=token_id,
            error=PolymarketUnreachable,
        )
        payload = envelope.json()
        if not isinstance(payload, dict):
            raise WireShapeError(f"midpoint response for {token_id} is not an object")
        return payload, envelope.provenance.raw_hash

    def get_price_history(
        self,
        token_id: str,
        *,
        start: dt.datetime | None = None,
        end: dt.datetime | None = None,
        fidelity_minutes: int | None = None,
        market: str | None = None,
    ) -> tuple[list[HistoryPoint], dict[str, Any], str]:
        """``GET /prices-history`` — the legacy surface, fidelity in minutes.

        ``fidelity`` is sent in the documented unit. The returned spacing is
        measured, because the newer documented history surface resolves
        age-dependently and does not always honour the requested fidelity.
        """
        params: dict[str, Any] = {
            "market": market or token_id,
            "startTs": int(start.timestamp()) if start else None,
            "endTs": int(end.timestamp()) if end else None,
            "fidelity": fidelity_minutes,
        }
        envelope = self._get(
            f"{self._clob}/prices-history",
            params=params,
            source="polymarket.clob.prices_history",
            record_id=token_id,
            error=PolymarketUnreachable,
        )
        clock = Clock.captured(
            source_time=None,
            received_time=envelope.received_time,
            monotonic_ns=envelope.monotonic_ns,
            uncertainty_seconds=0.0,
        )
        points = normalize_price_history(
            envelope.json(),
            contract_id=token_id,
            clock=clock,
            provenance=envelope.provenance,
            resolution_seconds=fidelity_minutes * 60 if fidelity_minutes else None,
        )
        spacing = inspect_history_spacing(points, requested_fidelity_minutes=fidelity_minutes)
        return points, spacing, envelope.provenance.raw_hash

    def get_trades(
        self,
        *,
        token_id: str | None = None,
        limit: int = 500,
        offset: int = 0,
        max_pages: int = 5,
    ) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        """``GET /trades`` on the documented data surface."""
        items: list[dict[str, Any]] = []
        hashes: list[str] = []
        for page in range(max_pages):
            envelope = self._get(
                f"{self._data}/trades",
                params={
                    "asset_id": token_id,
                    "limit": limit,
                    "offset": offset + page * limit,
                },
                source="polymarket.data.trades",
                record_id=f"trades-{page:05d}",
                error=PolymarketUnreachable,
            )
            hashes.append(envelope.provenance.raw_hash)
            payload = envelope.json()
            page_items = payload if isinstance(payload, list) else payload.get("data")
            if not isinstance(page_items, list):
                raise WireShapeError("trades response carries no list")
            items.extend(item for item in page_items if isinstance(item, dict))
            if len(page_items) < limit:
                break
        return items, tuple(hashes)

    def snapshot_quote(
        self,
        token_id: str,
        *,
        connection_id: str = "",
    ) -> tuple[Quote, BookEvent, str]:
        """One public quote/book capture, labelled as a snapshot.

        Returns the quote, the snapshot book event, and the raw hash. The quote's
        ``last_verified`` is the receipt instant — the moment this process
        actually observed the book — which is the honest freshness field. No
        ``last_trade`` is claimed, because the book response does not carry one.
        """
        payload, raw_hash = self.get_book(token_id)
        received = dt.datetime.now(dt.UTC)
        clock = Clock.captured(source_time=None, received_time=received, uncertainty_seconds=0.0)
        provenance = Provenance(
            raw_hash=raw_hash,
            record_id=stable_record_id(token_id, received, prefix="pm-book-"),
            source="polymarket.clob.book",
        )
        events = normalize_book_snapshot(
            payload,
            contract_id=token_id,
            clock=clock,
            provenance=provenance,
            connection_id=connection_id,
        )
        event = events[0]
        bids = tuple(getattr(event, "bids", ()) or ())
        asks = tuple(getattr(event, "asks", ()) or ())
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        if best_bid is None or best_ask is None:
            validity = QuoteValidity.MISSING
        elif best_bid > best_ask:
            validity = QuoteValidity.CROSSED
        else:
            validity = QuoteValidity.VALID
        quote = Quote(
            venue="polymarket",
            contract_id=token_id,
            clock=clock,
            provenance=provenance,
            bid=best_bid,
            ask=best_ask,
            bid_size=bids[0][1] if bids else None,
            ask_size=asks[0][1] if asks else None,
            validity=validity,
            last_price_change=None,
            last_verified=received,
            last_trade=None,
            replay_order="snapshot",
        )
        return quote, event, raw_hash

    def capture_polled_snapshots(
        self,
        token_ids: Sequence[str],
        *,
        duration_seconds: float = 60.0,
        interval_seconds: float = 5.0,
        sleep: Callable[[float], Any] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        """Bounded polling capture that is explicitly snapshot-only.

        Each poll is an independent public book read. The returned structure
        states its resolution and sets ``tick_complete`` to ``False``, because
        polling cannot observe the messages it did not ask for: every trade and
        quote change between two polls is invisible here. Reporting this as a
        tick feed would overstate the data.

        Stops on the duration bound, on ``should_stop`` going true, or on the
        first unreachable source, which is recorded rather than skipped.
        """
        import time as _time

        pause = sleep if sleep is not None else _time.sleep
        started = dt.datetime.now(dt.UTC)
        deadline = started + dt.timedelta(seconds=duration_seconds)
        observations: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        stopped_reason = "duration_elapsed"

        while dt.datetime.now(dt.UTC) < deadline:
            if should_stop is not None and should_stop():
                stopped_reason = "graceful_stop"
                break
            for token_id in token_ids:
                try:
                    quote, _, raw_hash = self.snapshot_quote(token_id)
                except (PolymarketUnreachable, TransportError) as exc:
                    blocked.append(
                        exc.blocked
                        if isinstance(exc, PolymarketUnreachable)
                        else exc.as_blocked_record()
                    )
                    stopped_reason = "blocked"
                    break
                observations.append(
                    {
                        "token_id": token_id,
                        "observed_at": quote.clock.received_time.isoformat(),
                        "bid": str(quote.bid) if quote.bid is not None else None,
                        "ask": str(quote.ask) if quote.ask is not None else None,
                        "validity": str(getattr(quote.validity, "value", quote.validity)),
                        "raw_hash": raw_hash,
                        "observation_kind": "snapshot",
                        "tick_complete": False,
                    }
                )
            if stopped_reason == "blocked":
                break
            remaining = (deadline - dt.datetime.now(dt.UTC)).total_seconds()
            if remaining <= 0:
                break
            pause(min(interval_seconds, remaining))

        return {
            "venue": "polymarket",
            "mode": "polling_snapshots",
            "tick_complete": False,
            "started_at": started.isoformat(),
            "stopped_reason": stopped_reason,
            "interval_seconds": interval_seconds,
            "resolution_seconds": interval_seconds,
            "observation_count": len(observations),
            "observations": observations,
            "blocked": blocked,
            "note": (
                "polled book snapshots at the stated interval; messages between "
                "polls were not observed and this is not a tick-complete record"
            ),
        }

    def _get(
        self,
        url: str,
        *,
        params: Mapping[str, Any],
        source: str,
        record_id: str,
        error: type[Exception],
    ) -> Any:
        try:
            return self._transport.get(url, params=params, source=source, record_id=record_id)
        except TransportError as exc:
            if error is PolymarketUnreachable:
                raise PolymarketUnreachable(
                    f"Polymarket endpoint {url} did not respond", blocked=exc.as_blocked_record()
                ) from exc
            raise


@dataclass(slots=True)
class _CollectorBook:
    """Collector-local book. Never shared with another collector.

    Holds the levels last seen on *this* connection for *this* asset. It is not a
    replay state and does no gap detection; that belongs to the replay owner.
    """

    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    snapshot_received: bool = False
    updates_applied: int = 0
    reconnects: int = 0


@dataclass(frozen=True, slots=True)
class CaptureResult:
    """Outcome of a bounded public subscription capture."""

    asset_ids: tuple[str, ...]
    started_at: dt.datetime
    ended_at: dt.datetime
    stopped_reason: str
    events_emitted: int
    books_emitted: int
    snapshots_received: int
    reconnects: int
    awaiting_snapshot_at_end: tuple[str, ...]
    raw_hashes: tuple[str, ...]
    message_kinds: Mapping[str, int]
    blocked: tuple[dict[str, Any], ...] = ()
    tick_complete: bool = False
    connection_ids: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": "polymarket",
            "mode": "websocket_market_channel",
            "asset_ids": list(self.asset_ids),
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "stopped_reason": self.stopped_reason,
            "events_emitted": self.events_emitted,
            "books_emitted": self.books_emitted,
            "snapshots_received": self.snapshots_received,
            "reconnects": self.reconnects,
            "awaiting_snapshot_at_end": list(self.awaiting_snapshot_at_end),
            "raw_hashes": list(self.raw_hashes),
            "message_kinds": dict(self.message_kinds),
            "blocked": list(self.blocked),
            "connection_ids": list(self.connection_ids),
            "tick_complete": self.tick_complete,
            "note": (
                "public subscription capture is bounded and may end awaiting a "
                "snapshot after a reconnect; such assets are not valid at end"
            ),
        }


def classify_channel_message(message: Mapping[str, Any]) -> str:
    """Return the documented message kind, refusing to guess.

    Some deployments send a bare array; those are handled by the caller. A
    message with no recognisable ``event_type`` raises, because treating an
    unknown message as a no-op is how a schema change becomes silent data loss.
    """
    event_type = message.get("event_type") or message.get("type")
    if not isinstance(event_type, str) or not event_type:
        raise WireShapeError(
            f"market-channel message has no event_type; keys present: {sorted(message)}"
        )
    if event_type not in CHANNEL_EVENTS:
        raise WireShapeError(
            f"unrecognised market-channel event_type {event_type!r}; known kinds are "
            f"{CHANNEL_EVENTS}. A new kind is a schema change, not a no-op."
        )
    return event_type


async def capture_market_channel(
    store: Any,
    asset_ids: Sequence[str],
    *,
    duration_seconds: float = 60.0,
    connect: Callable[..., Any] | None = None,
    url: str = POLYMARKET_MARKET_WS,
    max_reconnects: int = 3,
    should_stop: Callable[[], bool] | None = None,
    on_event: Callable[[BookEvent | Trade], None] | None = None,
) -> CaptureResult:
    """Bounded public subscription capture with graceful stop and reconnect invalidation.

    The public market channel needs no credentials, so the subscription itself is
    legitimate. What the capture refuses to do is overstate what it saw:

    * The duration bound, ``should_stop``, the reconnect bound and any connect
      failure all end the run with an explicit ``stopped_reason``.
    * A reconnect emits a ``disconnect`` event for every asset and clears the
      collector book, so the next delta cannot be applied to a book whose origin
      is unknown. Those assets stay ``awaiting_snapshot`` until a fresh snapshot
      arrives, and the assets still awaiting one at the end are named. The marker
      is archived as soon as the connection that built the book ends, including
      when its socket fails, so a reconnect that never connects still leaves the
      boundary on the wire.
    * Each connection ends with an explicit outcome (finished on the duration or
      stop bound, or lost), which is what decides whether to retry. ``stopped_reason``
      records why the run itself ended and is not reused as loop control, so a
      failed connection consumes the reconnect budget instead of being reported as
      a duration bound.
    * ``tick_complete`` is ``False``. A bounded subscription observed a window of
      messages, not every message that existed.
    * ``size`` semantics are recorded per update instead of assumed, and the
      ``book`` snapshot always replaces the state wholesale.

    ``connect`` is injectable so the message handling can be exercised against a
    recorded transcript without a live socket.
    """
    if connect is None:
        connect = _connect

    started = dt.datetime.now(dt.UTC)
    deadline = started + dt.timedelta(seconds=duration_seconds)
    books: dict[str, _CollectorBook] = {asset: _CollectorBook() for asset in asset_ids}
    counters: dict[str, int] = {}
    emitted: list[BookEvent | Trade] = []
    raw_hashes: list[str] = []
    blocked: list[dict[str, Any]] = []
    connection_ids: list[str] = []
    reconnects = 0
    snapshots = 0
    stopped_reason = "duration_elapsed"

    subscription = {
        "assets_ids": list(asset_ids),
        "type": "market",
    }

    while True:
        connection_id = f"pm-ws-{len(connection_ids)}"
        # Per-connection outcome, kept explicit. ``stopped_reason`` is the
        # run-level verdict and cannot double as loop control: a socket failure
        # leaves it at its initial value, which would end the run rather than
        # consume the reconnect budget, and would report a failed connection as
        # a duration bound that had not been reached.
        ended = "not_started"
        try:
            async with connect(url) as socket:  # type: ignore[operator]
                connection_ids.append(connection_id)
                await socket.send(json.dumps(subscription))
                ended = "running"
                while True:
                    if dt.datetime.now(dt.UTC) >= deadline:
                        stopped_reason = "duration_elapsed"
                        ended = "duration_elapsed"
                        break
                    if should_stop is not None and should_stop():
                        stopped_reason = "graceful_stop"
                        ended = "graceful_stop"
                        break
                    timeout = max(0.1, (deadline - dt.datetime.now(dt.UTC)).total_seconds())
                    try:
                        raw_message = await asyncio.wait_for(
                            socket.recv(), timeout=min(timeout, 5.0)
                        )
                    except TimeoutError:
                        continue
                    except Exception as exc:  # socket-level failure
                        blocked.append(
                            blocked_record(
                                url=url,
                                status_code=None,
                                reason=f"connection_error: {type(exc).__name__}",
                                attempts=reconnects + 1,
                                payload_hash=None,
                                observed_at=dt.datetime.now(dt.UTC),
                            )
                        )
                        ended = "connection_lost"
                        break

                    received_time = dt.datetime.now(dt.UTC)
                    provenance = store.put(
                        raw_message.encode("utf-8")
                        if isinstance(raw_message, str)
                        else raw_message,
                        source="polymarket.ws.market",
                        received_time=received_time,
                        record_id=f"{connection_id}-{len(raw_hashes):06d}",
                        metadata={
                            "channel": "market",
                            "url": url,
                            "connection_id": connection_id,
                            "credentials": "none",
                            "public_subscription": True,
                        },
                    )
                    raw_hashes.append(provenance.raw_hash)

                    for message in _iter_messages(raw_message):
                        try:
                            kind = classify_channel_message(message)
                        except WireShapeError:
                            # An unparseable message is a schema-change alarm. It
                            # is counted and skipped rather than silently dropped,
                            # and the raw bytes remain archived above.
                            counters["unrecognised"] = counters.get("unrecognised", 0) + 1
                            continue
                        counters[kind] = counters.get(kind, 0) + 1
                        if kind == "book":
                            snapshots += 1
                        new_events = _apply_message(
                            books,
                            message,
                            kind=kind,
                            received_time=received_time,
                            connection_id=connection_id,
                            monotonic_ns=None,
                            provenance=provenance,
                        )
                        for event in new_events:
                            emitted.append(event)
                            raw_hashes.append(event.provenance.raw_hash)
                            if on_event is not None:
                                on_event(event)

                if ended != "running":
                    # The connection has ended, so the books it built are no
                    # longer justified. Invalidate on the losing connection
                    # itself, then let the retry budget decide whether to reopen.
                    _invalidate_books(
                        store,
                        books,
                        connection_id=connection_id,
                        emitted=emitted,
                        raw_hashes=raw_hashes,
                        on_event=on_event,
                    )
                if ended in ("duration_elapsed", "graceful_stop"):
                    break
        except Exception as exc:  # connect failure or socket teardown
            blocked.append(
                blocked_record(
                    url=url,
                    status_code=None,
                    reason=f"connect_error: {type(exc).__name__}",
                    attempts=reconnects + 1,
                    payload_hash=None,
                    observed_at=dt.datetime.now(dt.UTC),
                )
            )
            ended = "connection_lost"
            # A connection whose teardown raised is over for the same reason one
            # whose recv failed: the books it built are no longer justified. This
            # is idempotent, since invalidation already cleared them.
            _invalidate_books(
                store,
                books,
                connection_id=connection_id,
                emitted=emitted,
                raw_hashes=raw_hashes,
                on_event=on_event,
            )

        reconnects += 1
        if reconnects > max_reconnects:
            stopped_reason = "max_reconnects"
            break
        if dt.datetime.now(dt.UTC) >= deadline:
            stopped_reason = "duration_elapsed"
            break
        await asyncio.sleep(
            min(1.0, max(0.0, (deadline - dt.datetime.now(dt.UTC)).total_seconds()))
        )

    awaiting = tuple(asset for asset, book in books.items() if not book.snapshot_received)
    return CaptureResult(
        asset_ids=tuple(asset_ids),
        started_at=started,
        ended_at=dt.datetime.now(dt.UTC),
        stopped_reason=stopped_reason,
        events_emitted=len(emitted),
        books_emitted=sum(
            1 for event in emitted if isinstance(event, BookEvent) and event.kind == "snapshot"
        ),
        snapshots_received=snapshots,
        reconnects=reconnects,
        awaiting_snapshot_at_end=awaiting,
        raw_hashes=tuple(raw_hashes),
        message_kinds=dict(counters),
        blocked=tuple(blocked),
        tick_complete=False,
        connection_ids=tuple(connection_ids),
    )


def _iter_messages(raw_message: Any) -> list[Mapping[str, Any]]:
    """Documented frames may be one object or a batch array."""
    try:
        payload = json.loads(raw_message)
    except (TypeError, ValueError):
        return []
    if isinstance(payload, Mapping):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    return []


def _apply_message(
    books: dict[str, _CollectorBook],
    message: Mapping[str, Any],
    *,
    kind: str,
    received_time: dt.datetime,
    connection_id: str,
    monotonic_ns: int | None,
    provenance: Provenance,
) -> list[BookEvent | Trade]:
    """Apply one channel message, returning the records it produced.

    A ``book`` message yields a snapshot, a ``price_change`` yields one level
    delta per changed level, and a ``last_trade_price`` yields a
    :class:`~market_propagation.domain.Trade`. The last one matters: a last-trade
    message describes a print, not a book change, so it is not emitted as a
    delta. ``tick_size_change`` and ``best_bid_ask`` carry no level change and no
    frozen record describes them, so they are counted in ``message_kinds`` and
    their bytes stay archived without a domain record being fabricated for them.

    ``price_change`` size semantics are recorded rather than assumed: the update
    is applied as an absolute replacement of the level (the documented reading).
    The interpretation and its ambiguity live in this module's docstring and in
    the archived payload, because the frozen delta has no field for either.
    """
    events: list[BookEvent | Trade] = []

    if kind == "book":
        asset = str(message.get("asset_id") or "")
        book = books.setdefault(asset, _CollectorBook())
        book.bids = {level[0]: level[1] for level in _levels_from(message.get("bids"), "bids")}
        book.asks = {level[0]: level[1] for level in _levels_from(message.get("asks"), "asks")}
        book.snapshot_received = True
        clock = Clock(
            source_time=_parse_timestamp(message.get("timestamp"), "unix_seconds")
            if message.get("timestamp") is not None
            else None,
            received_time=received_time,
            availability=Availability.captured(received_time, uncertainty_seconds=0.0),
            monotonic_ns=monotonic_ns,
        )
        events.append(
            BookEvent(
                venue="polymarket",
                contract_id=asset,
                kind="snapshot",
                clock=clock,
                provenance=provenance,
                connection_id=connection_id,
                sequence_scope=f"polymarket:{asset}:{connection_id}",
                sequence=None,
                bids=tuple(sorted(book.bids.items(), key=lambda level: -level[0])),
                asks=tuple(sorted(book.asks.items(), key=lambda level: level[0])),
                side=None,
                price=None,
                size=None,
                operation="replace",
            )
        )
        return events

    if kind == "price_change":
        updates = message.get("price_changes")
        if not isinstance(updates, list):
            updates = [message]
        for update in updates:
            if not isinstance(update, Mapping):
                continue
            asset = str(update.get("asset_id") or message.get("asset_id") or "")
            book = books.setdefault(asset, _CollectorBook())
            side = _book_side(update.get("side"))
            price = update.get("price")
            size = update.get("size")
            if price is None or size is None:
                continue
            price_dec = _decimal(price)
            size_dec = _decimal(size)
            target = book.bids if side is BookSide.BID else book.asks
            if size_dec == 0:
                # A zero size removes the level, so the delta carries a delete
                # operation and no size rather than a zero-sized replacement.
                target.pop(price_dec, None)
                events.append(
                    _delta_event(
                        asset=asset,
                        side=side,
                        price=price_dec,
                        size=None,
                        operation="delete",
                        clock=_message_clock(
                            update.get("timestamp", message.get("timestamp")),
                            received_time=received_time,
                            monotonic_ns=monotonic_ns,
                        ),
                        provenance=_message_provenance(update, provenance),
                        connection_id=connection_id,
                    )
                )
                continue
            target[price_dec] = size_dec
            book.updates_applied += 1
            book.snapshot_received = True
            events.append(
                _delta_event(
                    asset=asset,
                    side=side,
                    price=price_dec,
                    size=size_dec,
                    operation="replace",
                    clock=_message_clock(
                        update.get("timestamp", message.get("timestamp")),
                        received_time=received_time,
                        monotonic_ns=monotonic_ns,
                    ),
                    provenance=_message_provenance(update, provenance),
                    connection_id=connection_id,
                )
            )
        return events

    if kind == "last_trade_price":
        asset = str(message.get("asset_id") or "")
        books.setdefault(asset, _CollectorBook())
        price = message.get("price")
        size = message.get("size")
        if price is None or size is None:
            raise WireShapeError(
                f"a last_trade_price message on {asset!r} carries no price and size; "
                "a trade print without both cannot be normalized"
            )
        # A last-trade message is a print, not a book change, so it becomes a
        # Trade record. Emitting it as a delta would assert a level change the
        # message does not describe.
        events.append(
            Trade(
                venue="polymarket",
                contract_id=asset,
                trade_id=(
                    str(message["trade_id"]) if message.get("trade_id") is not None else None
                ),
                price=_decimal(price),
                size=_decimal(size),
                clock=_message_clock(
                    message.get("timestamp"),
                    received_time=received_time,
                    monotonic_ns=monotonic_ns,
                ),
                provenance=_message_provenance(message, provenance),
                aggressor=_optional_side(message.get("side")),
                is_block=(
                    message.get("is_block_trade")
                    if isinstance(message.get("is_block_trade"), bool)
                    else None
                ),
            )
        )
        return events

    if kind in ("tick_size_change", "best_bid_ask"):
        # Neither message changes a level: one publishes a tick size and the
        # other a best bid/ask pair with no side or size. The frozen domain has
        # no record for either, and a delta would have to invent a side, price
        # and size to exist. The messages stay counted in ``message_kinds`` and
        # their bytes stay archived, and no domain record is fabricated.
        return events

    return events


def _book_side(value: Any) -> BookSide:
    """Map a documented order side onto the closed vocabulary, refusing a guess."""
    text = str(value or "").strip().upper()
    if text == "BUY":
        return BookSide.BID
    if text == "SELL":
        return BookSide.ASK
    raise WireShapeError(
        f"unrecognised order side {value!r}; the documented values are BUY and SELL, "
        "and an unknown side cannot be assigned to a book side without guessing"
    )


def _optional_side(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _message_clock(
    timestamp_value: Any, *, received_time: dt.datetime, monotonic_ns: int | None
) -> Clock:
    return Clock(
        source_time=(
            _parse_timestamp(timestamp_value, "unix_seconds")
            if timestamp_value is not None
            else None
        ),
        received_time=received_time,
        availability=Availability.captured(received_time, uncertainty_seconds=0.0),
        monotonic_ns=monotonic_ns,
    )


def _delta_event(
    *,
    asset: str,
    side: BookSide,
    price: Decimal,
    size: Decimal | None,
    operation: str,
    clock: Clock,
    provenance: Provenance,
    connection_id: str,
) -> BookEvent:
    """One level change, carrying no whole-book levels.

    ``price_change`` size semantics are recorded rather than assumed: the update
    is applied as an absolute replacement of the level (the documented reading).
    The interpretation and its ambiguity live in this module's docstring and in
    the archived payload, because the frozen delta has no field for either.
    """
    return BookEvent(
        venue="polymarket",
        contract_id=asset,
        kind="delta",
        clock=clock,
        provenance=provenance,
        connection_id=connection_id,
        sequence_scope=f"polymarket:{asset}:{connection_id}",
        sequence=None,
        side=side,
        price=price,
        size=size,
        operation=operation,
    )


def _message_provenance(message: Mapping[str, Any], archived: Provenance) -> Provenance:
    """Occurrence identity for a channel message, pointing at the archived bytes.

    ``archived.raw_hash`` references the message payload this process actually
    stored. The occurrence identity is the venue's own ``hash`` when it supplies
    one, because that is a source-issued identifier rather than a content digest;
    otherwise it is a digest over the message's own identifying fields. Either
    way it is derived from source-visible values rather than from the payload
    bytes, so two genuinely distinct messages are never collapsed into one
    occurrence.

    ``connection_id`` is not carried on the record: ``Provenance`` has no field
    for it, and the emitting ``BookEvent`` already records the connection.
    """
    source_hash = message.get("hash")
    if source_hash:
        record_id = str(source_hash)
    else:
        asset = str(message.get("asset_id") or "")
        timestamp = message.get("timestamp")
        record_id = stable_record_id(asset, timestamp, message.get("price"), prefix="pm-msg-")
    return Provenance(
        raw_hash=archived.raw_hash,
        record_id=record_id,
        source="polymarket.ws.market",
    )


def _invalidate_books(
    store: Any,
    books: Mapping[str, _CollectorBook],
    *,
    connection_id: str,
    emitted: list[BookEvent | Trade],
    raw_hashes: list[str],
    on_event: Callable[[BookEvent | Trade], None] | None,
) -> None:
    """Archive a disconnect for every book this connection justified, then drop it.

    A connection whose socket has ended cannot vouch for a book it built, so the
    boundary is archived as a lifecycle marker and the book is cleared before the
    caller retries or stops. Archiving at the end of the failed connection rather
    than the start of the next one is what keeps the marker on the wire when the
    retry never connects.
    """
    for asset, book in books.items():
        if book.snapshot_received:
            disconnected = _emit(
                store,
                asset,
                kind="disconnect",
                source_time=None,
                connection_id=connection_id,
                sequence_scope=connection_id,
                operation="replace",
            )
            emitted.append(disconnected)
            if on_event is not None:
                on_event(disconnected)
            raw_hashes.append(disconnected.provenance.raw_hash)
        book.bids.clear()
        book.asks.clear()
        book.snapshot_received = False


def _emit(
    store: Any,
    asset: str,
    *,
    kind: str,
    source_time: dt.datetime | None,
    connection_id: str,
    sequence_scope: str,
    operation: str,
) -> BookEvent:
    """Archive a synthetic lifecycle marker and return its event.

    A ``disconnect`` has no wire bytes of its own, so its payload is a small JSON
    document describing the boundary. It is archived like any other payload, which
    keeps the invariant that every emitted event names a stored raw payload.
    """
    received = dt.datetime.now(dt.UTC)
    body = json.dumps(
        {
            "synthetic": True,
            "reason": kind,
            "asset_id": asset,
            "connection_id": connection_id,
            "recorded_at": received.isoformat(),
        },
        sort_keys=True,
    ).encode("utf-8")
    provenance = store.put(
        body,
        source="polymarket.ws.market.lifecycle",
        received_time=received,
        record_id=f"{connection_id}:{kind}:{asset}",
        metadata={
            "synthetic": True,
            "kind": kind,
            "connection_id": connection_id,
            "credentials": "none",
        },
    )
    clock = Clock(
        source_time=source_time,
        received_time=received,
        availability=Availability.captured(received, uncertainty_seconds=0.0),
    )
    return BookEvent(
        venue="polymarket",
        contract_id=asset,
        kind=kind,
        clock=clock,
        provenance=provenance,
        connection_id=connection_id,
        sequence_scope=sequence_scope,
        sequence=None,
        bids=(),
        asks=(),
        side=None,
        price=None,
        size=None,
        operation=operation,
    )


def _parse_timestamp(value: Any, unit: str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        if re_is_numeric(text):
            parsed = dt.datetime.fromtimestamp(float(text), tz=dt.UTC)
        else:
            try:
                parsed = dt.datetime.fromisoformat(text)
            except ValueError as exc:
                raise WireShapeError(f"unparseable timestamp {value!r}") from exc
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        # Documented Polymarket timestamps are seconds; a millisecond value is
        # recognisable by magnitude and is converted rather than misdated.
        if numeric > 1e11:
            numeric = numeric / 1000.0
        parsed = dt.datetime.fromtimestamp(numeric, tz=dt.UTC)
    else:
        raise WireShapeError(f"unsupported timestamp type {type(value).__name__}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def re_is_numeric(text: str) -> bool:
    return bool(text) and all(ch.isdigit() or ch in ".+-" for ch in text)


def _coerce_int(value: Any) -> int | None:
    """Coerce a documented numeric metadata field, refusing a silent guess.

    A bool is not a resolution, and a non-integral value is not a duration in
    seconds, so both are reported as unparseable rather than truncated.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise WireShapeError("a boolean is not an integer metadata value")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise WireShapeError(f"expected an integer metadata value, got {value!r}")
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError as exc:
            raise WireShapeError(f"not an integer metadata value: {value!r}") from exc
    raise WireShapeError(f"unsupported integer metadata type {type(value).__name__}")


def _decimal(value: Any) -> Decimal:
    from decimal import InvalidOperation

    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise WireShapeError("boolean is not a numeric price or size")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise WireShapeError(f"not a decimal value: {value!r}") from exc


__all__ = [
    "CHANNEL_EVENTS",
    "LEGACY_FIDELITY_UNITS",
    "POLYMARKET_CLOB_URL",
    "POLYMARKET_DATA_URL",
    "POLYMARKET_DOCS_URL",
    "POLYMARKET_GAMMA_URL",
    "POLYMARKET_HOSTS",
    "POLYMARKET_MARKET_WS",
    "CaptureResult",
    "HistoryPoint",
    "PolymarketPublicClient",
    "PolymarketUnreachable",
    "ReachabilityReport",
    "capture_market_channel",
    "classify_channel_message",
    "inspect_history_spacing",
    "normalize_book_snapshot",
    "normalize_price_history",
]
