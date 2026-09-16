"""Monthly capture of a venue's own listing and trades into sealed shards.

The venue's live partition retains roughly three months. A contract that leaves it
is served from ``/historical/markets``, but the *live* listing is what carries a
contract's rule text and its current lifecycle together, and the live trades route
reaches a contract's prints by ticker without naming a single ticker in advance.
So a window that is not captured while it is still live is a window whose bytes can
no longer be acquired. That is why this module exists: a forward cohort whose
extraction starts late loses its early windows permanently.

Five things are deliberate and load-bearing:

* **The live/historical split is decided, not guessed.**
  :meth:`~market_propagation.ingest.kalshi_rest.KalshiClient.resolve_partition`
  answers which endpoints serve a window under the venue's own advancing cutoff,
  and its decision is recorded in the output. This module never reaches for an
  endpoint the decision did not name.
* **The rows are the venue's, read by the venue's own normalizer.** Trade rows go
  through :func:`~market_propagation.ingest.normalize.normalize_kalshi_trade`, the
  same function the audit path uses. A second price or side parser is how two code
  paths come to disagree about one print.
* **The shards use the layout the pipeline already reads.** Both Kalshi layers are
  read by :func:`~market_propagation.ingest.external_history.extract_trades`
  through one projection, one time unit and one mapper, so a captured shard carries
  the archive's own column names and integer-cent prices rather than a schema
  nothing consumes.
* **``normalize_kalshi_contract`` is deliberately not used to produce them.** It
  requires ``strike_type`` and ``floor_strike``, which the archive layout does not
  carry, so routing an archived market record through it fails on the first row. It
  is the live-listing normalizer, and the live listing is a different wire shape
  from the shard this module writes.
* **Our bytes do not go into the vendor's directory.** The vendor layer is a
  third-party CC-BY-4.0 dataset; colocating our rows there would attribute our
  provenance to their licence. The capture writes under its own root.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from ..domain import UTC, Clock, Provenance
from ..storage import DECIMAL_TYPE, RawStore, hash_file
from .audit import series_of
from .external_history import KALSHI_OWN_LAYER
from .kalshi_rest import KALSHI_BASE_URL, KalshiClient
from .normalize import normalize_kalshi_trade, trade_direction_conflicts
from .pagination import PaginationResult
from .transport import HttpTransport, TransportError, WireShapeError

__all__ = [
    "BASIS_BOTH_LAYERS_COVER_THE_WINDOW",
    "BASIS_LAYER_NAMED_EXPLICITLY",
    "BASIS_NO_LAYER_COVERS_THE_WINDOW",
    "BASIS_SOLE_COVERING_LAYER",
    "DEFAULT_CAPTURE_ROOT",
    "MARKET_COLUMNS",
    "MARKET_LAYER",
    "TRADE_COLUMNS",
    "LayerCoverage",
    "MarketCapture",
    "MarketCaptureError",
    "MarketLayerAmbiguity",
    "MarketLayerResolution",
    "capture_window",
    "declared_market_layers",
    "declared_series",
    "market_row_from_record",
    "resolve_market_layer",
    "trade_row_from_record",
    "write_capture_shards",
]

#: Where this repository's own capture sits, relative to the archive root. It is a
#: sibling of the vendor's ``kalshi-trades/`` directory rather than inside it,
#: because that directory is a third-party dataset under its own licence.
DEFAULT_CAPTURE_ROOT = "kalshi-own"

#: The declared layer name for the captured market universe. The trade layer is
#: ``KALSHI_OWN_LAYER``, declared in :mod:`market_propagation.ingest.external_history`
#: because the extraction path dispatches on it.
MARKET_LAYER = "kalshi_own_markets"

#: The cohort key naming which series a capture covers. A declaration that names no
#: series is refused rather than widened into a sweep of every listed contract,
#: because that sweep is an unbounded pull whose size nobody declared.
CAPTURE_POLICY_SERIES_KEY = "policy_series"

#: The pipeline configuration that declares the archive layers. The layer names and
#: their patterns are read from it rather than carried here, so a market layer added
#: to the declaration reaches the authority decision without an edit in this module.
DEFAULT_PIPELINE_CONFIG = "configs/external_history_v1.yaml"

#: The captured trade shard's columns, in the archive's own names. ``yes_price`` and
#: ``no_price`` are integer cents, so the exactness the archive mapper depends on
#: survives the shard instead of being re-derived from a float.
#:
#: ``count`` is a decimal here while the vendor archive declares it ``int64``. That
#: is a measured difference rather than a preference: the venue states counts as
#: fixed-point values and really does publish fractional ones (27 of 97 counts on
#: one live contract), so an ``int64`` column could only hold them by rounding. The
#: exact decimal keeps what the venue stated, and the archive mapper reads either
#: physical type.
TRADE_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("trade_id", pa.string()),
    ("ticker", pa.string()),
    ("count", DECIMAL_TYPE),
    ("yes_price", pa.int64()),
    ("no_price", pa.int64()),
    ("taker_side", pa.string()),
    ("created_time", pa.timestamp("us", tz="UTC")),
)

#: The captured market shard's columns. The archive's own market columns are
#: reproduced, and ``rules_primary`` and ``rules_secondary`` are added: the listing
#: is the only route that serves a contract's published rule text beside its
#: lifecycle, and preserving that text is the point of capturing prospectively.
#: Everything else the listing returns stays in the archived response, which is
#: reachable through the raw store by content hash.
MARKET_COLUMNS: tuple[tuple[str, pa.DataType], ...] = (
    ("ticker", pa.string()),
    ("event_ticker", pa.string()),
    ("market_type", pa.string()),
    ("title", pa.string()),
    ("yes_sub_title", pa.string()),
    ("no_sub_title", pa.string()),
    ("status", pa.string()),
    ("yes_bid", pa.int64()),
    ("yes_ask", pa.int64()),
    ("no_bid", pa.int64()),
    ("no_ask", pa.int64()),
    ("last_price", pa.int64()),
    ("volume", DECIMAL_TYPE),
    ("volume_24h", DECIMAL_TYPE),
    ("open_interest", DECIMAL_TYPE),
    ("result", pa.string()),
    ("rules_primary", pa.string()),
    ("rules_secondary", pa.string()),
    ("created_time", pa.timestamp("us", tz="UTC")),
    ("open_time", pa.timestamp("us", tz="UTC")),
    ("close_time", pa.timestamp("us", tz="UTC")),
)

#: Fixed-point dollar fields the listing states, and the integer-cent column each
#: fills. The venue publishes dollars as strings and the archive stores cents.
_CENT_FIELDS: tuple[tuple[str, str], ...] = (
    ("yes_bid_dollars", "yes_bid"),
    ("yes_ask_dollars", "yes_ask"),
    ("no_bid_dollars", "no_bid"),
    ("no_ask_dollars", "no_ask"),
    ("last_price_dollars", "last_price"),
)

#: One contract count. The venue publishes a fixed-point string; the archive stores
#: an integer, so a fractional count is refused rather than rounded. A rounded
#: quantity is a fabricated one.
_COUNT_FIELD = "count_fp"

#: The fields a trade shard row cannot be built without. Each is refused by name
#: rather than written as a zero, because a zero is a value the venue did not state.
_TRADE_PRICE_FIELDS = ("yes_price_dollars", "no_price_dollars")

_CENTS = Decimal(100)


class MarketCaptureError(RuntimeError):
    """A capture that could not be taken, or a row that could not be written."""


@dataclass(frozen=True, slots=True)
class MarketCapture:
    """One capture run: the rows written, the bytes archived, and what was refused."""

    root: Path
    series: tuple[str, ...]
    window_start: dt.datetime
    window_end: dt.datetime
    partition: Mapping[str, Any]
    markets_written: int
    trades_written: int
    contracts_queried_for_trades: int
    market_shards: tuple[str, ...]
    trade_shards: tuple[str, ...]
    raw_hashes: tuple[str, ...]
    refusals: tuple[dict[str, Any], ...]
    flags: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "layer": KALSHI_OWN_LAYER,
            "market_layer": MARKET_LAYER,
            "root": str(self.root),
            "series": list(self.series),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "partition": dict(self.partition),
            "markets_written": self.markets_written,
            "trades_written": self.trades_written,
            "contracts_queried_for_trades": self.contracts_queried_for_trades,
            "market_shards": list(self.market_shards),
            "trade_shards": list(self.trade_shards),
            "raw_hashes": list(self.raw_hashes),
            "refusals": [dict(item) for item in self.refusals],
            "flags": list(self.flags),
        }


def declared_series(cohort_config: str | Path) -> tuple[str, ...]:
    """The declared policy series, read from the cohort declaration.

    The capture universe is a declared cohort decision, so it is read from the file
    that declares it rather than carried as a fallback here. A declaration naming
    none is refused: the alternative is a sweep of every listed contract.
    """
    path = Path(cohort_config)
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise MarketCaptureError(f"cohort declaration {path} could not be read: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise MarketCaptureError(f"cohort declaration {path} is not a YAML mapping")
    series = tuple(str(name) for name in (payload.get(CAPTURE_POLICY_SERIES_KEY) or ()))
    if not series:
        raise MarketCaptureError(
            f"{path} declares no {CAPTURE_POLICY_SERIES_KEY}; the capture universe is a "
            "declared cohort decision and this module keeps no fallback list of its own"
        )
    return series


def _decimal(value: Any, *, field: str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise MarketCaptureError(f"{field} is a boolean, not a number")
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise MarketCaptureError(f"{field} is an empty string")
        try:
            return Decimal(text)
        except InvalidOperation as exc:
            raise MarketCaptureError(f"{field} is not a decimal string: {value!r}") from exc
    raise MarketCaptureError(f"{field} is {type(value).__name__}, expected a decimal string")


def _cents(value: Any, *, field: str) -> int:
    """One fixed-point dollar value as exact integer cents.

    Multiplying the exact decimal by 100 and requiring an integral result keeps the
    conversion lossless. A value that is not a whole number of cents is refused
    rather than rounded, because the shard's unit is cents and a rounded cent is a
    price the venue never printed.
    """
    scaled = _decimal(value, field=field) * _CENTS
    if scaled != scaled.to_integral_value():
        raise MarketCaptureError(
            f"{field} is {value!r}, which is not a whole number of cents; rounding here "
            "would store a value the venue never published"
        )
    return int(scaled)


def _instant(value: Any, *, field: str) -> dt.datetime:
    if isinstance(value, dt.datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(text)
        except ValueError as exc:
            raise MarketCaptureError(f"{field} is not a parseable instant: {value!r}") from exc
    else:
        raise MarketCaptureError(f"{field} is {value!r}, which is not a timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_instant(value: Any, *, field: str) -> dt.datetime | None:
    if value is None or value == "":
        return None
    return _instant(value, field=field)


def _optional_count(value: Any, *, field: str) -> Decimal | None:
    """One venue activity count as an exact decimal, or ``None`` when unstated.

    ``None`` is written as a null rather than a zero: zero is a quantity the venue
    did not report, and the vendor archive's own ``volume`` column holds a zero
    where the venue states a real value, so a zero here would be read as agreement
    with a number the venue never published.

    The value is kept exact rather than coerced to an integer. The venue states
    counts as fixed-point strings and really does publish fractional ones, so
    rounding would store a quantity the venue never reported.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise MarketCaptureError(f"{field} is a boolean, not a count")
    return _decimal(value, field=field)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value) or None


def market_row_from_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """One listing record as a row in the captured market shard's layout.

    Fields the listing does not carry are written as null rather than defaulted.
    ``rules_primary`` and ``rules_secondary`` are carried verbatim: they are the
    contract's published rule text, the text a later rule-vintage check reads, so
    reformatting or truncating them here would damage the only copy of the evidence
    this capture exists to preserve.
    """
    ticker = record.get("ticker")
    if not isinstance(ticker, str) or not ticker:
        raise MarketCaptureError("market record has no usable 'ticker'")
    prices: dict[str, int | None] = {}
    for alias, column in _CENT_FIELDS:
        value = record.get(alias)
        prices[column] = None if value is None else _cents(value, field=alias)
    return {
        "ticker": ticker,
        "event_ticker": _optional_text(record.get("event_ticker")),
        "market_type": _optional_text(record.get("market_type")),
        "title": _optional_text(record.get("title")),
        "yes_sub_title": _optional_text(record.get("yes_sub_title")),
        "no_sub_title": _optional_text(record.get("no_sub_title")),
        "status": _optional_text(record.get("status")),
        **prices,
        "volume": _optional_count(record.get("volume_fp"), field="volume_fp"),
        "volume_24h": _optional_count(record.get("volume_24h_fp"), field="volume_24h_fp"),
        "open_interest": _optional_count(record.get("open_interest_fp"), field="open_interest_fp"),
        "result": _optional_text(record.get("result")),
        "rules_primary": _optional_text(record.get("rules_primary")),
        "rules_secondary": _optional_text(record.get("rules_secondary")),
        "created_time": _optional_instant(record.get("created_time"), field="created_time"),
        "open_time": _optional_instant(record.get("open_time"), field="open_time"),
        "close_time": _optional_instant(record.get("close_time"), field="close_time"),
    }


def trade_row_from_record(
    record: Mapping[str, Any], *, provenance: Provenance, clock: Clock
) -> dict[str, Any]:
    """One trade record as a row in the captured trade shard's layout.

    Normalization goes through
    :func:`~market_propagation.ingest.normalize.normalize_kalshi_trade`, so the
    venue's price and quantity are read by the same code the audit path uses. That
    normalizer yields the event-axis price on the 0-1 scale while the shard stores
    integer cents, so the cents come from the venue's own fixed-point fields and are
    then cross-checked against the normalized price. A disagreement is refused
    rather than written: it means the parser and the shard would disagree about the
    same print.

    The side is read from the record's own field, and a record whose deprecated and
    canonical side fields disagree is refused rather than resolved by preference.
    The shard's ``taker_side`` is the same column the archive carries, so writing a
    side the venue contradicted itself about would put an unreadable direction into
    a layer the archive reader treats as one.
    """
    trade = normalize_kalshi_trade(record, clock=clock, provenance=provenance)
    conflict = trade_direction_conflicts(record)
    if conflict["conflict"]:
        raise MarketCaptureError(
            f"trade {record.get('trade_id')!r} states disagreeing side fields "
            f"({conflict['agents']}); the deprecated and canonical fields disagree, so "
            "direction is not usable for this record and is not guessed"
        )
    cents: dict[str, int] = {}
    for field in _TRADE_PRICE_FIELDS:
        value = record.get(field)
        if value is None:
            raise MarketCaptureError(
                f"trade {record.get('trade_id')!r} states no {field}; the shard stores both "
                "sides in cents, so an absent side is refused rather than written as a zero"
            )
        cents[field] = _cents(value, field=field)
    if Decimal(cents["yes_price_dollars"]) != trade.price * _CENTS:
        raise MarketCaptureError(
            f"trade {record.get('trade_id')!r} normalizes to price {trade.price} while its own "
            f"yes_price_dollars is {record.get('yes_price_dollars')!r}; the parser and the shard "
            "would disagree about one print"
        )
    count = _optional_count(record.get(_COUNT_FIELD), field=_COUNT_FIELD)
    if count is None:
        raise MarketCaptureError(
            f"trade {record.get('trade_id')!r} carries no {_COUNT_FIELD}; a shard row with no "
            "quantity is not a row this capture can write"
        )
    if trade.size != count:
        raise MarketCaptureError(
            f"trade {record.get('trade_id')!r} normalizes to size {trade.size} while its own "
            f"{_COUNT_FIELD} is {record.get(_COUNT_FIELD)!r}; the parser and the shard would "
            "disagree about one quantity"
        )
    return {
        "trade_id": trade.trade_id,
        "ticker": trade.contract_id,
        "count": count,
        "yes_price": cents["yes_price_dollars"],
        "no_price": cents["no_price_dollars"],
        "taker_side": conflict["agents"]["outcome"] or conflict["agents"]["legacy"],
        "created_time": _instant(record.get("created_time"), field="created_time"),
    }


def _write_shard(
    rows: Sequence[Mapping[str, Any]],
    path: Path,
    *,
    columns: tuple[tuple[str, pa.DataType], ...],
    sort_by: Sequence[str],
) -> str:
    """Write one captured shard atomically and return its content hash.

    The file is linked into place through a temporary, so a reader never observes a
    half-written shard. Rows are ordered by their identifying columns, so two runs
    over the same listing produce byte-identical shards and a re-capture adds no
    second copy of rows the archive already holds.
    """
    ordered = sorted(rows, key=lambda row: tuple(str(row.get(name) or "") for name in sort_by))
    arrays = {
        name: pa.array([row.get(name) for row in ordered], type=dtype) for name, dtype in columns
    }
    table = pa.Table.from_arrays(
        [arrays[name] for name, _ in columns], schema=pa.schema(list(columns))
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy", version="2.6", write_statistics=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".partial")
    temporary.write_bytes(sink.getvalue().to_pybytes())
    temporary.replace(path)
    return hash_file(path)


def write_capture_shards(
    root: str | Path,
    *,
    market_rows: Sequence[Mapping[str, Any]],
    trade_rows: Sequence[Mapping[str, Any]],
    stamp: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Write the captured market and trade shards for one window.

    The two kinds go to separate directories so the market universe and the
    transaction tape are never reachable through one glob: a pattern that matched
    both would let a candidate-universe read silently include trade rows. The shard
    names carry the window's closing instant, so a reader can see which period a
    shard covers without opening it.
    """
    base = Path(root)
    written_markets: list[str] = []
    written_trades: list[str] = []
    if market_rows:
        path = base / "markets" / f"markets-{stamp}.parquet"
        _write_shard(market_rows, path, columns=MARKET_COLUMNS, sort_by=("ticker", "event_ticker"))
        written_markets.append(str(path))
    if trade_rows:
        path = base / "trades" / f"trades-{stamp}.parquet"
        _write_shard(trade_rows, path, columns=TRADE_COLUMNS, sort_by=("ticker", "created_time"))
        written_trades.append(str(path))
    return tuple(written_markets), tuple(written_trades)


def _stamp(window_end: dt.datetime) -> str:
    """The shard name for one window: its closing instant, to the second."""
    return window_end.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def capture_window(
    *,
    root: str | Path,
    series: Sequence[str],
    window_start: dt.datetime,
    window_end: dt.datetime,
    raw_store: RawStore,
    transport: HttpTransport,
    max_pages: int = 25,
    limit: int = 200,
    trade_limit: int = 1000,
    trade_max_pages: int = 20,
    max_contracts: int | None = 400,
) -> MarketCapture:
    """Capture one window's listing and trades for the declared series.

    The partition decision is taken first and recorded, and only the endpoints it
    names are queried. A refusal is recorded per series and per request rather than
    ending the run, so a partial capture reports exactly what it obtained and what
    it did not instead of presenting a short capture as a complete one.

    ``max_contracts`` bounds how many captured contracts have their trades
    requested, because the trade route filters by ticker and an unbounded per-ticker
    walk is a pull whose size depends on how many contracts the venue happens to
    list. The bound is applied to a sorted ticker list, so it stops at the same
    contract on every run, and a bound that hides contracts is reported.
    """
    client = KalshiClient(raw_store, transport=transport, base_url=KALSHI_BASE_URL)
    decision = client.resolve_partition(window_start, window_end)
    if not decision.available:
        raise MarketCaptureError(
            f"no endpoint serves the window [{window_start.isoformat()}, "
            f"{window_end.isoformat()}] under the venue's own cutoff "
            f"({decision.cutoff.market_settled_ts.isoformat()}): {decision.reason}"
        )
    historical = decision.partition == "historical"

    refusals: list[dict[str, Any]] = []
    raw_hashes: list[str] = [decision.cutoff.raw_hash]
    market_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    seen_markets: set[str] = set()
    seen_trades: set[str] = set()
    flags: set[str] = set()

    for series_ticker in series:
        try:
            listed = _list_markets(
                client,
                historical=historical,
                series_ticker=series_ticker,
                limit=limit,
                max_pages=max_pages,
            )
        except (TransportError, WireShapeError) as exc:
            refusals.append(
                {"series_ticker": series_ticker, "stage": "markets", "reason": str(exc)}
            )
            flags.add("listing_refused")
            continue
        raw_hashes.extend(listed.raw_hashes)
        if not listed.complete:
            flags.add("listing_incomplete")
            refusals.append(
                {
                    "series_ticker": series_ticker,
                    "stage": "markets",
                    "reason": f"pagination stopped before the last page: {listed.stop_reason}",
                }
            )
        for record in listed.items:
            if not isinstance(record, Mapping):
                refusals.append({"stage": "markets", "reason": "listing entry is not an object"})
                continue
            ticker = str(record.get("ticker") or "")
            if not ticker or ticker in seen_markets:
                if not ticker:
                    refusals.append({"stage": "markets", "reason": "listing entry has no ticker"})
                continue
            seen_markets.add(ticker)
            try:
                market_rows.append(market_row_from_record(record))
            except MarketCaptureError as exc:
                refusals.append({"contract_id": ticker, "stage": "markets", "reason": str(exc)})
                flags.add("market_row_refused")

    queried = sorted(seen_markets)
    if max_contracts is not None and len(queried) > max_contracts:
        flags.add("contracts_skipped_by_bound")
        refusals.append(
            {
                "stage": "trades",
                "reason": f"max_contracts={max_contracts} hid "
                f"{len(queried) - max_contracts} contract(s) from the trade walk",
            }
        )
        queried = queried[:max_contracts]

    for ticker in queried:
        try:
            printed = _list_trades(
                client,
                historical=historical,
                ticker=ticker,
                window_start=window_start,
                window_end=window_end,
                limit=trade_limit,
                max_pages=trade_max_pages,
            )
        except (TransportError, WireShapeError) as exc:
            refusals.append({"contract_id": ticker, "stage": "trades", "reason": str(exc)})
            flags.add("trades_refused")
            continue
        raw_hashes.extend(printed.raw_hashes)
        if not printed.complete:
            flags.add("trades_incomplete")
            refusals.append(
                {
                    "contract_id": ticker,
                    "stage": "trades",
                    "reason": f"pagination stopped before the last page: {printed.stop_reason}",
                }
            )
        for record, origin in zip(printed.items, printed.origins, strict=True):
            if not isinstance(record, Mapping):
                refusals.append({"stage": "trades", "reason": "trade entry is not an object"})
                continue
            trade_id = str(record.get("trade_id") or "")
            if trade_id and trade_id in seen_trades:
                continue
            if trade_id:
                seen_trades.add(trade_id)
            try:
                source_time = _instant(record.get("created_time"), field="created_time")
                trade_rows.append(
                    trade_row_from_record(
                        record,
                        # The page's own hash addresses the bytes this row was read
                        # from, so the locator resolves against archived data rather
                        # than against a re-serialization of the row.
                        provenance=Provenance(
                            raw_hash=origin.raw_hash,
                            record_id=(
                                f"{ticker}.{trade_id}"
                                if trade_id
                                else f"{ticker}.p{origin.page_index}r{origin.record_index}"
                            ),
                            source=f"capture.{KALSHI_OWN_LAYER}",
                        ),
                        # The venue states when the print happened and no receipt
                        # time for the print, so no availability interval is
                        # invented and the clock stays source-time only.
                        clock=Clock.historical(source_time),
                    )
                )
            except (MarketCaptureError, WireShapeError, ValueError) as exc:
                refusals.append({"contract_id": ticker, "stage": "trades", "reason": str(exc)})
                flags.add("trade_row_refused")

    stamp = _stamp(window_end)
    market_shards, trade_shards = write_capture_shards(
        root, market_rows=market_rows, trade_rows=trade_rows, stamp=stamp
    )
    return MarketCapture(
        root=Path(root),
        series=tuple(series),
        window_start=window_start,
        window_end=window_end,
        partition=decision.as_dict(),
        markets_written=len(market_rows),
        trades_written=len(trade_rows),
        contracts_queried_for_trades=len(queried),
        market_shards=market_shards,
        trade_shards=trade_shards,
        raw_hashes=tuple(dict.fromkeys(raw_hashes)),
        refusals=tuple(refusals),
        flags=tuple(sorted(flags)),
    )


def _list_markets(
    client: KalshiClient,
    *,
    historical: bool,
    series_ticker: str,
    limit: int,
    max_pages: int,
) -> PaginationResult:
    """The listing route the partition decision selected, for one series."""
    if historical:
        return client.list_historical_markets(
            series_ticker=series_ticker, limit=limit, max_pages=max_pages
        )
    return client.list_markets(series_ticker=series_ticker, limit=limit, max_pages=max_pages)


def _list_trades(
    client: KalshiClient,
    *,
    historical: bool,
    ticker: str,
    window_start: dt.datetime,
    window_end: dt.datetime,
    limit: int,
    max_pages: int,
) -> PaginationResult:
    """The trades route the partition decision selected, for one contract.

    Both routes filter by ticker, which is why the walk is per captured contract
    rather than per series: neither route accepts a series filter, so a series-wide
    read would be the global stream, and that is a pull whose size nobody declared.
    """
    min_ts = int(window_start.timestamp())
    max_ts = int(window_end.timestamp())
    if historical:
        return client.get_historical_trades(
            ticker=ticker, min_ts=min_ts, max_ts=max_ts, limit=limit, max_pages=max_pages
        )
    return client.get_live_trades(
        ticker=ticker, min_ts=min_ts, max_ts=max_ts, limit=limit, max_pages=max_pages
    )


# --- Two-source market authority ---------------------------------------------
#
# Two layers hold Kalshi market records: the vendor archive and this repository's
# own capture. They are not interchangeable, and the failure this section exists to
# prevent is a reader globbing both and counting one contract twice.
#
# Coverage is decided per window from each layer's own rows: the declared-series
# contracts the layer holds whose listing interval meets the window. That is exactly
# the denominator a candidate-universe read would draw, so an overlap in it is the
# double-count condition rather than a proxy for it.

#: The bases a market-layer resolution can rest on. Each names a different fact, so a
#: reader can tell a governed window from one this module refused to guess at.
BASIS_SOLE_COVERING_LAYER = "sole_covering_layer"
BASIS_LAYER_NAMED_EXPLICITLY = "layer_named_explicitly"
BASIS_NO_LAYER_COVERS_THE_WINDOW = "no_layer_covers_the_window"
BASIS_BOTH_LAYERS_COVER_THE_WINDOW = "both_layers_cover_the_window"

#: The declared role that marks a layer as a market record layer. Layers are
#: discovered through it rather than listed here, so a market layer added to the
#: configuration reaches the authority decision without an edit in this module.
MARKET_LAYER_ROLE = "market_metadata_snapshot"


class MarketLayerAmbiguity(MarketCaptureError):
    """A window both market layers cover, refused rather than resolved silently."""


@dataclass(frozen=True, slots=True)
class LayerCoverage:
    """One market layer's coverage of a window, measured from its own rows."""

    layer: str
    path_pattern: str
    shard_count: int
    contracts: tuple[str, ...]

    @property
    def covers(self) -> bool:
        return bool(self.contracts)

    def as_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "path_pattern": self.path_pattern,
            "shard_count": self.shard_count,
            "contracts": len(self.contracts),
            "covers": self.covers,
        }


@dataclass(frozen=True, slots=True)
class MarketLayerResolution:
    """Which market layer governs a window, on what basis, and what it saw."""

    layer: str | None
    basis: str
    available: bool
    reason: str
    coverages: tuple[LayerCoverage, ...]
    overlapping_contracts: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "basis": self.basis,
            "available": self.available,
            "reason": self.reason,
            "coverages": [coverage.as_dict() for coverage in self.coverages],
            "overlapping_contracts": list(self.overlapping_contracts),
        }


def declared_market_layers(
    config_path: str | Path = DEFAULT_PIPELINE_CONFIG,
) -> tuple[tuple[str, str], ...]:
    """Each declared market layer's name and pattern, in declaration order.

    The two layers are discovered from the configuration rather than listed here. A
    market layer this module has never heard of still reaches the authority decision,
    and a hardcoded pair cannot silently stop matching the file.
    """
    path = Path(config_path)
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise MarketCaptureError(f"pipeline configuration {path} could not be read: {exc}") from exc
    layers = ((payload or {}).get("inputs") or {}).get("layers") or []
    named = tuple(
        (str(entry["name"]), str(entry["path_pattern"]))
        for entry in layers
        if isinstance(entry, Mapping) and entry.get("role") == MARKET_LAYER_ROLE
    )
    if not named:
        raise MarketCaptureError(
            f"{path} declares no layer with role {MARKET_LAYER_ROLE!r}; the two-source "
            "authority rule has nothing to resolve"
        )
    return named


def _layer_coverage(
    root: Path,
    *,
    layer: str,
    pattern: str,
    window_start: dt.datetime,
    window_end: dt.datetime,
    series: Sequence[str],
) -> LayerCoverage:
    """One layer's declared-series contracts whose listing interval meets the window.

    A layer with no shard on this checkout is a coverage of nothing rather than an
    error: an absent layer must not read as an overlap, and the shard count recorded
    alongside is what lets a reader tell an absent layer from a covering one.
    """
    files = sorted(str(path) for path in root.glob(pattern))
    if not files:
        return LayerCoverage(layer=layer, path_pattern=pattern, shard_count=0, contracts=())
    connection = duckdb.connect()
    try:
        connection.execute("SET TimeZone='UTC'")
        placeholders = ", ".join("?" for _ in series)
        rows = connection.execute(
            f"""
            SELECT DISTINCT ticker
            FROM read_parquet({files!r})
            WHERE regexp_extract(ticker, '^[A-Z]+') IN ({placeholders})
              AND open_time <= ?
              AND (close_time IS NULL OR close_time >= ?)
            """,
            [*series, window_end, window_start],
        ).fetchall()
    finally:
        connection.close()
    declared = set(series)
    contracts = sorted(str(row[0]) for row in rows if series_of(str(row[0])) in declared)
    return LayerCoverage(
        layer=layer,
        path_pattern=pattern,
        shard_count=len(files),
        contracts=tuple(contracts),
    )


def resolve_market_layer(
    root: str | Path,
    *,
    window_start: dt.datetime,
    window_end: dt.datetime,
    series: Sequence[str],
    config_path: str | Path = DEFAULT_PIPELINE_CONFIG,
    layer: str | None = None,
) -> MarketLayerResolution:
    """Decide which market layer governs a window, or refuse to decide.

    Two layers hold Kalshi market records, and they overlap in contract rather than
    only in time: a contract present in both is the same contract from two vintages,
    so a read drawing from both counts it twice and inflates the denominator every
    rate is measured against. The decision is a rule, not a preference:

    * a layer named explicitly governs, and the resolution records the divergence it
      was chosen over rather than hiding it;
    * otherwise a window exactly one layer covers is governed by that layer;
    * a window both layers cover is **refused**, because the choice belongs to the
      caller who can state it and not to this function, which can only infer it;
    * a window neither layer covers is refused as uncovered, and is never served from
      a layer that does not cover it.

    ``root`` is the configured archive root; nothing outside it is read.
    """
    start = _instant(window_start, field="window_start")
    end = _instant(window_end, field="window_end")
    if end < start:
        raise MarketCaptureError("window_end must not precede window_start")
    base = Path(root)
    coverages = tuple(
        _layer_coverage(
            base,
            layer=name,
            pattern=pattern,
            window_start=start,
            window_end=end,
            series=series,
        )
        for name, pattern in declared_market_layers(config_path)
    )
    by_layer = {coverage.layer: coverage for coverage in coverages}
    covering = [coverage for coverage in coverages if coverage.covers]

    if layer is not None:
        if layer not in by_layer:
            raise MarketCaptureError(
                f"{layer!r} is not a declared market layer; the declared names are "
                f"{sorted(by_layer)}"
            )
        return MarketLayerResolution(
            layer=layer,
            basis=BASIS_LAYER_NAMED_EXPLICITLY,
            available=True,
            reason=(
                f"{layer} was named explicitly; {len(covering)} of {len(coverages)} declared "
                "layer(s) cover this window"
            ),
            coverages=coverages,
            overlapping_contracts=(),
        )

    if not covering:
        counts = ", ".join(
            f"{coverage.layer}={coverage.shard_count} shard(s)" for coverage in coverages
        )
        return MarketLayerResolution(
            layer=None,
            basis=BASIS_NO_LAYER_COVERS_THE_WINDOW,
            available=False,
            reason=(
                "no declared market layer holds a declared-series contract whose listing "
                f"interval meets [{start.isoformat()}, {end.isoformat()}]; the layers hold "
                f"{counts}"
            ),
            coverages=coverages,
            overlapping_contracts=(),
        )

    if len(covering) == 1:
        only = covering[0]
        return MarketLayerResolution(
            layer=only.layer,
            basis=BASIS_SOLE_COVERING_LAYER,
            available=True,
            reason=(
                f"{only.layer} is the only declared market layer covering this window "
                f"({len(only.contracts)} contract(s)); every other layer holds no "
                "declared-series contract whose listing interval meets it"
            ),
            coverages=coverages,
            overlapping_contracts=(),
        )

    overlap = sorted(set.intersection(*(set(coverage.contracts) for coverage in covering)))
    return MarketLayerResolution(
        layer=None,
        basis=BASIS_BOTH_LAYERS_COVER_THE_WINDOW,
        available=False,
        reason=(
            f"{len(covering)} declared market layers cover this window and they share "
            f"{len(overlap)} contract(s); drawing from both would count each of those twice, "
            "so the governing layer must be named explicitly"
        ),
        coverages=coverages,
        overlapping_contracts=tuple(overlap),
    )
