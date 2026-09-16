"""Bounded normalization of the external Kalshi and Polymarket trade archives.

Two archive layers hold prediction-market trades, and they do not agree on what a
price is. Kalshi stores two integer cent prices per row, and the archive's own
README documents each as lying in 1..99 while the real data contains a zero cent
price and rows whose two prices do not sum to 100. The cleaned Polymarket layers
store one float64 price for a token alongside that token's ``outcome_seq``, so a
row's relationship to the event it prices has to be recomputed rather than read
off the eventual winner.

Three decisions follow from that, and they are why this module is not a thin
column rename.

**Occurrence identity is the archive locator, not the venue's id.** A Kalshi row
carries a ``trade_id`` and a Polymarket row carries none at all, but both can
repeat: a two-sided fill or a re-ingested block produces the same fill twice under
two ids, or under one. A panel that silently collapsed those would lose the
frequency this study measures. So ``record_id`` is always the shard digest and the
row's position in the file, and the venue's own id travels in ``trade_id`` where a
downstream join can use it as evidence rather than as identity. The position is
the row's index in the shard file, not an offset into the layer, because the row
bound is applied per file and a cross-file offset would have to be recomputed
before every query.

**Nothing is clipped and nothing is repaired.** A zero cent Kalshi price is
recorded as a zero cent price with a flag, because a price the venue printed is
data and moving it to one cent would be inventing a trade. A ``yes_price +
no_price`` that is not 100 is reported, not normalized. A price outside the
documented range is reported, not clamped. Each of those is a boundary fact about
the archive, and a reader who cannot see it cannot judge the archive.

**A missing quantity stays null.** The cleaned Polymarket layers omit
``token_amount``, and :class:`~market_propagation.domain.HistoricalTrade` refuses
to carry a size that is not verified, so a Polymarket row's size is null with the
reason attached and it stays out of quantity-weighted flow. A Kalshi ``count`` is a
real source quantity on Kalshi's own axis and is carried as one.

Nothing here reads the eventual winner. ``winning_outcome_label`` and
``resolution_status`` are never selected and a ``column_map`` naming them is
refused, because a row's orientation has to follow the rule in force when it
printed, not how it later settled.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..domain import UTC, Clock, HistoricalTrade, Provenance, parse_utc_time
from ..storage import DatasetRef, read_parquet, write_parquet

__all__ = [
    "DEFAULT_PRICE_TOLERANCE",
    "KALSHI_EVENT_AXIS",
    "KALSHI_LAYER",
    "KALSHI_VENUE",
    "POLYMARKET_EVENT_AXIS_OUTCOME_1",
    "POLYMARKET_EVENT_AXIS_OUTCOME_2",
    "POLYMARKET_NEG_RISK_LAYER",
    "POLYMARKET_STANDARD_LAYER",
    "POLYMARKET_VENUE",
    "ExternalExtraction",
    "extract_trades",
    "kalshi_trade_from_row",
    "load_trades",
    "polymarket_trade_from_row",
    "row_occurrence_id",
    "write_trades",
]

#: Layer names, and the venue each one's rows belong to.
KALSHI_LAYER = "kalshi_trades"
POLYMARKET_STANDARD_LAYER = "polymarket_daily_aligned"
POLYMARKET_NEG_RISK_LAYER = "polymarket_daily_aligned_multi"
KALSHI_VENUE = "kalshi"
POLYMARKET_VENUE = "polymarket"

#: How a row's price is projected onto the event axis. Kalshi's ``yes_price``
#: already prices the event's yes side, so the projection is the identity and it
#: is still named, so a reader can see that no transform was applied. A
#: Polymarket row prices one token, and which side of the event that token is
#: depends on ``outcome_seq``, so the mapping is named per sequence rather than
#: once for the venue.
KALSHI_EVENT_AXIS = "yes_price_is_event_axis"
POLYMARKET_EVENT_AXIS_OUTCOME_1 = "outcome_seq_1_is_price"
POLYMARKET_EVENT_AXIS_OUTCOME_2 = "outcome_seq_2_is_1_minus_price"

#: Polymarket archives a float64 price. Two prices compared after float64 storage
#: agree only within a tolerance, so a difference below the source precision is
#: rounding noise rather than a disagreement, and flagging it would report a
#: defect where the archive has none.
DEFAULT_PRICE_TOLERANCE = 1.0e-9

#: Canonical field to archive column, per layer. These are the documented
#: schemas, and ``column_map`` renames an individual layer onto them when a shard
#: carries different names.
_KALSHI_COLUMNS: Mapping[str, str] = {
    "contract_id": "ticker",
    "size": "count",
    "yes_price": "yes_price",
    "no_price": "no_price",
    "direction": "taker_side",
    "trade_id": "trade_id",
    "time": "created_time",
}

_POLYMARKET_COLUMNS: Mapping[str, str] = {
    "contract_id": "condition_id",
    "token_id": "asset_id",
    "outcome_seq": "outcome_seq",
    "price": "price",
    "direction": "taker_direction",
    "event_direction": "D",
    "event_price": "p_event",
    "time": "block_timestamp",
}

_LAYERS: Mapping[str, Mapping[str, str]] = {
    KALSHI_LAYER: _KALSHI_COLUMNS,
    POLYMARKET_STANDARD_LAYER: _POLYMARKET_COLUMNS,
    POLYMARKET_NEG_RISK_LAYER: _POLYMARKET_COLUMNS,
}

#: The layer's own time column and the unit it is stored in. Kalshi archives a
#: timezone-aware microsecond timestamp; the cleaned Polymarket layers archive
#: Unix epoch seconds, which are never milliseconds.
_LAYER_TIME: Mapping[str, tuple[str, str]] = {
    KALSHI_LAYER: ("created_time", "timestamp_us_utc"),
    POLYMARKET_STANDARD_LAYER: ("block_timestamp", "epoch_seconds"),
    POLYMARKET_NEG_RISK_LAYER: ("block_timestamp", "epoch_seconds"),
}

#: Documented archive column to canonical field, per layer. The query projects
#: each canonical field's column under the name in the first position, so a
#: renamed shard still reaches a mapper reading the documented names.
_PROJECTIONS: Mapping[str, tuple[tuple[str, str], ...]] = {
    KALSHI_LAYER: (
        ("trade_id", "trade_id"),
        ("ticker", "contract_id"),
        ("count", "size"),
        ("yes_price", "yes_price"),
        ("no_price", "no_price"),
        ("taker_side", "direction"),
    ),
    POLYMARKET_STANDARD_LAYER: (
        ("condition_id", "contract_id"),
        ("asset_id", "token_id"),
        ("outcome_seq", "outcome_seq"),
        ("price", "price"),
        ("taker_direction", "direction"),
        ("D", "event_direction"),
        ("p_event", "event_price"),
    ),
}
_PROJECTIONS[POLYMARKET_NEG_RISK_LAYER] = _PROJECTIONS[POLYMARKET_STANDARD_LAYER]

#: The fields a row cannot be normalized without. Everything else in a layer's
#: projection is optional, so a shard that omits one yields a null field while a
#: shard that omits one of these is skipped with the column named: a row with no
#: price or no source time is not a row this module can report.
_REQUIRED_FIELDS: Mapping[str, tuple[str, ...]] = {
    KALSHI_LAYER: ("contract_id", "size", "yes_price", "no_price"),
    POLYMARKET_STANDARD_LAYER: ("contract_id", "outcome_seq", "price"),
}
_REQUIRED_FIELDS[POLYMARKET_NEG_RISK_LAYER] = _REQUIRED_FIELDS[POLYMARKET_STANDARD_LAYER]

#: Column names this module never reads. A row's orientation comes from the rule
#: in force when it printed; the eventual winner and the resolution status are
#: outcomes, and a projection built on them would depend on information no
#: participant had when the trade printed.
_NEVER_SELECTED: tuple[str, ...] = ("winning_outcome_label", "resolution_status")

_SCHEMA_VERSION = "1"

#: Kalshi archives integer cents. The README documents 1..99 and the archive
#: contains 0, so the range is reported as a boundary fact rather than enforced
#: as an input constraint.
_CENTS_LOWER_BOUND = 0
_CENTS_UPPER_BOUND = 99
_CENTS_SUM = 100

FLAG_ZERO_CENT_PRICE = "zero_cent_price"
FLAG_CENTS_SUM_NOT_100 = "yes_no_cents_sum_not_100"
FLAG_CENTS_OUT_OF_RANGE = "price_outside_documented_cents_range"
FLAG_P_EVENT_DISAGREES = "p_event_disagrees_with_documented_axis"
FLAG_AMBIGUOUS_OUTCOME_AXIS = "ambiguous_outcome_axis"
FLAG_SHARD_DIGEST_UNKNOWN = "shard_digest_unknown"
FLAG_TIMESTAMP_BOUNDS_UNUSABLE = "shard_timestamp_bounds_unusable"
FLAG_MAX_ROWS_BOUND_APPLIED = "max_rows_bound_applied"

#: Why a shard was not read. The first two are the window verdicts; the rest name
#: an archive fact that makes reading the shard pointless or impossible.
SKIP_AFTER_WINDOW = "shard_statistics_after_window_end"
SKIP_BEFORE_WINDOW = "shard_statistics_before_window_start"
SKIP_ZERO_ROWS = "shard_holds_zero_rows"
SKIP_UNREADABLE = "shard_unreadable_in_inventory"
SKIP_TIME_COLUMN_ABSENT = "layer_time_column_absent_from_shard_schema"
SKIP_SCHEMA_UNREADABLE = "shard_file_schema_unreadable"

#: A digest may be spelled differently by an inventory, and a plain content hash
#: is not a digest of its own bytes. A name that cannot be resolved to a digest is
#: reported as unknown instead of being hashed: hashing a whole shard is only
#: worth doing over content this module actually read, and it reads a bounded
#: window rather than the file.
_DIGEST_KEYS = ("sha256", "shard_sha256", "content_hash")
_UNKNOWN_SHARD = "unknown-shard"
_HEX_DIGEST_RE = re.compile(r"^[0-9a-f]{16,}$")

_ROW_POSITION_FIELD = "__row_position__"
_SOURCE_EPOCH_FIELD = "__source_epoch__"

_ISO_OFFSET_RE = re.compile(r"(?:[Zz]|[+-]\d{2}:?\d{2})$")

#: A second count at or beyond this instant is not a Unix seconds value. The
#: archive states its unit, so a value in another unit is refused rather than
#: rescaled: dividing a millisecond value by a thousand would be right for this
#: archive and silently wrong for the next one, and the two are told apart by the
#: magnitude the archive declares.
_IMPLAUSIBLE_SECONDS = 4_102_444_800

#: The Unix epoch, as the origin every archive time is measured from. Adding a
#: timedelta to it is exact in both directions, unlike integer division, which
#: truncates toward zero and so moves a pre-epoch instant forward.
_EPOCH = dt.datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class ExternalExtraction:
    """One bounded read of one archive layer, with the bounds it applied.

    ``trades`` holds only rows whose ``clock.source_time`` falls inside the
    window. A row with no readable time is counted in ``rows_scanned`` and stays
    out of ``trades``, because a row that cannot be placed in the window cannot be
    linked to a release. That drop and the ``max_rows`` bound are both reported,
    so a consumer can tell a complete extraction from a truncated one instead of
    inferring it from a row count.
    """

    layer: str
    venue: str
    window_start: dt.datetime
    window_end: dt.datetime
    shards_read: tuple[str, ...]
    shards_skipped: tuple[tuple[str, str], ...]
    rows_scanned: int
    trades: tuple[HistoricalTrade, ...]
    bounded: bool
    max_rows_applied: int | None
    flags: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "window_start",
            parse_utc_time(self.window_start, field_name="ExternalExtraction.window_start"),
        )
        object.__setattr__(
            self,
            "window_end",
            parse_utc_time(self.window_end, field_name="ExternalExtraction.window_end"),
        )
        if self.window_end < self.window_start:
            raise ValueError(
                f"window_end {self.window_end} precedes window_start {self.window_start}"
            )

    @property
    def trade_count(self) -> int:
        return len(self.trades)

    def as_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "venue": self.venue,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "shards_read": list(self.shards_read),
            "shards_skipped": [list(item) for item in self.shards_skipped],
            "rows_scanned": self.rows_scanned,
            "trade_count": len(self.trades),
            "bounded": self.bounded,
            "max_rows_applied": self.max_rows_applied,
            "flags": list(self.flags),
        }


def row_occurrence_id(shard_hash: str, row_position: int) -> str:
    """Occurrence locator for one archive row: which shard, which row inside it.

    This is a synthetic identity for a row the archive does not identify, so it
    must not be a digest of the row's own bytes: two identical repeated fills are
    two occurrences, and a content digest would collapse them into one. The shard
    digest names the file so the id is stable across runs, and the row position
    separates occurrences within it.
    """
    if not isinstance(shard_hash, str) or not shard_hash:
        raise ValueError("row_occurrence_id requires a non-empty shard hash")
    if isinstance(row_position, bool) or not isinstance(row_position, int):
        raise TypeError(f"row_occurrence_id row_position must be an int, got {row_position!r}")
    if row_position < 0:
        raise ValueError(f"row_occurrence_id row_position must not be negative: {row_position}")
    return f"{shard_hash}:{row_position}"


def kalshi_trade_from_row(
    record: Any,
    *,
    shard_hash: str,
    shard_relative_path: str,
    row_position: int,
    venue: str = KALSHI_VENUE,
) -> HistoricalTrade:
    """One Kalshi archive row as a bounded historical trade.

    Prices stay integer cents in ``raw_price`` and ``secondary_price`` and become
    dollars in ``price`` by exact division, so no float64 rounding enters a price
    the archive stored as an integer. The boundary facts the venue itself prints
    are reported as flags and never repaired.

    ``event_direction`` follows ``taker_side``, which is the side the taker
    crossed to: buying yes moves the event axis up, buying no moves it down. A row
    whose side is neither leaves the direction null rather than defaulting, since
    an unreadable side is not a flat signal.
    """
    yes_cents = _required_int(record, "yes_price")
    no_cents = _required_int(record, "no_price")
    created = _field(record, "created_time")
    count = _field(record, "count")
    ticker = _field(record, "ticker")
    trade_id = _field(record, "trade_id")
    taker_side = _field(record, "taker_side")
    location = f"{shard_relative_path}:{row_position}"
    if created is None:
        raise ValueError(
            f"kalshi row {location} carries no created_time; a row with no source time cannot "
            "be placed in a window"
        )
    if not isinstance(ticker, str) or not ticker:
        raise ValueError(
            f"kalshi row {location} carries no ticker; a row that cannot be attributed to a "
            "contract is not sealed"
        )
    if count is None:
        raise ValueError(f"kalshi row {location} carries no count; a missing quantity is not zero")
    if taker_side is not None and not isinstance(taker_side, str):
        raise TypeError(f"kalshi row {location} taker_side must be text or null")
    flags: list[str] = []
    if yes_cents + no_cents != _CENTS_SUM:
        flags.append(FLAG_CENTS_SUM_NOT_100)
    if yes_cents == 0 or no_cents == 0:
        flags.append(FLAG_ZERO_CENT_PRICE)
    if not (_CENTS_LOWER_BOUND <= yes_cents <= _CENTS_UPPER_BOUND) or not (
        _CENTS_LOWER_BOUND <= no_cents <= _CENTS_UPPER_BOUND
    ):
        flags.append(FLAG_CENTS_OUT_OF_RANGE)
    normalized_side = taker_side.strip().lower() if isinstance(taker_side, str) else ""
    if normalized_side == "yes":
        event_direction = 1
    elif normalized_side == "no":
        event_direction = -1
    else:
        event_direction = None
    price = Decimal(yes_cents) / _CENTS_SUM
    return HistoricalTrade(
        venue=venue,
        contract_id=ticker,
        price=price,
        raw_price_units="cents",
        price_precision="exact_integer_cents",
        size_quality="verified_source_quantity",
        clock=_source_clock(created),
        provenance=Provenance(
            raw_hash=shard_hash,
            record_id=row_occurrence_id(shard_hash, row_position),
            source=f"archive.{KALSHI_LAYER}",
            schema_version=_SCHEMA_VERSION,
        ),
        trade_id=trade_id if isinstance(trade_id, str) and trade_id else None,
        raw_price=Decimal(yes_cents),
        secondary_price=Decimal(no_cents),
        event_price=price,
        event_axis=KALSHI_EVENT_AXIS,
        direction=normalized_side or None,
        event_direction=event_direction,
        size=Decimal(count),
        flags=tuple(flags),
    )


def polymarket_trade_from_row(
    record: Any,
    *,
    layer: str,
    shard_hash: str,
    shard_relative_path: str,
    row_position: int,
    tolerance: float = DEFAULT_PRICE_TOLERANCE,
    venue: str = POLYMARKET_VENUE,
) -> HistoricalTrade:
    """One cleaned Polymarket row as a bounded historical trade.

    The row prices a token and ``outcome_seq`` says which side of its event that
    token is. The event projection is recomputed from that sequence and then
    compared against the stored ``p_event``, so the archive's own projection is
    treated as evidence about the archive: a disagreement is reported rather than
    silently resolved in either direction. An ``outcome_seq`` outside the
    documented pair leaves the event price and axis null with the
    ``ambiguous_outcome_axis`` flag, because an unlabelled token has no documented
    projection and naming one would invent an orientation.

    ``size`` is null and ``size_quality`` says why: the cleaned layers carry no
    ``token_amount``, and a quantity that is not in the archive is unknown rather
    than zero. Nothing is derived from ``winning_outcome_label`` or
    ``resolution_status``.
    """
    if layer not in (POLYMARKET_STANDARD_LAYER, POLYMARKET_NEG_RISK_LAYER):
        raise ValueError(f"polymarket normalization does not cover layer {layer!r}")
    location = f"{shard_relative_path}:{row_position}"
    condition_id = _field(record, "condition_id")
    asset_id = _field(record, "asset_id")
    outcome_seq = _field(record, "outcome_seq")
    price_value = _field(record, "price")
    taker_direction = _field(record, "taker_direction")
    axis_direction = _field(record, "D")
    stored_event_price = _field(record, "p_event")
    stamp = _field(record, "block_timestamp")
    if stamp is None:
        raise ValueError(
            f"polymarket row {location} carries no block_timestamp; a row with no source time "
            "cannot be placed in a window"
        )
    if not isinstance(condition_id, str) or not condition_id:
        raise ValueError(
            f"polymarket row {location} carries no condition_id; a row that cannot be "
            "attributed to a contract is not sealed"
        )
    if price_value is None:
        raise ValueError(f"polymarket row {location} carries no price; a missing price is not zero")
    if isinstance(outcome_seq, bool) or not isinstance(outcome_seq, int):
        raise ValueError(
            f"polymarket row {location} carries outcome_seq {outcome_seq!r}; the event axis is "
            "named by an integer sequence"
        )
    if isinstance(stamp, bool) or not isinstance(stamp, (int, dt.datetime)):
        raise TypeError(
            f"polymarket row {location} block_timestamp must be an integer epoch or an aware "
            f"instant, got {stamp!r}"
        )
    # The documented unit is Unix seconds, so an integer is read as seconds and a
    # millisecond magnitude is refused rather than rescaled. Rescaling would be a
    # guess about a unit the archive already states, and a wrong guess moves every
    # row by fifty years; a caller that has already converted the value hands over
    # the instant itself.
    if isinstance(stamp, int):
        if abs(stamp) >= _IMPLAUSIBLE_SECONDS:
            raise ValueError(
                f"polymarket row {location} carries block_timestamp {stamp}, which is not a "
                "Unix seconds value; the documented unit is seconds"
            )
        stamp = dt.datetime.fromtimestamp(stamp, tz=UTC)
    flags: list[str] = []
    # Converting through ``str`` keeps the source's own decimal spelling, so a
    # price stored as 0.6 stays 0.6 rather than the binary expansion of the
    # float64. It cannot recover precision float64 already lost, which is why the
    # precision is declared as the source's rather than claimed as exact.
    price = Decimal(str(price_value))
    if outcome_seq == 1:
        event_price: Decimal | None = price
        event_axis: str | None = POLYMARKET_EVENT_AXIS_OUTCOME_1
    elif outcome_seq == 2:
        event_price = Decimal(1) - price
        event_axis = POLYMARKET_EVENT_AXIS_OUTCOME_2
    else:
        event_price = None
        event_axis = None
        flags.append(FLAG_AMBIGUOUS_OUTCOME_AXIS)
    if (
        stored_event_price is not None
        and event_price is not None
        and abs(Decimal(str(stored_event_price)) - event_price) > Decimal(str(tolerance))
    ):
        flags.append(FLAG_P_EVENT_DISAGREES)
    # ``D`` is the archive's own stated axis direction and is carried through
    # unchanged. A missing one becomes an explicit flat 0 rather than a null: the
    # field is the row's stated direction, and a null there would be
    # indistinguishable from a row whose direction was never recorded.
    event_direction = 0 if axis_direction is None else int(axis_direction)
    if event_direction not in (-1, 0, 1):
        raise ValueError(
            f"polymarket row {location} carries D={axis_direction!r}; the documented axis "
            "direction is -1, 0 or 1"
        )
    return HistoricalTrade(
        venue=venue,
        contract_id=condition_id,
        token_id=asset_id if isinstance(asset_id, str) and asset_id else None,
        outcome_seq=outcome_seq,
        price=price,
        raw_price=price,
        raw_price_units="dollars",
        price_precision="float64_source_precision",
        size=None,
        size_quality="zero_price_row" if price == 0 else "unavailable_in_cleaned_layer",
        clock=_source_clock(stamp),
        provenance=Provenance(
            raw_hash=shard_hash,
            record_id=row_occurrence_id(shard_hash, row_position),
            source=f"archive.{layer}",
            schema_version=_SCHEMA_VERSION,
        ),
        direction=taker_direction if taker_direction else None,
        event_price=event_price,
        event_axis=event_axis,
        event_direction=event_direction,
        flags=tuple(flags),
    )


def extract_trades(
    root: str | Path,
    inventory: Any,
    *,
    layer: str,
    window_start: dt.datetime | str,
    window_end: dt.datetime | str,
    max_rows: int | None = None,
    batch_size: int = 65536,
    connection: Any | None = None,
    column_map: Mapping[str, str] | None = None,
    tickers: Sequence[str] | None = None,
) -> ExternalExtraction:
    """Read one layer's in-window rows, one shard at a time, and normalize them.

    Nothing materializes a whole layer. Each shard is queried with only the
    columns its mapping needs and a ``WHERE`` clause on the layer's own time
    column, so the row bound is applied by the reader instead of after the rows
    have arrived. A shard whose recorded timestamps fall outside the window is not
    queried at all, and the reason it was skipped is reported beside the read list
    so the selection can be audited after the fact. A recorded bound that cannot
    be read as an instant does not become a skip: the shard is read, and the
    unreadable bound is flagged, because a bound nobody can parse says nothing
    about the window.

    ``max_rows`` bounds the rows this extraction carries; it is not a sampling
    rule. Shards are read in a fixed order and the query orders by the row's
    position in its file, so the bound stops at the same row on every run and a
    bounded result is reproducible. A stopped read reports ``bounded=True`` and
    the rows it does carry are real archive rows rather than a convenience sample.

    ``connection`` lets a caller reuse one DuckDB connection across layers. A
    connection this function opened itself is closed on the way out, including on
    failure; one the caller supplied is left open, because its lifetime is the
    caller's to manage.
    """
    if layer not in _LAYERS:
        raise ValueError(f"unknown layer {layer!r}; this module normalizes {sorted(_LAYERS)}")
    if isinstance(max_rows, bool) or (max_rows is not None and not isinstance(max_rows, int)):
        raise TypeError(f"max_rows must be an int or None, got {max_rows!r}")
    if max_rows is not None and max_rows < 0:
        raise ValueError(f"max_rows must not be negative: {max_rows}")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise TypeError(f"batch_size must be an int, got {batch_size!r}")
    if batch_size < 1:
        raise ValueError(f"batch_size must be at least 1, got {batch_size}")
    selected: tuple[str, ...] | None = None
    if tickers is not None:
        if isinstance(tickers, (str, bytes)):
            raise TypeError("tickers must be a sequence of contract ids, not one string")
        names = {str(name).strip() for name in tickers}
        names.discard("")
        if not names:
            raise ValueError("tickers was given but names no contract")
        # A candidate list is a declared population, so it is sorted and deduped
        # before it reaches the query. The same list then produces the same
        # statement and the same row set on every run, which is what lets a
        # cohort-targeted extraction be reproduced rather than merely repeated.
        selected = tuple(sorted(names))
    start = parse_utc_time(window_start, field_name="extract_trades.window_start")
    end = parse_utc_time(window_end, field_name="extract_trades.window_end")
    if end < start:
        raise ValueError(f"window_end {end} precedes window_start {start}")
    mapping = _mapping_for(layer, column_map)
    venue = KALSHI_VENUE if layer == KALSHI_LAYER else POLYMARKET_VENUE
    # The archive name the projected source instant is keyed by, so one mapper
    # serves a real shard and a renamed synthetic one alike.
    time_column = _LAYER_TIME[layer][0]
    flags: set[str] = set()
    shards_read: list[str] = []
    shards_skipped: list[tuple[str, str]] = []
    trades: list[HistoricalTrade] = []
    rows_scanned = 0
    bounded = max_rows == 0
    if not bounded:
        opened_here = connection is None
        con = connection if connection is not None else _connect()
        try:
            for shard in _layer_shards(inventory, layer):
                relative = _shard_relative_path(shard)
                verdict, unreadable_bound = _shard_verdict(
                    shard, layer=layer, window_start=start, window_end=end
                )
                if unreadable_bound:
                    flags.add(FLAG_TIMESTAMP_BOUNDS_UNUSABLE)
                if verdict is not None:
                    shards_skipped.append((relative, verdict))
                    continue
                digest = _shard_hash(shard)
                if digest == _UNKNOWN_SHARD:
                    flags.add(FLAG_SHARD_DIGEST_UNKNOWN)
                path = Path(root) / relative
                real = _physical_columns(con, path)
                if not real:
                    shards_skipped.append((relative, SKIP_SCHEMA_UNREADABLE))
                    continue
                projection, missing = _shard_projection(layer, mapping, real)
                if not projection:
                    shards_skipped.append(
                        (relative, f"{SKIP_TIME_COLUMN_ABSENT}: {', '.join(missing)}")
                    )
                    continue
                shards_read.append(relative)
                cursor = con.execute(
                    _shard_sql(layer, mapping, projection, ticker_count=len(selected or ())),
                    _shard_params(
                        path,
                        start=start,
                        end=end,
                        unit=_LAYER_TIME[layer][1],
                        tickers=selected,
                    ),
                )
                # The connection carries the result's column metadata rather
                # than a separate cursor object, so the projection's names are
                # read once here instead of being assumed from the mapping.
                names = [entry[0] for entry in cursor.description]
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        rows_scanned += 1
                        projected = dict(zip(names, row, strict=True))
                        position = projected.pop(_ROW_POSITION_FIELD)
                        epoch = projected.pop(_SOURCE_EPOCH_FIELD)
                        if epoch is None:
                            # A null source time is a row that cannot be placed in
                            # the window, so it is counted as scanned and kept out
                            # rather than being dated by an assumption.
                            continue
                        projected[time_column] = _source_instant(
                            epoch, time_unit=_LAYER_TIME[layer][1]
                        )
                        trade = _trade_from_record(
                            projected,
                            layer=layer,
                            venue=venue,
                            shard_hash=digest,
                            shard_relative_path=relative,
                            row_position=position,
                        )
                        source_time = trade.clock.source_time
                        if source_time is None or not start <= source_time <= end:
                            continue
                        trades.append(trade)
                        if max_rows is not None and len(trades) >= max_rows:
                            bounded = True
                            break
                    if bounded:
                        break
                if bounded:
                    break
        finally:
            if opened_here:
                con.close()
    if bounded:
        flags.add(FLAG_MAX_ROWS_BOUND_APPLIED)
    return ExternalExtraction(
        layer=layer,
        venue=venue,
        window_start=start,
        window_end=end,
        shards_read=tuple(shards_read),
        shards_skipped=tuple(shards_skipped),
        rows_scanned=rows_scanned,
        trades=tuple(trades),
        bounded=bounded,
        max_rows_applied=max_rows,
        flags=tuple(sorted(flags)),
    )


def write_trades(trades: Iterable[HistoricalTrade], path: str | Path) -> DatasetRef:
    """Seal normalized trades as an immutable ``historical_trades`` dataset.

    Rows go through :func:`~market_propagation.storage.write_parquet`, so the
    sealed table carries the declared schema, sorts deterministically, and refuses
    to replace different content at the same path. A record whose size is null
    keeps that null in the sealed bytes: the table declares ``size`` optional
    precisely so an unknown quantity survives a round trip instead of being
    written as a zero.
    """
    return write_parquet(list(trades), path, table="historical_trades")


def load_trades(path: str | Path) -> tuple[HistoricalTrade, ...]:
    """Read a sealed ``historical_trades`` dataset back into records.

    The inverse of :func:`write_trades`, kept here rather than reconstructed by
    each caller, because the column-to-field projection and the null handling are
    this module's mapping and a second copy would be free to drift from the one
    that wrote the bytes. Reading goes through
    :func:`~market_propagation.storage.read_parquet`, so the content hash and the
    declared schema are verified before any row is interpreted.
    """
    frame = read_parquet(path, table="historical_trades")
    return tuple(_trade_from_sealed_row(row) for row in frame.to_dict(orient="records"))


def _connect() -> Any:
    """A local DuckDB connection for reading archive shards.

    The archive is read directly rather than through
    :func:`~market_propagation.storage.query_sealed`, because that helper verifies
    a content hash against a manifest. That check is what makes a sealed research
    dataset trustworthy, and no external archive shard carries a manifest, so
    routing archive rows through it would compare a hash that does not exist.
    """
    import duckdb

    return duckdb.connect(database=":memory:")


def _field(record: Any, name: str) -> Any:
    """One field of a row, whether the row is a mapping or an attribute record."""
    if isinstance(record, Mapping):
        return record.get(name)
    return getattr(record, name, None)


def _required_int(record: Any, name: str) -> int:
    value = _field(record, name)
    if value is None:
        raise ValueError(f"row carries no {name}; a missing cents price is not zero")
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"row {name} must be an integer cents value, got {value!r}")
    return value


def _source_instant(epoch: int, *, time_unit: str) -> dt.datetime:
    """One absolute epoch count as a UTC instant, in the layer's declared unit.

    The conversion is a timedelta from the epoch rather than a division, because
    integer division truncates toward zero and would move a pre-epoch instant
    forward by up to a second, while a timedelta is exact in both directions and
    carries microseconds through unchanged.
    """
    if time_unit == "epoch_seconds":
        return _EPOCH + dt.timedelta(seconds=int(epoch))
    return _EPOCH + dt.timedelta(microseconds=int(epoch))


def _source_clock(source_time: dt.datetime | str) -> Clock:
    """A clock for an archive row, with no receipt evidence invented.

    :meth:`Clock.historical` without a receipt yields unknown availability, so
    ``usable_time`` stays null and the row cannot leak into a point-in-time
    feature. The archive records when a trade happened, never when this process
    could have seen it.
    """
    return Clock.historical(parse_utc_time(source_time, field_name="HistoricalTrade.source_time"))


def _mapping_for(layer: str, column_map: Mapping[str, str] | None) -> Mapping[str, str]:
    """Resolve the layer's canonical field to column mapping.

    ``column_map`` may be the mapping itself, or a mapping keyed by layer name, so
    one call can serve several layers whose shards were renamed. Supplied names
    override the documented ones field by field, which is what lets a synthetic
    shard rename a single column without the caller restating the rest.
    """
    if column_map is None:
        return _LAYERS[layer]
    _reject_never_selected(column_map)
    if layer in column_map and isinstance(column_map[layer], Mapping):
        entries = column_map[layer]
        _reject_never_selected(entries)
    else:
        entries = column_map
    resolved = dict(_LAYERS[layer])
    for canonical, column in entries.items():
        if canonical not in resolved:
            raise ValueError(
                f"column_map names {canonical!r}, which is not a field of layer {layer!r}; "
                f"expected one of {sorted(resolved)}"
            )
        if not isinstance(column, str) or not column:
            raise ValueError(f"column_map[{canonical!r}] must be a non-empty column name")
        resolved[canonical] = column
    names = sorted(resolved.values())
    duplicated = [name for index, name in enumerate(names) if index and names[index - 1] == name]
    if duplicated:
        raise ValueError(f"column_map maps more than one field onto {duplicated}")
    return resolved


def _reject_never_selected(column_map: Mapping[str, Any]) -> None:
    """Refuse a mapping that reaches for an outcome column.

    This is a guard against a plausible mistake rather than a formality: the
    columns exist in the archive, so silently ignoring one would leave the caller
    believing an orientation was taken from it. Both sides of each entry are
    checked, including the nested entries of a layer-keyed mapping, because the
    outcome column can be named as either the field or the column.
    """
    names: set[str] = set()
    for key, value in column_map.items():
        names.add(str(key))
        if isinstance(value, Mapping):
            names.update(str(inner) for inner in value)
            names.update(str(inner) for inner in value.values())
        elif value is not None:
            names.add(str(value))
    for name in _NEVER_SELECTED:
        if name in names:
            raise ValueError(
                f"column_map references {name!r}; this module derives orientation from the "
                "documented axis and never from an eventual outcome"
            )


def _shard_hash(shard: Any) -> str:
    """A shard's content digest, for the occurrence locator.

    A record that carries no digest yields ``unknown-shard`` rather than being
    hashed here. Hashing is only worth doing over bytes this process read, and
    this module reads a bounded window of a shard rather than the file, so a hash
    computed from a partial read would not be the shard's address at all. The
    placeholder is stable within a run and flagged on the extraction, so an
    occurrence id is still unique without claiming an address that was never
    computed.
    """
    for name in _DIGEST_KEYS:
        value = getattr(shard, name, None)
        if isinstance(value, str) and value:
            return value
    if is_dataclass(shard) and not isinstance(shard, type):
        for entry in fields(shard):
            value = getattr(shard, entry.name, None)
            if isinstance(value, str) and _HEX_DIGEST_RE.match(value):
                return value
    return _UNKNOWN_SHARD


def _shard_relative_path(shard: Any) -> str:
    for name in ("relative_path", "path", "name"):
        value = getattr(shard, name, None)
        if isinstance(value, str) and value:
            return value
    raise ValueError(f"shard record {shard!r} carries no relative path this module can read")


def _shard_row_count(shard: Any) -> int | None:
    """A shard's row count when the inventory measured one, else ``None``.

    ``None`` means the count was not determined, not that the shard is empty, so
    it never becomes a zero and never triggers an empty-shard skip.
    """
    value = getattr(shard, "row_count", None)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _shard_column_names(shard: Any) -> tuple[str, ...]:
    columns = getattr(shard, "columns", None) or ()
    names: list[str] = []
    for entry in columns:
        if isinstance(entry, (tuple, list)) and entry:
            names.append(str(entry[0]))
        elif isinstance(entry, str):
            names.append(entry)
    return tuple(names)


def _timestamp_bounds(shard: Any, column: str) -> tuple[str | None, str | None, bool]:
    """The recorded min and max text for one timestamp column of a shard.

    The third element distinguishes a column the inventory never recorded from one
    it recorded with both bounds null. Only a recorded column can produce a skip,
    because an unrecorded column is an absence of evidence rather than evidence
    that the shard is out of range.
    """
    stats = getattr(shard, "timestamp_stats", None) or ()
    if isinstance(stats, Mapping):
        if column not in stats:
            return (None, None, False)
        entry = stats[column]
        if isinstance(entry, (tuple, list)) and len(entry) >= 3:
            return (_text_or_none(entry[1]), _text_or_none(entry[2]), True)
        return (None, None, True)
    for entry in stats:
        if not isinstance(entry, (tuple, list)) or len(entry) < 3:
            continue
        if str(entry[0]) == column:
            return (_text_or_none(entry[1]), _text_or_none(entry[2]), True)
    return (None, None, False)


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bound_instant(text: str | None, *, time_unit: str, column: str) -> dt.datetime | None:
    """Parse one recorded timestamp bound, or return ``None`` if it will not parse.

    A bound that cannot be read is not a bound that reports the shard as out of
    range, so the caller reads the shard instead and the ``WHERE`` clause still
    bounds the rows that come back. The two accepted forms are the ones the
    inventory produces: an ISO-8601 instant carrying an explicit offset, and, for a
    layer that declares Unix seconds, the bare seconds count. A naive reading is
    refused rather than assigned a zone, because which zone it belongs to is a
    documented property of the source and none is documented here.
    """
    if text is None:
        return None
    if time_unit == "epoch_seconds" and re.fullmatch(r"-?\d+", text):
        try:
            return dt.datetime.fromtimestamp(int(text), tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if not _ISO_OFFSET_RE.search(text):
        return None
    try:
        return parse_utc_time(text, field_name=f"timestamp_statistics[{column}]")
    except (TypeError, ValueError):
        return None


def _shard_verdict(
    shard: Any,
    *,
    layer: str,
    window_start: dt.datetime,
    window_end: dt.datetime,
) -> tuple[str | None, bool]:
    """Whether a shard can be skipped on its recorded bounds, and why.

    The second element reports that a recorded bound could not be read as an
    instant, so the caller flags the layer instead of silently reading every shard
    for a reason that no longer appears in the output.
    """
    status = getattr(shard, "status", None)
    if isinstance(status, str) and status != "read":
        return (SKIP_UNREADABLE, False)
    time_column = _LAYER_TIME[layer][0]
    columns = _shard_column_names(shard)
    if columns and time_column not in columns:
        return (SKIP_TIME_COLUMN_ABSENT, False)
    if _shard_row_count(shard) == 0:
        return (SKIP_ZERO_ROWS, False)
    low_text, high_text, recorded = _timestamp_bounds(shard, time_column)
    if not recorded:
        return (None, False)
    unit = _LAYER_TIME[layer][1]
    low = _bound_instant(low_text, time_unit=unit, column=time_column)
    high = _bound_instant(high_text, time_unit=unit, column=time_column)
    unusable = (low_text is not None and low is None) or (high_text is not None and high is None)
    if low is not None and low > window_end:
        return (SKIP_AFTER_WINDOW, unusable)
    if high is not None and high < window_start:
        return (SKIP_BEFORE_WINDOW, unusable)
    return (None, unusable)


def _layer_shards(inventory: Any, layer: str) -> tuple[Any, ...]:
    """The inventory's records for one layer.

    The inventory module is written in parallel with this one, so both of its
    published accessors are accepted rather than one spelling being required. A
    layer the inventory does not carry is refused here, because an empty shard
    list would be indistinguishable from a layer that has no shards.
    """
    for name in ("shards_for", "layer_shards"):
        accessor = getattr(inventory, name, None)
        if callable(accessor):
            shards = tuple(accessor(layer))
            if not shards:
                raise ValueError(f"inventory reports no shards for layer {layer!r}")
            return shards
    names = {getattr(entry, "name", None) for entry in (getattr(inventory, "layers", None) or ())}
    if layer not in names:
        raise ValueError(f"inventory does not carry layer {layer!r}")
    shards = tuple(
        shard
        for shard in (getattr(inventory, "shards", None) or ())
        if getattr(shard, "layer", None) == layer
    )
    if not shards:
        raise ValueError(f"inventory reports no shards for layer {layer!r}")
    return shards


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _shard_projection(
    layer: str,
    mapping: Mapping[str, str],
    real: tuple[str, ...],
) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    """The select expressions for one shard, and the columns it lacks.

    Every column is projected under its documented archive name, so a shard whose
    physical columns were renamed by ``column_map`` still reaches a mapper reading
    one set of names. Without that, each caller's mapping would have to be threaded
    through the row mapping as well as the query, and the two could disagree about
    which column is the price.

    ``real`` is the shard's actual columns, read from the file rather than assumed
    from the mapping. A column the mapping names but the shard does not have is
    returned as missing instead of being projected, because a reference to a
    nonexistent column is a binder error and a diagnostic naming the column is what
    a caller can act on.
    """
    selected: list[tuple[str, str]] = []
    missing: list[str] = []
    for archive_name, canonical in _PROJECTIONS[layer]:
        column = mapping.get(canonical)
        if column and column in real:
            selected.append(
                (f"{_quote_identifier(column)} AS {_quote_identifier(archive_name)}", archive_name)
            )
        elif canonical in _REQUIRED_FIELDS[layer]:
            missing.append(f"{canonical} ({column!r})")
        else:
            # An optional column the shard does not carry stays in the projection
            # as an explicit null, so the record still reports the field it is
            # supposed to have instead of the mapper having to guess whether the
            # field was absent from the shard or from the archive's design.
            selected.append((f"NULL AS {_quote_identifier(archive_name)}", archive_name))
    time_column = mapping["time"]
    if time_column not in real:
        return (), (*missing, f"time ({time_column!r})")
    selected.append(
        (
            f"{_time_epoch(layer, mapping)} AS {_quote_identifier(_SOURCE_EPOCH_FIELD)}",
            _SOURCE_EPOCH_FIELD,
        )
    )
    selected.append((f"file_row_number AS {_ROW_POSITION_FIELD}", _ROW_POSITION_FIELD))
    return tuple(selected), tuple(missing)


def _shard_sql(
    layer: str,
    mapping: Mapping[str, str],
    projection: tuple[tuple[str, str], ...],
    *,
    ticker_count: int = 0,
) -> str:
    """The projection and row bound for one shard.

    ``file_row_number`` supplies occurrence identity and comes from the scan, so a
    filter that removes rows still leaves every surviving row addressed at its
    position in the file. Ordering by that position makes ``max_rows`` stop at a
    fixed point instead of wherever a parallel scan happened to finish, which is
    what lets a bounded extraction be reproduced.

    The time column is filtered in its own type and projected again as an epoch
    count, because DuckDB cannot hand a timezone-aware timestamp to Python without
    ``pytz``, which this project does not depend on.
    """
    selects = ", ".join(expression for expression, _ in projection)
    time_column = _quote_identifier(mapping["time"])
    clauses = [f"{time_column} >= ?", f"{time_column} <= ?"]
    if ticker_count:
        # A declared candidate list narrows the scan before the read, so a
        # cohort-targeted extraction reads only the contracts it named instead of
        # every contract in the window and then discarding most of them.
        identifier = _quote_identifier(mapping["contract_id"])
        placeholders = ", ".join("?" for _ in range(ticker_count))
        clauses.append(f"{identifier} IN ({placeholders})")
    where = " AND ".join(clauses)
    return (
        f"SELECT {selects} FROM read_parquet(?, file_row_number = true) "
        f"WHERE {where} ORDER BY {_ROW_POSITION_FIELD}"
    )


def _physical_columns(connection: Any, path: Path) -> tuple[str, ...]:
    """A shard's real column names, read from the file.

    The registry is a statement about the archive's schema, and a shard that
    disagrees with it is an archive fact worth reporting rather than a query to
    repair. An unreadable file yields no names, which the caller turns into a skip
    so a corrupt shard flags the run instead of ending it.
    """
    try:
        cursor = connection.execute("SELECT * FROM read_parquet(?) LIMIT 0", [str(path)])
    except Exception:
        return ()
    return tuple(entry[0] for entry in cursor.description)


def _time_epoch(layer: str, mapping: Mapping[str, str]) -> str:
    """The row's source instant as an absolute epoch count, in the layer's unit.

    An epoch count is selected rather than a formatted string because DuckDB
    renders a timezone-aware timestamp in the session's local zone, and this host
    is not on UTC: formatting the column and labeling the result UTC would shift
    every instant by the session offset, and the offset would then depend on the
    machine the pipeline ran on. An epoch count names the instant absolutely and is
    independent of that setting.

    Kalshi archives microseconds and Polymarket archives seconds, so each layer is
    read in its own unit and converted once in Python, where microseconds cannot be
    lost to integer division.
    """
    column = _quote_identifier(mapping["time"])
    if layer == KALSHI_LAYER:
        return f"epoch_us({column})"
    return f"CAST({column} AS BIGINT)"


def _shard_params(
    path: Path,
    *,
    start: dt.datetime,
    end: dt.datetime,
    unit: str,
    tickers: Sequence[str] | None = None,
) -> list[Any]:
    """Parameters in statement order: the shard path, then the two window edges.

    The path is a parameter rather than interpolated text, so a shard name
    containing a quote cannot change the statement. The edges are bound as
    datetimes against a timestamp column and as integer seconds against an epoch
    column, so the comparison happens in the column's own type instead of relying
    on an implicit cast that a reader has to reason about.
    """
    if unit == "epoch_seconds":
        parameters: list[Any] = [str(path), int(start.timestamp()), int(end.timestamp())]
    else:
        parameters = [str(path), start, end]
    # The candidate ids bind after the two edges, in the order the SQL below
    # lists them, so the missing-values clause lines up with its placeholders.
    parameters.extend(tickers or ())
    return parameters


def _trade_from_record(
    record: Mapping[str, Any],
    *,
    layer: str,
    venue: str,
    shard_hash: str,
    shard_relative_path: str,
    row_position: int,
) -> HistoricalTrade:
    """Dispatch one projected row to the venue mapping that owns it.

    The two venues take different keyword arguments, and passing one venue's
    arguments to the other's mapper would be a ``TypeError`` at the first row
    rather than a wrong answer, so the dispatch is explicit.
    """
    if layer == KALSHI_LAYER:
        return kalshi_trade_from_row(
            record,
            shard_hash=shard_hash,
            shard_relative_path=shard_relative_path,
            row_position=row_position,
            venue=venue,
        )
    return polymarket_trade_from_row(
        record,
        layer=layer,
        shard_hash=shard_hash,
        shard_relative_path=shard_relative_path,
        row_position=row_position,
        venue=venue,
    )


def _optional_text(value: Any) -> str | None:
    if value is None or _is_na(value):
        return None
    return str(value)


def _optional_int(value: Any) -> int | None:
    if value is None or _is_na(value):
        return None
    return int(value)


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or _is_na(value):
        return None
    return Decimal(str(value))


def _optional_instant(value: Any) -> dt.datetime | None:
    if value is None or _is_na(value):
        return None
    if isinstance(value, str):
        return parse_utc_time(value, field_name="sealed historical_trades timestamp")
    as_datetime = getattr(value, "to_pydatetime", None)
    if callable(as_datetime):
        value = as_datetime()
    if not isinstance(value, dt.datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_na(value: Any) -> bool:
    """Whether a value is pandas' or numpy's missing marker rather than a value.

    A sealed decimal column arrives as ``Decimal`` or as ``NaN``, and the two are
    not the same thing: one is a recorded number and the other is the absence of
    one. Comparing a ``float`` to itself is the one test that catches every NaN
    without importing pandas into this module's boundary.
    """
    if isinstance(value, float) and value != value:
        return True
    return value is not None and type(value).__name__ in ("NAType", "NaTType")


def _trade_from_sealed_row(row: Mapping[str, Any]) -> HistoricalTrade:
    """Rebuild one record from a row of a sealed ``historical_trades`` dataset.

    Every column of the sealed table round trips, including the clock evidence and
    the provenance, so this reconstructs rather than re-derives: a value that was
    null when it was written stays null. When availability bounds were recorded
    they are restored through :meth:`Clock.historical` with the same uncertainty
    and basis, so a round-tripped row states the clock evidence it was sealed with.
    """
    quality = _optional_text(row.get("availability_quality")) or "unknown"
    basis = _optional_text(row.get("availability_basis")) or "historical_without_receipt"
    lower = _optional_instant(row.get("availability_lower"))
    upper = _optional_instant(row.get("availability_upper"))
    source_time = _optional_instant(row.get("source_time"))
    if lower is None or upper is None:
        clock = Clock.historical(source_time, quality=quality, basis=basis)
    else:
        received = _optional_instant(row.get("received_time")) or upper
        clock = Clock.historical(
            source_time,
            received,
            uncertainty_seconds=(upper - lower).total_seconds(),
            quality=quality,
            basis=basis,
        )
    flags = row.get("flags_json")
    if flags is None or _is_na(flags):
        flags = ()
    return HistoricalTrade(
        venue=str(row["venue"]),
        contract_id=str(row["contract_id"]),
        price=Decimal(str(row["price"])),
        raw_price_units=str(row["raw_price_units"]),
        price_precision=str(row["price_precision"]),
        size_quality=str(row["size_quality"]),
        clock=clock,
        provenance=Provenance(
            raw_hash=str(row["raw_hash"]),
            record_id=str(row["record_id"]),
            source=str(row["source"]),
            schema_version=_optional_text(row.get("schema_version")) or _SCHEMA_VERSION,
        ),
        token_id=_optional_text(row.get("token_id")),
        outcome_seq=_optional_int(row.get("outcome_seq")),
        trade_id=_optional_text(row.get("trade_id")),
        raw_price=_optional_decimal(row.get("raw_price")),
        secondary_price=_optional_decimal(row.get("secondary_price")),
        event_price=_optional_decimal(row.get("event_price")),
        event_axis=_optional_text(row.get("event_axis")),
        direction=_optional_text(row.get("direction")),
        event_direction=_optional_int(row.get("event_direction")),
        size=_optional_decimal(row.get("size")),
        flags=tuple(flags),
    )
