"""Behavioural tests for the bounded external trade adapters.

Every shard here is synthetic and written to ``tmp_path``: the real archives are
tens of gigabytes and are never read by a test. What the synthetic rows are chosen
to defend is the set of archive facts a wrong implementation would get wrong
silently, so each test below exists because a plausible bug survives a passing
suite otherwise.

* A zero cent Kalshi price. Clipping it to one cent, or rejecting the row, both
  look reasonable and both destroy a price the venue printed.
* ``yes_price + no_price`` that is not 100. Normalizing the sum would erase the
  boundary fact the archive is being read to find.
* An epoch that is seconds. Dividing by a thousand "to be safe" is the classic way
  a whole archive lands in 1970.
* ``outcome_seq == 2``. A row whose token prices the no side has to be projected
  as ``1 - price``, and reading it as the price is silently wrong on half the rows.
* A planted ``p_event`` disagreement. If the recomputed axis were simply trusted,
  the archive's own projection would never be checked.
* Repeated identical fills. If occurrence identity were a content digest, the
  frequency this study measures would collapse.
* A shard outside the window. A skip that is not reported hides rows.

Nothing asserts on internal call structure: the assertions are on the records, the
sealed bytes and the extraction summary a caller reads.
"""

from __future__ import annotations

import datetime as dt
import pathlib
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.domain import UTC, HistoricalTrade
from market_propagation.ingest.external_history import (
    DEFAULT_PRICE_TOLERANCE,
    FLAG_AMBIGUOUS_OUTCOME_AXIS,
    FLAG_CENTS_OUT_OF_RANGE,
    FLAG_CENTS_SUM_NOT_100,
    FLAG_MAX_ROWS_BOUND_APPLIED,
    FLAG_P_EVENT_DISAGREES,
    FLAG_ZERO_CENT_PRICE,
    KALSHI_EVENT_AXIS,
    KALSHI_LAYER,
    POLYMARKET_EVENT_AXIS_OUTCOME_1,
    POLYMARKET_EVENT_AXIS_OUTCOME_2,
    POLYMARKET_STANDARD_LAYER,
    SKIP_AFTER_WINDOW,
    SKIP_BEFORE_WINDOW,
    SKIP_SCHEMA_UNREADABLE,
    SKIP_ZERO_ROWS,
    extract_trades,
    kalshi_trade_from_row,
    load_trades,
    polymarket_trade_from_row,
    row_occurrence_id,
    write_trades,
)
from market_propagation.storage import read_parquet

WINDOW_START = dt.datetime(2025, 1, 1, tzinfo=UTC)
WINDOW_END = dt.datetime(2025, 1, 1, 23, 59, 59, tzinfo=UTC)

#: A shard digest, spelled the way a real inventory spells one.
SHARD_HASH = "a" * 64

_KALSHI_SCHEMA = pa.schema(
    [
        ("trade_id", pa.string()),
        ("ticker", pa.string()),
        ("count", pa.int64()),
        ("yes_price", pa.int64()),
        ("no_price", pa.int64()),
        ("taker_side", pa.string()),
        ("created_time", pa.timestamp("us", tz="UTC")),
    ]
)

_POLYMARKET_SCHEMA = pa.schema(
    [
        ("condition_id", pa.string()),
        ("asset_id", pa.string()),
        ("outcome_seq", pa.int64()),
        ("price", pa.float64()),
        ("taker_direction", pa.string()),
        ("D", pa.int8()),
        ("p_event", pa.float64()),
        ("block_timestamp", pa.int64()),
    ]
)


class Shard:
    """A stand-in for an inventory shard record.

    Deliberately not the real dataclass: this test owns its inputs, and the real
    record is built by a module this one does not import. The attributes are the
    ones the extraction contract names.
    """

    def __init__(
        self,
        relative_path: str,
        *,
        layer: str,
        timestamp_stats: tuple[tuple[str, str | None, str | None], ...] = (),
        row_count: int | None = 1,
        columns: tuple[tuple[str, str], ...] = (),
        status: str = "read",
        sha256: str | None = SHARD_HASH,
    ) -> None:
        self.relative_path = relative_path
        self.layer = layer
        self.timestamp_stats = timestamp_stats
        self.row_count = row_count
        self.columns = columns
        self.status = status
        self.sha256 = sha256


class Inventory:
    """A stand-in for an :class:`ExternalInventory`, exposing its shard accessor."""

    def __init__(self, shards: dict[str, tuple[Shard, ...]]) -> None:
        self._shards = shards
        self.layers = tuple(_Layer(name) for name in sorted(shards))

    def shards_for(self, name: str) -> tuple[Shard, ...]:
        return self._shards.get(name, ())


class _Layer:
    def __init__(self, name: str) -> None:
        self.name = name


def kalshi_shard(
    root: pathlib.Path,
    relative_path: str,
    rows: list[tuple[str, str, int, int, int, str, dt.datetime]],
) -> pathlib.Path:
    """Write one synthetic Kalshi shard in the documented archive schema."""
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [
            {
                "trade_id": trade_id,
                "ticker": ticker,
                "count": count,
                "yes_price": yes,
                "no_price": no,
                "taker_side": side,
                "created_time": created,
            }
            for trade_id, ticker, count, yes, no, side, created in rows
        ],
        schema=_KALSHI_SCHEMA,
    )
    pq.write_table(table, target)
    return target


def polymarket_shard(
    root: pathlib.Path,
    relative_path: str,
    rows: list[tuple[str, str, int, float, str, int, float, int]],
) -> pathlib.Path:
    """Write one synthetic Polymarket shard in the documented cleaned schema."""
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [
            {
                "condition_id": condition_id,
                "asset_id": asset_id,
                "outcome_seq": outcome_seq,
                "price": price,
                "taker_direction": direction,
                "D": axis_direction,
                "p_event": p_event,
                "block_timestamp": stamp,
            }
            for condition_id, asset_id, outcome_seq, price, direction, axis_direction, p_event, stamp in rows
        ],
        schema=_POLYMARKET_SCHEMA,
    )
    pq.write_table(table, target)
    return target


def in_window(hour: int = 12) -> dt.datetime:
    return dt.datetime(2025, 1, 1, hour, tzinfo=UTC)


def test_row_occurrence_id_is_the_shard_locator() -> None:
    assert row_occurrence_id("deadbeef", 4) == "deadbeef:4"
    # The same position in a different shard is a different occurrence.
    assert row_occurrence_id("deadbeef", 4) != row_occurrence_id("feedface", 4)
    with pytest.raises(ValueError):
        row_occurrence_id("", 1)
    with pytest.raises(ValueError):
        row_occurrence_id("deadbeef", -1)


def test_kalshi_cents_are_exact_and_dollars_are_derived() -> None:
    trade = kalshi_trade_from_row(
        {
            "trade_id": "trade-1",
            "ticker": "KXFED-25JAN",
            "count": 7,
            "yes_price": 17,
            "no_price": 83,
            "taker_side": "yes",
            "created_time": in_window(),
        },
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=0,
    )
    # Decimal division, not float: 17/100 must be exactly 0.17.
    assert trade.price == Decimal("0.17")
    assert trade.price.as_tuple().exponent == -2
    assert trade.raw_price == Decimal(17)
    assert trade.secondary_price == Decimal(83)
    assert trade.raw_price_units == "cents"
    assert trade.price_precision == "exact_integer_cents"
    assert trade.size == Decimal(7)
    assert trade.size_is_verified is True
    assert trade.trade_id == "trade-1"
    assert trade.flags == ()
    assert trade.event_price == trade.price
    assert trade.event_axis == KALSHI_EVENT_AXIS
    assert trade.provenance.record_id == f"{SHARD_HASH}:0"
    assert trade.clock.usable_time is None


def test_kalshi_event_direction_follows_the_taker_side() -> None:
    base = {
        "trade_id": "trade-2",
        "ticker": "KXFED-25JAN",
        "count": 1,
        "yes_price": 40,
        "no_price": 60,
        "created_time": in_window(),
    }
    yes = kalshi_trade_from_row(
        {**base, "taker_side": "yes"},
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=0,
    )
    no = kalshi_trade_from_row(
        {**base, "taker_side": "no"},
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=1,
    )
    unknown = kalshi_trade_from_row(
        {**base, "taker_side": "maybe"},
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=2,
    )
    assert yes.event_direction == 1
    assert no.event_direction == -1
    # A side neither yes nor no is null, not a flat zero.
    assert unknown.event_direction is None


def test_kalshi_zero_cent_price_is_flagged_and_not_clipped() -> None:
    trade = kalshi_trade_from_row(
        {
            "trade_id": "trade-3",
            "ticker": "KXFED-25JAN",
            "count": 12,
            "yes_price": 0,
            "no_price": 100,
            "taker_side": "no",
            "created_time": in_window(),
        },
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=0,
    )
    # The price is carried at the value the archive printed.
    assert trade.price == Decimal(0)
    assert trade.raw_price == Decimal(0)
    assert trade.event_price == Decimal(0)
    assert FLAG_ZERO_CENT_PRICE in trade.flags
    # 100 is above the documented 0..99 range, so that boundary is reported too.
    assert FLAG_CENTS_OUT_OF_RANGE in trade.flags
    # 0 + 100 is exactly 100, so the sum flag is absent: a genuine zero cent print
    # is not the same defect as an inconsistent pair.
    assert FLAG_CENTS_SUM_NOT_100 not in trade.flags


def test_kalshi_cents_sum_off_100_is_flagged() -> None:
    trade = kalshi_trade_from_row(
        {
            "trade_id": "trade-4",
            "ticker": "KXFED-25JAN",
            "count": 3,
            "yes_price": 40,
            "no_price": 55,
            "taker_side": "yes",
            "created_time": in_window(),
        },
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=0,
    )
    assert FLAG_CENTS_SUM_NOT_100 in trade.flags
    assert FLAG_ZERO_CENT_PRICE not in trade.flags
    assert FLAG_CENTS_OUT_OF_RANGE not in trade.flags
    # Reported, never repaired: the pair is carried as printed.
    assert trade.raw_price == Decimal(40)
    assert trade.secondary_price == Decimal(55)


def test_extract_reads_a_synthetic_kalshi_shard(tmp_path: pathlib.Path) -> None:
    kalshi_shard(
        tmp_path,
        "kalshi-trades/trades-0000.parquet",
        [
            ("t1", "KXFED-25JAN", 5, 30, 70, "yes", in_window(1)),
            ("t2", "KXFED-25JAN", 6, 0, 100, "no", in_window(2)),
            ("t3", "KXFED-25JAN", 7, 40, 55, "yes", in_window(3)),
        ],
    )
    inventory = Inventory(
        {
            KALSHI_LAYER: (
                Shard(
                    "kalshi-trades/trades-0000.parquet",
                    layer=KALSHI_LAYER,
                    row_count=3,
                    columns=(("ticker", "string"), ("created_time", "timestamp[us, tz=UTC]")),
                ),
            )
        }
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_read == ("kalshi-trades/trades-0000.parquet",)
    assert result.shards_skipped == ()
    assert result.rows_scanned == 3
    assert result.bounded is False
    assert result.flags == ()
    prices = [trade.price for trade in result.trades]
    assert prices == [Decimal("0.30"), Decimal(0), Decimal("0.40")]
    assert result.trades[1].flags == (FLAG_ZERO_CENT_PRICE, FLAG_CENTS_OUT_OF_RANGE)
    assert result.trades[2].flags == (FLAG_CENTS_SUM_NOT_100,)
    # Positions are distinct, so the three rows are three occurrences.
    assert len({trade.provenance.record_id for trade in result.trades}) == 3
    summary = result.as_dict()
    assert summary["layer"] == KALSHI_LAYER
    assert summary["trade_count"] == 3
    assert summary["shards_read"] == ["kalshi-trades/trades-0000.parquet"]


def test_extract_applies_max_rows_and_reports_the_bound(tmp_path: pathlib.Path) -> None:
    rows = [
        (f"t{index}", "KXFED-25JAN", 1, 30, 70, "yes", in_window(index + 1)) for index in range(5)
    ]
    kalshi_shard(tmp_path, "kalshi-trades/trades-0000.parquet", rows)
    inventory = Inventory(
        {KALSHI_LAYER: (Shard("kalshi-trades/trades-0000.parquet", layer=KALSHI_LAYER),)}
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        max_rows=2,
        batch_size=1,
    )
    assert len(result.trades) == 2
    assert result.bounded is True
    assert result.max_rows_applied == 2
    assert FLAG_MAX_ROWS_BOUND_APPLIED in result.flags
    # The bound stops at the same rows on a repeat run.
    again = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        max_rows=2,
        batch_size=1,
    )
    assert [trade.trade_id for trade in again.trades] == [trade.trade_id for trade in result.trades]


def test_out_of_window_shard_is_skipped_with_a_reason(tmp_path: pathlib.Path) -> None:
    kalshi_shard(
        tmp_path,
        "kalshi-trades/trades-0000.parquet",
        [("t1", "KXFED-25JAN", 1, 30, 70, "yes", in_window(1))],
    )
    kalshi_shard(
        tmp_path,
        "kalshi-trades/trades-0001.parquet",
        [("t9", "KXFED-24DEC", 1, 30, 70, "yes", dt.datetime(2024, 12, 1, tzinfo=UTC))],
    )
    inventory = Inventory(
        {
            KALSHI_LAYER: (
                Shard(
                    "kalshi-trades/trades-0000.parquet",
                    layer=KALSHI_LAYER,
                    timestamp_stats=(
                        ("created_time", "2025-01-01T01:00:00+00:00", "2025-01-01T02:00:00+00:00"),
                    ),
                ),
                Shard(
                    "kalshi-trades/trades-0001.parquet",
                    layer=KALSHI_LAYER,
                    timestamp_stats=(
                        ("created_time", "2024-12-01T00:00:00+00:00", "2024-12-01T23:59:59+00:00"),
                    ),
                ),
            )
        }
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_read == ("kalshi-trades/trades-0000.parquet",)
    assert result.shards_skipped == (("kalshi-trades/trades-0001.parquet", SKIP_BEFORE_WINDOW),)
    assert [trade.trade_id for trade in result.trades] == ["t1"]
    # The skipped shard's rows were never scanned, so they are not counted.
    assert result.rows_scanned == 1


def test_shard_with_only_later_timestamps_is_skipped_after_window(
    tmp_path: pathlib.Path,
) -> None:
    kalshi_shard(
        tmp_path,
        "kalshi-trades/trades-0009.parquet",
        [("t9", "KXFED-25JUN", 1, 30, 70, "yes", dt.datetime(2025, 6, 1, tzinfo=UTC))],
    )
    inventory = Inventory(
        {
            KALSHI_LAYER: (
                Shard(
                    "kalshi-trades/trades-0009.parquet",
                    layer=KALSHI_LAYER,
                    timestamp_stats=(
                        ("created_time", "2025-06-01T00:00:00+00:00", "2025-06-02T00:00:00+00:00"),
                    ),
                ),
            )
        }
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_skipped == (("kalshi-trades/trades-0009.parquet", SKIP_AFTER_WINDOW),)
    assert result.trades == ()


def test_empty_shard_is_skipped_rather_than_read(tmp_path: pathlib.Path) -> None:
    inventory = Inventory(
        {
            KALSHI_LAYER: (
                Shard("kalshi-trades/trades-0000.parquet", layer=KALSHI_LAYER, row_count=0),
            )
        }
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_read == ()
    assert result.shards_skipped == (("kalshi-trades/trades-0000.parquet", SKIP_ZERO_ROWS),)


def test_unreadable_shard_never_raises(tmp_path: pathlib.Path) -> None:
    inventory = Inventory(
        {
            KALSHI_LAYER: (
                Shard("kalshi-trades/trades-0000.parquet", layer=KALSHI_LAYER, status="unreadable"),
            )
        }
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_skipped == (
        ("kalshi-trades/trades-0000.parquet", "shard_unreadable_in_inventory"),
    )
    assert result.trades == ()


def test_column_map_renames_a_synthetic_shard(tmp_path: pathlib.Path) -> None:
    target = tmp_path / "renamed" / "part-0.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        [
            {
                "market": "KXFED-25JAN",
                "quantity": 4,
                "yes": 25,
                "no": 75,
                "side": "no",
                "at": in_window(6),
            }
        ],
        schema=pa.schema(
            [
                ("market", pa.string()),
                ("quantity", pa.int64()),
                ("yes", pa.int64()),
                ("no", pa.int64()),
                ("side", pa.string()),
                ("at", pa.timestamp("us", tz="UTC")),
            ]
        ),
    )
    pq.write_table(table, target)
    inventory = Inventory({KALSHI_LAYER: (Shard("renamed/part-0.parquet", layer=KALSHI_LAYER),)})
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        column_map={
            "contract_id": "market",
            "size": "quantity",
            "yes_price": "yes",
            "no_price": "no",
            "direction": "side",
            "time": "at",
        },
    )
    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.contract_id == "KXFED-25JAN"
    assert trade.size == Decimal(4)
    assert trade.price == Decimal("0.25")
    assert trade.direction == "no"
    assert trade.event_direction == -1
    assert trade.clock.source_time == in_window(6)


def test_column_map_refuses_an_outcome_column(tmp_path: pathlib.Path) -> None:
    inventory = Inventory({KALSHI_LAYER: (Shard("x.parquet", layer=KALSHI_LAYER),)})
    with pytest.raises(ValueError, match="winning_outcome_label"):
        extract_trades(
            tmp_path,
            inventory,
            layer=KALSHI_LAYER,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            column_map={"winning_outcome_label": "yes_price"},
        )


def test_polymarket_outcome_seq_two_projects_one_minus_price() -> None:
    base = {
        "condition_id": "0xabc",
        "asset_id": "token-yes",
        "price": 0.6,
        "taker_direction": "SELL",
        "D": -1,
        "p_event": 0.6,
        "block_timestamp": 1669060209,
    }
    first = polymarket_trade_from_row(
        {**base, "outcome_seq": 1, "p_event": 0.6},
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=0,
    )
    second = polymarket_trade_from_row(
        {**base, "outcome_seq": 2, "p_event": 0.4},
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=1,
    )
    assert first.event_price == Decimal("0.6")
    assert first.event_axis == POLYMARKET_EVENT_AXIS_OUTCOME_1
    assert second.event_price == Decimal("0.4")
    assert second.event_axis == POLYMARKET_EVENT_AXIS_OUTCOME_2
    # The token's own price is unchanged on both rows; only the axis differs.
    assert first.price == Decimal("0.6")
    assert second.price == Decimal("0.6")
    # Both planted values agree with the documented rule, so neither is flagged.
    assert FLAG_P_EVENT_DISAGREES not in first.flags
    assert FLAG_P_EVENT_DISAGREES not in second.flags


def test_polymarket_ambiguous_outcome_axis_leaves_the_event_axis_null() -> None:
    trade = polymarket_trade_from_row(
        {
            "condition_id": "0xabc",
            "asset_id": "token-x",
            "outcome_seq": 3,
            "price": 0.25,
            "taker_direction": "BUY",
            "D": 1,
            "p_event": 0.25,
            "block_timestamp": 1669060209,
        },
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=0,
    )
    assert trade.event_price is None
    assert trade.event_axis is None
    assert trade.has_event_axis is False
    assert FLAG_AMBIGUOUS_OUTCOME_AXIS in trade.flags
    # The token price itself is still a price and is carried.
    assert trade.price == Decimal("0.25")


def test_polymarket_planted_p_event_disagreement_is_flagged() -> None:
    trade = polymarket_trade_from_row(
        {
            "condition_id": "0xabc",
            "asset_id": "token-yes",
            "outcome_seq": 1,
            "price": 0.6,
            "taker_direction": "BUY",
            "D": 1,
            # The documented rule makes the event price 0.6; this row claims 0.9.
            "p_event": 0.9,
            "block_timestamp": 1669060209,
        },
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=0,
    )
    assert FLAG_P_EVENT_DISAGREES in trade.flags
    # The recomputed projection is carried, not the archive's stored one.
    assert trade.event_price == Decimal("0.6")


def test_polymarket_agreement_within_tolerance_is_not_flagged() -> None:
    trade = polymarket_trade_from_row(
        {
            "condition_id": "0xabc",
            "asset_id": "token-yes",
            "outcome_seq": 1,
            "price": 0.6,
            "taker_direction": "BUY",
            "D": 1,
            # A difference below the documented tolerance is float64 noise.
            "p_event": 0.6 + DEFAULT_PRICE_TOLERANCE / 10,
            "block_timestamp": 1669060209,
        },
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=0,
    )
    assert FLAG_P_EVENT_DISAGREES not in trade.flags


def test_polymarket_epoch_seconds_are_read_as_seconds() -> None:
    trade = polymarket_trade_from_row(
        {
            "condition_id": "0xabc",
            "asset_id": "token-yes",
            "outcome_seq": 1,
            "price": 0.5,
            "taker_direction": "BUY",
            "D": 1,
            "p_event": 0.5,
            # 2022-11-21T19:50:09Z. Read as milliseconds this would be 1970-01-20.
            "block_timestamp": 1669060209,
        },
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=0,
    )
    assert trade.clock.source_time == dt.datetime(2022, 11, 21, 19, 50, 9, tzinfo=UTC)
    assert trade.clock.source_time.year == 2022


def test_polymarket_millisecond_magnitude_is_refused_not_rescaled() -> None:
    with pytest.raises(ValueError, match="not a Unix seconds value"):
        polymarket_trade_from_row(
            {
                "condition_id": "0xabc",
                "asset_id": "token-yes",
                "outcome_seq": 1,
                "price": 0.5,
                "taker_direction": "BUY",
                "D": 1,
                "p_event": 0.5,
                "block_timestamp": 1669060209000,
            },
            layer=POLYMARKET_STANDARD_LAYER,
            shard_hash=SHARD_HASH,
            shard_relative_path="daily_aligned/part.parquet",
            row_position=0,
        )


def test_polymarket_size_is_null_with_its_reason() -> None:
    zero = polymarket_trade_from_row(
        {
            "condition_id": "0xabc",
            "asset_id": "token-yes",
            "outcome_seq": 1,
            "price": 0.0,
            "taker_direction": "BUY",
            "D": 1,
            "p_event": 0.0,
            "block_timestamp": 1669060209,
        },
        layer=POLYMARKET_STANDARD_LAYER,
        shard_hash=SHARD_HASH,
        shard_relative_path="daily_aligned/part.parquet",
        row_position=0,
    )
    assert zero.size is None
    # A zero price is its own flagged condition, distinct from an omitted quantity.
    assert zero.size_quality == "zero_price_row"
    assert zero.size_is_verified is False


def test_extract_reads_a_synthetic_polymarket_shard(tmp_path: pathlib.Path) -> None:
    polymarket_shard(
        tmp_path,
        "polymarket-v1/daily_aligned/2022_11_21.parquet",
        [
            ("0xabc", "tok-yes", 1, 0.5, "BUY", 1, 0.5, 1669060209),
            ("0xabc", "tok-no", 2, 0.5, "SELL", -1, 0.5, 1669060209),
        ],
    )
    inventory = Inventory(
        {
            POLYMARKET_STANDARD_LAYER: (
                Shard(
                    "polymarket-v1/daily_aligned/2022_11_21.parquet",
                    layer=POLYMARKET_STANDARD_LAYER,
                ),
            )
        }
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=POLYMARKET_STANDARD_LAYER,
        window_start=dt.datetime(2022, 11, 21, tzinfo=UTC),
        window_end=dt.datetime(2022, 11, 22, tzinfo=UTC),
    )
    assert result.venue == "polymarket"
    assert len(result.trades) == 2
    first, second = result.trades
    assert first.event_price == Decimal("0.5")
    assert second.event_price == Decimal("0.5")
    assert first.clock.source_time == dt.datetime(2022, 11, 21, 19, 50, 9, tzinfo=UTC)
    assert all(trade.size is None for trade in result.trades)
    assert all(trade.size_is_verified is False for trade in result.trades)


def test_repeated_identical_fills_keep_distinct_record_ids(tmp_path: pathlib.Path) -> None:
    identical = ("dup-1", "KXFED-25JAN", 5, 20, 80, "yes", in_window(4))
    kalshi_shard(tmp_path, "kalshi-trades/trades-0000.parquet", [identical, identical])
    inventory = Inventory(
        {KALSHI_LAYER: (Shard("kalshi-trades/trades-0000.parquet", layer=KALSHI_LAYER),)}
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert len(result.trades) == 2
    ids = {trade.provenance.record_id for trade in result.trades}
    assert len(ids) == 2
    # The venue's own id is retained as evidence, not as identity.
    assert {trade.trade_id for trade in result.trades} == {"dup-1"}
    assert ids == {f"{SHARD_HASH}:0", f"{SHARD_HASH}:1"}


def test_null_size_survives_a_write_and_read(tmp_path: pathlib.Path) -> None:
    trades = [
        kalshi_trade_from_row(
            {
                "trade_id": "t1",
                "ticker": "KXFED-25JAN",
                "count": 5,
                "yes_price": 20,
                "no_price": 80,
                "taker_side": "yes",
                "created_time": in_window(),
            },
            shard_hash=SHARD_HASH,
            shard_relative_path="trades-0000.parquet",
            row_position=0,
        ),
        polymarket_trade_from_row(
            {
                "condition_id": "0xabc",
                "asset_id": "token-yes",
                "outcome_seq": 2,
                "price": 0.25,
                "taker_direction": "BUY",
                "D": 1,
                "p_event": 0.75,
                "block_timestamp": 1669060209,
            },
            layer=POLYMARKET_STANDARD_LAYER,
            shard_hash=SHARD_HASH,
            shard_relative_path="daily_aligned/part.parquet",
            row_position=0,
        ),
    ]
    reference = write_trades(trades, tmp_path / "historical_trades.parquet")
    assert reference.row_count == 2
    frame = read_parquet(tmp_path / "historical_trades.parquet", table="historical_trades")
    # Only the Polymarket row has an unknown quantity; the Kalshi row has a real
    # one. A null is not a zero, so exactly one size is null and no null became 0.
    assert frame["size"].isna().sum() == 1
    kalshi_size = frame.loc[frame["venue"] == "kalshi", "size"].iloc[0]
    assert kalshi_size == Decimal(5)
    assert frame.loc[frame["venue"] == "polymarket", "size"].isna().all()

    reloaded = load_trades(tmp_path / "historical_trades.parquet")
    assert len(reloaded) == 2
    by_venue = {trade.venue: trade for trade in reloaded}
    assert by_venue["kalshi"].size == Decimal(5)
    assert by_venue["kalshi"].size_is_verified is True
    assert by_venue["polymarket"].size is None
    assert by_venue["polymarket"].size_is_verified is False
    assert by_venue["polymarket"].size_quality == "unavailable_in_cleaned_layer"
    # Clock evidence round trips: an archive row has no receipt, so usable time
    # stays null on the way back out.
    assert by_venue["polymarket"].clock.usable_time is None
    assert by_venue["polymarket"].clock.source_time == dt.datetime(
        2022, 11, 21, 19, 50, 9, tzinfo=UTC
    )
    assert by_venue["kalshi"].event_axis == KALSHI_EVENT_AXIS
    assert by_venue["kalshi"].raw_price == Decimal(20)
    assert by_venue["kalshi"].price == Decimal("0.20")
    # The event projection recomputed at read time is the one that comes back.
    assert by_venue["polymarket"].event_price == Decimal("0.75")
    assert by_venue["polymarket"].event_axis == POLYMARKET_EVENT_AXIS_OUTCOME_2
    assert by_venue["polymarket"].provenance.record_id == f"{SHARD_HASH}:0"
    assert by_venue["kalshi"].provenance.record_id == f"{SHARD_HASH}:0"


def test_flagged_record_survives_a_round_trip(tmp_path: pathlib.Path) -> None:
    trade = kalshi_trade_from_row(
        {
            "trade_id": "t1",
            "ticker": "KXFED-25JAN",
            "count": 0,
            "yes_price": 0,
            "no_price": 100,
            "taker_side": "no",
            "created_time": in_window(),
        },
        shard_hash=SHARD_HASH,
        shard_relative_path="trades-0000.parquet",
        row_position=0,
    )
    write_trades([trade], tmp_path / "flagged.parquet")
    reloaded = load_trades(tmp_path / "flagged.parquet")[0]
    assert reloaded.price == Decimal(0)
    assert reloaded.size == Decimal(0)
    assert reloaded.size_quality == "verified_source_quantity"
    assert set(reloaded.flags) == set(trade.flags)
    assert FLAG_ZERO_CENT_PRICE in reloaded.flags


def test_layer_keyed_column_map_renames_its_own_layer(tmp_path: pathlib.Path) -> None:
    target = tmp_path / "nested" / "part-0.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "market": "KXFED-25JAN",
                    "quantity": 3,
                    "yes": 45,
                    "no": 55,
                    "side": "yes",
                    "at": in_window(8),
                }
            ],
            schema=pa.schema(
                [
                    ("market", pa.string()),
                    ("quantity", pa.int64()),
                    ("yes", pa.int64()),
                    ("no", pa.int64()),
                    ("side", pa.string()),
                    ("at", pa.timestamp("us", tz="UTC")),
                ]
            ),
        ),
        target,
    )
    inventory = Inventory({KALSHI_LAYER: (Shard("nested/part-0.parquet", layer=KALSHI_LAYER),)})
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        column_map={
            KALSHI_LAYER: {
                "contract_id": "market",
                "size": "quantity",
                "yes_price": "yes",
                "no_price": "no",
                "direction": "side",
                "time": "at",
            }
        },
    )
    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade.contract_id == "KXFED-25JAN"
    assert trade.price == Decimal("0.45")
    assert trade.secondary_price == Decimal(55)
    assert trade.event_direction == 1
    assert trade.clock.source_time == in_window(8)


def test_layer_keyed_column_map_refuses_an_outcome_column(tmp_path: pathlib.Path) -> None:
    inventory = Inventory({KALSHI_LAYER: (Shard("x.parquet", layer=KALSHI_LAYER),)})
    with pytest.raises(ValueError, match="winning_outcome_label"):
        extract_trades(
            tmp_path,
            inventory,
            layer=KALSHI_LAYER,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            column_map={KALSHI_LAYER: {"yes_price": "winning_outcome_label"}},
        )


def test_column_map_refuses_an_unknown_field(tmp_path: pathlib.Path) -> None:
    inventory = Inventory({KALSHI_LAYER: (Shard("x.parquet", layer=KALSHI_LAYER),)})
    with pytest.raises(ValueError, match="not a field of layer"):
        extract_trades(
            tmp_path,
            inventory,
            layer=KALSHI_LAYER,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            column_map={"not_a_field": "yes_price"},
        )


def test_extract_refuses_an_unknown_layer(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="unknown layer"):
        extract_trades(
            tmp_path,
            Inventory({}),
            layer="not_a_layer",
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )


def test_extract_refuses_a_layer_with_no_shards(tmp_path: pathlib.Path) -> None:
    # An inventory carrying no shards for the layer is a refusal, not an empty
    # extraction: an extraction of nothing looks like a window with no trades.
    with pytest.raises(ValueError, match="no shards for layer"):
        extract_trades(
            tmp_path,
            Inventory({}),
            layer=KALSHI_LAYER,
            window_start=WINDOW_START,
            window_end=WINDOW_END,
        )


def test_shard_missing_an_optional_column_is_projected_as_null(
    tmp_path: pathlib.Path,
) -> None:
    # The layer's schema statement lists columns a shard does not have. Optional
    # ones are projected as null rather than silently dropped, so the row still
    # carries the field it needs to be identified.
    target = tmp_path / "partial" / "part-0.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "ticker": "KXFED-25JAN",
                    "count": 2,
                    "yes_price": 30,
                    "no_price": 70,
                    "created_time": in_window(9),
                }
            ],
            schema=pa.schema(
                [
                    ("ticker", pa.string()),
                    ("count", pa.int64()),
                    ("yes_price", pa.int64()),
                    ("no_price", pa.int64()),
                    ("created_time", pa.timestamp("us", tz="UTC")),
                ]
            ),
        ),
        target,
    )
    inventory = Inventory({KALSHI_LAYER: (Shard("partial/part-0.parquet", layer=KALSHI_LAYER),)})
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert len(result.trades) == 1
    trade = result.trades[0]
    # The columns that are present are mapped; the absent ones are null.
    assert trade.contract_id == "KXFED-25JAN"
    assert trade.price == Decimal("0.30")
    assert trade.trade_id is None
    assert trade.direction is None
    assert trade.event_direction is None


def test_shard_without_its_time_column_is_skipped_with_a_reason(
    tmp_path: pathlib.Path,
) -> None:
    target = tmp_path / "no-time" / "part-0.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [{"ticker": "KXFED-25JAN", "count": 1}],
            schema=pa.schema([("ticker", pa.string()), ("count", pa.int64())]),
        ),
        target,
    )
    inventory = Inventory({KALSHI_LAYER: (Shard("no-time/part-0.parquet", layer=KALSHI_LAYER),)})
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_read == ()
    assert len(result.shards_skipped) == 1
    relative, reason = result.shards_skipped[0]
    assert relative == "no-time/part-0.parquet"
    assert reason.startswith("layer_time_column_absent_from_shard_schema")
    assert "created_time" in reason


def test_corrupt_shard_file_is_skipped_not_fatal(tmp_path: pathlib.Path) -> None:
    target = tmp_path / "broken" / "part-0.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"not a parquet file")
    inventory = Inventory({KALSHI_LAYER: (Shard("broken/part-0.parquet", layer=KALSHI_LAYER),)})
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    assert result.shards_skipped == (("broken/part-0.parquet", SKIP_SCHEMA_UNREADABLE),)
    assert result.trades == ()


def test_trades_are_historical_records_without_receipt_evidence(
    tmp_path: pathlib.Path,
) -> None:
    kalshi_shard(
        tmp_path,
        "kalshi-trades/trades-0000.parquet",
        [("t1", "KXFED-25JAN", 1, 30, 70, "yes", in_window())],
    )
    inventory = Inventory(
        {KALSHI_LAYER: (Shard("kalshi-trades/trades-0000.parquet", layer=KALSHI_LAYER),)}
    )
    result = extract_trades(
        tmp_path,
        inventory,
        layer=KALSHI_LAYER,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
    )
    trade = result.trades[0]
    assert isinstance(trade, HistoricalTrade)
    assert trade.clock.received_time is None
    assert trade.clock.availability.is_known is False
    assert trade.market_key == "kalshi|KXFED-25JAN"
