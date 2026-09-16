"""Regression tests for the external-history record and its storage.

``HistoricalTrade`` is the bounded external-path record. It sits beside
``Trade`` rather than replacing it because the two carry different guarantees.
``Trade`` requires a verified non-null ``size``, which the ``trades`` table and
quantity-weighted flow depend on. A cleaned Polymarket archive row omits token
quantity entirely, so the external path has to be able to say "unknown size"
without saying "zero" and without weakening the existing contract.

The two properties these tests defend are therefore not incidental:

* An unknown size survives a Parquet round trip as null and never becomes a
  quantity, so a caller cannot silently weight a row by an invented number.
* A historical row's availability stays unknown. ``Clock.historical`` without a
  receipt yields no usable interval, so ``usable_time`` must come back null
  rather than carrying the source time onto the point-in-time axis.

The ``historical_trades`` and ``trade_panel`` declarations are also checked
against the real dataclass and the real panel column list, because a table whose
declared columns disagree with the record that fills it fails at seal time, and
that failure should surface here rather than during a real run.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from market_propagation.domain import Clock, HistoricalTrade, Provenance
from market_propagation.storage import (
    TABLE_SCHEMAS,
    TRADE_PANEL_COLUMNS,
    read_parquet,
    resolve_rows,
    write_parquet,
)

SHARD_HASH = "b3d1" * 16
OCCURRENCE = f"{SHARD_HASH}:41"
SOURCE_TIME = dt.datetime(2025, 3, 12, 12, 30, 15, 250000, tzinfo=dt.UTC)


def _kalshi(**overrides: object) -> HistoricalTrade:
    fields: dict[str, object] = {
        "venue": "kalshi",
        "contract_id": "KXCPI-26SEP-T0.4",
        "trade_id": "trade-1",
        "price": Decimal("0.42"),
        "raw_price_units": "cents",
        "price_precision": "exact_integer_cents",
        "raw_price": Decimal("42"),
        "secondary_price": Decimal("58"),
        "direction": "yes",
        "event_direction": 1,
        "event_price": Decimal("0.42"),
        "event_axis": "yes_price_is_event_axis",
        "size": Decimal("7"),
        "size_quality": "verified_source_quantity",
        "clock": Clock.historical(SOURCE_TIME),
        "provenance": Provenance(
            raw_hash=SHARD_HASH,
            record_id=OCCURRENCE,
            source="kalshi-trades",
        ),
    }
    fields.update(overrides)
    return HistoricalTrade(**fields)  # type: ignore[arg-type]


def _polymarket(**overrides: object) -> HistoricalTrade:
    fields: dict[str, object] = {
        "venue": "polymarket",
        "contract_id": "0xcondition",
        "token_id": "7788",
        "outcome_seq": 2,
        "price": Decimal("0.56"),
        "raw_price_units": "dollars",
        "price_precision": "float64_source_precision",
        "raw_price": Decimal("0.56"),
        "event_price": Decimal("0.44"),
        "event_axis": "outcome_seq_2_is_1_minus_price",
        "direction": "BUY",
        "event_direction": -1,
        "size": None,
        "size_quality": "unavailable_in_cleaned_layer",
        "clock": Clock.historical(SOURCE_TIME),
        "provenance": Provenance(
            raw_hash=SHARD_HASH,
            record_id=f"{SHARD_HASH}:7",
            source="polymarket-daily-aligned",
        ),
    }
    fields.update(overrides)
    return HistoricalTrade(**fields)  # type: ignore[arg-type]


def test_unknown_size_is_a_state_not_a_quantity() -> None:
    row = _polymarket()
    assert row.size is None
    assert row.size_is_verified is False
    assert _kalshi().size_is_verified is True


def test_verified_quality_requires_a_size() -> None:
    with pytest.raises(ValueError, match="quantity is verified but size is null"):
        _polymarket(size_quality="verified_source_quantity")


def test_size_present_under_an_unverified_quality_is_refused() -> None:
    with pytest.raises(ValueError, match="size_quality"):
        _polymarket(size=Decimal("3"))


def test_event_axis_must_accompany_a_projected_price() -> None:
    with pytest.raises(ValueError, match="event_axis"):
        _polymarket(event_axis=None)


def test_declared_enumerations_are_enforced() -> None:
    with pytest.raises(ValueError, match="raw_price_units"):
        _kalshi(raw_price_units="euros")
    with pytest.raises(ValueError, match="price_precision"):
        _kalshi(price_precision="guessed")
    with pytest.raises(ValueError, match="size_quality"):
        _kalshi(size_quality="probably_fine")
    with pytest.raises(ValueError, match="event_direction"):
        _kalshi(event_direction=2)


def test_historical_clock_establishes_no_usable_interval() -> None:
    row = _kalshi()
    assert row.clock.source_time == SOURCE_TIME
    assert row.clock.received_time is None
    assert row.clock.usable_time is None
    assert row.clock.availability.is_known is False


def test_historical_trades_schema_matches_the_record() -> None:
    schema = TABLE_SCHEMAS["historical_trades"]
    declared = set(schema.column_names)
    paths = resolve_rows([_kalshi()], "historical_trades")[0]
    assert set(paths) == declared
    # The quantity column is deliberately not required: an unknown quantity is a
    # real state that has to be sealable rather than refused.
    assert "size" not in schema.required
    assert "size_quality" in schema.required
    # The occurrence locator is required here, unlike the prospectively captured
    # ``trades`` table, because it is the only thing telling repeated identical
    # external fills apart.
    assert "record_id" in schema.required
    # Sorting by a column that is always null would look like a time ordering
    # while being none, so availability is excluded from the sort key.
    assert "usable_time" not in schema.sort_by
    assert "source_time" in schema.sort_by


def test_panel_schema_declares_no_quote_columns() -> None:
    declared = set(TRADE_PANEL_COLUMNS)
    for quote_only in (
        "bid",
        "ask",
        "spread_before",
        "spread_after",
        "depth_before",
        "depth_after",
    ):
        assert quote_only not in declared
    schema = TABLE_SCHEMAS["trade_panel"]
    assert "valid" in schema.required
    assert "post_release_trade_observed" in schema.required


def test_round_trip_preserves_null_size_and_unknown_availability(tmp_path: Path) -> None:
    reference = write_parquet(
        [_kalshi(), _polymarket()],
        tmp_path / "historical_trades.parquet",
        table="historical_trades",
    )
    frame = read_parquet(tmp_path / "historical_trades.parquet")

    assert reference.row_count == 2
    assert reference.table == "historical_trades"
    assert frame["usable_time"].isna().all()
    assert frame["received_time"].isna().all()

    by_venue = {row["venue"]: row for row in frame.to_dict("records")}
    assert by_venue["kalshi"]["size"] == Decimal("7.000000000000")
    assert by_venue["polymarket"]["size"] is None
    assert by_venue["polymarket"]["size_quality"] == "unavailable_in_cleaned_layer"
    assert by_venue["polymarket"]["event_direction"] == -1
    assert by_venue["kalshi"]["raw_price"] == Decimal("42.000000000000")


def test_panel_schema_round_trips_a_masked_row(tmp_path: Path) -> None:
    """A masked panel row must seal and read back with its nulls intact.

    The panel's whole point is that an unobserved endpoint is visibly missing
    rather than zero, so the storage declaration has to carry a null response
    and a null baseline age through a round trip. A declaration that could not
    hold them would force a caller to invent a number to get a row sealed.
    """
    row: dict[str, object] = dict.fromkeys(TRADE_PANEL_COLUMNS)
    row.update(
        {
            "event_id": "cpi_2025_03",
            "cluster_id": "cpi_2025_03",
            "family": "cpi",
            "venue": "kalshi",
            "contract_id": "KXCPI-26SEP-T0.4",
            "cohort": "external_transaction_response",
            "event_time": dt.datetime(2025, 3, 12, 12, 30, tzinfo=dt.UTC),
            "horizon_seconds": 300,
            "baseline_source_time": dt.datetime(2025, 3, 12, 12, 29, tzinfo=dt.UTC),
            "baseline": 0.42,
            "post_release_trade_observed": False,
            "valid": False,
            "exclusion_reason": "no_post_release_trade",
            "clock_mode": "source",
            "availability_status": "source_time_only",
            "size_verified": False,
            "provenance_locators_json": [f"{SHARD_HASH}:41"],
            "flags_json": ["no_post_release_trade"],
        }
    )
    reference = write_parquet([row], tmp_path / "trade_panel.parquet", table="trade_panel")
    frame = read_parquet(tmp_path / "trade_panel.parquet")

    assert reference.row_count == 1
    # Absence is asserted on the sealed bytes, not on the pandas view: a null
    # float64 column comes back from ``to_pandas`` as NaN, so comparing against
    # None there would test pandas' dtype choice rather than what was stored.
    stored = pq.read_table(tmp_path / "trade_panel.parquet")
    for absent in ("response", "endpoint", "endpoint_source_time", "endpoint_age_seconds"):
        assert stored[absent].null_count == 1, absent
    # A missing endpoint is not an observed zero, in either representation.
    assert not (frame["response"] == 0).any()

    sealed = frame.to_dict("records")[0]
    assert sealed["baseline"] == 0.42
    assert sealed["valid"] is False
    assert sealed["exclusion_reason"] == "no_post_release_trade"
    assert sealed["provenance_locators_json"] == [f"{SHARD_HASH}:41"]
    assert sealed["flags_json"] == ["no_post_release_trade"]


def test_panel_schema_requires_the_row_identifiers(tmp_path: Path) -> None:
    """A row that cannot be identified must fail to seal, not seal as blank."""
    row: dict[str, object] = dict.fromkeys(TRADE_PANEL_COLUMNS)
    row.update(
        {
            "event_id": "cpi_2025_03",
            "horizon_seconds": 300,
            "valid": True,
            "clock_mode": "source",
            "availability_status": "source_time_only",
            "post_release_trade_observed": True,
        }
    )
    with pytest.raises(ValueError, match="required columns"):
        write_parquet([row], tmp_path / "trade_panel.parquet", table="trade_panel")


def test_a_null_json_cell_round_trips_as_null(tmp_path: Path) -> None:
    """A sealed null JSON column must read back as a null rather than raise.

    A null JSON column arrives from pandas as ``NaN`` rather than ``None``, and a
    decode that tests only for ``None`` therefore hands a float to ``json.loads``
    and fails. A row whose provenance locators were genuinely absent has to stay
    readable, and absence has to stay distinguishable from an empty list, because
    "no locators were recorded" and "the locator list is empty" are different
    facts about a row's lineage.
    """
    row: dict[str, object] = dict.fromkeys(TRADE_PANEL_COLUMNS)
    row.update(
        {
            "event_id": "cpi_2025_03",
            "cluster_id": "cpi_2025_03",
            "family": "cpi",
            "venue": "kalshi",
            "contract_id": "FED-25DEC-T2.75",
            "cohort": "external_transaction_response",
            "event_time": dt.datetime(2025, 3, 12, 12, 30, tzinfo=dt.UTC),
            "horizon_seconds": 300,
            "valid": False,
            "clock_mode": "source",
            "availability_status": "source_time_only",
            "post_release_trade_observed": False,
            "provenance_locators_json": None,
            "flags_json": None,
        }
    )
    write_parquet([row], tmp_path / "nulls.parquet", table="trade_panel")
    sealed = read_parquet(tmp_path / "nulls.parquet")
    assert sealed["provenance_locators_json"].isna().all()
    assert sealed["flags_json"].isna().all()

    row["flags_json"] = []
    write_parquet([row], tmp_path / "empty.parquet", table="trade_panel")
    emptied = read_parquet(tmp_path / "empty.parquet")
    assert emptied["flags_json"].iloc[0] == []


def test_resealing_identical_content_keeps_the_same_identity(tmp_path: Path) -> None:
    path = tmp_path / "historical_trades.parquet"
    first = write_parquet([_kalshi(), _polymarket()], path, table="historical_trades")
    second = write_parquet([_polymarket(), _kalshi()], path, table="historical_trades")
    # Row order is not content: a differently ordered write is the same dataset.
    assert first.content_hash == second.content_hash
    assert first.manifest == second.manifest
