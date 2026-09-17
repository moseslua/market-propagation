"""The Kalshi contract universe, which is the union of two observation paths.

These tests pin the population rule rather than the numbers on any one checkout: a
contract observed through either path is a candidate, a contract both paths hold is one
contract with two observations, and a contract neither path observed cannot enter on a
provenance it does not have.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.ingest.kalshi_universe import (
    ARCHIVE_LAYER,
    ARCHIVE_MARKETS_GLOB,
    LIVE_LAYER,
    LIVE_MARKETS_GLOB,
    UniverseError,
    assert_union_identity,
    declared_layer_origin,
    layer_presence,
    union_market_rows,
)
from market_propagation.neighbors import (
    ORIGIN_ARCHIVED_AND_LIVE,
    ORIGIN_ARCHIVED_ONLY,
    ORIGIN_LIVE_ONLY,
)

COLUMNS = ("ticker", "event_ticker", "title", "status")
SERIES = ("KXFED",)

#: The canonical layer names, because a row's provenance is decided by which declared
#: layer held it rather than by the spelling of a glob.
FIXTURE_LAYERS = (
    (LIVE_LAYER, "live/*.parquet"),
    (ARCHIVE_LAYER, "archive/*.parquet"),
)


def write_shard(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            name: pa.array([row[index] for row in rows], type=pa.string())
            for index, name in enumerate(COLUMNS)
        }
    )
    pq.write_table(table, path)


def fixtures(tmp_path, monkeypatch, *, archive, live):
    monkeypatch.chdir(tmp_path)
    if archive:
        write_shard(tmp_path / "archive" / "markets-0000.parquet", archive)
    if live:
        write_shard(tmp_path / "live" / "markets-0000.parquet", live)
    return FIXTURE_LAYERS


def union(layers):
    return union_market_rows(SERIES, columns=COLUMNS, layers=layers)


def test_a_contract_only_one_path_observed_is_still_a_candidate(tmp_path, monkeypatch):
    """The union admits each path's own contracts, which is the whole point of the change.

    Under the archive-only universe the live contract below was not a candidate at all,
    because presence in a third-party transcription was the membership rule.
    """
    layers = fixtures(
        tmp_path,
        monkeypatch,
        archive=[("KXFED-25JAN-T4.25", "KXFED-25JAN", "Above 4.25%", "active")],
        live=[("KXFED-25MAR-T4.50", "KXFED-25MAR", "Above 4.50%", "active")],
    )
    markets, diagnostics = union(layers)
    by_ticker = {row["ticker"]: row for row in markets}

    assert set(by_ticker) == {"KXFED-25JAN-T4.25", "KXFED-25MAR-T4.50"}
    assert by_ticker["KXFED-25JAN-T4.25"]["observation_origin"] == ORIGIN_ARCHIVED_ONLY
    assert by_ticker["KXFED-25MAR-T4.50"]["observation_origin"] == ORIGIN_LIVE_ONLY
    assert diagnostics["provenance_counts"] == {
        ORIGIN_ARCHIVED_ONLY: 1,
        ORIGIN_LIVE_ONLY: 1,
        ORIGIN_ARCHIVED_AND_LIVE: 0,
    }


def test_a_contract_both_paths_hold_is_one_identity_with_two_observations(tmp_path, monkeypatch):
    row = ("KXFED-25JAN-T4.25", "KXFED-25JAN", "Above 4.25%", "active")
    layers = fixtures(tmp_path, monkeypatch, archive=[row], live=[row])
    markets, diagnostics = union(layers)

    assert [market["ticker"] for market in markets] == ["KXFED-25JAN-T4.25"]
    assert markets[0]["seen_archive"] is True
    assert markets[0]["seen_live"] is True
    assert markets[0]["observation_origin"] == ORIGIN_ARCHIVED_AND_LIVE
    assert diagnostics["union_contracts"] == 1
    assert diagnostics["archived_contracts"] == 1
    assert diagnostics["live_contracts"] == 1


def test_a_disagreement_is_resolved_by_the_declared_layer_order(tmp_path, monkeypatch):
    """The live layer is read first, so it wins a field both layers state.

    The archive still supplies the fields only it states, so the union is not the live
    layer with the archive's gaps left in it.
    """
    layers = fixtures(
        tmp_path,
        monkeypatch,
        archive=[("KXFED-25JAN-T4.25", "KXFED-25JAN", "archived title", "closed")],
        live=[("KXFED-25JAN-T4.25", "KXFED-25JAN", "live title", None)],
    )
    markets, _ = union(layers)

    assert markets[0]["title"] == "live title"
    assert markets[0]["status"] == "closed"


def test_a_layer_holding_nothing_is_reported_rather_than_widened(tmp_path, monkeypatch):
    """No capture yet reads as an empty live path, not as an undeclared one."""
    layers = fixtures(
        tmp_path,
        monkeypatch,
        archive=[("KXFED-25JAN-T4.25", "KXFED-25JAN", "Above 4.25%", "active")],
        live=[],
    )
    markets, diagnostics = union(layers)

    assert [market["ticker"] for market in markets] == ["KXFED-25JAN-T4.25"]
    assert diagnostics["layers_empty"] == [LIVE_LAYER]
    assert diagnostics["layers_present"] == [ARCHIVE_LAYER]
    assert diagnostics["live_contracts"] == 0
    assert layer_presence(layers)[LIVE_LAYER] == 0
    assert layer_presence(layers)[ARCHIVE_LAYER] == 1


def test_a_contract_no_declared_series_names_is_not_a_candidate(tmp_path, monkeypatch):
    """The union widens the observation paths, not the declared series."""
    layers = fixtures(
        tmp_path,
        monkeypatch,
        archive=[("KXCPI-25JAN-T300", "KXCPI-25JAN", "Above 300", "active")],
        live=[("KXFED-25MAR-T4.50", "KXFED-25MAR", "Above 4.50%", "active")],
    )
    markets, _ = union(layers)

    assert [market["ticker"] for market in markets] == ["KXFED-25MAR-T4.50"]


def test_the_union_identity_is_asserted_rather_than_reported():
    """Counts that disagree with the universe fail instead of being printed."""
    with pytest.raises(UniverseError, match="do not sum"):
        assert_union_identity(
            {
                "provenance_counts": {ORIGIN_ARCHIVED_ONLY: 2},
                "union_contracts": 3,
                "archived_contracts": 2,
                "live_contracts": 0,
                "archived_and_live_contracts": 0,
            }
        )
    with pytest.raises(UniverseError, match="subset of both"):
        assert_union_identity(
            {
                "provenance_counts": {ORIGIN_ARCHIVED_AND_LIVE: 2},
                "union_contracts": 2,
                "archived_contracts": 1,
                "live_contracts": 2,
                "archived_and_live_contracts": 2,
            }
        )


def test_a_caller_supplied_path_claims_no_declared_provenance():
    """A fixture directory is not an observation path, so it borrows no label."""
    assert declared_layer_origin(LIVE_MARKETS_GLOB) == ORIGIN_LIVE_ONLY
    assert declared_layer_origin(ARCHIVE_MARKETS_GLOB) == ORIGIN_ARCHIVED_ONLY
    assert declared_layer_origin("markets/*.parquet") is None
