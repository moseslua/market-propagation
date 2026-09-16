"""Acceptance tests for the immutable external-archive inventory.

The inventory exists so a run can say which bytes it read and notice when they move.
Each test below defends one property that a plausible mistake would break: a digest
taken over a root that a copy changes, an identity that a wall clock perturbs, a
corrupt shard that aborts the run instead of being recorded, and an absent footer
statistic that returns a null bound a caller would read as "nothing in range".

Every archive here is a real Parquet file written by ``pyarrow`` into ``tmp_path``.
No test reads ``data/external``; the two tests that touch the real configuration read
``configs/external_history_v1.yaml``, which declares patterns without opening them.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import pathlib
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.ingest.external_inventory import (
    CONFIG_PATH,
    DEFAULT_INVENTORY_VERSION,
    FLAG_DUPLICATE_SHARD_FILENAME,
    FLAG_HASH_UNAVAILABLE,
    FLAG_LAYER_MISSING,
    FLAG_SHARD_UNREADABLE,
    HASH_SCOPE_FULL,
    HASH_SCOPE_NONE,
    INPUT_CLASSES,
    INVENTORY_FILENAME,
    LAYER_STATUS_MISSING,
    LAYER_STATUS_PRESENT,
    SHARD_STATUS_READ,
    SHARD_STATUS_UNREADABLE,
    ExternalInventory,
    LayerSpec,
    build_inventory,
    load_inventory,
    load_layer_specs,
    verify_inventory,
    write_inventory,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

CREATED_TIME = pa.timestamp("us", tz="UTC")
CREATED_INSTANT = dt.datetime(2025, 3, 4, 5, 6, 7, tzinfo=dt.UTC)
TRADE_SCHEMA = pa.schema(
    [
        pa.field("trade_id", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("yes_price", pa.int64()),
        pa.field("created_time", CREATED_TIME),
    ]
)

KALSHI_TRADES = LayerSpec(
    name="kalshi_trades",
    path_pattern="kalshi/trades-*.parquet",
    input_class="external_historical_archive",
    role="source_trades",
    producer="a",
    license="CC-BY-4.0",
    venue="kalshi",
    time_column="created_time",
    time_unit="timestamp_us_utc",
)

POLYMARKET_ALIGNED = LayerSpec(
    name="polymarket_daily_aligned",
    path_pattern="polymarket/aligned/*.parquet",
    input_class="external_historical_archive",
    role="cleaned_standard_binary",
    producer="b",
    license="CC-BY-4.0",
    venue="polymarket",
    time_column="block_timestamp",
    time_unit="epoch_seconds",
)

CTF = LayerSpec(
    name="polymarket_ctf",
    path_pattern="polymarket/ctf/*.parquet",
    input_class="external_historical_archive",
    role="lifecycle_records",
    producer="b",
    license="CC-BY-4.0",
    venue="polymarket",
)


def trade_rows(*, created: dt.datetime, count: int = 2) -> pa.Table:
    return pa.table(
        {
            "trade_id": pa.array([f"t{index}" for index in range(count)], type=pa.string()),
            "ticker": pa.array(["KXFED-25JAN"] * count, type=pa.string()),
            "yes_price": pa.array([42] * count, type=pa.int64()),
            "created_time": pa.array([created] * count, type=CREATED_TIME),
        },
        schema=TRADE_SCHEMA,
    )


def trades_at(*times: dt.datetime) -> pa.Table:
    """One shard whose rows carry the given source times, one per row group."""
    count = len(times)
    return pa.table(
        {
            "trade_id": pa.array([f"t{index}" for index in range(count)], type=pa.string()),
            "ticker": pa.array(["KXFED-25JAN"] * count, type=pa.string()),
            "yes_price": pa.array([42] * count, type=pa.int64()),
            "created_time": pa.array(list(times), type=CREATED_TIME),
        },
        schema=TRADE_SCHEMA,
    )


def write_shard(
    path: pathlib.Path, table: pa.Table, *, write_statistics: bool = True, row_group_size: int = 1
) -> pathlib.Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, write_statistics=write_statistics, row_group_size=row_group_size)
    return path


def build_tree(root: pathlib.Path) -> pathlib.Path:
    """A tiny but complete archive: two Kalshi shards and one Polymarket shard."""
    write_shard(
        root / "kalshi" / "trades-0000.parquet",
        trade_rows(created=dt.datetime(2025, 1, 2, 3, 4, 5, tzinfo=dt.UTC), count=3),
    )
    write_shard(
        root / "kalshi" / "trades-0001.parquet",
        trade_rows(created=dt.datetime(2025, 2, 6, 7, 8, 9, tzinfo=dt.UTC), count=2),
    )
    write_shard(
        root / "polymarket" / "aligned" / "2025_01_02.parquet",
        pa.table(
            {
                "asset_id": pa.array(["a"], type=pa.string()),
                "block_timestamp": pa.array([1735787045], type=pa.int64()),
                "price": pa.array([0.42], type=pa.float64()),
            }
        ),
    )
    return root


def inventory_of(root: pathlib.Path, **kwargs: Any) -> ExternalInventory:
    return build_inventory(root, layers=(KALSHI_TRADES, POLYMARKET_ALIGNED), **kwargs)


def test_successful_inventory_records_footer_facts(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root)

    assert inventory.hash_scope == HASH_SCOPE_FULL
    assert inventory.inventory_version == DEFAULT_INVENTORY_VERSION
    assert inventory.created_at.tzinfo is not None
    assert [layer.name for layer in inventory.layers] == [
        KALSHI_TRADES.name,
        POLYMARKET_ALIGNED.name,
    ]
    assert all(layer.status == LAYER_STATUS_PRESENT for layer in inventory.layers)

    kalshi = inventory.layer(KALSHI_TRADES.name)
    assert kalshi.shard_count == 2
    assert kalshi.total_rows == 5
    assert kalshi.error is None

    shard = inventory.shards_for(KALSHI_TRADES.name)[0]
    assert shard.status == SHARD_STATUS_READ
    assert shard.row_count == 3
    assert shard.row_group_count == 3
    assert shard.bytes == (root / shard.relative_path).stat().st_size
    assert shard.sha256 == _sha256(root / shard.relative_path)
    assert shard.error is None
    assert dict(shard.columns)["created_time"] == "timestamp[us, tz=UTC]"
    assert [column for column, _, _ in shard.timestamp_stats] == ["created_time"]
    assert shard.missing_statistics == ()


def test_schema_fingerprint_separates_different_columns(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root)

    trade = inventory.shards_for(KALSHI_TRADES.name)[0]
    aligned = inventory.shards_for(POLYMARKET_ALIGNED.name)[0]

    assert trade.schema_fingerprint
    assert trade.schema_fingerprint != aligned.schema_fingerprint


def test_timestamp_statistics_report_the_created_bounds(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "external"
    early = dt.datetime(2025, 1, 2, 3, 4, 5, tzinfo=dt.UTC)
    late = dt.datetime(2025, 1, 2, 9, 8, 7, tzinfo=dt.UTC)
    # Two row groups with distinct rows, so the reported pair is a combination over
    # row groups rather than one group's own bound read back twice.
    write_shard(root / "kalshi" / "trades-0000.parquet", trades_at(early, late))

    inventory = build_inventory(root, layers=(KALSHI_TRADES,))
    column, low, high = inventory.shards[0].timestamp_stats[0]

    assert column == "created_time"
    assert low == early.isoformat()
    assert high == late.isoformat()
    assert low < high


def test_epoch_seconds_bounds_are_reported_as_instants(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root)

    column, low, high = inventory.shards_for(POLYMARKET_ALIGNED.name)[0].timestamp_stats[0]

    assert column == "block_timestamp"
    assert low == dt.datetime.fromtimestamp(1735787045, tz=dt.UTC).isoformat()
    assert high == low


def test_identity_is_stable_across_two_runs_and_roots(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    first = inventory_of(root)
    second = inventory_of(root)

    assert first.identity == second.identity
    assert len(first.identity) == 64

    # The identity is over relative paths and content, so the same tree copied to
    # another root is the same input version and the run instant does not enter it.
    copied = tmp_path / "elsewhere" / "external"
    copied.parent.mkdir(parents=True, exist_ok=True)
    _copy_tree(root, copied)
    third = inventory_of(copied)

    assert third.identity == first.identity
    assert third.root != first.root
    assert third.created_at >= first.created_at


def test_changed_byte_changes_the_identity_and_verify_catches_it(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root)
    target = root / "kalshi" / "trades-0001.parquet"

    before = verify_inventory(inventory, root)
    assert before["unchanged"] is True
    assert before["shards_verified"] == 3
    assert before["changed"] == []

    # A single appended byte is a different file to every consumer that hashes it.
    with target.open("ab") as handle:
        handle.write(b"x")

    after = inventory_of(root)
    assert after.identity != inventory.identity

    verified = verify_inventory(inventory, root)
    assert verified["unchanged"] is False
    assert verified["changed"] == ["kalshi/trades-0001.parquet"]
    assert verified["identity"] == inventory.identity

    fresh = verify_inventory(after, root)
    assert fresh["unchanged"] is True
    assert fresh["identity"] != inventory.identity


def test_verify_reports_a_removed_shard_as_missing(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root)

    (root / "kalshi" / "trades-0000.parquet").unlink()

    verified = verify_inventory(inventory, root)
    assert verified["missing"] == ["kalshi/trades-0000.parquet"]
    assert verified["changed"] == []
    assert verified["unchanged"] is False
    assert verified["shards_verified"] == 2


def test_no_hash_inventory_records_none_scope_and_cannot_verify(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root, verify_hashes=False)

    assert inventory.hash_scope == HASH_SCOPE_NONE
    assert all(shard.sha256 is None for shard in inventory.shards)
    assert len(inventory.identity) == 64

    verified = verify_inventory(inventory, root)
    assert verified["unchanged"] is False
    assert verified["shards_verified"] == 0
    assert verified["not_verified"] == [shard.relative_path for shard in inventory.shards]


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="zero_byte"),
        pytest.param(b"PAR1PAR1", id="magic_only"),
        pytest.param(b"not parquet at all", id="text"),
        pytest.param(b"PAR1" + b"\x00" * 64 + b"PAR1", id="zeroed_footer"),
        pytest.param(b'{"a":1}', id="json_body"),
    ],
)
def test_every_corrupt_body_becomes_an_unreadable_record(
    tmp_path: pathlib.Path, payload: bytes
) -> None:
    root = build_tree(tmp_path / "external")
    broken = root / "kalshi" / "trades-0009.parquet"
    broken.write_bytes(payload)

    inventory = inventory_of(root)

    record = next(
        shard for shard in inventory.shards if shard.relative_path == "kalshi/trades-0009.parquet"
    )
    assert record.status == SHARD_STATUS_UNREADABLE
    assert record.row_count is None
    assert record.error is not None
    # The readable shards are still inventoried alongside it.
    assert len(inventory.shards_for(POLYMARKET_ALIGNED.name)) == 1
    assert inventory.layer(KALSHI_TRADES.name).total_rows == 5


def test_truncated_shard_is_unreadable_without_raising(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "external"
    intact = write_shard(
        root / "kalshi" / "trades-0000.parquet",
        trade_rows(created=CREATED_INSTANT, count=4),
    )
    body = intact.read_bytes()
    truncated = root / "kalshi" / "trades-0001.parquet"
    truncated.write_bytes(body[: len(body) // 2])

    inventory = build_inventory(root, layers=(KALSHI_TRADES,))

    assert [shard.status for shard in inventory.shards] == [
        SHARD_STATUS_READ,
        SHARD_STATUS_UNREADABLE,
    ]
    assert inventory.layer(KALSHI_TRADES.name).total_rows == 4


def test_a_directory_matching_the_glob_is_not_a_shard(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "external"
    (root / "polymarket" / "ctf" / "merges.parquet").mkdir(parents=True)

    inventory = build_inventory(root, layers=(CTF,))
    record = inventory.layer(CTF.name)

    # A directory is not a shard file, so it is not inventoried as one, and a layer
    # whose pattern matched only a directory has produced nothing to read.
    assert inventory.shards == ()
    assert record.status == LAYER_STATUS_MISSING
    assert record.shard_count == 0


def test_undigestible_shard_is_flagged_and_still_inventoried(tmp_path: pathlib.Path) -> None:
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip("a root process can read a mode-0 file, so the failure cannot be provoked")
    root = build_tree(tmp_path / "external")
    blocked = root / "kalshi" / "trades-0002.parquet"
    write_shard(blocked, trade_rows(created=CREATED_INSTANT, count=1))
    blocked.chmod(0o000)
    try:
        inventory = inventory_of(root)
    finally:
        blocked.chmod(0o600)

    record = next(
        shard for shard in inventory.shards if shard.relative_path == "kalshi/trades-0002.parquet"
    )
    assert FLAG_HASH_UNAVAILABLE in record.flags
    assert FLAG_HASH_UNAVAILABLE in inventory.flags
    assert FLAG_HASH_UNAVAILABLE in inventory.as_dict()["absent_evidence"]
    assert record.sha256 is None
    assert record.error is not None
    # The size was measured before the digest was attempted, so it is a real size.
    assert record.bytes == blocked.stat().st_size

    # A shard with no digest cannot be verified at all, and saying so is not the same
    # as saying its bytes changed.
    verified = verify_inventory(inventory, root)
    assert verified["unchanged"] is False
    assert verified["not_verified"] == ["kalshi/trades-0002.parquet"]
    assert verified["changed"] == []


def test_missing_layer_is_reported_with_its_pattern(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    # The layer is supplied but has no directory to match, which is the real case: a
    # configured input that files nothing is reported missing, not omitted.
    inventory = build_inventory(root, layers=(KALSHI_TRADES, POLYMARKET_ALIGNED, CTF))

    record = inventory.layer(CTF.name)

    assert record.status == LAYER_STATUS_MISSING
    assert record.shard_count == 0
    assert record.total_rows == 0
    assert record.error is not None
    assert CTF.path_pattern in record.error
    assert FLAG_LAYER_MISSING in record.flags
    assert FLAG_LAYER_MISSING in inventory.flags
    assert FLAG_LAYER_MISSING in inventory.as_dict()["absent_evidence"]
    assert inventory.as_dict()["layers_missing"] == [CTF.name]

    # A layer that was never supplied is not in the inventory at all, and asking for
    # one is a caller error rather than a missing input.
    smaller = inventory_of(root)
    with pytest.raises(KeyError):
        smaller.layer(CTF.name)
    with pytest.raises(KeyError):
        smaller.shards_for("no_such_layer")


def test_corrupt_shard_is_recorded_without_raising(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    corrupt = root / "kalshi" / "trades-0002.parquet"
    corrupt.write_text("this file is not a parquet shard")

    inventory = inventory_of(root)

    record = next(
        shard for shard in inventory.shards if shard.relative_path == "kalshi/trades-0002.parquet"
    )
    assert record.status == SHARD_STATUS_UNREADABLE
    assert record.error is not None
    assert record.row_count is None
    assert record.row_group_count is None
    assert record.schema_fingerprint == ""
    assert record.columns == ()
    assert record.bytes == corrupt.stat().st_size
    assert record.sha256 == _sha256(corrupt)

    kalshi = inventory.layer(KALSHI_TRADES.name)
    assert kalshi.status == LAYER_STATUS_PRESENT
    assert kalshi.shard_count == 3
    # An unreadable shard contributes no rows: unknown is not zero.
    assert kalshi.total_rows == 5
    assert FLAG_SHARD_UNREADABLE in kalshi.flags
    assert FLAG_SHARD_UNREADABLE in inventory.as_dict()["absent_evidence"]
    assert inventory.as_dict()["shards_unreadable"] == ["kalshi/trades-0002.parquet"]

    # The rest of the tree is still inventoried, which is the point of not raising.
    assert inventory.layer(POLYMARKET_ALIGNED.name).status == LAYER_STATUS_PRESENT


def test_corrupt_timestamp_column_is_named_in_missing_statistics(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    (root / "kalshi" / "trades-0002.parquet").write_bytes(b"PAR1" + b"\x00" * 32)

    inventory = inventory_of(root)
    record = next(
        shard for shard in inventory.shards if shard.relative_path == "kalshi/trades-0002.parquet"
    )

    assert record.status == SHARD_STATUS_UNREADABLE
    assert record.timestamp_stats == (("created_time", None, None),)
    assert any("created_time" in reason for reason in record.missing_statistics)


def test_absent_timestamp_statistics_are_named(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "external"
    write_shard(
        root / "kalshi" / "trades-0000.parquet",
        trade_rows(created=dt.datetime(2025, 1, 2, 3, 4, 5, tzinfo=dt.UTC)),
        write_statistics=False,
    )

    inventory = build_inventory(root, layers=(KALSHI_TRADES,))
    record = inventory.shards[0]

    assert record.status == SHARD_STATUS_READ
    assert record.row_count == 2
    assert record.timestamp_stats == (("created_time", None, None),)
    assert record.missing_statistics == ("created_time:footer_statistics_absent",)
    # A null bound with a reason is not an empty range.
    assert record.flags == ()


def test_partial_timestamp_statistics_are_named(tmp_path: pathlib.Path) -> None:
    root = tmp_path / "external"
    # An all-null row group writes no min/max while its neighbour does, which is the
    # partial case: the reported bound is a real bound over some of the shard only.
    write_shard(
        root / "kalshi" / "trades-0000.parquet",
        pa.table({"created_time": pa.array([None, CREATED_INSTANT], type=CREATED_TIME)}),
    )

    record = build_inventory(root, layers=(KALSHI_TRADES,)).shards[0]

    assert record.status == SHARD_STATUS_READ
    assert record.row_group_count == 2
    assert record.timestamp_stats == (
        ("created_time", CREATED_INSTANT.isoformat(), CREATED_INSTANT.isoformat()),
    )
    assert record.missing_statistics == ("created_time:footer_statistics_partially_absent",)


def test_undeclared_time_unit_leaves_a_numeric_bound_unreported(tmp_path: pathlib.Path) -> None:
    root = pathlib.Path(tmp_path / "external")
    write_shard(
        root / "polymarket" / "aligned" / "2025_01_02.parquet",
        pa.table({"block_timestamp": pa.array([1735787045], type=pa.int64())}),
    )
    spec = LayerSpec(
        name=POLYMARKET_ALIGNED.name,
        path_pattern=POLYMARKET_ALIGNED.path_pattern,
        input_class=POLYMARKET_ALIGNED.input_class,
        role=POLYMARKET_ALIGNED.role,
        producer=POLYMARKET_ALIGNED.producer,
        license=POLYMARKET_ALIGNED.license,
        venue="polymarket",
        time_column="block_timestamp",
        time_unit=None,
    )

    record = build_inventory(root, layers=(spec,)).shards[0]

    assert record.timestamp_stats == (("block_timestamp", None, None),)
    assert record.missing_statistics == ("block_timestamp:bound_not_expressible_as_instant",)


def test_time_column_absent_from_schema_is_named(tmp_path: pathlib.Path) -> None:
    root = pathlib.Path(tmp_path / "external")
    write_shard(
        root / "kalshi" / "trades-0000.parquet",
        pa.table({"trade_id": pa.array(["t0"], type=pa.string())}),
    )

    record = build_inventory(root, layers=(KALSHI_TRADES,)).shards[0]

    assert record.timestamp_stats == (("created_time", None, None),)
    assert record.missing_statistics == ("created_time:time_column_absent_from_schema",)


def test_layer_without_a_declared_time_column_reports_no_statistics(tmp_path: pathlib.Path) -> None:
    root = pathlib.Path(tmp_path / "external")
    write_shard(
        root / "polymarket" / "ctf" / "merges.parquet",
        pa.table({"id": pa.array(["m0"], type=pa.string())}),
    )

    inventory = build_inventory(root, layers=(CTF,))
    record = inventory.shards[0]

    assert record.timestamp_stats == ()
    assert record.missing_statistics == ()
    assert inventory.layer(CTF.name).shard_count == 1


def test_duplicate_filenames_across_layers_are_flagged(tmp_path: pathlib.Path) -> None:
    root = pathlib.Path(tmp_path / "external")
    shared = "2025_01_02.parquet"
    write_shard(
        root / "polymarket" / "aligned" / shared,
        pa.table({"block_timestamp": pa.array([1735787045], type=pa.int64())}),
    )
    write_shard(
        root / "polymarket" / "ctf" / shared,
        pa.table({"id": pa.array(["m0"], type=pa.string())}),
    )

    inventory = build_inventory(root, layers=(POLYMARKET_ALIGNED, CTF))

    assert FLAG_DUPLICATE_SHARD_FILENAME in inventory.flags
    for shard in inventory.shards:
        assert FLAG_DUPLICATE_SHARD_FILENAME in shard.flags
    assert FLAG_DUPLICATE_SHARD_FILENAME in inventory.layer(POLYMARKET_ALIGNED.name).flags
    assert FLAG_DUPLICATE_SHARD_FILENAME in inventory.layer(CTF.name).flags
    # A shared filename is a naming fact, not absent evidence.
    assert FLAG_DUPLICATE_SHARD_FILENAME not in inventory.as_dict()["absent_evidence"]
    assert pathlib.Path(inventory.shards[0].relative_path).name == shared


def test_distinct_filenames_are_not_flagged(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")

    inventory = inventory_of(root)

    assert FLAG_DUPLICATE_SHARD_FILENAME not in inventory.flags
    assert all(FLAG_DUPLICATE_SHARD_FILENAME not in shard.flags for shard in inventory.shards)


def test_inventory_round_trips_through_write_and_load(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    # A corrupt shard is still a shard: it is counted and recorded, not dropped.
    (root / "kalshi" / "trades-0002.parquet").write_text("not parquet")
    inventory = inventory_of(root)

    output = tmp_path / "out"
    written = write_inventory(inventory, output)

    path = pathlib.Path(written["path"])
    assert path == output / INVENTORY_FILENAME
    assert path.exists()
    assert written["identity"] == inventory.identity
    assert written["sha256"] == _sha256(path)
    assert written["layer_count"] == 2
    assert written["shard_count"] == 4
    assert written["total_bytes"] == sum(shard.bytes for shard in inventory.shards)
    # Three Kalshi rows plus two more plus one Polymarket row; the corrupt shard
    # contributes no rows because its row count is unknown, not zero.
    assert written["total_rows"] == 6
    assert written["flags"] == list(inventory.flags)

    reloaded = load_inventory(path)
    assert reloaded == inventory
    assert reloaded.as_dict() == inventory.as_dict()
    assert load_inventory(output) == inventory

    reloaded_shard = next(
        shard for shard in reloaded.shards if shard.status == SHARD_STATUS_UNREADABLE
    )
    original_shard = next(
        shard for shard in inventory.shards if shard.status == SHARD_STATUS_UNREADABLE
    )
    assert reloaded_shard == original_shard
    assert reloaded_shard.columns == ()
    assert reloaded_shard.timestamp_stats == (("created_time", None, None),)
    assert reloaded_shard.error == original_shard.error

    # An unreadable parquet body is still a readable sequence of bytes, so its digest
    # verifies: verification answers "are these the same bytes", not "is this valid".
    verified = verify_inventory(reloaded, root)
    assert verified["unchanged"] is True
    assert verified["shards_verified"] == 4
    # The unreadable shard is the case that matters: its footer is garbage, but its
    # bytes still verify, so it is not reported as changed or missing.
    assert reloaded_shard.sha256 == _sha256(root / reloaded_shard.relative_path)
    assert verified["changed"] == []


def test_written_json_is_the_audit_view(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    inventory = inventory_of(root)
    written = write_inventory(inventory, tmp_path / "out")

    document = json.loads((tmp_path / "out" / INVENTORY_FILENAME).read_text(encoding="utf-8"))

    assert document == {
        key: value for key, value in written.items() if key not in {"path", "sha256"}
    }
    assert document["shards"][0]["columns"][0][0] == "trade_id"
    assert document["shards"][0]["layer"] == KALSHI_TRADES.name
    assert isinstance(document["shards"], list)
    assert isinstance(document["layers"], list)


def test_load_inventory_refuses_an_edited_identity(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    write_inventory(inventory_of(root), tmp_path / "out")
    path = tmp_path / "out" / INVENTORY_FILENAME
    document = json.loads(path.read_text(encoding="utf-8"))
    document["shards"][0]["bytes"] = document["shards"][0]["bytes"] + 1
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="identity"):
        load_inventory(path)


def test_selected_layers_are_the_configured_names(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    config = _write_config(tmp_path)

    inventory = build_inventory(root, layers=("kalshi_trades",), config_path=config)

    assert [layer.name for layer in inventory.layers] == ["kalshi_trades"]
    assert inventory.layer("kalshi_trades").shard_count == 2

    with pytest.raises(ValueError, match="unknown layer"):
        build_inventory(root, layers=("not_a_layer",), config_path=config)


def test_layer_patterns_never_escape_the_root(tmp_path: pathlib.Path) -> None:
    escaped = LayerSpec(
        name="escaped",
        path_pattern="../outside/*.parquet",
        input_class="external_historical_archive",
        role="source_trades",
        producer="a",
        license="CC-BY-4.0",
    )

    with pytest.raises(ValueError, match="escapes"):
        build_inventory(tmp_path, layers=(escaped,))


def test_real_configuration_declares_the_documented_layers() -> None:
    specs = load_layer_specs(REPO_ROOT / CONFIG_PATH)

    by_name = {spec.name: spec for spec in specs}
    assert set(by_name) == {
        "kalshi_trades",
        "kalshi_markets",
        "polymarket_orderfilled",
        "polymarket_daily_aligned",
        "polymarket_daily_aligned_multi",
        "polymarket_ctf",
        "forecast_snapshots",
    }
    assert by_name["kalshi_trades"].path_pattern == "kalshi-trades/trades-*.parquet"
    assert by_name["kalshi_trades"].time_column == "created_time"
    assert by_name["kalshi_trades"].time_unit == "timestamp_us_utc"
    assert by_name["polymarket_daily_aligned"].time_column == "block_timestamp"
    assert by_name["polymarket_daily_aligned"].time_unit == "epoch_seconds"
    assert by_name["polymarket_ctf"].time_column is None
    assert by_name["polymarket_ctf"].time_unit is None
    assert (
        by_name["forecast_snapshots"].path_pattern
        == "forecast-snapshots-*/snapshot_dataset.parquet"
    )
    assert {spec.input_class for spec in specs} <= set(INPUT_CLASSES)
    assert all(spec.producer and spec.license and spec.role for spec in specs)


def test_default_config_path_is_the_pipeline_configuration() -> None:
    assert CONFIG_PATH == "configs/external_history_v1.yaml"
    assert (REPO_ROOT / CONFIG_PATH).is_file()


def test_load_layer_specs_rejects_a_malformed_configuration(tmp_path: pathlib.Path) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("inputs:\n  layers: []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="empty"):
        load_layer_specs(broken)

    unknown = tmp_path / "unknown.yaml"
    unknown.write_text(
        "inputs:\n"
        "  layers:\n"
        "    - name: l\n"
        "      path_pattern: a/*.parquet\n"
        "      input_class: invented_class\n"
        "      role: r\n"
        "      producer: p\n"
        "      license: MIT\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="input_class"):
        load_layer_specs(unknown)

    missing = tmp_path / "missing.yaml"
    with pytest.raises(ValueError, match="could not be read"):
        load_layer_specs(missing)


def test_build_inventory_defaults_to_every_configured_layer(tmp_path: pathlib.Path) -> None:
    root = build_tree(tmp_path / "external")
    config = _write_config(tmp_path)

    inventory = build_inventory(root, config_path=config)

    assert [layer.name for layer in inventory.layers] == ["kalshi_trades", "polymarket_ctf"]
    assert inventory.layer("polymarket_ctf").status == LAYER_STATUS_MISSING


def _write_config(tmp_path: pathlib.Path) -> pathlib.Path:
    config = tmp_path / "external_history_v1.yaml"
    config.write_text(
        "inputs:\n"
        "  layers:\n"
        "    - name: kalshi_trades\n"
        "      path_pattern: kalshi/trades-*.parquet\n"
        "      input_class: external_historical_archive\n"
        "      role: source_trades\n"
        "      venue: kalshi\n"
        "      producer: a\n"
        "      license: CC-BY-4.0\n"
        "      time_column: created_time\n"
        "      time_unit: timestamp_us_utc\n"
        "    - name: polymarket_ctf\n"
        "      path_pattern: polymarket/ctf/*.parquet\n"
        "      input_class: external_historical_archive\n"
        "      role: lifecycle_records\n"
        "      venue: polymarket\n"
        "      producer: b\n"
        "      license: CC-BY-4.0\n"
        "      time_column: null\n"
        "      time_unit: null\n",
        encoding="utf-8",
    )
    return config


def _sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _copy_tree(source: pathlib.Path, target: pathlib.Path) -> None:
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        destination = target / path.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(path.read_bytes())
