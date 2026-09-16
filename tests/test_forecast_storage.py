"""Regression tests for sealing the production forecast table.

``forecast.target`` is the future change in the target contract's price, in
absolute probability units. It is a number, so the declared Arrow type is
``float64`` and a sealed forecast dataset must give back the same numbers any
model would read from the simulator. Declaring the column as text instead makes
``write_parquet`` refuse the real production frame outright, and coercing the
labels to text to get past that check would change what the column means.

The sealed table is also the artifact a consumer rebuilds the comparison from,
so these tests cover more than the label. Every predictor the fitted ladder
reads and the release unit the splits group by must survive the round trip, and
a table sealed under an older, narrower declaration must be refused rather than
read as if it carried the current columns.

These tests use the real ``simulate_scenario``/``primary_target`` frame projected
onto the declared forecast columns, so a schema that cannot hold the production
table fails here rather than at report time.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import market_propagation.evaluation as evaluation
import market_propagation.models as models
import market_propagation.simulation as simulation
import market_propagation.storage as storage
from market_propagation.point_in_time import FORECAST_COLUMNS, forecast_frame
from market_propagation.storage import read_parquet, write_parquet

SEED = 101
EVENTS = 12


def _projected(scenario: str = "communication", *, seed: int = SEED):
    frame = simulation.simulate_scenario(scenario, seed=seed, n_events=EVENTS)
    return simulation.primary_target(frame).loc[:, list(FORECAST_COLUMNS)].reset_index(drop=True)


def _seal_legacy_forecast(
    source: Path,
    legacy: Path,
    *,
    version: str,
    columns: tuple[str, ...],
    target_as_text: bool = False,
) -> str:
    """Rewrite a sealed dataset the way an earlier declaration would have.

    ``version``/``columns``/``target_as_text`` describe the older build: which
    version string its metadata carried, which columns it declared, and whether
    it typed ``target`` as text. The rows themselves are the current ones, so the
    refusal being tested is about the declared artifact, not about the data.
    """
    table = pq.read_table(source)
    if target_as_text:
        index = table.schema.get_field_index("target")
        values = [
            None if value is None else str(value) for value in table.column("target").to_pylist()
        ]
        table = table.set_column(
            index, pa.field("target", pa.string()), pa.array(values, type=pa.string())
        )
    dropped = [name for name in table.schema.names if name not in columns]
    if dropped:
        table = table.drop_columns(dropped)
    legacy_table = table.replace_schema_metadata(
        {
            **(table.schema.metadata or {}),
            b"market_propagation.schema_version": version.encode(),
        }
    )
    pq.write_table(legacy_table, legacy)
    dataset = storage.DatasetRef(
        path=str(legacy),
        table="forecast",
        schema_version=version,
        coverage_epoch="legacy",
        content_hash=storage.hash_file(legacy),
        row_count=legacy_table.num_rows,
    )
    legacy.with_name(legacy.name + ".manifest.json").write_text(
        json.dumps(dataset.manifest), encoding="utf-8"
    )
    return dataset.content_hash


#: The column set version 2 declared: no ``neighbor_lag_control``, no release
#: unit, no cohort, orientation or exclusion field.
_V2_COLUMNS: tuple[str, ...] = (
    "event_id",
    "family",
    "contract_id",
    "event_time",
    "prediction_time",
    "horizon_seconds",
    "target",
    "target_available_time",
    "max_input_available_time",
    "current_price",
    "own_lag",
    "shock",
    "delayed_shock",
    "neighbor_lag",
    "valid",
)


def test_the_production_target_column_is_sealed_as_a_number(tmp_path: Path) -> None:
    """The real forecast frame seals and reads back without string conversion.

    The values matter as much as the type: the comparison is exact equality, so a
    round trip that went through text and lost precision on a probability change
    would fail even though the read succeeded.
    """
    projected = _projected()
    path = tmp_path / "forecast.parquet"
    reference = write_parquet(projected, path, table="forecast", coverage_epoch="fixture")

    sealed_type = pq.read_schema(path).field("target").type
    assert sealed_type == pa.float64()

    back = read_parquet(path)
    assert back["target"].dtype.name == "float64"
    assert back["target"].tolist() == projected["target"].tolist()
    assert back["target"].abs().max() <= 1.0
    assert len(back) == reference.row_count == len(projected)


def test_a_label_target_is_refused_by_both_boundaries(tmp_path: Path) -> None:
    """A target-kind label is neither a number nor a silently converted string.

    ``forecast_frame`` is the point-in-time boundary and ``write_parquet`` is the
    storage boundary; a caller that hands either one a label must be told the
    column is numeric rather than having the label coerced into a text column.
    """
    rows = _projected().to_dict("records")
    rows[0]["target"] = "up"

    with pytest.raises(ValueError, match="target"):
        forecast_frame(rows)
    with pytest.raises(TypeError, match="target"):
        write_parquet(rows, tmp_path / "label.parquet", table="forecast", coverage_epoch="fixture")
    assert not (tmp_path / "label.parquet").exists()


def test_a_missing_target_is_missing_and_never_zero(tmp_path: Path) -> None:
    """An unavailable label stays null through normalization and through storage.

    Zero is a real, observed absence of movement on the probability scale, so
    filling an unquoted endpoint with zero would record a flat outcome the
    simulator never measured. The mask is what carries unavailability, and it is
    read from the real frame rather than synthesized here.
    """
    frame = simulation.simulate_scenario("coarse_sampling", seed=3, n_events=EVENTS)
    projected = simulation.primary_target(frame).loc[:, list(FORECAST_COLUMNS)]
    unavailable = projected["target"].isna()
    assert unavailable.any()

    frame = forecast_frame(projected.to_dict("records"))
    assert frame["target"].isna().tolist() == unavailable.tolist()

    path = tmp_path / "forecast.parquet"
    write_parquet(frame, path, table="forecast", coverage_epoch="fixture")
    back = read_parquet(path)
    assert back["target"].isna().tolist() == unavailable.tolist()


def test_a_dataset_sealed_under_an_older_declaration_is_refused(tmp_path: Path) -> None:
    """Both older artifacts are refused instead of read as if current.

    Version 1 typed ``target`` as text; version 2 declared a narrower table that
    omitted ``neighbor_lag_control`` and the release unit. Reading either under
    the current declaration would hand a consumer a table it cannot fit the
    production network model on -- and, for version 2, splits it cannot
    reproduce -- so both are refused. The version travels in the file's own
    metadata, which makes the refusal a check on the artifact and not on the
    filename.
    """
    path = tmp_path / "forecast.parquet"
    reference = write_parquet(_projected(), path, table="forecast", coverage_epoch="fixture")
    assert reference.schema_version == "3"
    assert read_parquet(path)["target"].dtype.name == "float64"

    legacy_v1 = tmp_path / "legacy-v1.parquet"
    _seal_legacy_forecast(path, legacy_v1, version="1", columns=_V2_COLUMNS, target_as_text=True)
    assert pq.read_schema(legacy_v1).field("target").type == pa.string()
    with pytest.raises(ValueError, match="version '1'"):
        read_parquet(legacy_v1)

    legacy_v2 = tmp_path / "legacy-v2.parquet"
    _seal_legacy_forecast(path, legacy_v2, version="2", columns=_V2_COLUMNS)
    assert "neighbor_lag_control" not in pq.read_schema(legacy_v2).names
    with pytest.raises(ValueError, match="version '2'"):
        read_parquet(legacy_v2)


def test_the_sealed_table_rebuilds_the_comparison_without_losing_a_predictor(
    tmp_path: Path,
) -> None:
    """A consumer rebuilds the fitted comparison from the sealed bytes alone.

    If the artifact dropped a predictor the network design matrix would change
    and its loss would move, and if it dropped the release unit the fold
    assignment would change. So the check is not that the label survived, but
    that cluster splits, feature names and the held-out predictions and losses
    all match the in-memory frame the report ran on.
    """
    projected = _projected()
    path = tmp_path / "forecast.parquet"
    write_parquet(projected, path, table="forecast", coverage_epoch="fixture")
    back = read_parquet(path)

    assert set(models.FEATURE_SPECS["network"]) <= set(back.columns)
    for kind in models.MODEL_KINDS:
        for column in models.FEATURE_SPECS[kind]:
            assert column in back.columns, (
                f"{kind} predictor {column} is absent from the sealed table"
            )

    assert back["cluster_id"].tolist() == projected["cluster_id"].tolist()
    assert back["cohort"].tolist() == projected["cohort"].tolist()
    assert back["orientation_sign"].tolist() == projected["orientation_sign"].tolist()

    from_memory = models.nested_comparison(projected, seed=SEED).as_record()
    from_sealed = models.nested_comparison(back, seed=SEED).as_record()

    assert from_sealed["folds"] == from_memory["folds"]
    assert from_sealed["sample"] == from_memory["sample"]
    assert from_sealed["common_sample_violations"] == []
    for kind in models.MODEL_KINDS:
        assert (
            from_sealed["models"][kind]["feature_names"]
            == from_memory["models"][kind]["feature_names"]
        )
        sealed_eval = from_sealed["evaluations"][kind]
        memory_eval = from_memory["evaluations"][kind]
        assert sealed_eval["mae"] == pytest.approx(memory_eval["mae"], abs=0.0)
        assert sealed_eval["event_ids"] == memory_eval["event_ids"]
        sealed_predictions = {
            (row["event_id"], row["contract_id"], row["horizon_seconds"]): row
            for row in sealed_eval["predictions"]
        }
        memory_predictions = {
            (row["event_id"], row["contract_id"], row["horizon_seconds"]): row
            for row in memory_eval["predictions"]
        }
        assert sealed_predictions.keys() == memory_predictions.keys()
        for key, memory_row in memory_predictions.items():
            assert sealed_predictions[key]["predicted"] == pytest.approx(
                memory_row["predicted"], abs=0.0
            )
            assert sealed_predictions[key]["actual"] == pytest.approx(memory_row["actual"], abs=0.0)
    # Every kind is scored on one identical held-out sample, before and after.
    assert len({row["n_rows"] for row in from_sealed["scores"]}) == 1


def test_label_availability_rules_are_unchanged_by_the_round_trip(tmp_path: Path) -> None:
    """The existing split authority sees the same folds before and after sealing.

    Numeric storage is only safe if it feeds the same sample the in-memory frame
    would have: same fold membership, same label-availability mask, same purge.
    """
    projected = _projected()
    path = tmp_path / "forecast.parquet"
    write_parquet(projected, path, table="forecast", coverage_epoch="fixture")
    back = read_parquet(path)

    sealed_folds = evaluation.chronological_splits(back)
    memory_folds = evaluation.chronological_splits(projected)
    identity = ["event_id", "contract_id", "horizon_seconds"]
    for fold in ("train", "validation", "test"):
        sealed = sealed_folds[fold][identity].sort_values(identity).reset_index(drop=True)
        memory = memory_folds[fold][identity].sort_values(identity).reset_index(drop=True)
        assert sealed.equals(memory)
        assert sealed_folds[fold].attrs["exclusions"] == memory_folds[fold].attrs["exclusions"]

    # A row whose label the split has not purged yet must still carry a real
    # label: a sealed null label cannot become an admissible training row.
    train = sealed_folds["train"]
    assert train["target"].notna().all()
    assert (train["target_available_time"] > train["prediction_time"]).all()
