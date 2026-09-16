"""Acceptance tests for the source-time ladder bridge and the shared registry.

The ladder is fitted on real forecast rows or not at all. Each test here defends
one of the ways a comparison could read as fitted when it was not: a rung whose
declared regressor has no column, a rung fitted on a different sample from its
baseline because a column only it needed was missing, and a per-run registry file
that lets two runs of one study each look complete.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from market_propagation import study
from market_propagation.models import FEATURE_SPECS, MODEL_KINDS
from market_propagation.registry import ExperimentRegistry

RELEASES = [
    (f"empsit_2025_0{index}", dt.datetime(2025, 1, 10 + index, 13, 30, tzinfo=dt.UTC))
    for index in range(1, 6)
]


def _forecast_frame(*, with_shock: bool, with_control: bool = False) -> pd.DataFrame:
    """A sealed-shape forecast table: the ladder's declared columns, real values.

    ``recipient_own_lag`` is absent from the sealed schema, so the column is named
    here as the stored ``own_lag``; every other name is a declared forecast column.
    """
    rows = []
    for index, (event_id, release) in enumerate(RELEASES):
        rows.append(
            {
                "event_id": event_id,
                "cluster_id": event_id,
                "family": "employment",
                "receiver_contract_id": f"KXPAY-25{index:02d}",
                "release_time": release,
                "forecast_origin": release + dt.timedelta(seconds=300),
                "max_input_source_time": release + dt.timedelta(seconds=360),
                "recipient_anchor": 0.40 + 0.01 * index,
                "own_lag": 0.005 * index,
                "neighbor_lag": 0.02 * index - 0.03,
                "target": 0.01 * index - 0.02,
                "surprise": 0.1 * index if with_shock else None,
                "control_lag": -0.004 * index if with_control else None,
            }
        )
    return pd.DataFrame(rows)


def _surprise() -> dict[str, dict[str, float]]:
    return {
        event_id: {study.FAMILY_SURPRISE_STATISTIC["employment"]: 0.1 * index}
        for index, (event_id, _) in enumerate(RELEASES, start=1)
    }


def test_ladder_support_names_every_absent_declared_regressor() -> None:
    """A declared column with no value is named rather than fitted as a constant."""
    support = study.ladder_support(_forecast_frame(with_shock=False))

    assert support["kinds"]["own"]["supported"] is True
    assert support["kinds"]["news"]["missing_columns"] == ["shock", "delayed_shock"]
    assert support["kinds"]["network"]["missing_columns"] == [
        "shock",
        "delayed_shock",
        "neighbor_lag_control",
    ]
    assert support["comparison_supported"] is False
    assert support["comparison_common_missing_columns"] == [
        "delayed_shock",
        "neighbor_lag_control",
        "shock",
    ]
    assert set(support["unsupplied_columns"]) == {"shock", "delayed_shock", "neighbor_lag_control"}


def test_every_declared_column_is_accounted_for() -> None:
    """The support audit covers the ladder's own declaration, not a copy of it."""
    support = study.ladder_support(
        _forecast_frame(with_shock=True, with_control=True),
        surprise=_surprise(),
        control_column="control_lag",
    )
    for kind in MODEL_KINDS:
        declared = set(FEATURE_SPECS[kind])
        covered = set(support["kinds"][kind]["declared_columns"])
        assert declared == covered
    assert support["comparison_supported"] is True
    assert support["comparison_common_missing_columns"] == []


def test_a_rung_is_blocked_on_its_own_columns_when_they_are_absent() -> None:
    """With no surprise vector the news rung names the terms it cannot supply."""
    ladder = study.fit_forecast_ladder(_forecast_frame(with_shock=False), kinds=("news",))

    news = ladder["rungs"]["news"]
    assert news["status"] == study.STATUS_BLOCKED
    assert news["reason"] == study.REASON_LADDER_COLUMNS_ABSENT
    assert news["missing_columns"] == ["shock", "delayed_shock"]
    assert "comparison" not in news


def test_a_supported_rung_is_still_blocked_when_the_common_sample_is_incomplete() -> None:
    """One sample for every rung: a column only the network needs empties the own fit too."""
    frame = _forecast_frame(with_shock=True)
    ladder = study.fit_forecast_ladder(frame, surprise=_surprise(), kinds=("own", "network"))

    assert ladder["rungs"]["own"]["status"] == study.STATUS_BLOCKED
    assert ladder["rungs"]["own"]["reason"] == study.REASON_LADDER_COMMON_SAMPLE_INCOMPLETE
    assert ladder["rungs"]["own"]["missing_columns"] == ["neighbor_lag_control"]
    assert ladder["support"]["supplied_columns"].count("shock") == 1


def test_the_own_rung_fits_when_the_whole_ladder_design_is_supplied() -> None:
    """With every declared column present the rung is fitted on the real rows."""
    frame = _forecast_frame(with_shock=True, with_control=True)
    ladder = study.fit_forecast_ladder(
        frame,
        surprise=_surprise(),
        control_column="control_lag",
        kinds=("no_change", "own"),
    )

    own = ladder["rungs"]["own"]
    assert own["status"] == study.STATUS_COMPLETE
    assert own["reason"] is None
    assert own["missing_columns"] == []
    assert own["comparison"]["folds"]["train_events"]
    assert own["comparison"]["metric"]
    assert own["n_rows"] == len(frame)
    assert ladder["design"]["column_map"]["current_price"] == "recipient_anchor"


def test_the_shared_registry_is_one_store_for_every_run() -> None:
    """Two runs of one study record into one ledger rather than one store each."""
    path = study.shared_registry_path()

    assert path.name == study.REGISTRY_DB_NAME
    assert path.parent.name == Path(study.SHARED_REGISTRY_DIR).name
    assert path.parent.parent == Path(study.__file__).resolve().parents[2] / "data"


def test_a_run_records_its_fitted_rows_params_predictions_and_losses(tmp_path: Path) -> None:
    """The stored record points at the evidence, not only at the run."""
    registry_path = tmp_path / "registry.sqlite3"
    frame = pd.DataFrame(
        {
            "event_id": ["e1", "e2"],
            "cluster_id": ["e1", "e2"],
            "family": ["employment", "employment"],
            "contract_id": ["K1", "K1"],
            "valid": [True, True],
        }
    )
    section = study._registry_section(
        output=tmp_path,
        registry_path=registry_path,
        run_id="run-fitted",
        panel_hash="a" * 64,
        spec_hash="b" * 64,
        source_hash="c" * 64,
        environment_hash="d" * 64,
        families=("employment",),
        frame=frame,
        fits={},
    )

    assert section["recorded"] is True
    assert section["shared"] is False
    with ExperimentRegistry(registry_path) as registry:
        snapshot = registry.snapshot()
    record = snapshot["runs"][0]
    assert record["run_id"] == "run-fitted"
    assert record["source_hash"] == "c" * 64
    assert set(record["metrics"]) >= {
        "panel_rows",
        "valid_rows",
        "fitted_row_ids",
        "fitted_row_ids_digest",
        "model_comparisons",
    }


def test_a_run_without_an_input_identity_is_not_recorded(tmp_path: Path) -> None:
    """A record that identifies nothing would make the store look populated."""
    section = study._registry_section(
        output=tmp_path,
        registry_path=tmp_path / "registry.sqlite3",
        run_id="run-empty",
        panel_hash=None,
        spec_hash="b" * 64,
        source_hash="c" * 64,
        environment_hash="d" * 64,
        families=(),
        frame=pd.DataFrame({"event_id": ["e1"], "valid": [False]}),
        fits={},
    )

    assert section["recorded"] is False
    assert "identifying nothing" in section["reason"]
    assert not (tmp_path / "registry.sqlite3").exists()
