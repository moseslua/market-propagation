"""Acceptance tests for the experiment registry.

Each test defends a property the registry exists to protect: that a locked-test
cohort is consumed once, that a rejected write leaves no partial state, that a
recorded run is never rewritten, and that the store is the durable truth rather
than a process-local cache. Assertions are on what a reader of the registry
observes (snapshot, export, raised error), not on internal wiring.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from market_propagation.registry import (
    ExperimentRegistry,
    ExperimentRegistryError,
    RegistryConflictError,
)

CREATED = "2026-09-13T12:00:00+00:00"


def run_record(**changes: object) -> dict[str, object]:
    record: dict[str, object] = {
        "run_id": "run-1",
        "spec_hash": "spec-a",
        "data_hash": "data-1",
        "source_hash": "source-1",
        "environment_hash": "env-1",
        "event_ids": ["e2", "e1"],
        "seed": 20260913,
        "metrics": {"mae": 0.012, "n_events": 2},
        "synthetic": True,
        "created_at": CREATED,
    }
    record.update(changes)
    return record


def test_a_run_round_trips_through_snapshot(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        run_id = registry.record_run(run_record())
        stored = registry.snapshot()["runs"]

    assert run_id == "run-1"
    assert len(stored) == 1
    assert stored[0]["metrics"] == {"mae": 0.012, "n_events": 2}
    assert stored[0]["event_ids"] == ["e1", "e2"], "cohort order is canonical, not caller order"
    assert stored[0]["synthetic"] is True


def test_a_generated_run_id_is_returned_and_recorded(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        first = registry.record_run(run_record(run_id=None, metrics={"mae": 0.1}))
        second = registry.record_run(run_record(run_id=None, metrics={"mae": 0.2}))
        stored = registry.snapshot()["runs"]

    assert first != second
    assert {entry["run_id"] for entry in stored} == {first, second}


def test_replaying_an_identical_run_is_idempotent(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        assert registry.record_run(run_record()) == "run-1"
        assert registry.record_run(run_record()) == "run-1"
        runs = registry.snapshot()["runs"]

    assert len(runs) == 1


def test_changed_content_under_one_run_id_conflicts_and_preserves_the_row(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        registry.record_run(run_record())
        with pytest.raises(RegistryConflictError):
            registry.record_run(run_record(metrics={"mae": 0.9}))
        with pytest.raises(RegistryConflictError):
            registry.record_run(run_record(seed=7))
        stored = registry.snapshot()["runs"]

    assert len(stored) == 1
    assert stored[0]["metrics"] == {"mae": 0.012, "n_events": 2}
    assert stored[0]["seed"] == 20260913


def test_an_omitted_created_at_is_stamped_once_and_reused_on_replay(tmp_path: Path):
    pending = run_record()
    pending.pop("created_at")
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        registry.record_run(pending)
        registry.record_run(dict(pending))
        stored = registry.snapshot()["runs"]

    assert len(stored) == 1
    assert dt.datetime.fromisoformat(str(stored[0]["created_at"])).tzinfo is not None


def test_a_reserved_specification_is_never_given_a_second_token(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-a", ["e2", "e1"], dataset_hash="data-1")
        state = registry.snapshot()

    assert [item["token"] for item in state["reservations"]] == [token]
    assert [item["event_ids"] for item in state["reservations"]] == [["e1", "e2"]]
    assert [claim["event_id"] for claim in state["event_claims"]] == ["e1", "e2"]
    assert state["reservations"][0]["status"] == "reserved"
    assert state["reservations"][0]["result"] is None


def test_a_renamed_specification_cannot_reuse_the_cohort(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-b", ["e1"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-b", ["e2", "e3"], dataset_hash="data-2")
        fresh = registry.reserve_locked_test("spec-b", ["e3", "e4"], dataset_hash="data-2")
        state = registry.snapshot()

    assert fresh != token
    assert len(state["reservations"]) == 2
    assert [claim["event_id"] for claim in state["event_claims"]] == ["e1", "e2", "e3", "e4"]


def test_a_conflicting_reservation_rolls_back_completely(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        registry.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-b", ["e2", "e3", "e4"], dataset_hash="data-1")
        after_conflict = registry.snapshot()
        registry.reserve_locked_test("spec-c", ["e3", "e4"], dataset_hash="data-1")

    assert len(after_conflict["reservations"]) == 1, "the rejected reservation left no row"
    assert [claim["event_id"] for claim in after_conflict["event_claims"]] == ["e1", "e2"]


def test_one_specification_reserves_one_cohort(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-a", ["e2"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-9")
        state = registry.snapshot()

    assert [item["token"] for item in state["reservations"]] == [token]


def test_a_failed_attempt_keeps_its_reservation_and_cohort(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
        registry.finish_locked_test(token, status="failed", result={"reason": "solver_error"})
        registry.finish_locked_test(token, status="failed", result={"reason": "solver_error"})
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            registry.reserve_locked_test("spec-b", ["e1"], dataset_hash="data-1")
        state = registry.snapshot()

    assert len(state["reservations"]) == 1
    assert state["reservations"][0]["status"] == "failed"
    assert state["reservations"][0]["result"] == {"reason": "solver_error"}
    assert state["reservations"][0]["finished_at"] is not None
    assert [claim["event_id"] for claim in state["event_claims"]] == ["e1", "e2"]


def test_a_finished_locked_test_is_never_reopened(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-1")
        registry.finish_locked_test(token, status="complete", result={"mae": 0.01})
        registry.finish_locked_test(token, status="complete", result={"mae": 0.01})
        with pytest.raises(RegistryConflictError):
            registry.finish_locked_test(token, status="complete", result={"mae": 0.02})
        with pytest.raises(RegistryConflictError):
            registry.finish_locked_test(token, status="failed", result={"mae": 0.01})
        state = registry.snapshot()

    assert state["reservations"][0]["status"] == "complete"
    assert state["reservations"][0]["result"] == {"mae": 0.01}


def test_unknown_finish_statuses_and_tokens_are_rejected(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-1")
        for status in ("reserved", "reopened", "open", "COMPLETE"):
            with pytest.raises(ExperimentRegistryError):
                registry.finish_locked_test(token, status=status, result={})
        with pytest.raises(ExperimentRegistryError):
            registry.finish_locked_test("no-such-token", status="complete", result={})
        status = registry.snapshot()["reservations"][0]["status"]

    assert status == "reserved"


def test_state_survives_close_and_reopen(tmp_path: Path):
    path = tmp_path / "registry.sqlite"
    first = ExperimentRegistry(path)
    first.record_run(run_record())
    token = first.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
    first.finish_locked_test(token, status="complete", result={"mae": 0.01})
    before = first.snapshot()
    first.close()

    with ExperimentRegistry(path) as reopened:
        after = reopened.snapshot()
        with pytest.raises(RegistryConflictError):
            reopened.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")
        with pytest.raises(RegistryConflictError):
            reopened.reserve_locked_test("spec-b", ["e1"], dataset_hash="data-1")

    assert after == before
    assert after["runs"][0]["run_id"] == "run-1"
    assert after["reservations"][0]["token"] == token


def test_close_is_idempotent_and_later_use_fails(tmp_path: Path):
    registry = ExperimentRegistry(tmp_path / "registry.sqlite")
    registry.close()
    registry.close()
    with pytest.raises(ExperimentRegistryError):
        registry.snapshot()


def test_export_writes_the_actual_rows_as_canonical_jsonl(tmp_path: Path):
    path = tmp_path / "registry.sqlite"
    export = tmp_path / "reports" / "experiment_registry.jsonl"
    with ExperimentRegistry(path) as registry:
        registry.record_run(run_record(run_id="run-a", created_at="2026-09-13T10:00:00+00:00"))
        registry.record_run(run_record(run_id="run-b", created_at="2026-09-13T11:00:00+00:00"))
        expected = registry.snapshot()["runs"]
        registry.export_jsonl(export)

    lines = export.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == expected
    assert lines[0] == json.dumps(
        expected[0], sort_keys=True, ensure_ascii=True, separators=(",", ":")
    )
    assert not [item for item in export.parent.iterdir() if item.name != export.name]


def test_export_follows_reopened_state_and_stays_parseable(tmp_path: Path):
    path = tmp_path / "registry.sqlite"
    export = tmp_path / "experiment_registry.jsonl"
    with ExperimentRegistry(path) as registry:
        registry.record_run(run_record(run_id="run-a"))
        registry.export_jsonl(export)
        registry.record_run(run_record(run_id="run-b", metrics={"mae": 0.2}))
        registry.export_jsonl(export)

    with ExperimentRegistry(path) as reopened:
        assert [json.loads(line) for line in export.read_text(encoding="utf-8").splitlines()] == (
            reopened.snapshot()["runs"]
        )


def test_non_finite_metrics_are_rejected_rather_than_stored(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        for value in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ExperimentRegistryError):
                registry.record_run(run_record(metrics={"mae": value}))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(metrics={"nested": {"mae": float("nan")}}))
        assert registry.snapshot()["runs"] == []


def test_non_finite_results_are_rejected_and_leave_the_reservation_open(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        token = registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-1")
        with pytest.raises(ExperimentRegistryError):
            registry.finish_locked_test(token, status="complete", result={"mae": float("nan")})
        with pytest.raises(ExperimentRegistryError):
            registry.finish_locked_test(
                token, status="complete", result={"note": "ok", "mae": float("inf")}
            )
        state = registry.snapshot()

    assert state["reservations"][0]["status"] == "reserved"
    assert state["reservations"][0]["result"] is None


def test_missing_provenance_is_rejected_without_a_placeholder(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        for field in ("spec_hash", "data_hash", "source_hash", "environment_hash"):
            incomplete = run_record()
            incomplete.pop(field)
            with pytest.raises(ExperimentRegistryError):
                registry.record_run(incomplete)
        for field in ("spec_hash", "data_hash", "source_hash", "environment_hash"):
            blank = run_record()
            blank[field] = "   "
            with pytest.raises(ExperimentRegistryError):
                registry.record_run(blank)
        assert registry.snapshot()["runs"] == []


def test_undeclared_fields_and_bad_types_are_rejected(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(git_commit="abc123"))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(synthetic="yes"))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(seed=True))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(seed="20260913"))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(metrics=["mae"]))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(created_at="2026-09-13 12:00:00"))
        assert registry.snapshot()["runs"] == []


def test_empty_and_duplicate_cohorts_are_rejected(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(event_ids=[]))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(event_ids=["e1", "e1"]))
        with pytest.raises(ExperimentRegistryError):
            registry.record_run(run_record(event_ids="e1"))
        with pytest.raises(ExperimentRegistryError):
            registry.reserve_locked_test("spec-a", [], dataset_hash="data-1")
        with pytest.raises(ExperimentRegistryError):
            registry.reserve_locked_test("spec-a", ["e1", "e1"], dataset_hash="data-1")
        state = registry.snapshot()

    assert state["runs"] == []
    assert state["reservations"] == []
    assert state["event_claims"] == []


def test_a_reservation_requires_its_own_provenance(tmp_path: Path):
    with ExperimentRegistry(tmp_path / "registry.sqlite") as registry:
        with pytest.raises(ExperimentRegistryError):
            registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="")
        with pytest.raises(ExperimentRegistryError):
            registry.reserve_locked_test("", ["e1"], dataset_hash="data-1")
        with pytest.raises(TypeError):
            registry.reserve_locked_test("spec-a", ["e1"])
        assert registry.snapshot()["reservations"] == []


def test_runs_and_reservations_share_one_durable_file(tmp_path: Path):
    path = tmp_path / "registry.sqlite"
    with ExperimentRegistry(path) as registry:
        registry.record_run(run_record())
        registry.reserve_locked_test("spec-a", ["e1"], dataset_hash="data-1")
    with ExperimentRegistry(path) as registry:
        registry.reserve_locked_test("spec-b", ["e2"], dataset_hash="data-1")
        state = registry.snapshot()

    assert len(state["runs"]) == 1
    assert [item["spec_hash"] for item in state["reservations"]] == ["spec-a", "spec-b"]
