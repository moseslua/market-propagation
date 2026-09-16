"""Reporting tests: strict JSON, real raw-to-report provenance, and no cohort consumption.

These defend behaviour a consumer of ``reproduce`` depends on: every number in
the report is strict JSON, every hash traces to bytes on disk or in the raw
store, the classification of the result is honest, and a synthetic reproduction
leaves a real locked test cohort untouched.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
import yaml

import market_propagation.models as models
import market_propagation.reporting as reporting
import market_propagation.simulation as simulation
from market_propagation.registry import ExperimentRegistry
from market_propagation.storage import RawStore, hash_file, read_parquet

REPO_ROOT = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO_ROOT / "configs" / "study_v1.yaml"
COHORT_PATH = REPO_ROOT / "configs" / "cohort.yaml"

#: Small caller settings that keep one reproduction fast; the module's own
#: defaults are the production ones.
N_EVENTS = 8
REPETITIONS = 20
BOOTSTRAP = 25

_HEX = re.compile(r"^[0-9a-f]{64}$")


@pytest.fixture(scope="session")
def run(tmp_path_factory) -> dict:
    output = tmp_path_factory.mktemp("reproduce")
    return reporting.reproduce(
        output,
        spec_path=SPEC_PATH,
        n_events=N_EVENTS,
        repetitions=REPETITIONS,
        bootstrap_samples=BOOTSTRAP,
    )


@pytest.fixture(scope="session")
def second_run(tmp_path_factory) -> dict:
    output = tmp_path_factory.mktemp("reproduce-again")
    return reporting.reproduce(
        output,
        spec_path=SPEC_PATH,
        n_events=N_EVENTS,
        repetitions=REPETITIONS,
        bootstrap_samples=BOOTSTRAP,
    )


def _output(run: dict) -> Path:
    return Path(run["output_dir"])


def test_json_ready_converts_each_declared_type():
    ready = reporting.json_ready(
        {
            "finite": 0.5,
            "infinite": float("inf"),
            "nan": float("nan"),
            "exact": Decimal("0.0350"),
            "instant": dt.datetime(2031, 1, 2, 13, 30, tzinfo=dt.UTC),
            "naive": dt.datetime(2031, 1, 2, 13, 30),
            "numpy": pd.array([1], dtype="int64")[0],
            "series": pd.Series([1.5, 2.5]),
            "frame": pd.DataFrame({"a": [1.0]}),
            "missing": pd.NA,
            "timestamp": pd.Timestamp("2031-01-02T13:30:00Z"),
            "path": Path("configs/study_v1.yaml"),
            "bytes": b"\x01\x02",
            "flags": {True, False},
        }
    )
    assert ready["finite"] == 0.5
    assert ready["infinite"] is None
    assert ready["nan"] is None
    assert ready["exact"] == "0.0350"
    assert ready["instant"] == "2031-01-02T13:30:00+00:00"
    assert ready["naive"] == "2031-01-02T13:30:00+00:00"
    assert ready["numpy"] == 1
    assert ready["series"] == [1.5, 2.5]
    assert ready["frame"] == [{"a": 1.0}]
    assert ready["missing"] is None
    assert ready["timestamp"].startswith("2031-01-02T13:30:00")
    assert ready["path"] == "configs/study_v1.yaml"
    assert ready["bytes"] == "0102"
    assert sorted(ready["flags"], key=str) == [False, True]
    json.dumps(ready, allow_nan=False)


def test_json_ready_refuses_to_stringify_an_unknown_object():
    class Estimator:
        def __repr__(self) -> str:  # pragma: no cover - the repr must never be used
            return "Estimator(network=True)"

    with pytest.raises(TypeError, match="no declared JSON representation"):
        reporting.json_ready({"model": Estimator()})


def test_json_ready_uses_as_record_when_the_object_provides_it():
    class Recorded:
        def as_record(self) -> dict:
            return {"kind": "network", "intercept": 0.0}

    assert reporting.json_ready({"model": Recorded()}) == {
        "model": {"kind": "network", "intercept": 0.0}
    }


@pytest.mark.parametrize("name", [reporting.METRICS_NAME, reporting.MANIFEST_NAME])
def test_written_json_parses_strictly_without_non_finite_tokens(run, name):
    text = (_output(run) / name).read_text(encoding="utf-8")
    for token in ("NaN", "Infinity"):
        assert token not in text, f"{name} carries the non-standard JSON token {token}"
    payload = json.loads(text)
    assert payload, f"{name} is empty"


def test_metrics_records_actual_inputs_and_notes_every_null(run):
    metrics = json.loads((Path(run["output_dir"]) / reporting.METRICS_NAME).read_text())
    assert metrics["settings"]["bootstrap_samples_used"] == BOOTSTRAP
    assert metrics["settings"]["horizon_seconds"] == 300
    assert metrics["seeds"]["master_seed"] == metrics["settings"]["master_seed"]
    assert metrics["interpretation_notes"], "a null statistic must carry an interpretation note"
    assert any("never a zero" in note for note in metrics["interpretation_notes"])


def test_real_gate_statuses_and_no_fabricated_empirical_result(run):
    gates = run["gates"]
    assert gates["G0"]["status"] == "blocked", "no real eligible cohort exists in this run"
    assert gates["G1"]["status"] == "ok"
    assert gates["G2"]["status"] == "ok"
    empirical = run["empirical"]
    assert empirical["status"] == "blocked"
    assert empirical["synthetic_results_are_empirical_results"] is False
    assert run["classification"] == "synthetic_software_methods_reproduction"


def test_null_audit_reports_its_own_inconclusiveness_without_inferring_power(run):
    audit = run["null_vs_communication"]
    assert audit["repetitions"] == REPETITIONS
    assert audit["metric"] == "probability_point_mae"
    assert "observed recovery count" in audit["power_source"]
    recovery = audit["recovery"]
    assert recovery["meets_target_power"] is (
        recovery["one_sided_lower_bound"] is not None and recovery["one_sided_lower_bound"] >= 0.8
    )
    if audit["status"] == "inconclusive":
        assert audit["inconclusive_reasons"], "an inconclusive audit must name its reason"
    assert run["gates"]["G3"]["status"] == ("ok" if audit["status"] == "ok" else "blocked")


def test_caller_settings_are_recorded_apart_from_configuration(run):
    settings = run["settings"]
    assert settings["config_bootstrap_samples_default"] == 200
    assert settings["bootstrap_samples_used"] == BOOTSTRAP
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    assert manifest["settings_sources"]["bootstrap_samples_used"] == "reproduce(bootstrap_samples=)"
    assert manifest["inputs"]["n_events_source"].startswith("reproduce(n_events=)")
    assert any("randomness.bootstrap_samples_default" in warning for warning in run["warnings"])


def test_manifest_hashes_are_real_digests_and_never_unknown(run):
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    hashes = manifest["hashes"]
    assert hashes["spec"] == hash_file(SPEC_PATH)
    assert hashes["event_windows"] == hash_file(REPO_ROOT / "configs" / "event_windows.yaml")
    assert hashes["universe"] == hash_file(COHORT_PATH)
    for field in ("spec", "event_windows", "universe", "source", "data", "environment"):
        value = hashes[field]
        assert _HEX.match(str(value)), f"{field} is not a sha256 digest: {value!r}"
        assert "unknown" not in str(value).lower()
    assert _HEX.match(str(manifest["environment_lock_hash"]))
    assert manifest["runtime"]["python_version"]
    assert manifest["dependencies"]["pandas"], "dependency versions must be recorded"


def test_git_head_is_recorded_separately_from_the_source_tree_digest(run):
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    git = manifest["git"]
    if git.get("head"):
        assert re.match(r"^[0-9a-f]{7,40}$", git["head"])
        assert git["source_tree_sha256"] != git["head"]
        assert git["source_tree_file_count"] > 0
        assert isinstance(git["dirty"], bool)


def test_every_panel_raw_hash_resolves_through_the_raw_store(run):
    provenance = run["sample"]["raw_provenance"]
    assert provenance["panel_referenced_hashes"] > 0
    assert provenance["panel_hashes_failed"] == [], (
        "a cited raw hash did not resolve to stored bytes"
    )
    assert provenance["panel_hashes_verified"] == provenance["panel_referenced_hashes"]
    assert provenance["fixture_sha256_matches_bytes"] is True
    assert provenance["bytes_read_back"] > 0

    store = RawStore(provenance["raw_root"])
    assert provenance["fixture_hash"] in store.stored_hashes()
    for panel_name in (reporting.SOURCE_PANEL_NAME, reporting.USABLE_PANEL_NAME):
        manifest_path = _output(run) / f"{panel_name}.manifest.json"
        assert manifest_path.is_file()
        assert _HEX.match(json.loads(manifest_path.read_text())["content_hash"])


def test_sealed_panels_masked_rows_keep_a_reason_and_a_null_measurement(run):
    coverage = run["coverage"]
    for fold in ("source", "usable"):
        panel = coverage[fold]
        assert panel["row_count"] > 0
        assert panel["totals"]["valid_rows"] <= panel["totals"]["rows"]
        reason = (run["sample"]["replay_disagreements"] or {}).get("agreement")
        assert reason in {True, False}
    masked = {
        row["exclusion_reason"]: row["rows"]
        for fold in ("source", "usable")
        for row in coverage[fold]["masked_by_reason"]
    }
    if masked:
        assert all(name != "(none)" for name in masked), "a masked row must name its reason"


def test_fitted_models_record_transforms_parameters_and_event_counts(run):
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    fitted = manifest["fitted_models"]["communication"]
    assert set(fitted) == set(models.MODEL_KINDS)
    for kind, record in fitted.items():
        # ``no_change`` is a real baseline with no regressors, so an empty
        # feature list is correct there and a missing one is not anywhere.
        assert "feature_names" in record, f"{kind} recorded no feature names field"
        if kind != "no_change":
            assert record["feature_names"], f"{kind} recorded no feature names"
        assert record["train_event_ids_count"] > 0, f"{kind} recorded no training releases"
        assert record["train_cutoff"]
        assert isinstance(record["coefficients"], dict)
        assert isinstance(record["feature_means"], dict)
        assert isinstance(record["feature_scales"], dict)
    cutoffs = manifest["cutoffs_and_event_counts"]["communication"]
    assert cutoffs["n_events_train"] > 0
    assert cutoffs["n_events_test"] > 0
    assert cutoffs["train_cutoff"] and cutoffs["validation_cutoff"]


def test_synthetic_run_reserves_and_claims_nothing(run):
    registry = run["registry"]
    assert registry["locked_test_reserved"] is False
    assert registry["n_reservations"] == 0
    assert registry["n_event_claims"] == 0
    assert registry["n_runs"] >= 1
    assert registry["recorded"] is True

    with ExperimentRegistry(_output(run) / reporting.REGISTRY_DB_NAME) as opened:
        snapshot = opened.snapshot()
    assert snapshot["reservations"] == []
    assert snapshot["event_claims"] == []
    assert snapshot["runs"], "the run itself must be recorded"


def test_the_real_cohort_stays_reservable_after_a_synthetic_run(run, tmp_path):
    cohort = yaml.safe_load(COHORT_PATH.read_text(encoding="utf-8"))
    cohort_events = sorted(str(entry["event_id"]) for entry in cohort["events"])
    fixture_events = set(run["sample"]["event_ids"])
    assert not fixture_events & set(cohort_events), (
        "the synthetic fixture must not reuse a scientific cohort release id"
    )

    registry_path = _output(run) / reporting.REGISTRY_DB_NAME
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    with ExperimentRegistry(registry_path) as opened:
        claimed = {claim["event_id"] for claim in opened.snapshot()["event_claims"]}
    assert not claimed & set(cohort_events)

    fresh = tmp_path / "fresh.sqlite3"
    with ExperimentRegistry(fresh) as opened:
        token = opened.reserve_locked_test(
            manifest["hashes"]["spec_digest"],
            cohort_events,
            dataset_hash=manifest["hashes"]["data"],
        )
        assert token
        snapshot = opened.snapshot()
        assert [row["event_ids"] for row in snapshot["reservations"]] == [cohort_events]
        assert {row["event_id"] for row in snapshot["event_claims"]} == set(cohort_events)


def test_registry_export_matches_the_recorded_run(run):
    lines = (_output(run) / reporting.JSONL_NAME).read_text(encoding="utf-8").splitlines()
    assert lines
    payloads = [json.loads(line) for line in lines]
    recorded = [payload for payload in payloads if payload["run_id"] == run["registry"]["run_id"]]
    assert len(recorded) == 1
    assert recorded[0]["synthetic"] is True
    assert recorded[0]["metrics"]["classification"] == "synthetic_software_methods_reproduction"
    assert recorded[0]["event_ids"], "the recorded run must name the events it covers"


def test_data_card_declares_the_synthetic_scope_and_blocked_empirical_status(run):
    text = (_output(run) / reporting.DATA_CARD_NAME).read_text(encoding="utf-8")
    assert "synthetic_software_methods_reproduction" in text
    assert "not `configs/cohort.yaml`" in text
    assert "no release expectation" in text.lower()
    assert "locked-test reservation" in text


def test_paper_classifies_the_result_and_does_not_claim_an_empirical_effect(run):
    text = (_output(run) / reporting.PAPER_NAME).read_text(encoding="utf-8")
    assert "synthetic software and methods reproduction" in text
    assert "not claimed" in text, "human sign-off must not be claimed"
    assert "no external registration exists" in text
    assert str(run["empirical"]["status"]) in text
    for forbidden in ("we find that", "our estimate of the effect is"):
        assert forbidden not in text.lower()


def test_conditional_report_repeats_the_measured_mae_values(run):
    text = (_output(run) / reporting.CONDITIONAL_REPORT_NAME).read_text(encoding="utf-8")
    comparisons = run["model_comparison"]
    for process in ("shared_news_delay", "communication"):
        promotion = comparisons[process]["promotion"]
        for field in ("baseline_mae", "candidate_mae", "mae_reduction"):
            value = promotion[field]
            assert f"{value:.6f}" in text, f"{process}.{field} is missing from the report"
        assert promotion["status"] in text


def test_reports_name_the_placebos_that_could_not_run(run):
    placebos = run["placebos"]
    assert "endpoint_sensitivity" in placebos["available"]
    assert "leave_one_event_out" in placebos["available"]
    reasons = {entry["placebo"]: entry["reason"] for entry in placebos["unavailable"]}
    assert "shock_based_placebos" in reasons
    assert "expectation" in reasons["shock_based_placebos"]
    text = (_output(run) / reporting.CONDITIONAL_REPORT_NAME).read_text(encoding="utf-8")
    for name in reasons:
        assert name in text, f"{name} is unevaluated and must be reported as such"


def test_methods_used_lists_what_ran_and_what_did_not(run):
    methods = run["methods_used"]
    assert methods["response_estimation"]["method"] == "local_projections"
    assert methods["response_estimation"]["shock_column"] is None
    assert methods["network_comparison"]["method"] == "nested_held_out_comparison"
    assert methods["null_audit"]["method"] == "network_falsification"
    not_run = {entry["method"] for entry in methods["not_run"]}
    assert "iid_resampling_of_individual_quote_snapshots" in not_run
    assert "surprise_slope_estimation" in not_run


def test_coherence_reports_a_feasible_inconsistent_and_infeasible_box(run):
    coherence = run["coherence"]
    by_label = {example["label"]: example for example in coherence["examples"]}
    assert by_label["feasible_box"]["feasible"] is True
    assert by_label["feasible_box"]["distance"] is not None
    assert by_label["infeasible_box"]["feasible"] is False
    assert by_label["infeasible_box"]["distance"] > by_label["feasible_box"]["distance"]
    assert by_label["infeasible_box"]["midpoint_distance"] is not None
    # The plan's key constructed case: a box that meets the coherent set while
    # its own midpoints break the family identity.
    inconsistent = by_label["feasible_inconsistent_midpoints"]
    assert inconsistent["feasible"] is True
    assert inconsistent["distance"] == 0.0
    assert inconsistent["midpoint_feasible"] is False
    assert inconsistent["midpoint_distance"] > 0.0
    assert coherence["rejected"]["status"] == "rejected", "a crossed book must be refused"


def test_every_coherence_example_carries_its_own_payout_matrix(run):
    """Each constructed box states the assumptions its distance was computed under.

    Two of these boxes use different payoff families, so a reader cannot read the
    distance off one shared assumption set. Every example therefore carries its
    own payout matrix and its own stated assumptions, and the note says outright
    that none of them is an observed quote.
    """
    coherence = run["coherence"]
    examples = coherence["examples"]
    assert len(examples) >= 3
    for example in examples:
        assert example["input_class"] == "constructed_illustrative"
        assert example["payouts"], f"{example['label']} carries no payout matrix"
        assert example["assumptions"], f"{example['label']} carries no assumptions"
        assert example["bids"] and example["asks"]
        assert len(example["payouts"]) == example["n_contracts"]
    families = {example["family"] for example in examples}
    assert len(families) > 1, "the examples must not all share one payoff family"
    assert "not an observed quote" in coherence["note"]


def test_coherence_figures_and_panel_exist_for_a_complete_run(run):
    assert run["complete"] is True
    assert run["blocked_stages"] == []
    output = _output(run)
    for name in (
        reporting.METRICS_NAME,
        reporting.MANIFEST_NAME,
        reporting.JSONL_NAME,
        reporting.FORECAST_PANEL_NAME,
        reporting.SOURCE_PANEL_NAME,
        reporting.USABLE_PANEL_NAME,
        *reporting.REPORT_NAMES,
    ):
        assert (output / name).is_file(), f"{name} was not written"
    figures = output / "figures"
    for figure in reporting.FIGURE_NAMES:
        assert (figures / figure).is_file(), f"{figure} was not written"
        assert (figures / figure).stat().st_size > 0
    forecast = read_parquet(output / reporting.FORECAST_PANEL_NAME)
    assert len(forecast) == run["forecast"]["row_count"]
    assert forecast["target"].dtype.kind == "f", "the sealed label must stay numeric"


def test_the_sealed_forecast_panel_rebuilds_the_model_comparison(run):
    """The published panel is the comparison's own input, not a summary of it.

    If the artifact dropped a predictor, the network design matrix would change
    and its loss would move; if it dropped the release unit, the folds would
    change. So this runs the production estimator over the sealed bytes and
    compares cluster splits, feature names and per-row predictions and losses
    against the in-memory result the report itself scored.
    """
    output = _output(run)
    sealed = read_parquet(output / reporting.FORECAST_PANEL_NAME)
    settings = run["settings"]
    memory = models.nested_comparison(
        sealed,
        seed=settings["simulation_seed"],
        train_fraction=settings["train_fraction"],
        validation_fraction=settings["validation_fraction"],
        kinds=models.MODEL_KINDS,
        minimum_mae_gain=settings["smallest_relevant_mae_gain_probability_points"],
    ).as_record()

    reported = run["model_comparison"]["communication"]
    kinds = list(run["model_comparison"]["kinds"])
    assert memory["folds"]["train_events"] == reported["folds"]["train_events"]
    assert memory["folds"]["validation_events"] == reported["folds"]["validation_events"]
    assert memory["folds"]["test_events"] == reported["folds"]["test_events"]
    assert memory["folds"]["policy"] == reported["folds"]["policy"]
    assert memory["sample"]["n_clusters_test"] == reported["sample"]["n_clusters_test"]

    assert [row["kind"] for row in memory["scores"]] == kinds
    for kind in kinds:
        recorded = reported["evaluations"][kind]
        recomputed = memory["evaluations"][kind]
        assert recomputed["mae"] == pytest.approx(recorded["mae"], abs=0.0)
        assert recomputed["n_rows"] == recorded["n_rows"]
        assert recomputed["n_clusters"] == recorded["n_clusters"]
        # The panel carries the predictor columns the fit reads, so the feature
        # names and the coefficients come back identical.
        assert memory["models"][kind]["feature_names"] == reported["models"][kind]["feature_names"]
        assert memory["models"][kind]["coefficients"] == pytest.approx(
            reported["models"][kind]["coefficients"]
        )
        assert recomputed["event_ids"] == recorded["event_ids"]


def test_the_excluded_column_note_lists_only_fields_models_do_not_read(run):
    """Nothing a model fits on may be published as excluded truth or audit data.

    The note is what a reader uses to decide the panel is safe to reuse, so a
    predictor listed there would be a false statement about the artifact.
    """
    forecast = run["forecast"]
    excluded = set(forecast["truth_only_columns_excluded"])
    assert excluded, "the truth-only quantities must still be reported as excluded"
    assert excluded == set(simulation.TRUTH_ONLY_COLUMNS) - set(forecast["columns"])

    predictors = {name for kind in models.MODEL_KINDS for name in models.FEATURE_SPECS[kind]}
    assert not predictors & excluded, "a fitted predictor is reported as excluded"
    for column in (*predictors, "cluster_id", "cohort", "orientation_sign", "exclusion_reason"):
        assert column in forecast["columns"], f"{column} is missing from the sealed panel"
    assert forecast["cluster_column"] == "cluster_id"
    assert forecast["n_clusters"] > 0
    assert forecast["required_schema_version"] == forecast["schema_version"]


def test_all_four_model_kinds_appear_in_the_comparison_and_report(run):
    """The ordinary comparison reports every implemented kind, not just the pair."""
    expected = ("no_change", "own", "news", "network")
    assert tuple(run["model_comparison"]["kinds"]) == expected
    for process in ("shared_news_delay", "communication"):
        record = run["model_comparison"][process]
        assert tuple(record["evaluations"]) == expected
        assert tuple(record["models"]) == expected
        assert {row["kind"] for row in record["scores"]} == set(expected)
        # One identical held-out sample still underlies all of them.
        assert len({row["n_rows"] for row in record["scores"]}) == 1

    text = (_output(run) / reporting.CONDITIONAL_REPORT_NAME).read_text(encoding="utf-8")
    assert "held-out MAE" in text
    for kind in expected:
        assert kind in text, f"{kind} is implemented and must be reported"
    # Falsification still reads exactly the news-versus-network contrast.
    promotion = run["model_comparison"]["communication"]["promotion"]
    assert promotion["baseline_kind"] == "news"
    assert promotion["candidate_kind"] == "network"
    assert run["methods_used"]["network_comparison"]["kinds"] == list(expected)


def test_every_figure_title_states_the_result_is_synthetic(run):
    """A PNG read on its own must not read as observed market evidence."""
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    records = {entry["name"]: entry for entry in manifest["files"] if entry.get("title")}
    for figure in reporting.FIGURE_NAMES:
        record = records.get(figure)
        assert record is not None, f"{figure} carries no recorded title"
        assert record["synthetic"] is True
        assert record["title"].startswith(reporting.SYNTHETIC_TITLE_PREFIX), figure
        assert "Synthetic" in record["title"]


def test_repeated_reproduction_converges_on_identical_immutable_artifacts(run, second_run):
    first, second = _output(run), _output(second_run)
    for name in (
        reporting.SOURCE_PANEL_NAME,
        reporting.USABLE_PANEL_NAME,
        reporting.FORECAST_PANEL_NAME,
        *[f"figures/{figure}" for figure in reporting.FIGURE_NAMES],
    ):
        assert (first / name).is_file() and (second / name).is_file()
        assert hash_file(first / name) == hash_file(second / name), f"{name} is not reproducible"

    for field in ("fixture_hash", "raw_hash_count"):
        assert run["sample"][field] == second_run["sample"][field]
    for fold in ("source", "usable"):
        assert run["coverage"][fold]["content_hash"] == second_run["coverage"][fold]["content_hash"]
    assert run["forecast"]["content_hash"] == second_run["forecast"]["content_hash"]
    assert _scientific(run) == _scientific(second_run)
    assert run["registry"]["run_id"] == second_run["registry"]["run_id"]


def test_rerunning_into_the_same_directory_respects_the_immutable_write_contract(tmp_path):
    output = tmp_path / "same"
    first = reporting.reproduce(
        output,
        spec_path=SPEC_PATH,
        n_events=N_EVENTS,
        repetitions=REPETITIONS,
        bootstrap_samples=BOOTSTRAP,
    )
    sealed = {
        name: hash_file(output / name)
        for name in (
            reporting.SOURCE_PANEL_NAME,
            reporting.USABLE_PANEL_NAME,
            reporting.FORECAST_PANEL_NAME,
        )
    }
    second = reporting.reproduce(
        output,
        spec_path=SPEC_PATH,
        n_events=N_EVENTS,
        repetitions=REPETITIONS,
        bootstrap_samples=BOOTSTRAP,
    )
    assert first["complete"] is True and second["complete"] is True
    for name, digest in sealed.items():
        assert hash_file(output / name) == digest, f"{name} was rewritten on an identical rerun"
    assert first["sample"]["fixture_hash"] == second["sample"]["fixture_hash"]
    assert first["registry"]["run_id"] == second["registry"]["run_id"], (
        "an identical run reuses its registry identity instead of writing a second run"
    )
    with ExperimentRegistry(output / reporting.REGISTRY_DB_NAME) as opened:
        assert len(opened.snapshot()["runs"]) == 1


def _scientific(run: dict) -> str:
    """The numeric content of a run, with per-run identifiers removed."""
    manifest = json.loads((Path(run["output_dir"]) / reporting.MANIFEST_NAME).read_text())
    payload = {
        "coverage": {fold: run["coverage"][fold]["content_hash"] for fold in ("source", "usable")},
        "forecast": run["forecast"]["content_hash"],
        "promotion": {
            process: run["model_comparison"][process]["promotion"]["status"]
            for process in ("shared_news_delay", "communication")
        },
        "comparison_kinds": list(run["model_comparison"]["kinds"]),
        "falsification": {
            "status": run["null_vs_communication"]["status"],
            "null": run["null_vs_communication"]["null"]["false_positive_count"],
            "recovery": run["null_vs_communication"]["recovery"]["recovery_count"],
        },
        "power": run["power"]["status"],
        "dependencies": manifest["dependencies"],
        "runtime": manifest["runtime"],
    }
    return json.dumps(reporting.json_ready(payload), sort_keys=True)
