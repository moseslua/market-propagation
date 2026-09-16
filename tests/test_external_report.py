"""External-report tests: verified inputs, honest gates, and no substituted analysis.

These defend what a consumer of ``report-external`` depends on. The report is
built only from a panel that verified through the sealed read path, so the fixture
is sealed with ``storage.write_parquet(table="trade_panel")`` rather than handed
over as an in-memory frame. A blocked panel produces a blocked report and never a
substituted estimate. A missing panel raises instead of being discovered. A
request outside the run's declared specification returns a structured reason
instead of a rounded, widened or re-labelled analysis.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

import pytest
import yaml

from market_propagation import external_report
from market_propagation.storage import hash_file, write_parquet

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "configs" / "external_history_v1.yaml"

_HEX = re.compile(r"^[0-9a-f]{64}$")

#: The events the fixture panel reports on, all in the declared `cpi` family. EVENT_C
#: is masked at every horizon, so it exercises the missing denominator.
EVENT_A = "cpi-2031-01"
EVENT_B = "cpi-2031-02"
EVENT_C = "cpi-2031-03"

#: The recorded clock mode and availability status of external archive rows: the
#: venue's own time, with no receipt evidence, so no usable interval is claimed.
CLOCK_MODE = "source"
AVAILABILITY = "source_time_only"

COHORT = "external_transaction_response"

#: The report fields a replay must reproduce exactly. The output directory and the
#: absolute paths derived from it are the caller's choice, not a measurement.
_MEASURED_KEYS: tuple[str, ...] = (
    "spec_digest",
    "run_id",
    "gate",
    "gate_detail",
    "status",
    "blocked",
    "capability_flags",
    "capabilities",
    "counts",
    "flags",
    "blockers",
    "unsupported",
    "evidence_gates",
    "estimates",
    "coverage",
    "event_cards",
    "response",
    "config_hash",
    "analysis_spec_hash",
    "panel_file_sha256",
    "settings",
)

#: The pipeline configuration, as the fixture reads it. The report reads the same
#: file, so a change to the declared horizons or families fails this fixture loudly
#: rather than silently testing a specification the report no longer applies.
_CONFIG = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
HORIZONS = tuple(int(value) for value in _CONFIG["response"]["horizons_seconds"])
PRIMARY = int(_CONFIG["response"]["primary_horizon_seconds"])


def _row(
    *,
    event_id: str,
    contract_id: str,
    horizon: int,
    response: float | None,
    valid: bool = True,
    exclusion_reason: str | None = None,
    family: str = "cpi",
    cluster_id: str | None = None,
    post_release_trade_observed: bool = True,
) -> dict:
    """One trade-panel row. A null response is written as null, never as a zero."""
    event_time = dt.datetime(2031, 1, 15, 13, 30, tzinfo=dt.UTC)
    return {
        "event_id": event_id,
        "cluster_id": cluster_id or event_id,
        "family": family,
        "venue": "kalshi",
        "contract_id": contract_id,
        "cohort": COHORT,
        "event_time": event_time,
        "horizon_seconds": horizon,
        "baseline_source_time": event_time - dt.timedelta(seconds=30),
        "endpoint_source_time": event_time + dt.timedelta(seconds=horizon),
        "baseline_time_basis": "source_time_strictly_before_release",
        "endpoint_time_basis": "source_time_at_or_before_horizon",
        "baseline": 0.40,
        "endpoint": None if response is None else 0.40 + response,
        "response": response,
        "baseline_raw_price": 40.0,
        "endpoint_raw_price": None if response is None else 40.0 + 100.0 * response,
        "price_scale": "absolute_probability_units_0_to_1",
        "price_convention": "declared_event_axis",
        "event_axis": "yes_price_is_event_axis",
        "baseline_age_seconds": 30.0,
        "endpoint_age_seconds": float(horizon),
        "baseline_trade_count": 2,
        "endpoint_trade_count": 0 if response is None else 3,
        "tie_group_size": 1,
        "tie_group_response_min": response,
        "tie_group_response_max": response,
        "endpoint_envelope_low": response,
        "endpoint_envelope_high": response,
        "post_release_trade_observed": post_release_trade_observed,
        "rule_version": None,
        "rule_evidence_quality": None,
        "valid": valid,
        "exclusion_reason": exclusion_reason,
        "clock_mode": CLOCK_MODE,
        "availability_status": AVAILABILITY,
        "label_time_basis": "source_time",
        "size_quality": "verified_source_quantity",
        "size_verified": True,
        "provenance_locators_json": {"shard": "trades-fixture.parquet", "row": 1},
        "flags_json": ["zero_cent_price"] if response == 0.0 else [],
    }


@pytest.fixture
def coverage_artifact(tmp_path: Path) -> Path:
    """A minimal coverage artifact shaped like the real one, so no repo data is read."""
    path = tmp_path / "coverage.json"
    path.write_text(
        json.dumps(
            {
                "operation": "audit",
                "status": "blocked",
                "complete": False,
                "gate": "G0",
                "event_count": 2,
                "unsatisfied_gates": ["rule_vintage_verified"],
                "events": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def config_path(tmp_path: Path, coverage_artifact: Path) -> Path:
    """The pipeline configuration, redirected at this test's own inputs.

    Only the two input paths are changed: the declared horizons, families,
    clock modes, capabilities and seeds stay the values the frozen configuration
    states, so the report is exercised against the real specification.
    """
    spec_path = tmp_path / "study_v2.yaml"
    spec_path.write_text("study:\n  spec_version: 'v2'\n", encoding="utf-8")
    raw = json.loads(json.dumps(_CONFIG))
    raw["inputs"]["audit_coverage"] = str(coverage_artifact)
    raw["inputs"]["release_dataset"] = str(tmp_path / "releases.parquet")
    raw["registered_estimation"]["analysis_spec"] = str(spec_path)
    path = tmp_path / "external_history_v1.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return path


def _healthy_rows() -> list[dict]:
    """Two observed releases, one release masked at every horizon, and a declared gap."""
    return [
        _row(event_id=EVENT_A, contract_id="KXCPI-A", horizon=60, response=0.10),
        _row(event_id=EVENT_A, contract_id="KXCPI-A", horizon=PRIMARY, response=0.20),
        _row(event_id=EVENT_A, contract_id="KXCPI-B", horizon=PRIMARY, response=0.30),
        # A masked row: no post-release print, so a null response with its reason.
        _row(
            event_id=EVENT_B,
            contract_id="KXCPI-C",
            horizon=60,
            response=None,
            valid=False,
            exclusion_reason="no_post_release_trade",
            post_release_trade_observed=False,
        ),
        # An observed genuine zero: two distinct fresh prints at the same price.
        _row(event_id=EVENT_B, contract_id="KXCPI-C", horizon=PRIMARY, response=0.0),
        # A release with no observed response anywhere, so it must still be counted as
        # missing at the primary horizon rather than disappearing from the denominator.
        _row(
            event_id=EVENT_C,
            contract_id="KXCPI-D",
            horizon=PRIMARY,
            response=None,
            valid=False,
            exclusion_reason="missing_baseline",
            post_release_trade_observed=False,
        ),
    ]


def _seal(tmp_path: Path, rows: list[dict], *, declare_synthetic: bool = True):
    """Seal rows through the real write path so the run exercises the verified read."""
    metadata = {"synthetic": "true"} if declare_synthetic else None
    return write_parquet(
        rows,
        tmp_path / "trade_panel.parquet",
        table="trade_panel",
        coverage_epoch="fixture",
        metadata=metadata,
    )


@pytest.fixture
def report(tmp_path: Path, config_path: Path):
    rows = _healthy_rows()
    _seal(tmp_path, rows)
    output = tmp_path / "report"
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", output, run_id="fixture-run"
    )
    return payload, output, rows


def test_report_writes_the_named_artifacts_with_a_specification_hash(report):
    payload, output, _rows = report
    assert payload["status"] == "ok"
    assert payload["blocked"] is False
    assert payload["gate"] == external_report.GATE_SATISFIED

    for name in (
        external_report.EXTERNAL_REPORT_NAME,
        external_report.COVERAGE_REPORT_NAME,
        external_report.EVENT_CARDS_NAME,
        external_report.LINEAGE_NAME,
        external_report.CAPABILITY_TABLE_NAME,
        "baseline_report.md",
    ):
        path = output / name
        assert path.is_file(), f"{name} was not written"
        assert path.stat().st_size > 0
    for name in external_report.FIGURE_NAMES:
        path = output / "figures" / name
        assert path.is_file(), f"{name} was not written"
        assert path.stat().st_size > 0

    # The specification digest is a real hash over the inputs that produced the rows.
    digest = payload["spec_digest"]
    assert _HEX.match(digest)
    assert payload["run_id"] == "fixture-run"
    assert payload["config_hash"] == hash_file(payload["inputs"]["config_path"])
    analysis = payload["inputs"]["analysis_specification"]
    assert analysis["resolved"] is True
    assert payload["analysis_spec_hash"] == hash_file(analysis["path"])
    assert payload["spec_digest"] != payload["config_hash"]

    # Every exported artifact repeats the digest, so a figure or a table read alone
    # still names the specification it came from.
    lineage = json.loads((output / external_report.LINEAGE_NAME).read_text(encoding="utf-8"))
    coverage = json.loads(
        (output / external_report.COVERAGE_REPORT_NAME).read_text(encoding="utf-8")
    )
    cards = json.loads((output / external_report.EVENT_CARDS_NAME).read_text(encoding="utf-8"))
    assert lineage["spec_digest"] == digest
    assert lineage["config"]["sha256"] == payload["config_hash"]
    assert lineage["analysis_specification"]["sha256"] == payload["analysis_spec_hash"]
    assert lineage["panel"]["content_hash"] == payload["inputs"]["panel"]["content_hash"]
    assert coverage["spec_digest"] == digest
    assert cards["spec_digest"] == digest
    assert all(card["spec_digest"] == digest for card in cards["cards"])
    table = (output / external_report.CAPABILITY_TABLE_NAME).read_text(encoding="utf-8")
    assert digest in table
    assert digest in (output / "baseline_report.md").read_text(encoding="utf-8")


def test_the_report_artifact_on_disk_is_the_returned_payload(report):
    payload, output, _rows = report
    written = output / external_report.EXTERNAL_REPORT_NAME
    assert payload["self"]["sha256"] == hash_file(written)
    assert payload["outputs"][external_report.EXTERNAL_REPORT_NAME]["sha256"] == hash_file(written)
    on_disk = json.loads(written.read_text(encoding="utf-8"))
    assert on_disk["spec_digest"] == payload["spec_digest"]
    assert on_disk["gate"] == payload["gate"]
    assert on_disk["blocked"] is payload["blocked"]
    assert on_disk["capability_flags"] == payload["capability_flags"]
    assert on_disk["counts"] == payload["counts"]
    assert set(payload["outputs"]) == {
        external_report.EXTERNAL_REPORT_NAME,
        external_report.COVERAGE_REPORT_NAME,
        external_report.EVENT_CARDS_NAME,
        external_report.LINEAGE_NAME,
        external_report.CAPABILITY_TABLE_NAME,
        "baseline_report.md",
        *[f"{external_report.FIGURES_DIRECTORY}/{name}" for name in external_report.FIGURE_NAMES],
    }
    # Every ledger entry names the bytes it recorded, and the self entry is the only
    # one whose hash had to be added after the document was written.
    for key, record in payload["outputs"].items():
        assert record["sha256"] is not None, key
        assert record["bytes"] > 0, key
        assert hash_file(output / key) == record["sha256"], key
    figures = {record["name"]: record for record in payload["figures"]}
    assert set(figures) == set(external_report.FIGURE_NAMES)
    for name, record in figures.items():
        # Each title is headed by what the figure shows, and every one repeats the
        # clock caveat so a PNG read on its own states the same limit the payload does.
        assert record["title"].splitlines()[0], name
        assert external_report.CLOCK_CAVEAT in record["title"], name
        assert record["panel_declared_synthetic"] is True, name
        assert record["evidence_class"] == external_report.EVIDENCE_CLASS, name
        assert (output / "figures" / name).stat().st_size == record["bytes"], name


def test_the_panel_is_read_through_the_verified_sealed_path(report):
    payload, _output, rows = report
    panel = payload["inputs"]["panel"]
    assert panel["table"] == "trade_panel"
    assert panel["verified"] is True
    assert panel["row_count"] == len(rows)
    assert panel["manifest_row_count"] == len(rows)
    assert panel["declared_synthetic"] is True
    # The content hash the report cites is the manifest's, and it matches the bytes.
    assert panel["content_hash"] == panel["file_sha256"]

    coverage = payload["coverage"]
    entry = next(item for item in coverage["by_horizon"] if item["horizon_seconds"] == PRIMARY)
    # Four rows sit at the primary horizon: three carry a response and the masked
    # release is counted as present-but-masked rather than dropped from the grid.
    assert entry["rows"] == 4
    assert entry["valid_rows"] == 3
    assert entry["masked_rows"] == 1
    assert entry["response_observed_rows"] == 3
    assert entry["response_missing_rows"] == 1
    masked = next(item for item in coverage["by_horizon"] if item["horizon_seconds"] == 60)
    assert masked["rows"] == 2
    assert masked["masked_rows"] == 1
    assert {"exclusion_reason": "no_post_release_trade", "rows": 1} in coverage[
        "by_exclusion_reason"
    ]
    assert {"exclusion_reason": "missing_baseline", "rows": 1} in coverage["by_exclusion_reason"]
    assert coverage["by_clock_mode"] == [{"clock_mode": CLOCK_MODE, "rows": len(rows)}]
    assert coverage["post_release_trade_observed"]["rows_without"] == 2


def test_missing_results_stay_missing_and_are_never_zero(report):
    payload, _output, rows = report
    assert payload["counts"]["response_null_rows"] == sum(
        1 for row in rows if row["response"] is None
    )
    assert payload["coverage"]["missing_cell_count"] > 0
    assert "missing_coverage_cells_kept_in_the_grid" in payload["flags"]

    cards = {card["event_id"]: card for card in payload["event_cards"]}
    masked_cell = next(
        cell for cell in cards[EVENT_B]["responses"] if cell["horizon_seconds"] == 60
    )
    assert masked_cell["response"] is None
    assert masked_cell["valid"] is False
    assert masked_cell["exclusion_reason"] == "no_post_release_trade"
    assert {item["event_id"] for item in payload["coverage"]["missing_cells"]} == {
        EVENT_A,
        EVENT_B,
        EVENT_C,
    }
    # A release masked at every horizon stays in the cohort, so the missing fraction
    # is read against the preselected denominator rather than against the observed set.
    cohort = payload["response"]["cohorts"][0]
    assert cohort["event_ids"] == [EVENT_A, EVENT_B, EVENT_C]
    assert cohort["n_events_observed"] == 2
    primary = next(entry for entry in cohort["by_horizon"] if entry["horizon_seconds"] == PRIMARY)
    assert {event["event_id"] for event in primary["events"]} == {EVENT_A, EVENT_B}
    # The masked release is reported in its card with its reason, not omitted.
    assert cards[EVENT_C]["masked_rows"] == 1
    assert cards[EVENT_C]["observed_response_rows"] == 0
    assert {reason["exclusion_reason"] for reason in cards[EVENT_C]["exclusion_reasons"]} == {
        "missing_baseline"
    }
    assert all(cell["response"] is None for cell in cards[EVENT_C]["responses"])


def test_response_is_weighted_by_release_and_keeps_the_observed_zero(report):
    payload, _output, _rows = report
    response = payload["response"]
    assert response["kind"] == external_report.ESTIMATE_KIND
    assert response["confirmatory"] is False
    assert response["primary_horizon_seconds"] == PRIMARY

    cohort = response["cohorts"][0]
    assert cohort["cohort"] == COHORT
    primary = next(entry for entry in cohort["by_horizon"] if entry["horizon_seconds"] == PRIMARY)
    # The release's two contracts average inside the release before it is pooled:
    # (0.20 + 0.30) / 2 = 0.25 for EVENT_A, and the observed zero for EVENT_B.
    assert primary["n_events_observed"] == 2
    by_event = {item["event_id"]: item["mean_response"] for item in primary["events"]}
    assert by_event[EVENT_A] == pytest.approx(0.25)
    assert by_event[EVENT_B] == pytest.approx(0.0)  # a genuine observed zero, not a gap
    assert primary["mean_response"] == pytest.approx(0.125)
    # Row-weighting instead would give 0.1667..., so the unit really is the release.
    assert primary["mean_response"] != pytest.approx(0.5 / 3.0)

    estimate = cohort["primary_estimate"]
    assert estimate["point"] == pytest.approx(0.125)
    assert estimate["bootstrap"]["n_clusters"] == 2
    interval = estimate["interval"]
    if interval is not None:
        assert interval["lower"] <= 0.125 <= interval["upper"]
    # Only the declared seed is used; nothing wall-clock enters the resampling.
    assert estimate["bootstrap"]["seed_source"] == "configuration reproducibility.seeds[0]"
    assert estimate["bootstrap"]["seed"] == _CONFIG["reproducibility"]["seeds"][0]


def test_capabilities_are_declared_beside_what_was_observed(report):
    payload, _output, _rows = report
    flags = payload["capability_flags"]
    assert flags["historical_trades"] is True
    # No quote column exists, so quote-derived analysis stays off regardless of files.
    assert flags["historical_quotes"] is False
    assert flags["receipt_clock"] is False
    assert flags["expectation_verified"] is False
    assert flags["economic_size_verified"] is False
    # A verified contract count is not rule evidence, so the vintage stays unknown.
    assert flags["rule_vintage_verified"] == "unknown"
    assert set(flags) == set(external_report.CAPABILITY_NAMES)

    capabilities = payload["capabilities"]
    assert capabilities["observed"]["historical_trades"] is True
    assert "rule version" in capabilities["reason"]["rule_vintage_verified"]
    table = (_output / external_report.CAPABILITY_TABLE_NAME).read_text(encoding="utf-8")
    for name in external_report.CAPABILITY_NAMES:
        assert name in table


def test_evidence_gates_are_reported_separately_from_the_report_gate(report):
    payload, _output, _rows = report
    # A satisfied report over a blocked empirical gate is the expected development
    # outcome; the two answers must not be collapsed into one.
    assert payload["gate"] == external_report.GATE_SATISFIED
    gates = payload["evidence_gates"]
    assert gates["G0"]["status"] == "blocked"
    assert gates["G1"]["status"] == "blocked"
    assert gates["G4"]["status"] == "blocked"
    assert gates["G4"]["basis"] == "declared_default_no_predictive_stage_in_this_report"
    assert gates["G2"]["name"] == "measurement"
    assert all("status" in gate for gate in gates.values())
    for blocker in payload["blockers"]:
        assert set(blocker) >= {"code", "scope", "reason", "blocks"}


def test_a_blocked_panel_records_the_blocked_gate_and_no_estimate(tmp_path, config_path):
    """Every row masked: the report blocks and reports no response, never a zero."""
    rows = [
        _row(
            event_id=EVENT_A,
            contract_id="KXCPI-A",
            horizon=PRIMARY,
            response=None,
            valid=False,
            exclusion_reason="no_post_release_trade",
            post_release_trade_observed=False,
        ),
        _row(
            event_id=EVENT_B,
            contract_id="KXCPI-B",
            horizon=PRIMARY,
            response=None,
            valid=False,
            exclusion_reason="missing_baseline",
            post_release_trade_observed=False,
        ),
    ]
    _seal(tmp_path, rows)
    output = tmp_path / "blocked-report"
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", output
    )

    assert payload["blocked"] is True
    assert payload["status"] == external_report.STATUS_BLOCKED
    assert payload["gate"] == external_report.GATE_BLOCKED
    assert payload["gate_detail"]["checks"]
    failed = {
        check["id"] for check in payload["gate_detail"]["checks"] if check["status"] == "blocked"
    }
    assert {"governed_valid_row", "governed_observed_response"} <= failed
    assert any(check["id"] == "governed_valid_row" for check in payload["gate_detail"]["checks"])
    assert {blocker["code"] for blocker in payload["blockers"]} >= {"no_governed_valid_row"}

    # No estimate, and no interval substituted from a coarser window.
    assert payload["estimates"]["reported"] is False
    assert payload["estimates"]["by_cohort"][0]["interval"] is None
    for cohort in payload["response"]["cohorts"]:
        assert cohort["primary_estimate"]["status"] == "unavailable"
        assert cohort["primary_estimate"]["point"] is None
        assert "no governed row carries a response" in cohort["primary_estimate"]["reason"]
        for entry in cohort["by_horizon"]:
            assert entry["mean_response"] is None
            assert entry["n_events_observed"] == 0
    assert payload["response"]["tests_reported"] is False

    # The masked reasons survive into the artifacts rather than being dropped.
    assert all(
        cell["response"] is None for card in payload["event_cards"] for cell in card["responses"]
    )
    reasons = {item["exclusion_reason"] for item in payload["coverage"]["by_exclusion_reason"]}
    assert reasons == {"no_post_release_trade", "missing_baseline"}
    assert payload["outputs"][external_report.EXTERNAL_REPORT_NAME]["sha256"] is not None


def test_an_unverifiable_panel_blocks_without_raising(tmp_path, config_path):
    """A panel whose bytes were altered after sealing is a blocked result, not a crash."""
    _seal(tmp_path, _healthy_rows())
    sealed = tmp_path / "trade_panel.parquet"
    sealed.write_bytes(sealed.read_bytes() + b"\x00")
    payload = external_report.run_external_report(config_path, sealed, tmp_path / "altered")
    assert payload["blocked"] is True
    assert payload["gate"] == external_report.GATE_BLOCKED
    assert payload["inputs"]["panel"]["verified"] is False
    assert payload["inputs"]["verified"] is False
    assert payload["counts"]["rows"] == 0
    assert payload["registry"]["recorded"] is False
    assert "panel_unverified" in {check["id"] for check in payload["inputs"]["checks"]}


def test_an_unsupported_request_returns_a_structured_reason_without_a_substitute(
    tmp_path, config_path
):
    """A horizon the configuration does not declare is refused, and its rows stay counted."""
    undeclared_horizon = max(HORIZONS) + 60
    rows = [
        *_healthy_rows(),
        _row(event_id=EVENT_A, contract_id="KXCPI-A", horizon=undeclared_horizon, response=9.0),
        _row(
            event_id=EVENT_A,
            contract_id="KXCPI-A",
            horizon=PRIMARY,
            response=9.0,
            family="housing",
        ),
    ]
    _seal(tmp_path, rows)
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", tmp_path / "unsupported"
    )

    unsupported = payload["unsupported"]
    assert unsupported is not None
    assert unsupported["supported"] is False
    assert unsupported["count"] == 2
    codes = {record["code"] for record in unsupported["records"]}
    assert codes == {"horizon_not_declared", "family_not_declared"}

    horizon_record = next(
        record for record in unsupported["records"] if record["code"] == "horizon_not_declared"
    )
    assert horizon_record["horizons_seconds"] == [undeclared_horizon]
    assert horizon_record["declared_horizons_seconds"] == list(HORIZONS)
    assert horizon_record["rows"] == 1
    assert horizon_record["substitute_analysis_used"] is False
    assert "widening or interpolating" in horizon_record["reason"]

    family_record = next(
        record for record in unsupported["records"] if record["code"] == "family_not_declared"
    )
    assert family_record["families"] == ["housing"]
    assert family_record["declared_families"] == list(_CONFIG["coverage"]["families"])

    # The refused request is a blocked report, and nothing from it entered an estimate.
    assert payload["blocked"] is True
    assert payload["status"] == external_report.STATUS_BLOCKED
    assert "unsupported_request_refused_without_substitution" in payload["flags"]
    assert {blocker["scope"] for blocker in payload["blockers"]} >= {"unsupported_request"}
    # The two refused rows are counted as out of scope and excluded from every estimate.
    assert payload["counts"]["governed_rows"] == len(_healthy_rows())
    assert payload["counts"]["out_of_scope_rows"] == 2
    assert payload["response"]["cohorts"][0]["primary_estimate"]["point"] == pytest.approx(0.125)
    assert all(
        entry["horizon_seconds"] in HORIZONS
        for cohort in payload["response"]["cohorts"]
        for entry in cohort["by_horizon"]
    )
    # The event card still accounts for the refused rows, marked as outside the specification,
    # rather than dropping them from the record of what the panel held.
    card = next(item for item in payload["event_cards"] if item["event_id"] == EVENT_A)
    undeclared = {
        entry["horizon_seconds"]: entry["declared_in_specification"] for entry in card["by_horizon"]
    }
    assert undeclared[undeclared_horizon] is False
    assert undeclared[PRIMARY] is True
    assert card["family_declared_in_specification"] is True
    table = (tmp_path / "unsupported" / external_report.CAPABILITY_TABLE_NAME).read_text(
        encoding="utf-8"
    )
    assert "horizon_not_declared" in table


def test_a_usable_clock_over_source_only_availability_blocks_the_report(tmp_path, config_path):
    """Source alignment is retrospective; it must never be reported as usable time."""
    rows = _healthy_rows()
    for row in rows:
        row["clock_mode"] = "usable"
        row["availability_status"] = "source_time_only"
    _seal(tmp_path, rows)
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", tmp_path / "overclaim"
    )
    assert payload["blocked"] is True
    assert payload["gate"] == external_report.GATE_BLOCKED
    check = next(
        item
        for item in payload["gate_detail"]["checks"]
        if item["id"] == "clock_mode_does_not_overclaim_availability"
    )
    assert check["status"] == "blocked"
    assert check["contracts"] == ["KXCPI-A", "KXCPI-B", "KXCPI-C", "KXCPI-D"]
    assert "never becomes usable time" in check["reason"]
    # The capability stays off because no receipt evidence exists for these rows.
    assert payload["capability_flags"]["receipt_clock"] is False


def test_a_usable_clock_with_unidentifiable_availability_is_permitted(tmp_path, config_path):
    """The documented external case: usable mode reported as unidentifiable, not overclaimed."""
    rows = _healthy_rows()
    for row in rows:
        row["clock_mode"] = "usable"
        row["availability_status"] = "unidentifiable"
    _seal(tmp_path, rows)
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", tmp_path / "unidentifiable"
    )
    check = next(
        item
        for item in payload["gate_detail"]["checks"]
        if item["id"] == "clock_mode_does_not_overclaim_availability"
    )
    assert check["status"] == "ok"
    assert check["contracts"] == []
    assert payload["gate"] == external_report.GATE_SATISFIED
    # No usable interval was invented, so the receipt capability still stays off.
    assert payload["capability_flags"]["receipt_clock"] is False
    assert "unidentifiable" in next(
        entry["availability_status"] for entry in payload["coverage"]["by_availability_status"]
    )


def test_a_blocked_panel_records_the_reasons_at_the_report_level(tmp_path, config_path):
    """A reader of the report artifact alone must learn what blocked it.

    ``blocked_stages`` is reserved for a stage that failed to run, so it stays empty
    here: the measurement ran and reported itself blocked. The causes are carried by
    ``blocked_reason``, ``gate_detail.blocked_by`` and ``blockers`` instead, and the
    artifact on disk carries the same text the returned mapping does.
    """
    rows = [
        _row(
            event_id=EVENT_A,
            contract_id="KXCPI-A",
            horizon=PRIMARY,
            response=None,
            valid=False,
            exclusion_reason="rule_version_unknown",
            post_release_trade_observed=False,
        ),
        _row(
            event_id=EVENT_B,
            contract_id="KXCPI-B",
            horizon=PRIMARY,
            response=None,
            valid=False,
            exclusion_reason="rule_evidence_missing",
            post_release_trade_observed=False,
        ),
    ]
    _seal(tmp_path, rows)
    output = tmp_path / "level"
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", output
    )

    assert payload["blocked"] is True
    assert payload["gate"] == external_report.GATE_BLOCKED
    # No stage failed to run, so no blocked stage is claimed.
    assert payload["blocked_stages"] == []
    assert payload["warnings"] == []

    # The reasons are structured and reachable without parsing a nested estimate.
    assert payload["blocked_reason"] and isinstance(payload["blocked_reason"], str)
    assert payload["gate_detail"]["blocked_by"]
    assert payload["gate_detail"]["blocked_reasons"]
    for record in payload["gate_detail"]["blocked_reasons"]:
        assert set(record) == {"code", "scope", "reason", "blocks", "rows"}
        assert record["reason"]
        assert record["blocks"]
    codes = {record["code"] for record in payload["blockers"] if record["gate_blocking"]}
    assert codes == set(payload["gate_detail"]["blocked_by"])
    assert {"no_governed_valid_row", "panel_reports_no_response"} <= codes

    # The masks the panel itself stated are reported with their counts, so the reader
    # learns the specific reasons rather than only that nothing was usable.
    masked = next(
        record for record in payload["blockers"] if record["code"] == "rows_masked_by_reason"
    )
    assert {entry["exclusion_reason"] for entry in masked["by_reason"]} == {
        "rule_version_unknown",
        "rule_evidence_missing",
    }
    assert masked["rows"] == 2

    # A limit of the run is reported without blocking the gate, and says which it is.
    limits = {record["code"]: record for record in payload["limits"]}
    assert "rule_evidence_missing" in limits
    assert "pair_window_counts_not_joined" in limits
    assert all(
        record["gate_blocking"] is False
        for record in payload["blockers"]
        if record["code"] in limits
    )

    # The artifact on disk carries the same reasons, so stderr is not the only route.
    on_disk = json.loads(
        (output / external_report.EXTERNAL_REPORT_NAME).read_text(encoding="utf-8")
    )
    assert on_disk["blocked_reason"] == payload["blocked_reason"]
    assert on_disk["gate_detail"]["reason"] == payload["gate_detail"]["reason"]
    assert on_disk["gate_detail"]["blocked_by"] == payload["gate_detail"]["blocked_by"]
    assert on_disk["blocked"] is True
    table = (output / external_report.CAPABILITY_TABLE_NAME).read_text(encoding="utf-8")
    assert "no_governed_valid_row" in table
    # The CLI reads gate_detail.reason, so it must never be the empty placeholder.
    assert on_disk["gate_detail"]["reason"] != "no reason recorded"
    assert "no reason recorded" not in on_disk["gate_detail"]["reason"]


def test_the_gate_and_the_blocked_flag_never_disagree(report, tmp_path, config_path):
    """The two answers are one answer, on every panel this report accepts."""
    healthy, _output, _rows = report
    assert healthy["gate"] == external_report.GATE_SATISFIED
    assert healthy["blocked"] is False
    assert healthy["blocked_reason"] is None
    assert healthy["gate_detail"]["blocked_by"] == []

    # A panel whose every row is outside the specification is blocked by that refusal,
    # and the same invariant holds. It is sealed to its own path because a sealed
    # dataset refuses to be replaced with different bytes.
    rows = [
        _row(event_id=EVENT_A, contract_id="KXCPI-A", horizon=max(HORIZONS) + 60, response=0.4),
        _row(
            event_id=EVENT_B, contract_id="KXCPI-B", horizon=PRIMARY, response=0.2, family="housing"
        ),
    ]
    refused_panel = tmp_path / "refused.parquet"
    write_parquet(
        rows,
        refused_panel,
        table="trade_panel",
        coverage_epoch="fixture",
        metadata={"synthetic": "true"},
    )
    refused = external_report.run_external_report(config_path, refused_panel, tmp_path / "refused")
    assert refused["gate"] == external_report.GATE_BLOCKED
    assert refused["blocked"] is True
    assert refused["blocked_reason"]
    assert set(refused["gate_detail"]["blocked_by"]) >= {
        external_report._HORIZON_NOT_DECLARED,
        external_report._FAMILY_NOT_DECLARED,
    }
    for payload in (healthy, refused):
        assert (payload["gate"] == external_report.GATE_BLOCKED) == payload["blocked"]
        assert (payload["blocked_reason"] is not None) == payload["blocked"]


def test_a_missing_panel_raises_a_clear_error(tmp_path, config_path):
    missing = tmp_path / "absent-panel.parquet"
    with pytest.raises(FileNotFoundError) as error:
        external_report.run_external_report(config_path, missing, tmp_path / "out")
    assert "absent-panel.parquet" in str(error.value)
    assert "never from one selected on the caller's behalf" in str(error.value)
    assert not (tmp_path / "out").exists() or not any((tmp_path / "out").iterdir())


def test_a_panel_that_declares_no_provenance_is_not_recorded_in_the_registry(tmp_path, config_path):
    """The registry never assumes whether a run is synthetic, so an undeclared one is refused."""
    _seal(tmp_path, _healthy_rows(), declare_synthetic=False)
    payload = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", tmp_path / "undeclared"
    )
    assert payload["inputs"]["panel"]["declared_synthetic"] is None
    assert "panel_provenance_not_declared" in payload["flags"]
    assert payload["registry"]["recorded"] is False
    assert "never assumes" in payload["registry"]["reason"]
    assert not (tmp_path / "undeclared" / external_report.REGISTRY_DB_NAME).exists()
    # The report still describes every row it read.
    assert payload["counts"]["rows"] == len(_healthy_rows())
    assert payload["gate"] == external_report.GATE_SATISFIED


def test_a_declared_run_is_recorded_with_the_digest_it_was_produced_under(report):
    payload, output, _rows = report
    registry = payload["registry"]
    assert registry["recorded"] is True
    assert registry["run_id"] == payload["run_id"]
    assert registry["spec_digest"] == payload["spec_digest"]
    assert registry["data_hash"] == payload["inputs"]["panel"]["content_hash"]
    assert registry["source_hash"] == payload["config_hash"]
    assert registry["n_runs"] == 1
    assert registry["n_reservations"] == 0
    assert (output / external_report.REGISTRY_DB_NAME).is_file()
    recorded = [
        json.loads(line)
        for line in (output / external_report.JSONL_NAME).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert recorded[0]["run_id"] == payload["run_id"]
    assert recorded[0]["synthetic"] is True
    assert recorded[0]["event_ids"] == [EVENT_A, EVENT_B, EVENT_C]
    assert recorded[0]["metrics"]["confirmatory"] is False


def test_an_unreadable_coverage_artifact_blocks_the_report(tmp_path, config_path):
    """The configuration names the coverage artifact, so its absence is a blocked input."""
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["inputs"]["audit_coverage"] = str(tmp_path / "no-such-coverage.json")
    stripped = tmp_path / "no-coverage.yaml"
    stripped.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    _seal(tmp_path, _healthy_rows())
    payload = external_report.run_external_report(
        stripped, tmp_path / "trade_panel.parquet", tmp_path / "no-coverage"
    )
    assert payload["blocked"] is True
    assert payload["gate"] == external_report.GATE_BLOCKED
    artifact = payload["inputs"]["coverage_artifact"]
    assert artifact["status"] == "unreadable"
    assert artifact["sha256"] is None
    assert "no-such-coverage.json" in artifact["reason"]
    assert "coverage_artifact" in {
        check["id"] for check in payload["gate_detail"]["checks"] if check["status"] == "blocked"
    }


def test_an_unresolved_analysis_spec_is_recorded_without_a_hash(tmp_path, config_path):
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["registered_estimation"]["analysis_spec"] = str(tmp_path / "absent-spec.yaml")
    stripped = tmp_path / "no-spec.yaml"
    stripped.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    _seal(tmp_path, _healthy_rows())
    payload = external_report.run_external_report(
        stripped, tmp_path / "trade_panel.parquet", tmp_path / "no-spec"
    )
    analysis = payload["inputs"]["analysis_specification"]
    assert analysis["resolved"] is False
    assert analysis["sha256"] is None
    assert payload["analysis_spec_hash"] is None
    assert "does not resolve to a file" in analysis["reason"]
    assert "analysis_specification_unresolved" in payload["flags"]
    # The digest still changes when the specification path changes, so two runs
    # under different specifications cannot share one digest.
    assert payload["spec_digest"] != _CONFIG["config_version"]


def test_a_missing_configuration_raises(tmp_path):
    _seal(tmp_path, _healthy_rows())
    with pytest.raises(FileNotFoundError) as error:
        external_report.run_external_report(
            tmp_path / "no-config.yaml", tmp_path / "trade_panel.parquet", tmp_path / "out"
        )
    assert "pipeline configuration not found" in str(error.value)


def test_an_invalid_run_id_is_refused(tmp_path, config_path):
    _seal(tmp_path, _healthy_rows())
    with pytest.raises(ValueError):
        external_report.run_external_report(
            config_path, tmp_path / "trade_panel.parquet", tmp_path / "out", run_id="   "
        )


def test_a_primary_horizon_outside_the_declared_curve_is_refused(tmp_path, config_path):
    """Widening the curve to admit a primary horizon would change the estimand."""
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["response"]["primary_horizon_seconds"] = max(HORIZONS) + 60
    broken = tmp_path / "broken-horizon.yaml"
    broken.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    _seal(tmp_path, _healthy_rows())
    with pytest.raises(ValueError) as error:
        external_report.run_external_report(
            broken, tmp_path / "trade_panel.parquet", tmp_path / "broken"
        )
    assert "is not one of the declared horizons" in str(error.value)


def test_the_same_inputs_land_the_same_digest_and_artifact_bytes(tmp_path, config_path):
    """Two runs over identical inputs land identical bytes, with no wall-clock in them.

    The panel deliberately carries a row outside the declared specification, so this
    also proves a refused request is refused identically on a replay rather than
    being reported once and dropped the second time.
    """
    rows = [
        *_healthy_rows(),
        _row(event_id=EVENT_A, contract_id="KXCPI-A", horizon=max(HORIZONS) + 60, response=9.0),
    ]
    _seal(tmp_path, rows)
    first = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", tmp_path / "first", run_id="repeat"
    )
    second = external_report.run_external_report(
        config_path, tmp_path / "trade_panel.parquet", tmp_path / "second", run_id="repeat"
    )
    assert first["spec_digest"] == second["spec_digest"]
    assert first["unsupported"] == second["unsupported"]
    assert first["blocked"] is second["blocked"] is True
    for name in (
        external_report.COVERAGE_REPORT_NAME,
        external_report.EVENT_CARDS_NAME,
        external_report.LINEAGE_NAME,
        external_report.CAPABILITY_TABLE_NAME,
        "baseline_report.md",
    ):
        assert hash_file(tmp_path / "first" / name) == hash_file(tmp_path / "second" / name), name
    for name in external_report.FIGURE_NAMES:
        assert hash_file(tmp_path / "first" / "figures" / name) == hash_file(
            tmp_path / "second" / "figures" / name
        ), name

    # The report document differs in exactly the respects the caller chose: the output
    # directory it was written to, and the absolute paths derived from it. Nothing
    # measured differs, which is what makes a replay auditable.
    def _document(directory: Path) -> dict:
        payload = json.loads(
            (tmp_path / directory / external_report.EXTERNAL_REPORT_NAME).read_text(
                encoding="utf-8"
            )
        )
        return {key: payload[key] for key in _MEASURED_KEYS}

    assert _document(Path("first")) == _document(Path("second"))
    on_disk_coverage = json.loads(
        (tmp_path / "first" / external_report.COVERAGE_REPORT_NAME).read_text(encoding="utf-8")
    )
    # The coverage section embedded in the report is the one written to its own
    # artifact, so the two cannot drift into reporting different grids.
    assert _document(Path("first"))["coverage"] == on_disk_coverage
    assert first["counts"] == second["counts"]
    recorded = [
        json.loads(line)
        for line in (tmp_path / "first" / external_report.JSONL_NAME)
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert recorded[0]["created_at"]  # the one wall-clock stamp lives in the registry
    assert "wall-clock" in first["lineage"]["timing"]["reason"]
    assert first["lineage"]["timing"]["elapsed_seconds"] is None
