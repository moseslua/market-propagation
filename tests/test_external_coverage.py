"""Regression tests for the external release/rule coverage grid.

The coverage grid is where the study is most tempted to promote a convenience
into a result, so these tests defend the specific boundaries that stop it:

* A rule that cannot bind contract identity, the raw rule hash, the source, the
  observation time, the in-force interval and the settlement semantics is not
  verified, and a trade-history join never waives that. A contract whose rule
  document is missing is a blocked candidate, which is a reportable outcome
  rather than an omission.
* An unmeasured pair-level window count is ``None``, not ``0``. "No trade was
  joined for this pair" and "this pair genuinely traded nothing" are different
  facts, and a grid that reported both as zero would erase the distinction the
  plan requires it to keep.
* Series identity is the leading capital run of the ticker, never a substring.
  Kalshi event identifiers embed hexadecimal, so a substring match on ``FED``
  returns Monday Night Football markets; this is checked against the real
  identifier that exposed the problem.
* G0 stays blocked with a stated reason unless rules, cohort and supported
  frequency all pass.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest
import yaml

from market_propagation.ingest import audit
from market_propagation.storage import write_parquet

RELEASE_ROWS = [
    {
        "event_id": "cpi_2025_01",
        "family": "cpi",
        "scheduled_at": dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC),
        "reference_period": "2024-12",
        "values_json": {"headline_mom": "0.3"},
        "revisions_json": {},
        "raw_hash": "a" * 64,
    },
    {
        "event_id": "empsit_2025_01",
        "family": "employment",
        "scheduled_at": dt.datetime(2025, 1, 10, 13, 30, tzinfo=dt.UTC),
        "reference_period": "2024-12",
        "values_json": {"payroll_change": "256000"},
        "revisions_json": {},
        "raw_hash": "b" * 64,
    },
]

COMPLETE_EVIDENCE = {
    "contract_id": "FED-25DEC-T2.75",
    "rule_hash": "c" * 64,
    "source_url": "https://example.invalid/rules/fed-25dec",
    "verified_by": "analyst:retrospective-rule-read",
    "in_force_from": "2024-12-01T00:00:00+00:00",
    "in_force_to": "2025-12-10T00:00:00+00:00",
    "observed_at": "2025-06-01T00:00:00+00:00",
    "settlement_semantics": "resolves_yes_if_target_rate_at_or_above_strike_at_deadline",
}


def _pair(**overrides: object) -> audit.ExternalCandidatePair:
    fields: dict[str, object] = {
        "event_id": "cpi_2025_01",
        "family": "cpi",
        "ticker": "FED-25DEC-T2.75",
        "series_ticker": "FED",
        "lifecycle_eligible": True,
        "rule_version_verified": True,
        "rule_version_evidence": COMPLETE_EVIDENCE,
        "rule_hash": "c" * 64,
        "close_time": "2025-12-10T18:55:00+00:00",
        "window_overlap": True,
        "selection_basis": "series_lifecycle_only_no_post_event_volume_filter",
        "cohort": "downstream",
    }
    fields.update(overrides)
    return audit.ExternalCandidatePair(**fields)  # type: ignore[arg-type]


def _settings() -> object:
    from market_propagation.trade_panel import PanelSettings

    return PanelSettings()


def _build(pairs, *, releases=None, config=None, series=("KXFED", "FED")):
    return audit.build_external_coverage(
        releases=releases if releases is not None else RELEASE_ROWS,
        pairs=pairs,
        settings=_settings(),
        config=config if config is not None else {"blocking": {"release_dataset": "x.parquet"}},
        policy_series=series,
    )


def test_complete_evidence_verifies() -> None:
    result = audit.validate_rule_evidence(
        COMPLETE_EVIDENCE, ticker="FED-25DEC-T2.75", release_family="cpi"
    )
    assert result["verified"] is True
    assert result["missing"] == []
    assert result["reason"] is None


def test_missing_rule_hash_does_not_verify() -> None:
    evidence = dict(COMPLETE_EVIDENCE)
    evidence.pop("rule_hash")
    result = audit.validate_rule_evidence(evidence, ticker="FED-25DEC-T2.75", release_family="cpi")
    assert result["verified"] is False
    assert "rule_hash" in result["missing"]
    assert result["reason"] == audit.REASON_RULE_EVIDENCE_MISSING


def test_missing_settlement_semantics_does_not_verify() -> None:
    evidence = dict(COMPLETE_EVIDENCE)
    evidence.pop("settlement_semantics")
    result = audit.validate_rule_evidence(evidence, ticker="FED-25DEC-T2.75", release_family="cpi")
    assert result["verified"] is False
    assert "settlement_semantics" in result["missing"]


def test_absent_evidence_does_not_verify() -> None:
    result = audit.validate_rule_evidence(None, ticker="FED-25DEC-T2.75", release_family="cpi")
    assert result["verified"] is False
    assert set(result["missing"]) == {
        "contract_id",
        "rule_hash",
        "source_url",
        "verified_by",
        "in_force_from",
        "observed_at",
        "settlement_semantics",
    }


def test_evidence_the_audits_own_parser_accepts_also_verifies_here() -> None:
    """Coverage must not refuse a record the bounded audit accepts.

    The two required sets are separate code paths over one study contract, so they
    can drift apart silently. Building a record through the audit's own parser and
    then running it through this check is what pins them together: if coverage
    demanded a field the canonical record does not carry, no rule could ever be
    verified and the cohort could never leave its blocked state. That is not a style
    concern, it is the difference between a study that can run and one that cannot.
    """
    config = {
        audit.POLICY_COHORT_CONFIG_KEY: {
            audit.POLICY_COHORT_SECTION: [
                {
                    "family_key": audit.POLICY_FAMILY_KEY,
                    audit.RULE_VERSION_EVIDENCE_KEY: [
                        {
                            "contract_id": "FED-25DEC-T2.75",
                            "rule_hash": "c" * 64,
                            "source_url": "https://example.invalid/rules/fed-25dec",
                            "verified_by": "analyst:retrospective-rule-read",
                            "in_force_from": "2024-12-01T00:00:00+00:00",
                            # An open interval is stated as open, not demanded.
                            "in_force_to": None,
                            "observed_at": "2025-06-01T00:00:00+00:00",
                            "settlement_semantics": "resolves_yes_if_target_rate_at_or_above_strike",
                        }
                    ],
                }
            ]
        }
    }
    records = audit.rule_version_evidence(config)
    assert len(records) == 1

    result = audit.validate_rule_evidence(
        records[0].as_dict(), ticker="FED-25DEC-T2.75", release_family="cpi"
    )
    assert result["verified"] is True, result
    assert result["missing"] == []

    # The binding names are the study's own required set, in the same terms.
    assert {name for name, _ in audit.RULE_EVIDENCE_BINDINGS} == set(
        audit.REQUIRED_RULE_VERSION_EVIDENCE_FIELDS
    )

    # And the interval end is genuinely optional.
    assert {name for name, _ in audit.RULE_EVIDENCE_OPTIONAL_BINDINGS} == {"in_force_to"}


def test_a_non_digest_rule_hash_is_refused() -> None:
    """An identifier or a truncated digest cannot stand in for a digest of text."""
    evidence = dict(COMPLETE_EVIDENCE, rule_hash="FED-25DEC-T2.75")
    result = audit.validate_rule_evidence(evidence, ticker="FED-25DEC-T2.75", release_family="cpi")
    assert result["verified"] is False
    assert result["reason"] == "rule_hash_is_not_a_digest_of_rule_text"


def test_evidence_for_another_contract_establishes_nothing() -> None:
    result = audit.validate_rule_evidence(
        COMPLETE_EVIDENCE, ticker="FED-25MAR-T3.00", release_family="cpi"
    )
    assert result["verified"] is False
    assert result["reason"] == "rule_evidence_names_a_different_contract"


def test_series_identity_is_a_prefix_not_a_substring() -> None:
    assert audit.series_of("FED-25DEC-T2.75") == "FED"
    assert audit.series_of("KXFEDDECISION-25DEC-C25") == "KXFEDDECISION"
    # The real identifier that exposed the substring trap: it contains "FED" in a
    # hexadecimal suffix but is a Monday Night Football market.
    sports = "KXMVENFLSINGLEGAME-S2025FED4B0DA5B1"
    assert audit.series_of(sports) == "KXMVENFLSINGLEGAME"
    assert audit.is_exact_series(sports, "FED") is False
    assert audit.is_exact_series("FED-25DEC-T2.75", "FED") is True


def test_a_candidate_outside_the_configured_series_is_refused() -> None:
    report = _build([_pair(ticker="KXMVENFLSINGLEGAME-S2025FED4B0DA5B1", series_ticker=None)])
    assert report.counts["overall"]["candidate_pairs"] == 0
    assert report.counts["overall"]["series_refused_by_exact_identity"] == 1
    refused = report.counts["refused_series"][0]
    assert refused["reason"] == audit.REASON_SUBSTRING_SERIES_MATCH_REFUSED


def test_unverified_rule_blocks_the_candidate_and_g0() -> None:
    report = _build([_pair(rule_version_verified=False, rule_version_evidence=None)])
    overall = report.counts["overall"]
    assert overall["candidate_pairs"] == 1
    assert overall["lifecycle_eligible_pairs"] == 1
    assert overall["rule_verified_pairs"] == 0
    assert overall["blocked_rule_candidates"] == 1
    assert report.gate_g0 == audit.GATE_BLOCKED
    assert any(entry.get("reason") == audit.REASON_RULE_VERSION_UNKNOWN for entry in report.blocked)
    assert report.pairs[0].rule_verified is False
    assert report.pairs[0].valid_horizons == ()


def test_verified_rule_admits_the_candidate() -> None:
    report = _build([_pair()])
    overall = report.counts["overall"]
    assert overall["rule_verified_pairs"] == 1
    assert overall["blocked_rule_candidates"] == 0
    assert report.pairs[0].valid_horizons == tuple(_settings().horizons_seconds)


def test_unmeasured_window_counts_are_null_not_zero() -> None:
    report = _build([_pair()])
    overall = report.counts["overall"]
    assert overall["baseline_observed_pairs"] is None
    assert overall["endpoint_observed_pairs"] is None
    assert overall["unmeasured"] == {
        "baseline_observed_pairs": audit.REASON_TRADE_COUNTS_NOT_JOINED,
        "endpoint_observed_pairs": audit.REASON_TRADE_COUNTS_NOT_JOINED,
    }
    assert report.pairs[0].baseline_observed is None
    assert audit.REASON_TRADE_COUNTS_NOT_JOINED in report.pairs[0].exclusion_reasons


def test_measured_zero_activity_is_a_distinct_fact_from_unmeasured() -> None:
    pair = _pair(
        pre_window_trades=9,
        baseline_window_trades=0,
        release_window_trades=4,
        endpoint_window_trades=0,
    )
    report = _build([pair])
    overall = report.counts["overall"]
    # Measured, and genuinely zero: distinguishable from the null above.
    assert overall["baseline_observed_pairs"] == 0
    assert overall["endpoint_observed_pairs"] == 0
    assert overall["unmeasured"] == {}
    assert report.pairs[0].baseline_observed is False
    assert report.pairs[0].pre_window_trades == 9


def test_missing_cells_stay_in_the_grid() -> None:
    report = _build(
        [
            _pair(ticker="FED-25DEC-T2.75"),
            _pair(
                ticker="FED-25SEP-T3.75", rule_version_verified=False, rule_version_evidence=None
            ),
            _pair(ticker="FED-25MAR-T3.00", lifecycle_eligible=False, window_overlap=False),
        ]
    )
    # All three candidates are present, including the blocked and ineligible ones.
    assert len(report.pairs) == 3
    assert report.counts["overall"]["candidate_pairs"] == 3
    tickers = {pair.ticker for pair in report.pairs}
    assert tickers == {"FED-25DEC-T2.75", "FED-25SEP-T3.75", "FED-25MAR-T3.00"}
    reasons = {reason for pair in report.pairs for reason in pair.exclusion_reasons}
    assert audit.REASON_LIFECYCLE_INELIGIBLE in reasons
    assert audit.REASON_NO_WINDOW_OVERLAP in reasons


def test_counts_are_separated_by_pair_class() -> None:
    report = _build(
        [
            _pair(ticker="FED-25DEC-T2.75", baseline_window_trades=3, endpoint_window_trades=2),
            _pair(
                ticker="FED-25MAR-T3.00",
                rule_version_verified=False,
                rule_version_evidence=None,
                baseline_window_trades=1,
                endpoint_window_trades=0,
            ),
            _pair(ticker="FED-25JUN-T3.25", lifecycle_eligible=False),
        ]
    )
    overall = report.counts["overall"]
    assert overall["candidate_pairs"] == 3
    assert overall["lifecycle_eligible_pairs"] == 2
    assert overall["rule_verified_pairs"] == 1
    assert overall["blocked_rule_candidates"] == 2
    assert overall["distinct_release_clusters"] == 1
    assert overall["release_clusters"] == ["cpi_2025_01"]


def test_g0_stays_blocked_with_a_stated_reason_without_rule_evidence() -> None:
    report = _build([_pair(rule_version_verified=False, rule_version_evidence=None)])
    assert report.gate_g0 == audit.GATE_BLOCKED
    assert report.gates["rule_gate"]["satisfied"] is False
    cohort_reasons = [entry["reason"] for entry in report.blocked if entry.get("scope") == "cohort"]
    assert cohort_reasons, "a blocked gate must name its reason"
    assert all(reason for reason in cohort_reasons)


def test_g0_does_not_pass_on_frequency_alone() -> None:
    report = _build([_pair()])
    assert report.gates["supported_frequency_gate"]["satisfied"] is True
    # Frequency passing is not enough: the cohort gates are not established, so G0
    # is still blocked rather than inferred from the one gate that happens to pass.
    assert report.gate_g0 == audit.GATE_BLOCKED
    assert report.gates["rule_vintage_gate"]["satisfied"] is False


def _write_inputs(tmp_path: Path, *, max_contracts: int = 2) -> Path:
    """A minimal but real configuration pointing at synthetic sealed inputs."""
    releases_path = tmp_path / "releases.parquet"
    write_parquet(RELEASE_ROWS, releases_path, table="releases")

    coverage_path = tmp_path / "coverage.json"
    coverage_path.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "event_id": "cpi_2025_01",
                        "family": "cpi",
                        "candidates": [
                            {
                                "ticker": "FED-25DEC-T2.75",
                                "series_ticker": "FED",
                                "lifecycle_eligible": True,
                                "rule_version_verified": False,
                                "rule_version_evidence": None,
                                "close_time": "2025-12-10T18:55:00+00:00",
                                "window_overlap": True,
                                "selection_basis": "series_lifecycle_only",
                            },
                            {
                                "ticker": "FED-25MAR-T3.00",
                                "series_ticker": "FED",
                                "lifecycle_eligible": True,
                                "rule_version_verified": False,
                                "window_overlap": True,
                            },
                            {
                                "ticker": "KXMVENFLSINGLEGAME-S2025FED4B0DA5B1",
                                "series_ticker": "KXMVENFLSINGLEGAME",
                                "lifecycle_eligible": True,
                                "window_overlap": True,
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    template = yaml.safe_load(Path("configs/external_history_v1.yaml").read_text(encoding="utf-8"))
    template["inputs"]["release_dataset"] = str(releases_path)
    template["inputs"]["rule_evidence_source"] = str(tmp_path / "absent-registry.json")
    template["inputs"]["audit_coverage"] = str(coverage_path)
    template["extraction"]["max_contracts_per_event"] = max_contracts
    config_path = tmp_path / "external_history_v1.yaml"
    config_path.write_text(yaml.safe_dump(template, sort_keys=False), encoding="utf-8")
    return config_path


def test_absent_rule_evidence_leaves_rules_unverified(tmp_path: Path) -> None:
    config_path = _write_inputs(tmp_path)
    inputs = audit.load_coverage_inputs(config_path)

    assert inputs["inputs"]["rule_evidence_present"] is False
    assert inputs["inputs"]["release_count"] == 2
    assert inputs["inputs"]["post_event_activity_used_for_selection"] is False
    assert all(pair.rule_version_verified is False for pair in inputs["pairs"])

    report = audit.build_external_coverage(**inputs)
    assert report.counts["overall"]["rule_verified_pairs"] == 0
    assert report.gate_g0 == audit.GATE_BLOCKED


def test_max_contracts_is_recorded_as_applied(tmp_path: Path) -> None:
    config_path = _write_inputs(tmp_path, max_contracts=1)
    inputs = audit.load_coverage_inputs(config_path)
    assert inputs["inputs"]["max_contracts"] == 1
    assert inputs["inputs"]["max_contracts_applied"] == 1
    # The cap applies to the candidates the configured series admitted, so an
    # out-of-series ticker never consumes a slot.
    assert inputs["inputs"]["candidate_pairs"] == 1
    assert inputs["inputs"]["events_capped_by_max_contracts"] == ["cpi_2025_01"]


def test_a_null_release_timestamp_serializes_as_null(tmp_path: Path) -> None:
    """A stored null must reach the artifact as null, never as a NaT instant.

    ``pandas.NaT`` is an instance of ``datetime``, so a NaT that survives into the
    record reaches the serializer as an instant and fails in ``astimezone``. The
    sealed release dataset carries null clock columns, which is the real case.
    """
    config_path = _write_inputs(tmp_path)
    inputs = audit.load_coverage_inputs(config_path)
    report = audit.build_external_coverage(**inputs)
    payload = report.as_dict()
    text = json.dumps(payload, default=str)
    assert "NaT" not in text
    assert all(event["usable_time"] is None for event in payload["events"])
    assert all(event["received_time"] is None for event in payload["events"])


def test_a_missing_release_dataset_raises_rather_than_substituting(tmp_path: Path) -> None:
    config_path = _write_inputs(tmp_path)
    with pytest.raises(FileNotFoundError, match="release dataset"):
        audit.load_coverage_inputs(config_path, release_dataset=str(tmp_path / "absent.parquet"))


def test_load_coverage_inputs_returns_only_the_consumed_keys(tmp_path: Path) -> None:
    config_path = _write_inputs(tmp_path)
    inputs = audit.load_coverage_inputs(config_path)
    assert set(inputs) == {"releases", "pairs", "settings", "config", "inputs"}
