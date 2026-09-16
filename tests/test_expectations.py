"""Acceptance tests for the point-in-time expectation contract.

Every test here defends one substitution that would manufacture a surprise the
study never had: a forecast published after the release, a forecast scored against
a revision, a unit read from the magnitude of the number, a consensus with no
identity, evidence bytes that changed after the record was written, and a news
vector fitted on the statistics that happened to be available.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from market_propagation.domain import UTC
from market_propagation.ingest.expectations import (
    REFUSAL_EVIDENCE_BYTES_MISMATCH,
    REFUSAL_EVIDENCE_UNREADABLE,
    REFUSAL_INCOMPLETE_NEWS_VECTOR,
    REFUSAL_MARKET_IMPLIED,
    REFUSAL_POST_RELEASE_FORECAST,
    REFUSAL_REFERENCE_PERIOD_MISMATCH,
    REFUSAL_REVISED_ACTUAL_TARGET,
    REFUSAL_SOURCE_ABSENT,
    REFUSAL_UNIT_MISMATCH,
    REFUSAL_UNKNOWN_STATISTIC,
    ExpectationError,
    ExpectationRecord,
    ExpectationSourceError,
    ReleaseFacts,
    declared_unit,
    load_expectations,
    news_vector,
    surprise,
    validate_expectation,
)

RELEASE = dt.datetime(2025, 1, 10, 13, 30, tzinfo=UTC)
PUBLISHED = dt.datetime(2025, 1, 8, 15, 0, tzinfo=UTC)
EVIDENCE = b"consensus-table-2025-01-08\n"
EVIDENCE_NAME = "consensus/2025-01-08.tsv"

STATISTICS = ("payrolls_change_thousands", "unemployment_rate_pct")


def _facts(**overrides: object) -> ReleaseFacts:
    payload: dict[str, object] = {
        "event_id": "empsit_2025_01",
        "family": "employment",
        "release_time": RELEASE,
        "reference_period": "2024-12",
        "statistics": {
            "payrolls_change_thousands": Decimal("256"),
            "unemployment_rate_pct": Decimal("4.1"),
        },
        "revised_statistics": ("payrolls_change_jobs_revised_November",),
        "raw_hash": "b" * 64,
    }
    payload.update(overrides)
    return ReleaseFacts(**payload)  # type: ignore[arg-type]


def _root(tmp_path: Path, *, payload: bytes = EVIDENCE) -> Path:
    target = tmp_path / EVIDENCE_NAME
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return tmp_path


def _record(**overrides: object) -> ExpectationRecord:
    payload: dict[str, object] = {
        "event_id": "empsit_2025_01",
        "statistic": "payrolls_change_thousands",
        "unit": "thousands_of_count",
        "reference_period": "2024-12",
        "value": Decimal("150"),
        "published_at": PUBLISHED,
        "source_kind": "licensed_consensus",
        "revision_status": "initial",
        "consensus_id": "example_consensus_panel",
        "verified_by": "read_from_the_archived_consensus_table",
        "source_url": "https://example.invalid/consensus/2025-01-08",
        "evidence_path": EVIDENCE_NAME,
        "evidence_sha256": hashlib.sha256(EVIDENCE).hexdigest(),
    }
    payload.update(overrides)
    return ExpectationRecord(**payload)  # type: ignore[arg-type]


def test_a_validated_expectation_carries_its_publication_clock_and_evidence_identity(
    tmp_path: Path,
) -> None:
    """The value is admitted only with the identity that makes it re-derivable."""
    expectation = validate_expectation(_record(), _facts(), evidence_root=_root(tmp_path))

    assert expectation.event_id == "empsit_2025_01"
    assert expectation.statistic == "payrolls_change_thousands"
    assert expectation.value == Decimal("150")
    assert expectation.source_kind == "licensed_consensus"
    assert expectation.revision_status == "initial"
    # The publication instant is the source time, and the receipt stays unknown.
    assert expectation.clock.source_time == PUBLISHED
    assert expectation.clock.received_time is None
    assert expectation.clock.usable_time is None
    assert expectation.provenance.raw_hash == hashlib.sha256(EVIDENCE).hexdigest()
    assert expectation.provenance.source.endswith("/2025-01-08")
    # The surprise is the first print minus the expectation, in the release's unit.
    assert surprise(expectation, _facts()) == Decimal("106")


def test_a_post_release_forecast_is_refused(tmp_path: Path) -> None:
    """A forecast published at or after the release restates the outcome."""
    root = _root(tmp_path)
    for published in (RELEASE, RELEASE + dt.timedelta(seconds=1)):
        with pytest.raises(ExpectationError) as caught:
            validate_expectation(_record(published_at=published), _facts(), evidence_root=root)
        assert caught.value.refusal == REFUSAL_POST_RELEASE_FORECAST


def test_a_revised_actual_is_not_the_first_print(tmp_path: Path) -> None:
    """A forecast scored against a revision measures the revision, not the news."""
    root = _root(tmp_path)
    # A statistic the release publishes only in its revisions block is refused
    # whether or not the release declares a statistic of a similar name.
    with pytest.raises(ExpectationError) as revised:
        validate_expectation(
            _record(statistic="payrolls_change_jobs_revised_November"),
            _facts(),
            evidence_root=root,
        )
    assert revised.value.refusal == REFUSAL_REVISED_ACTUAL_TARGET

    with pytest.raises(ExpectationError) as absent:
        validate_expectation(
            _record(statistic="payrolls_change_jobs_revised_October"),
            _facts(),
            evidence_root=root,
        )
    assert absent.value.refusal == REFUSAL_UNKNOWN_STATISTIC

    with pytest.raises(ExpectationError) as declared_revision:
        validate_expectation(_record(revision_status="revised"), _facts(), evidence_root=root)
    assert declared_revision.value.refusal == REFUSAL_REVISED_ACTUAL_TARGET


def test_a_unit_read_from_the_magnitude_is_refused(tmp_path: Path) -> None:
    """The declared unit is compared against the release's own, never inferred."""
    root = _root(tmp_path)
    with pytest.raises(ExpectationError) as caught:
        validate_expectation(_record(unit="count"), _facts(), evidence_root=root)
    assert caught.value.refusal == REFUSAL_UNIT_MISMATCH


def test_a_reference_period_or_statistic_the_release_does_not_state_is_refused(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    with pytest.raises(ExpectationError) as period:
        validate_expectation(_record(reference_period="2024-11"), _facts(), evidence_root=root)
    assert period.value.refusal == REFUSAL_REFERENCE_PERIOD_MISMATCH

    with pytest.raises(ExpectationError) as statistic:
        validate_expectation(
            _record(statistic="headline_cpi_yoy_pct"), _facts(), evidence_root=root
        )
    assert statistic.value.refusal == REFUSAL_UNKNOWN_STATISTIC


def test_an_unnamed_consensus_or_a_market_implied_value_is_refused(tmp_path: Path) -> None:
    """The market cannot validate itself, and an anonymous number cannot be re-derived."""
    root = _root(tmp_path)
    with pytest.raises(ExpectationError) as implied:
        validate_expectation(_record(source_kind="market_implied"), _facts(), evidence_root=root)
    assert implied.value.refusal == REFUSAL_MARKET_IMPLIED

    with pytest.raises(ExpectationError) as unnamed:
        validate_expectation(_record(consensus_id="   "), _facts(), evidence_root=root)
    assert unnamed.value.refusal == "consensus_identity_is_not_named"


def test_altered_evidence_bytes_stop_validating(tmp_path: Path) -> None:
    """A payload edited after the record was written does not keep its verdict."""
    root = _root(tmp_path)
    validate_expectation(_record(), _facts(), evidence_root=root)

    (root / EVIDENCE_NAME).write_bytes(EVIDENCE + b"revised\n")
    with pytest.raises(ExpectationError) as caught:
        validate_expectation(_record(), _facts(), evidence_root=root)
    assert caught.value.refusal == REFUSAL_EVIDENCE_BYTES_MISMATCH


def test_evidence_outside_the_declared_root_is_refused(tmp_path: Path) -> None:
    root = _root(tmp_path)
    with pytest.raises(ExpectationError) as caught:
        validate_expectation(
            _record(evidence_path="../elsewhere.tsv"), _facts(), evidence_root=root
        )
    assert caught.value.refusal == REFUSAL_EVIDENCE_UNREADABLE


def test_the_declared_unit_comes_from_the_release_statistic_name() -> None:
    """The unit table reads the release's own declaration rather than a second one."""
    assert declared_unit("payrolls_change_jobs") == "count"
    assert declared_unit("payrolls_change_thousands") == "thousands_of_count"
    assert declared_unit("unemployment_rate_pct") == "percent"
    assert declared_unit("avg_hourly_earnings_mom_pct") == "percent_change"
    assert declared_unit("cpi_u_nsa_index_level") == "index_level"
    assert declared_unit("cpi_u_nsa_yoy_pct") == "percent_change"
    assert declared_unit("some_undeclared_statistic") is None


def _document(tmp_path: Path, records: list[dict]) -> Path:
    path = tmp_path / "expectations.json"
    path.write_text(json.dumps({"records": records}), encoding="utf-8")
    return path


def _entry(statistic: str, *, unit: str, value: str) -> dict:
    return {
        "event_id": "empsit_2025_01",
        "statistic": statistic,
        "unit": unit,
        "reference_period": "2024-12",
        "value": value,
        "published_at": PUBLISHED.isoformat(),
        "source_kind": "licensed_consensus",
        "revision_status": "initial",
        "consensus_id": "example_consensus_panel",
        "verified_by": "read_from_the_archived_consensus_table",
        "source_url": "https://example.invalid/consensus/2025-01-08",
        "evidence_path": EVIDENCE_NAME,
        "evidence_sha256": hashlib.sha256(EVIDENCE).hexdigest(),
    }


def test_a_complete_news_vector_becomes_a_surprise_per_statistic(tmp_path: Path) -> None:
    root = _root(tmp_path)
    source = _document(
        tmp_path,
        [
            _entry("payrolls_change_thousands", unit="thousands_of_count", value="150"),
            _entry("unemployment_rate_pct", unit="percent", value="4.2"),
        ],
    )

    validated = load_expectations(
        source, facts={"empsit_2025_01": _facts()}, statistics=STATISTICS, evidence_root=root
    )
    vector = news_vector(validated, {"empsit_2025_01": _facts()}, statistics=STATISTICS)

    assert vector == {
        "empsit_2025_01": {
            "payrolls_change_thousands": Decimal("106"),
            "unemployment_rate_pct": Decimal("-0.1"),
        }
    }


def test_an_incomplete_news_vector_is_refused(tmp_path: Path) -> None:
    """A partial vector attributes the missing term's variation to what is present."""
    root = _root(tmp_path)
    source = _document(
        tmp_path,
        [_entry("payrolls_change_thousands", unit="thousands_of_count", value="150")],
    )

    with pytest.raises(ExpectationError) as caught:
        load_expectations(
            source, facts={"empsit_2025_01": _facts()}, statistics=STATISTICS, evidence_root=root
        )
    assert caught.value.refusal == REFUSAL_INCOMPLETE_NEWS_VECTOR


def test_an_absent_expectation_source_is_reported_as_absent(tmp_path: Path) -> None:
    """No packaged source is an absence, never a zero-valued surprise."""
    with pytest.raises(ExpectationSourceError) as caught:
        load_expectations(
            tmp_path / "missing.json",
            facts={"empsit_2025_01": _facts()},
            statistics=STATISTICS,
            evidence_root=_root(tmp_path),
        )
    assert caught.value.refusal == REFUSAL_SOURCE_ABSENT


def test_a_record_missing_a_required_field_is_malformed(tmp_path: Path) -> None:
    """A record that does not state its provenance is not defaulted."""
    entry = _entry("payrolls_change_thousands", unit="thousands_of_count", value="150")
    del entry["verified_by"]
    source = _document(tmp_path, [entry])

    with pytest.raises(ExpectationSourceError) as caught:
        load_expectations(
            source,
            facts={"empsit_2025_01": _facts()},
            statistics=STATISTICS,
            evidence_root=_root(tmp_path),
        )
    assert caught.value.refusal == "expectation_source_is_malformed"


def test_a_record_for_an_unsealed_release_is_refused(tmp_path: Path) -> None:
    """An expectation for a release the sealed dataset does not carry is not fitted."""
    source = _document(
        tmp_path,
        [_entry("payrolls_change_thousands", unit="thousands_of_count", value="150")],
    )

    with pytest.raises(ExpectationError) as caught:
        load_expectations(
            source,
            facts={"some_other_release": _facts()},
            statistics=STATISTICS,
            evidence_root=_root(tmp_path),
        )
    assert caught.value.refusal == REFUSAL_UNKNOWN_STATISTIC
