"""Integrity regressions for the quality report, the file hash and sealed coverage.

Every path here reads the archive back instead of trusting a manifest, and each
test pins one way that could silently weaken. A blob shared by several occurrences
stays one blob but must still be counted once per occurrence, and corrupting it
must be reported against every receipt that cites it. Verification is re-done on
each call, so a blob that goes bad after an earlier report cannot pass on a later
one. The coverage numbers must remain the numbers the sealed bytes themselves
carry, and the streamed file hash must equal the byte hash of the same content.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from market_propagation import reporting
from market_propagation.operations import quality_report
from market_propagation.storage import (
    RawStore,
    hash_bytes,
    hash_file,
    read_parquet,
    write_parquet,
)

FIXED_NOW = dt.datetime(2026, 9, 13, 12, 0, tzinfo=dt.UTC)
SHARED_BODY = json.dumps({"orderbook": {"yes": [[50, 10]]}}).encode("utf-8")

EVENT_TIME = dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)


def _blob_path(store: RawStore, raw_hash: str) -> Path:
    return store.root / "blobs" / raw_hash[:2] / f"{raw_hash}.bin"


def _store_sharing_one_body(tmp_path: Path, occurrences: int) -> RawStore:
    """One payload archived as several distinct occurrences under one hash."""
    store = RawStore(tmp_path / "raw")
    for index in range(occurrences):
        store.put(
            SHARED_BODY,
            source="kalshi.orderbook",
            received_time=FIXED_NOW + dt.timedelta(seconds=index),
            record_id=f"snap-{index}",
            metadata={"method": "GET", "http_status": 200},
        )
    return store


def _panel_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, (contract_id, horizon, valid, reason) in enumerate(
        (
            ("A", 60, True, None),
            ("B", 60, True, None),
            ("A", 300, True, None),
            ("B", 300, False, "quote_gap"),
            ("C", 300, False, None),
        )
    ):
        rows.append(
            {
                "event_id": "cpi_2025_01",
                "cluster_id": "cpi_2025_01",
                "family": "cpi",
                "contract_id": contract_id,
                "venue": "kalshi",
                "cohort": "cpi",
                "event_time": EVENT_TIME,
                "horizon_seconds": horizon,
                "baseline_time": EVENT_TIME,
                "endpoint_time": EVENT_TIME + dt.timedelta(seconds=horizon),
                "valid": valid,
                "exclusion_reason": reason,
                "replay_order": "usable" if index % 2 else "source",
            }
        )
    return rows


def test_shared_body_receipts_stay_distinct_and_fully_accounted(tmp_path: Path) -> None:
    """Content addressing dedupes bytes; it must not dedupe the occurrences."""
    store = _store_sharing_one_body(tmp_path, occurrences=2)
    assert store.stored_hashes() == [hash_bytes(SHARED_BODY)]
    assert len(store.receipts()) == 2

    report = quality_report(tmp_path / "raw", tmp_path / "quality.json")

    receipts = report["receipts"]
    assert receipts["count"] == 2
    assert receipts["verified_payloads"] == 2
    assert receipts["failed_verifications"] == 0
    assert receipts["by_source"] == {"kalshi.orderbook": 2}
    # Both occurrences are counted, each at the shared body's own size.
    assert receipts["byte_total"] == 2 * len(SHARED_BODY)
    assert report["blobs"]["count"] == 1
    assert report["blobs"]["referenced_by_a_receipt"] == 1
    assert report["blobs"]["orphan_count"] == 0


def test_corruption_is_reported_for_every_referencing_occurrence(tmp_path: Path) -> None:
    """One altered blob fails every receipt that cites it, not just the first."""
    store = _store_sharing_one_body(tmp_path, occurrences=3)
    (raw_hash,) = store.stored_hashes()
    _blob_path(store, raw_hash).write_bytes(b"{}")

    report = quality_report(tmp_path / "raw", tmp_path / "quality.json")

    receipts = report["receipts"]
    assert receipts["verified_payloads"] == 0
    assert receipts["failed_verifications"] == 3
    assert receipts["byte_total"] == 0
    failures = receipts["verification_failures"]
    assert {failure["reason"] for failure in failures} == {"payload_hash_mismatch"}
    assert {failure["raw_hash"] for failure in failures} == {raw_hash}
    assert len({failure["receipt_id"] for failure in failures}) == 3
    assert all(failure["detail"] for failure in failures)


def test_a_missing_blob_is_reported_for_every_referencing_occurrence(tmp_path: Path) -> None:
    store = _store_sharing_one_body(tmp_path, occurrences=2)
    (raw_hash,) = store.stored_hashes()
    _blob_path(store, raw_hash).unlink()

    report = quality_report(tmp_path / "raw", tmp_path / "quality.json")

    receipts = report["receipts"]
    assert receipts["verified_payloads"] == 0
    assert receipts["failed_verifications"] == 2
    assert {failure["reason"] for failure in receipts["verification_failures"]} == {
        "payload_missing"
    }


def test_a_later_report_catches_bytes_corrupted_since_an_earlier_one(tmp_path: Path) -> None:
    """Verification is per call: a blob corrupting after one report fails the next."""
    store = _store_sharing_one_body(tmp_path, occurrences=1)
    (raw_hash,) = store.stored_hashes()

    first = quality_report(tmp_path / "raw", tmp_path / "quality-first.json")
    assert first["receipts"]["verified_payloads"] == 1
    assert first["receipts"]["failed_verifications"] == 0
    assert first["receipts"]["byte_total"] == len(SHARED_BODY)

    _blob_path(store, raw_hash).write_bytes(SHARED_BODY + b" ")

    second = quality_report(tmp_path / "raw", tmp_path / "quality-second.json")
    assert second["receipts"]["verified_payloads"] == 0
    assert second["receipts"]["failed_verifications"] == 1
    assert second["receipts"]["verification_failures"][0]["reason"] == "payload_hash_mismatch"


def test_panel_coverage_metrics_equal_the_sealed_artifact(tmp_path: Path) -> None:
    """The published coverage is what the sealed Parquet holds, restated not recomputed."""
    path = tmp_path / "event_panel.parquet"
    reference = write_parquet(_panel_rows(), path, table="event_panel", coverage_epoch="test")

    coverage = reporting._panel_coverage("source", path)
    frame = read_parquet(path)

    assert coverage["content_hash"] == reference.content_hash
    assert coverage["row_count"] == reference.row_count == len(frame)
    assert coverage["totals"] == {
        "rows": len(frame),
        "events": int(frame["event_id"].nunique()),
        "contracts": int(frame["contract_id"].nunique()),
        "families": int(frame["family"].nunique()),
        "replay_orders": int(frame["replay_order"].nunique()),
        "valid_rows": int(frame["valid"].sum()),
    }

    expected_horizons = [
        {
            "horizon_seconds": int(horizon),
            "rows": len(subset),
            "valid_rows": int(subset["valid"].sum()),
            "masked_rows": int((~subset["valid"]).sum()),
        }
        for horizon in sorted(frame["horizon_seconds"].unique())
        for subset in (frame[frame["horizon_seconds"] == horizon],)
    ]
    assert coverage["by_horizon"] == expected_horizons

    masked = frame[~frame["valid"]]
    named = (
        masked["exclusion_reason"].where(masked["exclusion_reason"].notna(), "(none)").astype(str)
    )
    counts = named.value_counts()
    assert coverage["masked_by_reason"] == [
        {"exclusion_reason": reason, "rows": int(rows)}
        for reason, rows in sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
    ]
    assert coverage["families"] == sorted(str(value) for value in frame["family"].unique())
    assert coverage["cohorts"] == sorted(str(value) for value in frame["cohort"].unique())
    assert coverage["replay_order"] == sorted(
        str(value) for value in frame["replay_order"].unique()
    )


def test_streamed_file_hash_matches_the_byte_hash_of_its_content(tmp_path: Path) -> None:
    """The streamed digest is the same digest the byte helper produces."""
    payload = b"market-propagation\n" * 100_000
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)

    assert hash_file(path) == hash_bytes(payload)
