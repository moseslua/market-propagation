import importlib.util
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_propagation.storage import RawStore


def load_report():
    spec = importlib.util.spec_from_file_location(
        "research_evidence_report",
        Path(__file__).parents[1] / "scripts/research_evidence_report.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_fixed_pair_sign_changes_are_not_erased_by_reordering_venues(tmp_path):
    report = load_report()
    for day, binance, bybit in ((1, "0.04", "0.01"), (2, "0.02", "0.05"), (3, "0.06", "0.01")):
        rows = [
            {
                "venue": venue,
                "symbol": "BTCUSDT",
                "build_time": datetime(2026, 9, day, tzinfo=UTC),
                "funding_apr": Decimal(apr),
            }
            for venue, apr in (("binance", binance), ("bybit", bybit))
        ]
        rows.append({**rows[0], "symbol": "BTCUSDC", "funding_apr": Decimal("100")})
        pq.write_table(pa.Table.from_pylist(rows), tmp_path / f"2026090{day}.parquet")
    result = report.signed_pair_funding(tmp_path)
    assert [r["signed_apr_spread"] for r in result["observations"]] == ["-0.03", "0.03", "-0.05"]
    assert result["observed_adjacent_sign_reversals"] == 2
    assert (result["positive"], result["negative"]) == (1, 2)


def test_duplicate_pair_identity_is_refused(tmp_path):
    report = load_report()
    row = {
        "venue": "binance",
        "symbol": "BTCUSDT",
        "build_time": datetime(2026, 9, 1, tzinfo=UTC),
        "funding_apr": Decimal("0.03"),
    }
    pq.write_table(pa.Table.from_pylist([row, row]), tmp_path / "20260901.parquet")
    with pytest.raises(ValueError, match="duplicate venue/instrument"):
        report.signed_pair_funding(tmp_path)


def test_replay_refuses_corrupted_source_bytes(tmp_path):
    report = load_report()
    store = RawStore(tmp_path / "raw")
    provenance = store.put(
        b'{"value":1}', source="test", received_time=datetime(2026, 9, 1, tzinfo=UTC)
    )
    (tmp_path / "sources.json").write_text(
        json.dumps({"source": {"raw_hash": provenance.raw_hash}})
    )
    raw_path = tmp_path / "raw" / "blobs" / provenance.raw_hash[:2] / f"{provenance.raw_hash}.bin"
    raw_path.write_bytes(b'{"value":2}')
    with pytest.raises(ValueError, match="store is corrupt"):
        report.source_rows(tmp_path)


def test_absent_capture_directory_is_not_reported_as_no_rule_changes(tmp_path):
    with pytest.raises(FileNotFoundError, match="capture directory"):
        load_report().definition_changes([tmp_path / "absent"])


def test_unmatched_revision_value_is_refused(monkeypatch):
    report = load_report()
    frame = pd.DataFrame(
        [
            {
                "family": "employment",
                "event_id": "empsit_2025_01",
                "scheduled_at": "2025-01-10T13:30:00+00:00",
                "revisions_json": {"payrolls_change_thousands_revised_October": "43"},
                "raw_hash": "source",
            }
        ]
    )
    monkeypatch.setattr(report, "read_parquet", lambda *_args, **_kwargs: frame)
    with pytest.raises(ValueError, match=r"unmatched.*October"):
        report.revision_diagnostics(Path("unused"))


def test_release_without_reported_revisions_stays_in_the_denominator(monkeypatch):
    report = load_report()
    frame = pd.DataFrame(
        [
            {
                "family": "employment",
                "event_id": "empsit_2025_01",
                "scheduled_at": "2025-01-10T13:30:00+00:00",
                "revisions_json": {},
                "raw_hash": "source",
            }
        ]
    )
    monkeypatch.setattr(report, "read_parquet", lambda *_args, **_kwargs: frame)
    result = report.revision_diagnostics(Path("unused"))
    assert result["input_employment_releases"] == 1
    assert result["release_outcomes"] == [
        {"event_id": "empsit_2025_01", "revision_steps": 0, "state": "no_revision_pairs_reported"}
    ]
