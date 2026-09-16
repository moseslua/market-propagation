"""Acceptance tests for the command-line surface.

These drive ``cli.main`` through the real parser and the real handlers, and they
assert on what a caller observes: the exit code, the JSON on stdout, the file the
command wrote, and the diagnostic note on stderr. Nothing here pins help text or a
handler's internal name.

Three contracts are defended because each is otherwise invisible:

* Exit ``2`` means a blocked or absent result, never a bad argument. An argument
  error exits ``1``, so a scheduler can act on ``2`` alone.
* stdout carries exactly one JSON document. A note about a blocked result goes to
  stderr, so a consumer can pipe stdout into a JSON parser.
* ``capture`` refuses to run without a contract id. Nothing guesses a ticker.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from market_propagation.cli import main
from market_propagation.ingest import external_history
from market_propagation.registry import ExperimentRegistry
from market_propagation.storage import RawStore, write_parquet

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
STUDY_SPEC = REPO_ROOT / "configs" / "study_v1.yaml"
EXTERNAL_CONFIG = REPO_ROOT / "configs" / "external_history_v1.yaml"


def run(argv: list[str]) -> int:
    """Run one command, turning argparse's SystemExit into its code."""
    try:
        return main(argv)
    except SystemExit as exc:
        assert isinstance(exc.code, int), f"non-integer exit from {argv}: {exc.code!r}"
        return exc.code


def stdout_json(capsys: pytest.CaptureFixture[str]) -> Any:
    """Parse the single JSON document a command wrote to stdout."""
    captured = capsys.readouterr()
    assert captured.out, "the command wrote no JSON to stdout"
    return json.loads(captured.out)


def run_record() -> dict[str, object]:
    return {
        "run_id": "run-cli",
        "spec_hash": "spec-a",
        "data_hash": "data-1",
        "source_hash": "source-1",
        "environment_hash": "env-1",
        "event_ids": ["e1", "e2"],
        "seed": 20260913,
        "metrics": {"mae": 0.012, "n_events": 2},
        "synthetic": True,
        "created_at": "2026-09-13T12:00:00+00:00",
    }


def test_registry_review_prints_durable_state(tmp_path: pathlib.Path, capsys):
    registry_path = tmp_path / "registry.sqlite"
    with ExperimentRegistry(registry_path) as registry:
        registry.record_run(run_record())
        registry.reserve_locked_test("spec-a", ["e1", "e2"], dataset_hash="data-1")

    code = run(["registry-review", str(registry_path)])
    payload = stdout_json(capsys)

    assert code == 0
    assert payload["registry_path"] == str(registry_path)
    assert [item["run_id"] for item in payload["runs"]] == ["run-cli"]
    assert [item["spec_hash"] for item in payload["reservations"]] == ["spec-a"]
    assert sorted(claim["event_id"] for claim in payload["event_claims"]) == ["e1", "e2"]


def test_registry_review_refuses_to_create_a_missing_registry(tmp_path: pathlib.Path, capsys):
    missing = tmp_path / "absent.sqlite"

    code = run(["registry-review", str(missing)])

    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert str(missing) in captured.err
    assert not missing.exists(), "a review must not create the registry it inspects"


def test_registry_review_reads_an_empty_registry_as_empty(tmp_path: pathlib.Path, capsys):
    code = run(["registry-review", ":memory:"])
    payload = stdout_json(capsys)

    assert code == 0
    assert payload["runs"] == []
    assert payload["reservations"] == []
    assert payload["event_claims"] == []


def test_quality_reports_a_verified_store(tmp_path: pathlib.Path, capsys):
    raw = tmp_path / "raw"
    store = RawStore(raw)
    store.put(
        b'{"book": "archived"}',
        source="test.source",
        received_time=dt.datetime(2026, 9, 13, 12, 30, tzinfo=dt.UTC),
        record_id="rec-1",
    )
    output = tmp_path / "quality.json"

    code = run(["quality", str(raw), "--output", str(output)])

    assert code == 0
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["operation"] == "quality_report"
    assert written["receipts"]["count"] == 1
    assert written["receipts"]["verified_payloads"] == 1
    assert written["receipts"]["failed_verifications"] == 0


def test_quality_exits_two_when_a_payload_no_longer_matches_its_hash(
    tmp_path: pathlib.Path, capsys
):
    raw = tmp_path / "raw"
    store = RawStore(raw)
    provenance = store.put(
        b'{"book": "archived"}',
        source="test.source",
        received_time=dt.datetime(2026, 9, 13, 12, 30, tzinfo=dt.UTC),
        record_id="rec-1",
    )
    blob = raw / "blobs" / provenance.raw_hash[:2] / f"{provenance.raw_hash}.bin"
    blob.write_bytes(b'{"book": "tampered"}')

    code = run(["quality", str(raw), "--output", str(tmp_path / "quality.json")])
    captured = capsys.readouterr()

    assert code == 2
    assert json.loads(captured.out)["receipts"]["failed_verifications"] == 1
    assert "failed verification" in captured.err


def test_quality_exits_two_for_a_store_that_holds_nothing(tmp_path: pathlib.Path, capsys):
    raw = tmp_path / "empty-raw"
    raw.mkdir()

    code = run(["quality", str(raw), "--output", str(tmp_path / "quality.json")])
    captured = capsys.readouterr()

    assert code == 2
    assert json.loads(captured.out)["receipts"]["count"] == 0
    assert "no receipt" in captured.err


def write_coverage(
    directory: pathlib.Path, *, events: list[dict[str, Any]], complete: bool
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "coverage.json").write_text(
        json.dumps(
            {
                "status": "complete" if complete else "blocked",
                "complete": complete,
                "cohort_definition_hash": "hash-of-cohort",
                "events": events,
            }
        ),
        encoding="utf-8",
    )


def write_cited_payload(directory: pathlib.Path, payload: bytes) -> str:
    """Archive one payload the audit's event cites, returning its content hash."""
    store = RawStore(directory / "raw")
    provenance = store.put(
        payload,
        source="test.source",
        received_time=dt.datetime(2026, 9, 13, 12, 30, tzinfo=dt.UTC),
        record_id="rec-1",
    )
    return provenance.raw_hash


def test_event_card_assembles_a_card_from_an_audit_directory(tmp_path: pathlib.Path, capsys):
    """A real card is written from a partial audit, and that exits 0.

    The card's existence and the audit's empirical standing are separate facts.
    This audit is incomplete and the event carries an unsatisfied gate, so the
    card must report both while still being a produced artifact.
    """
    audit = tmp_path / "audit"
    raw_hash = write_cited_payload(audit, b'{"release": "archived"}')
    write_coverage(
        audit,
        complete=False,
        events=[
            {
                "event_id": "cpi-2025-01",
                "family": "cpi",
                "status": "audited",
                "candidates": [],
                "raw_hashes": [raw_hash],
                "coverage_gates": [
                    {
                        "gate": "listing_pagination_complete",
                        "satisfied": False,
                        "detail": "the listing walk hit its page cap",
                        "blocks": ["the candidate universe is complete"],
                    }
                ],
                "unsatisfied_gates": ["listing_pagination_complete"],
            }
        ],
    )
    output = tmp_path / "card.json"

    code = run(["event-card", str(audit), "--output", str(output), "--event-id", "cpi-2025-01"])
    captured = capsys.readouterr()

    assert code == 0, captured.err
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["status"] == "created"
    assert written["operation"] == "event_card"
    assert written["event_id"] == "cpi-2025-01"
    assert written["event_status"] == "audited"
    # The artifact exists and the gate it fails to satisfy survives in the JSON.
    assert written["unsatisfied_gates"] == ["listing_pagination_complete"]
    assert written["audit_complete"] is False
    blocked = written["claims"]["blocked_by_unsatisfied_gates"]
    assert blocked["listing_pagination_complete"] == ["the candidate universe is complete"]
    # ...and the note tells the caller the file is real but empirically unusable.
    assert "written" in captured.err
    assert "listing_pagination_complete" in captured.err
    assert "blocked" in captured.err


def test_event_card_accepts_a_verified_card_from_a_partial_audit_as_a_real_artifact(
    tmp_path: pathlib.Path, capsys
):
    """Every cited hash re-reads, so the only failing thing here is an empirical gate."""
    audit = tmp_path / "audit"
    raw_hash = write_cited_payload(audit, b'{"release": "archived"}')
    write_coverage(
        audit,
        complete=False,
        events=[
            {
                "event_id": "cpi-2025-01",
                "family": "cpi",
                "status": "audited",
                "candidates": [],
                "raw_hashes": [raw_hash],
                "coverage_gates": [],
                "unsatisfied_gates": ["candle_quotes_two_sided"],
            }
        ],
    )

    code = run(["event-card", str(audit), "--output", str(tmp_path / "card.json")])
    captured = capsys.readouterr()
    written = json.loads(captured.out)

    assert code == 0, captured.err
    assert written["status"] == "created"
    assert written["evidence_verification"]["verified_hash_count"] == 1
    assert written["evidence_verification"]["failed_hash_count"] == 0


def test_event_card_exits_two_for_an_event_the_audit_holds_no_record_of(
    tmp_path: pathlib.Path, capsys
):
    audit = tmp_path / "audit"
    write_coverage(
        audit,
        complete=True,
        events=[{"event_id": "cpi-2025-01", "family": "cpi", "status": "audited"}],
    )
    output = tmp_path / "card.json"

    code = run(
        ["event-card", str(audit), "--output", str(output), "--event-id", "payrolls-2025-01"]
    )
    captured = capsys.readouterr()

    assert code == 2
    payload = json.loads(captured.out)
    assert payload["status"] == "event_not_in_audit"
    assert payload["audited_event_ids"] == ["cpi-2025-01"]
    assert "payrolls-2025-01" in captured.err


def test_event_card_exits_two_when_the_audit_holds_no_event_at_all(tmp_path: pathlib.Path, capsys):
    audit = tmp_path / "audit"
    write_coverage(audit, complete=False, events=[])
    output = tmp_path / "card.json"

    code = run(["event-card", str(audit), "--output", str(output)])
    captured = capsys.readouterr()

    assert code == 2
    payload = json.loads(captured.out)
    assert payload["status"] == "event_not_in_audit"
    assert payload["audit_complete"] is False
    assert payload["audited_event_ids"] == []
    assert "no card describes event" in captured.err


def test_event_card_exits_two_when_a_cited_payload_no_longer_matches_its_hash(
    tmp_path: pathlib.Path, capsys
):
    """A written card whose evidence does not verify is not a usable result."""
    audit = tmp_path / "audit"
    raw_hash = write_cited_payload(audit, b'{"release": "archived"}')
    write_coverage(
        audit,
        complete=True,
        events=[
            {
                "event_id": "cpi-2025-01",
                "family": "cpi",
                "status": "audited",
                "candidates": [],
                "raw_hashes": [raw_hash],
                "coverage_gates": [],
                "unsatisfied_gates": [],
            }
        ],
    )
    blob = audit / "raw" / "blobs" / raw_hash[:2] / f"{raw_hash}.bin"
    blob.write_bytes(b'{"release": "tampered"}')
    output = tmp_path / "card.json"

    code = run(["event-card", str(audit), "--output", str(output), "--event-id", "cpi-2025-01"])
    captured = capsys.readouterr()

    assert code == 2
    written = json.loads(captured.out)
    assert written["status"] == "created"
    assert written["evidence_verification"]["failed_hash_count"] == 1
    assert written["evidence_verification"]["failures"][0]["reason"] == "payload_hash_mismatch"
    assert output.exists(), "the card is still written; it is the evidence that failed"
    assert raw_hash in captured.err
    assert "content hash" in captured.err


def test_event_card_exits_two_when_a_cited_payload_is_missing(tmp_path: pathlib.Path, capsys):
    audit = tmp_path / "audit"
    raw_hash = write_cited_payload(audit, b'{"release": "archived"}')
    write_coverage(
        audit,
        complete=True,
        events=[
            {
                "event_id": "cpi-2025-01",
                "family": "cpi",
                "status": "audited",
                "candidates": [],
                "raw_hashes": [raw_hash],
                "coverage_gates": [],
                "unsatisfied_gates": [],
            }
        ],
    )
    (audit / "raw" / "blobs" / raw_hash[:2] / f"{raw_hash}.bin").unlink()

    code = run(["event-card", str(audit), "--output", str(tmp_path / "card.json")])
    captured = capsys.readouterr()

    assert code == 2
    written = json.loads(captured.out)
    assert written["evidence_verification"]["failures"][0]["reason"] == "payload_missing"


def test_event_card_reports_a_missing_audit_directory(tmp_path: pathlib.Path, capsys):
    code = run(["event-card", str(tmp_path / "absent"), "--output", str(tmp_path / "card.json")])
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert "FileNotFoundError" in captured.err


def test_audit_forwards_the_cohort_path_it_was_given(tmp_path: pathlib.Path, capsys):
    missing = tmp_path / "no-such-cohort.yaml"

    code = run(["audit", "--output", str(tmp_path / "audit"), "--cohort", str(missing)])
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert str(missing) in captured.err, "the requested cohort path must reach the operation"


def test_capture_rejects_a_duration_the_operation_cannot_honour(tmp_path: pathlib.Path, capsys):
    code = run(
        [
            "capture",
            "--output",
            str(tmp_path / "capture"),
            "--contract",
            "SOME-PUBLIC-MARKET",
            "--duration",
            "99999",
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert "duration_seconds" in captured.err


def test_audit_refuses_an_archive_raw_root_without_a_release_dataset(
    tmp_path: pathlib.Path, capsys
):
    """The flag locates a named dataset's raw store, so alone it must not be ignored.

    Accepting it would leave the run on the live network path while the caller
    believes it is reading an offline archive.
    """
    code = run(
        [
            "audit",
            "--output",
            str(tmp_path / "audit"),
            "--archive-raw-root",
            str(tmp_path / "raw"),
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert "--release-dataset" in captured.err
    assert not (tmp_path / "audit").exists()


def test_reproduce_refuses_a_specification_that_does_not_exist(tmp_path: pathlib.Path, capsys):
    missing = tmp_path / "no-such-spec.yaml"

    code = run(["reproduce", "--output", str(tmp_path / "reproduce"), "--spec", str(missing)])
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert str(missing) in captured.err


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["not-a-command"],
        ["capture", "--output", "out"],
        ["capture", "--contract", "SOME-MARKET"],
        ["capture", "--output", "out", "--contract", "SOME-MARKET", "--venue", "nasdaq"],
        ["audit"],
        ["reproduce"],
        ["quality"],
        ["quality", "raw"],
        ["event-card", "audit"],
        ["registry-review"],
        ["registry-review", "a.sqlite", "b.sqlite"],
    ],
)
def test_bad_arguments_exit_one_and_never_two(argv: list[str], capsys):
    code = run(argv)
    captured = capsys.readouterr()

    assert code == 1, f"{argv} must exit 1 for an argument error"
    assert captured.out == "", "an argument error must not print a result document"
    assert captured.err, "an argument error must explain itself"


def test_every_documented_command_parses_and_dispatches(capsys):
    """Each documented subcommand exists and reaches its handler.

    A misspelled command name is rejected by the parser with exit 1, so a command
    that resolves to its own parser exits 0 here without pinning any help text.
    """
    for name in ("reproduce", "audit", "capture", "quality", "registry-review", "event-card"):
        assert run([name, "--help"]) == 0, f"{name} does not parse"

    assert run(["--help"]) == 0


def test_reproduce_runs_the_real_offline_path(tmp_path: pathlib.Path, capsys):
    """One meaningful end-to-end run of the packaged synthetic reproduction.

    This asserts the reproduction's own completeness and its artifacts rather
    than a summary string.
    """
    output = tmp_path / "reproduce"

    code = run(
        [
            "reproduce",
            "--output",
            str(output),
            "--spec",
            str(STUDY_SPEC),
            "--events",
            "10",
            "--repetitions",
            "20",
            "--bootstrap",
            "25",
        ]
    )
    record = stdout_json(capsys)

    assert code == 0, f"blocked stages: {record.get('blocked_stages')}"
    assert record["complete"] is True
    assert record["status"] == "ok"
    assert record["files"], "a complete reproduction names the artifacts it wrote"
    for name in ("metrics.json", "manifest.json", "source_panel.parquet", "paper.md"):
        assert (output / name).exists(), f"{name} was not written"
    assert (output / "figures").is_dir()


# --- the external-history pipeline commands ---------------------------------
#
# These drive the real parser, the real handlers and the real library modules. The
# archive they read is a synthetic shard written into ``tmp_path``, matching the
# archive's own column names and types, so nothing here depends on the 58 GB of
# real archives being present or unchanged.

EVENT_ID = "cpi-2025-01"
EVENT_TIME = dt.datetime(2025, 1, 15, 13, 30, tzinfo=dt.UTC)
KALSHI_TICKER = "KXCPI-25JAN"
WINDOW = ("2025-01-15T00:00:00Z", "2025-01-16T00:00:00Z")


def write_kalshi_archive(root: pathlib.Path, ticker: str = KALSHI_TICKER) -> pathlib.Path:
    """Write one synthetic Kalshi trade shard in the archive's own schema.

    The shard carries a print just before the release and one just after it, which is
    what a transaction response needs, plus a zero-cent print so the flags a real
    archive produces are exercised rather than assumed away.
    """
    directory = root / "kalshi-trades"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "trades-0000.parquet"
    pq.write_table(
        pa.table(
            {
                "trade_id": pa.array(["t1", "t2", "t3"], pa.string()),
                "ticker": pa.array([ticker] * 3, pa.string()),
                "count": pa.array([5, 7, 2], pa.int64()),
                "yes_price": pa.array([40, 55, 0], pa.int64()),
                "no_price": pa.array([60, 45, 100], pa.int64()),
                "taker_side": pa.array(["yes", "no", "yes"], pa.string()),
                "created_time": pa.array(
                    [
                        dt.datetime(2025, 1, 15, 13, 29, tzinfo=dt.UTC),
                        dt.datetime(2025, 1, 15, 13, 31, tzinfo=dt.UTC),
                        dt.datetime(2025, 1, 15, 13, 32, tzinfo=dt.UTC),
                    ],
                    pa.timestamp("us", tz="UTC"),
                ),
            }
        ),
        path,
    )
    return path


#: Every configured layer, with the path one shard of it would occupy and the time
#: column the configuration declares. ``inventory-external`` reads the whole
#: configuration and takes no ``--layer``, so a complete inventory needs one readable
#: shard per entry here, and a layer is absent exactly when its own file is not written.
ARCHIVE_SHARDS: tuple[tuple[str, str, str | None], ...] = (
    ("kalshi_trades", "kalshi-trades/trades-0000.parquet", "created_time"),
    ("kalshi_markets", "kalshi-trades/markets-0000.parquet", "created_time"),
    ("kalshi_own_markets", "kalshi-own/markets/markets-0000.parquet", "created_time"),
    ("kalshi_own_trades", "kalshi-own/trades/trades-0000.parquet", "created_time"),
    ("polymarket_orderfilled", "polymarket-v1/OrderFilled/fills-0000.parquet", "block_timestamp"),
    (
        "polymarket_daily_aligned",
        "polymarket-v1/daily_aligned/2025_01_15.parquet",
        "block_timestamp",
    ),
    (
        "polymarket_daily_aligned_multi",
        "polymarket-v1/daily_aligned_multi/2025_01_15_multi.parquet",
        "block_timestamp",
    ),
    ("polymarket_ctf", "polymarket-v1/CTF/merges.parquet", None),
    ("forecast_snapshots", "forecast-snapshots-2025/snapshot_dataset.parquet", "snapshot_time"),
)


def write_archive(root: pathlib.Path, *, without: str | None = None) -> pathlib.Path:
    """Write one readable shard for every configured layer except ``without``.

    The placeholder shards are real Parquet with the layer's declared time column, so the
    inventory measures a real footer and a real timestamp bound for each one rather than
    reporting a layer present on the strength of a file name.
    """
    for name, relative, time_column in ARCHIVE_SHARDS:
        if name == without:
            continue
        if name == "kalshi_trades":
            write_kalshi_archive(root)
            continue
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        columns: dict[str, Any] = {"layer": pa.array([name], pa.string())}
        if time_column is not None:
            columns[time_column] = pa.array([1], pa.int64())
        pq.write_table(pa.table(columns), path)
    return root


def write_external_config(
    tmp_path: pathlib.Path,
    *,
    release_dataset: pathlib.Path,
    root: pathlib.Path | None = None,
    rule_evidence: pathlib.Path | None = None,
    audit_coverage: pathlib.Path | None = None,
    satisfied_gates: bool = False,
    window: tuple[str, str] = WINDOW,
) -> pathlib.Path:
    """A pipeline configuration derived from the study's own, pointing at synthetic inputs.

    Every path is absolute, so a test reads the fixture it wrote rather than whatever a
    relative path resolves to from the working directory.
    """
    text = EXTERNAL_CONFIG.read_text(encoding="utf-8")
    text = text.replace("root: data/external", f"root: {root or tmp_path}")
    text = text.replace(
        "release_dataset: data/public/bls-normalized/releases.parquet",
        f"release_dataset: {release_dataset}",
    )
    if rule_evidence is not None:
        text = text.replace(
            "rule_evidence_source: reports/contract_rule_registry.json",
            f"rule_evidence_source: {rule_evidence}",
        )
    if audit_coverage is not None:
        text = text.replace(
            "audit_coverage: data/public/final-audit/coverage.json",
            f"audit_coverage: {audit_coverage}",
        )
    text = text.replace('window_start: "2025-01-01T00:00:00Z"', f'window_start: "{window[0]}"')
    text = text.replace('window_end: "2025-05-31T23:59:59Z"', f'window_end: "{window[1]}"')
    if satisfied_gates:
        # The gate a coverage grid consults. Declaring them satisfied is what lets a
        # fixture reach the passing branch at all; the real run reads them from audit
        # evidence and does not pass.
        text += (
            "\nstudy_eligibility:\n"
            "  rule_vintage_gate:\n"
            "    satisfied: true\n"
            "    reason: synthetic fixture declares the gate satisfied\n"
            "  source_semantics_gate:\n"
            "    satisfied: true\n"
            "    reason: synthetic fixture declares the gate satisfied\n"
        )
    config = tmp_path / "external_history_v1.yaml"
    config.write_text(text, encoding="utf-8")
    return config


def write_release_dataset(
    tmp_path: pathlib.Path,
    *,
    event_id: str = EVENT_ID,
    family: str = "cpi",
    scheduled_at: dt.datetime = EVENT_TIME,
) -> pathlib.Path:
    """A sealed archived-release dataset holding one development release."""
    path = tmp_path / "releases.parquet"
    write_parquet(
        [
            {
                "event_id": event_id,
                "family": family,
                "scheduled_at": scheduled_at,
                "reference_period": "2024-12",
                "values_json": {},
                "revisions_json": {},
                "raw_hash": "a" * 64,
                "record_id": "release-1",
                "source": "synthetic.test",
            }
        ],
        path,
        table="releases",
        coverage_epoch="test_fixture",
    )
    return path


def write_panel_rule_evidence(tmp_path: pathlib.Path, *, event_id: str = EVENT_ID) -> pathlib.Path:
    """Rule evidence the panel's own loader reads, keyed by the event it covers."""
    path = tmp_path / "panel_rule_evidence.json"
    path.write_text(
        json.dumps(
            {"events": [{"event_id": event_id, "rule_version": "rule-v1"}]},
        ),
        encoding="utf-8",
    )
    return path


def write_coverage_rule_registry(tmp_path: pathlib.Path, *, ticker: str) -> pathlib.Path:
    """A complete rule binding for one contract, as the coverage join reads it.

    The field names are the study's canonical ones. A fixture that used a near-miss
    spelling would pass only while an alias table happened to accept it, which is
    exactly how coverage's required set and the bounded audit's drifted apart once.
    """
    path = tmp_path / "coverage_rule_evidence.json"
    path.write_text(
        json.dumps(
            {
                ticker: {
                    "contract_id": ticker,
                    "rule_hash": "b" * 64,
                    "source_url": "https://example.invalid/rules/kxfed",
                    "verified_by": "analyst:retrospective-rule-read",
                    "in_force_from": "2025-01-01T00:00:00+00:00",
                    "in_force_to": "2025-02-01T00:00:00+00:00",
                    "observed_at": "2025-01-01T00:00:00+00:00",
                    "settlement_semantics": "pays 1 if the released statistic falls in the stated range",
                }
            }
        ),
        encoding="utf-8",
    )
    return path


def write_candidate_artifact(
    tmp_path: pathlib.Path, *, event_id: str, family: str, ticker: str
) -> pathlib.Path:
    """The pre-event candidate artifact the coverage grid takes its universe from."""
    path = tmp_path / "coverage_candidates.json"
    path.write_text(
        json.dumps(
            {
                "events": [
                    {"event_id": event_id, "family": family, "candidates": [{"ticker": ticker}]}
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def write_sealed_trades(tmp_path: pathlib.Path, *, ticker: str = KALSHI_TICKER) -> pathlib.Path:
    """Seal the two response prints as a ``historical_trades`` dataset."""
    rows = [
        {
            "trade_id": "t1",
            "ticker": ticker,
            "count": 5,
            "yes_price": 40,
            "no_price": 60,
            "taker_side": "yes",
            "created_time": "2025-01-15T13:29:00+00:00",
        },
        {
            "trade_id": "t2",
            "ticker": ticker,
            "count": 7,
            "yes_price": 55,
            "no_price": 45,
            "taker_side": "yes",
            "created_time": "2025-01-15T13:31:00+00:00",
        },
    ]
    trades = tuple(
        external_history.kalshi_trade_from_row(
            row, shard_hash="c" * 64, shard_relative_path="trades-0000.parquet", row_position=index
        )
        for index, row in enumerate(rows)
    )
    path = tmp_path / "historical_trades.parquet"
    external_history.write_trades(trades, path)
    return path


def panel_ready_inputs(
    tmp_path: pathlib.Path, *, ticker: str = KALSHI_TICKER
) -> tuple[pathlib.Path, pathlib.Path]:
    """A configuration and sealed trades that build a panel with a valid row.

    A ruled release with a print on each side of it is the smallest case that yields a
    governed response, so it is what the complete-exit case has to run on.
    """
    write_kalshi_archive(tmp_path, ticker=ticker)
    releases = write_release_dataset(tmp_path)
    evidence = write_panel_rule_evidence(tmp_path)
    candidates = write_candidate_artifact(tmp_path, event_id=EVENT_ID, family="cpi", ticker=ticker)
    config = write_external_config(
        tmp_path,
        release_dataset=releases,
        root=tmp_path,
        rule_evidence=evidence,
        audit_coverage=candidates,
    )
    return config, write_sealed_trades(tmp_path, ticker=ticker)


def test_every_external_command_is_listed_and_parses(capsys):
    """Each new subcommand exists, is listed at the top level, and dispatches."""
    names = (
        "inventory-external",
        "normalize-external",
        "coverage-external",
        "build-trade-panel",
        "report-external",
        "study-external",
    )
    listing = run(["--help"])
    top_level = capsys.readouterr().out
    assert listing == 0
    for name in names:
        assert name in top_level, f"{name} is not listed by --help"

    for name in names:
        assert run([name, "--help"]) == 0, f"{name} does not parse"
    capsys.readouterr()


def test_study_external_reports_a_blocked_propagation_rung(tmp_path: pathlib.Path, capsys):
    """The propagation rung is reported blocked rather than fitted as a substitute.

    A panel with no neighbour column and no verified surprise cannot carry the
    claim however the model is specified, so the command exits 2 and the artifact
    names the missing inputs instead of producing a number.
    """
    config, trades = panel_ready_inputs(tmp_path)
    panel_dir = tmp_path / "panel"
    run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(panel_dir),
        ]
    )
    capsys.readouterr()

    code = run(
        [
            "study-external",
            "--panel",
            str(panel_dir / "trade_panel.parquet"),
            "--output",
            str(tmp_path / "study"),
            # The default is the shared cross-run store; a test names its own so a
            # check does not write a run into the checkout's ledger.
            "--registry",
            str(tmp_path / "registry.sqlite3"),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == 2
    assert payload["claim_not_made"] == "conditional_predictive_propagation"
    assert payload["claim"] == "absorption_response_in_source_time_transaction_data"
    assert payload["propagation"]["supported"] is False
    assert payload["propagation"]["missing_neighbor_columns"] == ["neighbor_lag"]
    assert payload["propagation"]["missing_news_columns"] == ["surprise"]
    assert payload["registry"]["recorded"] is True
    assert payload["registry"]["shared"] is False


def test_inventory_external_reports_a_complete_archive_and_exits_zero(
    tmp_path: pathlib.Path, capsys
):
    write_archive(tmp_path)
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))
    output = tmp_path / "inventory"

    code = run(
        [
            "inventory-external",
            "--root",
            str(tmp_path),
            "--output",
            str(output),
            "--config",
            str(config),
            "--no-hashes",
        ]
    )
    result = stdout_json(capsys)

    assert code == 0, capsys.readouterr().err
    assert (output / "inventory.json").exists()
    assert result["absent_evidence"] == []
    assert [layer["name"] for layer in result["layers"]] == [name for name, _, _ in ARCHIVE_SHARDS]
    assert all(layer["status"] == "present" for layer in result["layers"])
    kalshi = next(shard for shard in result["shards"] if shard["layer"] == "kalshi_trades")
    assert kalshi["status"] == "read"
    assert kalshi["row_count"] == 3


def test_inventory_external_exits_two_when_a_named_layer_is_absent(tmp_path: pathlib.Path, capsys):
    """An archive that does not hold a configured layer is blocked, not an error."""
    write_archive(tmp_path, without="kalshi_trades")
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))

    code = run(
        [
            "inventory-external",
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "inventory"),
            "--config",
            str(config),
            "--no-hashes",
        ]
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert code == 2
    assert result["absent_evidence"] == ["layer_missing"]
    assert result["layers_missing"] == ["kalshi_trades"]
    assert (tmp_path / "inventory" / "inventory.json").exists(), "a blocked result is still written"
    assert "absent-evidence" in captured.err


def test_inventory_external_keeps_hashes_unless_asked_to_skip_them(tmp_path: pathlib.Path, capsys):
    """``--no-hashes`` is the only thing that removes the digest scope.

    An omitted flag leaves the library's own default in place, so the inventory still
    carries a digest per shard and can detect a changed byte.
    """
    write_archive(tmp_path)
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))

    hashing = run(
        [
            "inventory-external",
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "hashed"),
            "--config",
            str(config),
        ]
    )
    hashed = stdout_json(capsys)
    assert hashing == 0
    assert hashed["hash_scope"] != "none"
    assert all(shard["sha256"] for shard in hashed["shards"])

    skipping = run(
        [
            "inventory-external",
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "unhashed"),
            "--config",
            str(config),
            "--no-hashes",
        ]
    )
    unhashed = stdout_json(capsys)
    assert skipping == 0
    assert unhashed["hash_scope"] == "none"
    assert all(shard["sha256"] is None for shard in unhashed["shards"])
    assert unhashed["identity"] != hashed["identity"], "dropping the digests changes the identity"


def test_inventory_external_reads_the_default_configuration(
    tmp_path: pathlib.Path, capsys, monkeypatch
):
    """An omitted ``--config`` reaches the pipeline's own configuration file.

    The default declares nine layers, all of them absent from this synthetic root, so
    the artifact naming exactly those nine is how the default is observed rather than
    assumed.
    """
    monkeypatch.chdir(REPO_ROOT)

    code = run(
        [
            "inventory-external",
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "inventory"),
            "--no-hashes",
        ]
    )
    result = stdout_json(capsys)

    assert code == 2
    assert len(result["layers"]) == 9
    assert "kalshi_trades" in result["layers_missing"]


def test_inventory_external_refuses_an_unwritable_configuration(tmp_path: pathlib.Path, capsys):
    code = run(
        [
            "inventory-external",
            "--root",
            str(tmp_path),
            "--output",
            str(tmp_path / "inventory"),
            "--config",
            str(tmp_path / "absent.yaml"),
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert captured.err


def normalize_arguments(config: pathlib.Path, output: pathlib.Path) -> list[str]:
    return ["normalize-external", "--config", str(config), "--output", str(output)]


def test_normalize_external_seals_trades_and_exits_zero(tmp_path: pathlib.Path, capsys):
    """A window that holds prints is a complete extraction, and it exits 0."""
    write_kalshi_archive(tmp_path)
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))
    output = tmp_path / "normalized"

    code = run(normalize_arguments(config, output))
    record = stdout_json(capsys)

    assert code == 0, capsys.readouterr().err
    # All three prints fall inside the window, including the zero-cent one, which is
    # reported by a flag on its row rather than clipped out of the extraction.
    assert record["trade_count"] == 3
    assert record["bounded"] is False
    assert record["layer"] == "kalshi_trades"
    sealed = external_history.load_trades(output / "historical_trades.parquet")
    assert len(sealed) == 3
    # The sealed DECIMAL column carries a fixed scale, so a price comes back with
    # trailing zeros; the value is what has to survive, and the venue's own integer
    # cents stay intact beside it rather than being re-derived from the dollars.
    by_trade = {trade.trade_id: trade for trade in sealed}
    assert by_trade["t1"].price == Decimal("0.40")
    assert by_trade["t2"].price == Decimal("0.55")
    assert by_trade["t3"].price == Decimal(0)
    assert {str(trade.raw_price_units) for trade in sealed} == {"cents"}
    assert by_trade["t2"].raw_price == Decimal(55)
    assert by_trade["t3"].flags, "the zero-cent print is flagged rather than clipped"


def test_normalize_external_exits_two_when_the_read_was_bounded(tmp_path: pathlib.Path, capsys):
    """A capped read is incomplete for the window it claims, which is a blocked result."""
    write_kalshi_archive(tmp_path)
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))

    code = run([*normalize_arguments(config, tmp_path / "normalized"), "--max-rows", "1"])
    captured = capsys.readouterr()
    record = json.loads(captured.out)

    assert code == 2
    assert record["bounded"] is True
    assert record["max_rows_applied"] == 1
    assert "max_rows" in captured.err


def test_normalize_external_exits_two_when_the_window_holds_nothing(tmp_path: pathlib.Path, capsys):
    """No print in the window is absent evidence, never a silent empty panel."""
    write_kalshi_archive(tmp_path)
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))

    code = run(
        [
            *normalize_arguments(config, tmp_path / "normalized"),
            "--window-start",
            "2026-01-01T00:00:00Z",
            "--window-end",
            "2026-01-02T00:00:00Z",
        ]
    )
    captured = capsys.readouterr()

    assert code == 2
    assert json.loads(captured.out)["trade_count"] == 0
    assert "holds no trade inside the window" in captured.err


def test_normalize_external_omitted_flags_reach_the_configuration(tmp_path: pathlib.Path, capsys):
    """An omitted ``--layer`` and window keep the pipeline's own configured values."""
    write_kalshi_archive(tmp_path)
    releases = write_release_dataset(tmp_path)
    config = write_external_config(tmp_path, release_dataset=releases)
    declared = yaml.safe_load(config.read_text(encoding="utf-8"))

    code = run(normalize_arguments(config, tmp_path / "normalized"))
    record = stdout_json(capsys)

    assert code == 0
    assert record["layer"] == declared["normalization"]["kalshi_layer"]
    assert record["window_start"].startswith(declared["extraction"]["window_start"].rstrip("Z"))
    assert record["window_end"].startswith(declared["extraction"]["window_end"].rstrip("Z"))


@pytest.mark.parametrize(
    "extra",
    [
        ["--layer", "no_such_layer"],
        ["--window-start", "not-an-instant"],
    ],
)
def test_normalize_external_refuses_bad_arguments_with_exit_one(
    tmp_path: pathlib.Path, capsys, extra: list[str]
):
    """An unusable argument exits 1, so exit 2 keeps meaning a blocked finding."""
    write_kalshi_archive(tmp_path)
    config = write_external_config(tmp_path, release_dataset=write_release_dataset(tmp_path))

    code = run([*normalize_arguments(config, tmp_path / "normalized"), *extra])
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert captured.err


def coverage_fixture(
    tmp_path: pathlib.Path, *, satisfied_gates: bool
) -> tuple[pathlib.Path, pathlib.Path]:
    """A configuration and candidate artifact for the coverage grid.

    The ticker carries a configured policy series, because the candidate universe is
    filtered to the series the study starts from rather than to any market that traded.
    """
    ticker = "KXFED-25JAN"
    release = write_release_dataset(
        tmp_path,
        event_id="fed-2025-01",
        family="cpi",
        scheduled_at=dt.datetime(2025, 1, 29, 19, 0, tzinfo=dt.UTC),
    )
    registry = write_coverage_rule_registry(tmp_path, ticker=ticker)
    candidates = write_candidate_artifact(
        tmp_path, event_id="fed-2025-01", family="cpi", ticker=ticker
    )
    config = write_external_config(
        tmp_path,
        release_dataset=release,
        rule_evidence=registry,
        audit_coverage=candidates,
        satisfied_gates=satisfied_gates,
    )
    return config, candidates


def test_coverage_external_exits_zero_for_a_grid_whose_gates_pass(tmp_path: pathlib.Path, capsys):
    """A rule-verified candidate with the eligibility gates satisfied is a complete grid."""
    config, _ = coverage_fixture(tmp_path, satisfied_gates=True)
    output = tmp_path / "coverage"

    code = run(
        [
            "coverage-external",
            "--config",
            str(config),
            "--output",
            str(output),
            "--max-contracts",
            "1",
        ]
    )
    record = stdout_json(capsys)

    assert code == 0, capsys.readouterr().err
    assert record["gate_g0"] == "pass"
    assert record["counts"]["overall"]["rule_verified_pairs"] == 1
    assert (output / "coverage_external.json").exists()


def test_coverage_external_exits_two_when_g0_is_blocked(tmp_path: pathlib.Path, capsys):
    """An unverified rule vintage blocks G0, and the grid is still produced."""
    config, _ = coverage_fixture(tmp_path, satisfied_gates=False)

    code = run(
        ["coverage-external", "--config", str(config), "--output", str(tmp_path / "coverage")]
    )
    captured = capsys.readouterr()
    record = json.loads(captured.out)

    assert code == 2
    assert record["gate_g0"] == "blocked"
    assert record["blocked_count"] > 0
    assert (tmp_path / "coverage" / "coverage_external.json").exists()
    assert "G0 is 'blocked'" in captured.err


def test_coverage_external_keeps_the_configured_contract_cap(tmp_path: pathlib.Path, capsys):
    """An omitted ``--max-contracts`` leaves the configuration's own cap in force."""
    config, _ = coverage_fixture(tmp_path, satisfied_gates=True)
    declared = yaml.safe_load(config.read_text(encoding="utf-8"))
    configured = declared["extraction"]["max_contracts_per_event"]

    code = run(
        ["coverage-external", "--config", str(config), "--output", str(tmp_path / "coverage")]
    )
    record = stdout_json(capsys)

    assert code == 0
    assert record["inputs_read"]["max_contracts_applied"] == configured


def test_build_trade_panel_seals_a_panel_with_a_governed_response(tmp_path: pathlib.Path, capsys):
    """A release with a print on each side of it yields a valid row, and that exits 0."""
    config, trades = panel_ready_inputs(tmp_path)
    output = tmp_path / "panel"

    code = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(output),
        ]
    )
    record = stdout_json(capsys)

    assert code == 0, capsys.readouterr().err
    assert record["counts"]["overall"]["valid_rows"] > 0
    assert record["counts"]["overall"]["endpoint_observed_pairs"] == 1
    assert (output / "trade_panel.parquet").exists()
    # The JSON reports the panel rather than repeating its rows, and a quote column
    # never appears in it: these are transactions, not quotes.
    assert "rows" not in record
    assert all("spread" not in name and "depth" not in name for name in record["columns"])


def test_build_trade_panel_exits_two_when_no_row_is_valid(tmp_path: pathlib.Path, capsys):
    """Without verified rule evidence every row is masked, which is a blocked panel."""
    write_kalshi_archive(tmp_path)
    releases = write_release_dataset(tmp_path)
    candidates = write_candidate_artifact(
        tmp_path, event_id=EVENT_ID, family="cpi", ticker=KALSHI_TICKER
    )
    config = write_external_config(
        tmp_path,
        release_dataset=releases,
        rule_evidence=tmp_path / "no-such-rule-evidence.json",
        audit_coverage=candidates,
    )
    trades = write_sealed_trades(tmp_path)

    code = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(tmp_path / "panel"),
        ]
    )
    captured = capsys.readouterr()
    record = json.loads(captured.out)

    assert code == 2
    assert record["counts"]["overall"]["valid_rows"] == 0
    assert (tmp_path / "panel" / "trade_panel.parquet").exists()
    assert "no panel row is valid" in captured.err


def test_build_trade_panel_omitted_horizon_keeps_the_configured_set(tmp_path: pathlib.Path, capsys):
    """An omitted ``--horizon`` measures every configured horizon and reports so."""
    config, trades = panel_ready_inputs(tmp_path)
    declared = yaml.safe_load(config.read_text(encoding="utf-8"))
    configured = declared["response"]["horizons_seconds"]
    arguments = [
        "build-trade-panel",
        "--config",
        str(config),
        "--trades",
        str(trades),
        "--output",
        str(tmp_path / "panel"),
    ]

    code = run(arguments)
    full = stdout_json(capsys)
    assert code == 0
    assert full["counts"]["horizons_seconds"] == configured
    assert (
        full["counts"]["primary_horizon_seconds"] == declared["response"]["primary_horizon_seconds"]
    )
    assert "requested_horizon_seconds" not in full

    narrowed = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(tmp_path / "narrowed"),
            "--horizon",
            "60",
        ]
    )
    single = stdout_json(capsys)
    assert narrowed == 0, capsys.readouterr().err
    # Narrowing sets the horizon set and the primary together, so the primary horizon is
    # one the panel actually contains and is still reported.
    assert single["counts"]["horizons_seconds"] == [60]
    assert single["counts"]["primary_horizon_seconds"] == 60
    assert single["counts"]["overall"]["primary_horizon_rows"] > 0
    assert single["horizon_narrowed"] is True
    assert single["requested_horizon_seconds"] == 60
    assert single["configured_horizons_seconds"] == configured
    assert single["settings_digest"] != full["settings_digest"]


@pytest.mark.parametrize(
    "extra",
    [
        ["--horizon", "7"],
        ["--clock-mode", "wall_clock"],
    ],
)
def test_build_trade_panel_refuses_an_unconfigured_choice(
    tmp_path: pathlib.Path, capsys, extra: list[str]
):
    """A horizon or clock mode the pipeline does not carry is an argument error, not a 2."""
    config, trades = panel_ready_inputs(tmp_path)

    code = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(tmp_path / "panel"),
            *extra,
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert captured.err


def test_build_trade_panel_declares_the_universe_from_a_candidate_grid(
    tmp_path: pathlib.Path, capsys
):
    """A declared grid puts a contract that never traded in the denominator.

    The grid is the pre-event substitute for a universe that would otherwise be
    selected by what traded afterwards, so the quiet contract has to appear as a
    missing cell rather than vanish with its evidence.
    """
    config, trades = panel_ready_inputs(tmp_path)
    grid = tmp_path / "candidates.json"
    grid.write_text(
        json.dumps(
            {
                EVENT_ID: [
                    ["kalshi", KALSHI_TICKER],
                    ["kalshi", "KXCPI-QUIET"],
                ]
            }
        ),
        encoding="utf-8",
    )

    code = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(tmp_path / "panel"),
            "--candidate-grid",
            str(grid),
        ]
    )
    record = stdout_json(capsys)

    assert code == 0, capsys.readouterr().err
    counts = record["counts"]
    assert counts["candidate_universe"] == "declared_listing_grid"
    assert counts["overall"]["candidate_pairs"] == 2
    assert counts["overall"]["declared_candidate_pairs"] == 2
    assert counts["overall"]["pairs_without_any_window_trade"] == 1
    # The quiet pair is masked, so it cannot contribute a response it never observed.
    assert counts["overall"]["endpoint_observed_pairs"] == 1
    assert "candidate_universe_from_declared_listing_grid" in record["flags"]


def test_build_trade_panel_rejects_a_malformed_candidate_grid(tmp_path: pathlib.Path, capsys):
    """A grid that is not a release-to-pairs mapping is an argument error, not a panel."""
    config, trades = panel_ready_inputs(tmp_path)
    grid = tmp_path / "candidates.json"
    grid.write_text(json.dumps({EVENT_ID: [KALSHI_TICKER]}), encoding="utf-8")

    code = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(tmp_path / "panel"),
            "--candidate-grid",
            str(grid),
        ]
    )
    captured = capsys.readouterr()

    # An argument error is 1: exit 2 is reserved for a run that produced a blocked result.
    assert code == 1
    assert "not a (venue, contract_id) pair" in captured.err
    assert not (tmp_path / "panel" / "trade_panel.parquet").exists()


def test_build_trade_panel_rejects_a_grid_that_omits_a_declared_release(
    tmp_path: pathlib.Path, capsys
):
    """An uncovered release has no denominator, so the run fails instead of guessing one."""
    config, trades = panel_ready_inputs(tmp_path)
    grid = tmp_path / "candidates.json"
    grid.write_text(json.dumps({"some-other-release": []}), encoding="utf-8")

    code = run(
        [
            "build-trade-panel",
            "--config",
            str(config),
            "--trades",
            str(trades),
            "--output",
            str(tmp_path / "panel"),
            "--candidate-grid",
            str(grid),
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert EVENT_ID in captured.err


def test_report_external_exits_zero_for_an_unblocked_panel(tmp_path: pathlib.Path, capsys):
    """A verifiable panel with governed responses produces an unblocked report."""
    config, trades = panel_ready_inputs(tmp_path)
    panel = tmp_path / "panel"
    assert (
        run(
            [
                "build-trade-panel",
                "--config",
                str(config),
                "--trades",
                str(trades),
                "--output",
                str(panel),
            ]
        )
        == 0
    )
    capsys.readouterr()
    output = tmp_path / "report"

    code = run(
        [
            "report-external",
            "--config",
            str(config),
            "--panel",
            str(panel / "trade_panel.parquet"),
            "--output",
            str(output),
        ]
    )
    record = stdout_json(capsys)

    assert code == 0, capsys.readouterr().err
    assert record["blocked"] is False
    assert record["gate"] == "satisfied"
    # An omitted --run-id is derived from the inputs, not invented here.
    assert record["run_id"] == record["spec_digest"]
    assert record["inputs"]["verified"] is True
    assert (output / "external_report.json").exists()


def test_report_external_exits_two_for_a_panel_it_cannot_verify(tmp_path: pathlib.Path, capsys):
    """A panel with no rows blocks the report while still producing the artifacts."""
    write_kalshi_archive(tmp_path)
    release = write_release_dataset(tmp_path)
    config = write_external_config(tmp_path, release_dataset=release)
    empty = tmp_path / "empty_panel.parquet"
    write_parquet([], empty, table="trade_panel", coverage_epoch="test_fixture")
    output = tmp_path / "report"

    code = run(
        [
            "report-external",
            "--config",
            str(config),
            "--panel",
            str(empty),
            "--output",
            str(output),
        ]
    )
    captured = capsys.readouterr()
    record = json.loads(captured.out)

    assert code == 2
    assert record["blocked"] is True
    assert record["gate"] == "blocked"
    assert (output / "external_report.json").exists()
    assert "gate is 'blocked'" in captured.err


def test_report_external_forwards_the_run_id_it_was_given(tmp_path: pathlib.Path, capsys):
    """A supplied ``--run-id`` is what the report records."""
    config, trades = panel_ready_inputs(tmp_path)
    panel = tmp_path / "panel"
    assert (
        run(
            [
                "build-trade-panel",
                "--config",
                str(config),
                "--trades",
                str(trades),
                "--output",
                str(panel),
            ]
        )
        == 0
    )
    capsys.readouterr()

    code = run(
        [
            "report-external",
            "--config",
            str(config),
            "--panel",
            str(panel / "trade_panel.parquet"),
            "--output",
            str(tmp_path / "report"),
            "--run-id",
            "run-cli-0001",
        ]
    )
    record = stdout_json(capsys)

    assert code == 0
    assert record["run_id"] == "run-cli-0001"


def test_report_external_exits_one_for_a_panel_that_does_not_exist(tmp_path: pathlib.Path, capsys):
    config, _ = panel_ready_inputs(tmp_path)

    code = run(
        [
            "report-external",
            "--config",
            str(config),
            "--panel",
            str(tmp_path / "absent.parquet"),
            "--output",
            str(tmp_path / "report"),
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert captured.err


@pytest.mark.parametrize(
    "argv",
    [
        ["inventory-external"],
        ["inventory-external", "--root", "r"],
        ["normalize-external", "--config", "c"],
        ["coverage-external", "--config", "c"],
        ["build-trade-panel", "--config", "c", "--trades", "t"],
        ["report-external", "--config", "c", "--panel", "p"],
    ],
)
def test_external_commands_refuse_missing_required_arguments(argv: list[str], capsys):
    """Every new command needs its own inputs, and an argument error is exit 1."""
    code = run(argv)
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert captured.err
