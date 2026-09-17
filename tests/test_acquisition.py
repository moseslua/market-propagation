from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess

from market_propagation.acquisition import Coordinator, Settings, due_slots

UTC = dt.UTC


def settings(tmp_path: pathlib.Path, *, attempts: int = 2) -> Settings:
    return Settings(
        root=tmp_path / "collector",
        rules_root=tmp_path / "rules",
        rules_time=dt.time(6, 41),
        markets_time=dt.time(6, 47),
        timezone="UTC",
        timeout_seconds=1,
        max_attempts=attempts,
        backoff_seconds=60,
        loop_seconds=1,
        bounds={
            "rules_max_pages": 2,
            "rules_page_size": 10,
            "markets_max_pages": 2,
            "markets_page_size": 10,
            "trades_max_pages": 2,
            "trades_page_size": 10,
            "markets_max_contracts": 10,
        },
    )


def test_due_slots_obey_daily_and_monthly_boundaries(tmp_path: pathlib.Path) -> None:
    config = settings(tmp_path)
    before = dt.datetime(2026, 2, 1, 6, 40, tzinfo=UTC)
    assert due_slots(config, before) == []
    after = dt.datetime(2026, 2, 1, 6, 48, tzinfo=UTC)
    assert [job for job, *_rest in due_slots(config, after)] == [
        "rules",
        "attest",
        "metadata",
        "markets",
    ]
    markets = due_slots(config, after)[3]
    assert markets[3] == dt.datetime(2025, 12, 28, 0, 0, tzinfo=UTC)


def test_successful_slot_is_not_restarted_and_receipt_is_immutable(tmp_path: pathlib.Path) -> None:
    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        output = pathlib.Path(command[command.index("--output") + 1])
        output.write_text(json.dumps({"captures_written": 1}), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "ok", "")

    coordinator = Coordinator(settings(tmp_path), runner=runner)
    now = dt.datetime(2026, 2, 2, 7, tzinfo=UTC)
    first = coordinator.locked_run(now)
    second = coordinator.locked_run(now)
    assert first["results"][0]["status"] == "success"
    assert second["results"][0]["status"] == "already_successful"
    assert len(list((tmp_path / "collector" / "receipts" / "rules").rglob("*.json"))) == 1


def test_zero_exit_with_partial_summary_stays_retryable(tmp_path: pathlib.Path) -> None:
    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        output = pathlib.Path(command[command.index("--output") + 1])
        output.write_text(json.dumps({"captures_written": 1, "pages_blocked": 1}), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    coordinator = Coordinator(settings(tmp_path), runner=runner)
    now = dt.datetime(2026, 2, 2, 7, tzinfo=UTC)
    outcome = coordinator.locked_run(now)
    assert outcome["results"][0]["status"] == "blocked"
    assert outcome["results"][0]["flags"] == ["pages_blocked"]
    assert coordinator.locked_run(now)["results"][0]["status"] == "backoff"
    report = coordinator.status(now)
    assert "rules" in report["next_due"]
    assert "rules:2026-02-02" in report["failed_slots"]


def test_timeout_is_recorded_as_blocked(tmp_path: pathlib.Path) -> None:
    def runner(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired("collector", 1)

    coordinator = Coordinator(settings(tmp_path), runner=runner)
    outcome = coordinator.locked_run(dt.datetime(2026, 2, 2, 7, tzinfo=UTC))
    assert outcome["results"][0]["status"] == "blocked"
    receipt = pathlib.Path(outcome["results"][0]["receipt"])
    assert json.loads(receipt.read_text(encoding="utf-8"))["reason"] == "timeout"


def test_duplicate_lock_is_reported(tmp_path: pathlib.Path) -> None:
    coordinator = Coordinator(settings(tmp_path))
    coordinator.settings.root.mkdir(parents=True)
    lock_path = coordinator.settings.root / ".collector.lock"
    with lock_path.open("a+") as lock:
        import fcntl

        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert coordinator.locked_run(dt.datetime(2026, 2, 2, 7, tzinfo=UTC)) == {
            "status": "blocked",
            "reason": "duplicate_writer",
        }


def test_child_environment_keeps_the_checkout_package_importable(tmp_path: pathlib.Path) -> None:
    environment = Coordinator(settings(tmp_path))._environment()
    assert str(pathlib.Path(__file__).resolve().parents[1] / "src") in environment["PYTHONPATH"]


def test_timeout_preserves_captured_bytes_and_a_failure_receipt(tmp_path: pathlib.Path) -> None:
    def runner(_command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(
            "collector", 1, output=b"partial payload", stderr=b"timeout"
        )

    result = Coordinator(settings(tmp_path), runner=runner).locked_run(
        dt.datetime(2026, 2, 2, 7, tzinfo=UTC)
    )
    receipt = json.loads(pathlib.Path(result["results"][0]["receipt"]).read_text())
    assert receipt["reason"] == "timeout"
    assert (
        pathlib.Path(receipt["attempt_directory"]) / "stdout.log"
    ).read_text() == "partial payload"


def test_zero_exit_without_a_summary_is_not_success(tmp_path: pathlib.Path) -> None:
    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, "", "")

    result = Coordinator(settings(tmp_path), runner=runner).locked_run(
        dt.datetime(2026, 2, 2, 7, tzinfo=UTC)
    )
    assert all(item["status"] == "blocked" for item in result["results"])


def test_trade_refusal_is_not_lost_when_exit_and_flags_look_successful(
    tmp_path: pathlib.Path,
) -> None:
    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        output = pathlib.Path(command[command.index("--output") + 1])
        output.write_text(
            json.dumps({"markets_written": 1, "refusals": [{"reason": "trade_page_missing"}]})
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    result = Coordinator(settings(tmp_path), runner=runner).locked_run(
        dt.datetime(2026, 2, 2, 7, tzinfo=UTC)
    )
    assert result["results"][-1]["status"] == "blocked"


def test_monthly_capture_uses_host_schedule_and_utc_midnight_window(tmp_path: pathlib.Path) -> None:
    from dataclasses import replace

    config = replace(settings(tmp_path), timezone="Asia/Kuala_Lumpur")
    now = dt.datetime(2026, 10, 1, 0, 0, tzinfo=UTC)
    due = due_slots(config, now)
    assert due[-1][0] == "markets"
    command = Coordinator(config)._command(due[-1][0], tmp_path, due[-1][2], due[-1][3])
    end = dt.datetime.fromisoformat(command[command.index("--window-end") + 1])
    start = dt.datetime.fromisoformat(command[command.index("--window-start") + 1])
    assert end == dt.datetime(2026, 9, 30, 0, 0, tzinfo=UTC)
    assert end - start == dt.timedelta(days=35)


def test_partial_attestation_totals_do_not_complete_the_slot(tmp_path: pathlib.Path) -> None:
    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        output = pathlib.Path(command[command.index("--output") + 1])
        if "attest-rules" in command:
            output.mkdir()
            output = output / "rule_attestation_report.json"
            summary = {
                "totals": {"contracts_attested": 1, "contracts_unattested": 1, "refusals": 1}
            }
        else:
            summary = {"captures_written": 1}
        output.write_text(json.dumps(summary))
        return subprocess.CompletedProcess(command, 0, "", "")

    result = Coordinator(settings(tmp_path), runner=runner).locked_run(
        dt.datetime(2026, 9, 17, 12, tzinfo=UTC)
    )
    attestation = next(row for row in result["results"] if row["job"] == "attest")
    assert attestation["status"] == "blocked"
    assert "partial_attestation" in attestation["flags"]


def test_restart_reacquires_a_changed_monthly_window(tmp_path: pathlib.Path) -> None:
    from dataclasses import replace

    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        output = pathlib.Path(command[command.index("--output") + 1])
        if "attest-rules" in command:
            output.mkdir()
            output = output / "rule_attestation_report.json"
        output.write_text(json.dumps({"captures_written": 1}))
        return subprocess.CompletedProcess(command, 0, "", "")

    config = settings(tmp_path)
    now = dt.datetime(2026, 9, 17, 12, tzinfo=UTC)
    first = Coordinator(config, runner=runner).locked_run(now)
    first_path = pathlib.Path(first["results"][-1]["receipt"])
    original_bytes = first_path.read_bytes()
    second = Coordinator(replace(config, timezone="Asia/Kuala_Lumpur"), runner=runner).locked_run(
        now
    )
    assert second["results"][-1]["status"] == "success"
    second_receipt = json.loads(pathlib.Path(second["results"][-1]["receipt"]).read_text())
    manifest = json.loads(pathlib.Path(second_receipt["manifest"]).read_text())
    assert manifest["window_end"] == "2026-08-31T00:00:00+00:00"
    assert first_path.read_bytes() == original_bytes


def test_restart_does_not_recapture_for_an_alias_of_the_same_interpreter(tmp_path, monkeypatch):
    import sys

    first_python = tmp_path / "python"
    second_python = tmp_path / "python3"
    first_python.symlink_to(sys.executable)
    second_python.symlink_to(sys.executable)

    def runner(command, **_kwargs):
        output = pathlib.Path(command[command.index("--output") + 1])
        if "attest-rules" in command:
            output.mkdir()
            output = output / "rule_attestation_report.json"
        output.write_text(json.dumps({"captures_written": 1}))
        return subprocess.CompletedProcess(command, 0, "", "")

    now = dt.datetime(2026, 9, 17, 12, tzinfo=UTC)
    monkeypatch.setattr(sys, "executable", str(first_python))
    Coordinator(settings(tmp_path), runner=runner).locked_run(now)
    monkeypatch.setattr(sys, "executable", str(second_python))
    resumed = Coordinator(settings(tmp_path), runner=runner).locked_run(now)
    assert all(row["status"] == "already_successful" for row in resumed["results"])
