from __future__ import annotations

import datetime as dt
import fcntl
import json
import math
import os
import pathlib
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

import yaml

from .ingest.audit import series_of
from .storage import RawStore

UTC = dt.UTC


@dataclass(frozen=True)
class Settings:
    root: pathlib.Path
    rules_root: pathlib.Path
    rules_time: dt.time
    markets_time: dt.time
    timezone: str
    timeout_seconds: float
    max_attempts: int
    backoff_seconds: float
    loop_seconds: float
    bounds: Mapping[str, int]
    job_timeout_seconds: float = 900


def load_settings(path: str | pathlib.Path) -> Settings:
    source = pathlib.Path(path)
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, Mapping)
        or payload.get("config_version") != "evidence_acquisition_v1"
    ):
        raise ValueError("evidence acquisition configuration must be evidence_acquisition_v1")
    cadence = payload.get("cadence")
    execution = payload.get("execution")
    bounds = payload.get("bounds")
    if (
        not isinstance(cadence, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(bounds, Mapping)
    ):
        raise ValueError("configuration requires cadence, execution, and bounds mappings")
    root = pathlib.Path(str(payload["state_root"]))
    settings = Settings(
        root=root,
        rules_root=pathlib.Path(str(payload["rules_capture_root"])),
        rules_time=dt.time.fromisoformat(str(cadence["rules_daily_host_time"])),
        markets_time=dt.time.fromisoformat(str(cadence["markets_monthly_host_time"])),
        timezone=str(cadence.get("host_timezone") or "local"),
        timeout_seconds=float(execution["timeout_seconds"]),
        max_attempts=int(execution["max_attempts"]),
        backoff_seconds=float(execution["retry_backoff_seconds"]),
        loop_seconds=float(execution["loop_seconds"]),
        bounds={str(key): int(value) for key, value in bounds.items()},
        job_timeout_seconds=float(execution.get("job_timeout_seconds", 900)),
    )
    if settings.timezone != "local":
        ZoneInfo(settings.timezone)
    for value in (
        settings.timeout_seconds,
        settings.job_timeout_seconds,
        settings.max_attempts,
        settings.backoff_seconds,
        settings.loop_seconds,
        *settings.bounds.values(),
    ):
        if not math.isfinite(value) or value <= 0:
            raise ValueError("all execution limits and bounds must be finite and positive")
    return settings


def due_slots(
    settings: Settings, now: dt.datetime
) -> list[tuple[str, str, dt.datetime, dt.datetime | None]]:
    if now.tzinfo is None:
        raise ValueError("scheduler time must carry an offset")
    local = (
        now.astimezone()
        if settings.timezone == "local"
        else now.astimezone(ZoneInfo(settings.timezone))
    )
    due: list[tuple[str, str, dt.datetime, dt.datetime | None]] = []
    rule_at = local.replace(
        hour=settings.rules_time.hour,
        minute=settings.rules_time.minute,
        second=settings.rules_time.second,
        microsecond=0,
    )
    if local >= rule_at:
        due.append(("rules", rule_at.date().isoformat(), rule_at, None))
        due.append(("attest", rule_at.date().isoformat(), rule_at, None))
        due.append(("metadata", rule_at.date().isoformat(), rule_at, None))
    market_at = local.replace(
        day=1,
        hour=settings.markets_time.hour,
        minute=settings.markets_time.minute,
        second=settings.markets_time.second,
        microsecond=0,
    )
    if local >= market_at:
        window_end = market_at.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        due.append(
            ("markets", market_at.date().isoformat(), market_at, window_end - dt.timedelta(days=35))
        )
    return due


def _read_json(path: pathlib.Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_json(path: pathlib.Path, document: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _partial(summary: Mapping[str, Any]) -> list[str]:
    flags = [str(flag) for flag in summary.get("flags", [])]
    if summary.get("blocked"):
        flags.append("blocked")
    if summary.get("pages_blocked"):
        flags.append("pages_blocked")
    if summary.get("page_bounded_queries"):
        flags.append("page_bounded_queries")
    if summary.get("records_bound_reached"):
        flags.append("records_bound_reached")
    if summary.get("refusals"):
        flags.append("refusals")
    if summary.get("skipped"):
        flags.append("skipped_records")
    totals = summary.get("totals", {})
    if not isinstance(totals, Mapping):
        flags.append("summary_unreadable")
    elif (
        totals.get("contracts_unattested")
        or totals.get("refusals")
        or summary.get("refusals_by_reason")
    ):
        flags.append("partial_attestation")
    return sorted(set(flags))


class Coordinator:
    def __init__(self, settings: Settings, *, runner: Callable[..., Any] = subprocess.run) -> None:
        self.settings = settings
        self.runner = runner
        self.state_path = settings.root / "state.json"
        self.receipts_root = settings.root / "receipts"
        self.attempts_root = settings.root / "attempts"

    def _state(self) -> dict[str, Any]:
        return _read_json(
            self.state_path,
            {"version": 1, "successful_slots": {}, "failed_slots": {}},
        )

    def _completed(
        self,
        saved: Mapping[str, Any],
        job: str,
        due_at: dt.datetime,
        window_start: dt.datetime | None,
    ) -> bool:
        if not saved.get("receipt"):
            return False
        try:
            receipt = _read_json(pathlib.Path(saved["receipt"]), {})
            attempt = pathlib.Path(receipt["attempt_directory"])
            summary_path = (
                attempt / "attestation" / "rule_attestation_report.json"
                if job == "attest"
                else attempt / "command-output.json"
            )
            summary = _read_json(summary_path, {})
            recorded_command = receipt.get("command")
            expected_command = self._command(job, attempt, due_at, window_start)
            if not isinstance(recorded_command, list) or not recorded_command:
                return False
            recorded_python = pathlib.Path(recorded_command[0])
            expected_python = pathlib.Path(expected_command[0])
            return (
                bool(summary)
                and isinstance(summary, Mapping)
                and not _partial(summary)
                and recorded_command[1:] == expected_command[1:]
                and recorded_python.parent.resolve() == expected_python.parent.resolve()
                and recorded_python.resolve() == expected_python.resolve()
            )
        except (OSError, ValueError, KeyError, TypeError):
            return False

    def _command(
        self, job: str, attempt: pathlib.Path, due_at: dt.datetime, window_start: dt.datetime | None
    ) -> list[str]:
        output = attempt / "command-output.json"
        b = self.settings.bounds
        base = [sys.executable, "-m", "market_propagation"]
        if job == "rules":
            return [
                *base,
                "capture-rules",
                "--root",
                str(self.settings.rules_root),
                "--max-pages",
                str(b["rules_max_pages"]),
                "--limit",
                str(b["rules_page_size"]),
                "--timeout",
                str(self.settings.timeout_seconds),
                "--output",
                str(output),
            ]
        if job == "markets":
            assert window_start is not None
            return [
                *base,
                "capture-markets",
                "--root",
                str(attempt / "markets"),
                "--raw-store",
                str(attempt / "raw"),
                "--market-layer",
                "kalshi_own_markets",
                "--window-start",
                window_start.isoformat(),
                "--window-end",
                due_at.astimezone(UTC)
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .isoformat(),
                "--max-pages",
                str(b["markets_max_pages"]),
                "--limit",
                str(b["markets_page_size"]),
                "--trade-max-pages",
                str(b["trades_max_pages"]),
                "--trade-limit",
                str(b["trades_page_size"]),
                "--max-contracts",
                str(b["markets_max_contracts"]),
                "--timeout",
                str(self.settings.timeout_seconds),
                "--output",
                str(output),
            ]
        if job == "metadata":
            return [
                *base,
                "capture-venue-metadata",
                "--root",
                str(attempt / "metadata"),
                "--timeout",
                str(self.settings.timeout_seconds),
                "--output",
                str(output),
            ]
        if job == "attest":
            return [
                *base,
                "attest-rules",
                "--root",
                str(self.settings.rules_root),
                "--output",
                str(attempt / "attestation"),
                "--emit-graph-records",
                str(attempt / "rule-vintage.json"),
            ]
        raise ValueError(f"unknown job {job!r}")

    @staticmethod
    def _environment() -> dict[str, str]:
        environment = os.environ.copy()
        source_root = str(pathlib.Path(__file__).resolve().parents[1])
        inherited = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root if not inherited else f"{source_root}{os.pathsep}{inherited}"
        )
        return environment

    def _write_manifest(
        self,
        job: str,
        attempt: pathlib.Path,
        summary: Mapping[str, Any],
        due_at: dt.datetime,
        window_start: dt.datetime | None,
    ) -> pathlib.Path:
        observations: list[dict[str, Any]] = []
        if job in {"rules", "attest"}:
            raw = RawStore(self.settings.rules_root / "raw")
            for path in sorted((self.settings.rules_root / "captures").glob("*/*.json")):
                record = _read_json(path, {})
                payload = json.loads(raw.get(record["raw_hash"]))
                definition = next(
                    (
                        m
                        for m in payload.get("markets", [])
                        if m.get("ticker") == record["contract_id"]
                    ),
                    None,
                )
                if definition is None:
                    raise ValueError("rule capture identity absent from cited raw page")
                observations.append(
                    {
                        "identity": record.get("contract_id"),
                        "series": series_of(record["contract_id"]),
                        "window": None,
                        "source_url": record.get("source_url"),
                        "retrieved_at": record.get("captured_at"),
                        "source_observed_at": record.get("source_observed_at"),
                        "raw_hash": record.get("raw_hash"),
                        "raw_store": str(raw.root),
                        "definition": definition,
                        "parsed_fields": {
                            "rule_text": record.get("rule_text"),
                            "settlement_criterion": definition.get("settlement_criterion"),
                            "effective_at": record.get("stated_in_force_from"),
                            "reference_period": definition.get("reference_period"),
                        },
                        "absent_reasons": {
                            "effective_at": "source_did_not_state_in_force_time"
                            if record.get("stated_in_force_from") is None
                            else None,
                            "reference_period": "not_explicitly_stated"
                            if definition.get("reference_period") is None
                            else None,
                            "settlement_criterion": "requires_rule_text_interpretation"
                            if definition.get("settlement_criterion") is None
                            else None,
                        },
                    }
                )
        if job == "metadata":
            raw = RawStore(attempt / "metadata" / "raw")
            receipts = {receipt["raw_hash"]: receipt for receipt in raw.receipts()}
            for path in sorted((attempt / "metadata" / "markets").glob("*.json")):
                record = _read_json(path, {})
                raw.get(record["raw_hash"])
                observations.append(
                    {
                        "identity": record.get("condition_id"),
                        "series": None,
                        "window": None,
                        "source_url": record.get("source_url"),
                        "retrieved_at": receipts[record["raw_hash"]]["received_time"],
                        "source_observed_at": record.get("source_observed_at"),
                        "raw_hash": record.get("raw_hash"),
                        "raw_store": str(raw.root),
                        "parsed_fields": {
                            "question": record.get("question"),
                            "description": record.get("description"),
                            "effective_at": None,
                            "reference_period": None,
                            "settlement_criterion": None,
                        },
                        "absent_reasons": {
                            "effective_at": "not_stated_by_market_metadata",
                            "reference_period": "not_stated_by_market_metadata",
                            "settlement_criterion": "requires_predicate_parsing",
                        },
                    }
                )
        if job == "markets":
            raw = RawStore(attempt / "raw")
            for receipt in raw.receipts():
                payload = json.loads(raw.get(receipt["raw_hash"]))
                if not isinstance(payload, Mapping):
                    continue
                for market in payload.get("markets", []):
                    observations.append(
                        {
                            "identity": market.get("ticker"),
                            "series": series_of(market.get("ticker", "")),
                            "source_url": receipt["metadata"].get("url"),
                            "retrieved_at": receipt["received_time"],
                            "raw_hash": receipt["raw_hash"],
                            "raw_store": str(raw.root),
                            "definition": market,
                            "source_observed_at": None,
                            "effective_at": None,
                            "absent_reasons": {
                                "source_observed_at": "not_retained_in_transport_receipt",
                                "effective_at": "not_stated",
                            },
                        }
                    )
        document = {
            "document_version": "1",
            "job": job,
            "schedule_slot": due_at.isoformat(),
            "window_start": window_start.isoformat() if window_start else None,
            "window_end": due_at.astimezone(UTC)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat()
            if window_start
            else None,
            "summary_flags": _partial(summary),
            "observations": observations,
        }
        target = attempt / "observation-manifest.json"
        _atomic_json(target, document)
        return target

    def run_due(self, now: dt.datetime | None = None) -> dict[str, Any]:
        now = now or dt.datetime.now(tz=UTC)
        self.settings.root.mkdir(parents=True, exist_ok=True)
        state = self._state()
        state.setdefault("failed_slots", {})
        results: list[dict[str, Any]] = []
        for job, slot, due_at, window_start in due_slots(self.settings, now):
            key = f"{job}:{slot}"
            if self._completed(state["successful_slots"].get(key, {}), job, due_at, window_start):
                results.append({"job": job, "slot": slot, "status": "already_successful"})
                continue
            prior_failure = state["failed_slots"].get(key, {})
            retry_at = prior_failure.get("retry_at")
            if retry_at and now < dt.datetime.fromisoformat(str(retry_at)):
                results.append(
                    {"job": job, "slot": slot, "status": "backoff", "retry_at": retry_at}
                )
                continue
            if int(prior_failure.get("attempts", 0)) >= self.settings.max_attempts:
                results.append({"job": job, "slot": slot, "status": "retry_exhausted"})
                continue
            attempt = self.attempts_root / job / slot / f"attempt-{uuid.uuid4().hex}"
            attempt.mkdir(parents=True)
            command = self._command(job, attempt, due_at, window_start)
            started = dt.datetime.now(tz=UTC)
            try:
                completed = self.runner(
                    command,
                    cwd=pathlib.Path.cwd(),
                    env=self._environment(),
                    text=True,
                    capture_output=True,
                    timeout=self.settings.job_timeout_seconds,
                )
                (attempt / "stdout.log").write_text(completed.stdout or "", encoding="utf-8")
                (attempt / "stderr.log").write_text(completed.stderr or "", encoding="utf-8")
                summary_path = (
                    attempt / "attestation" / "rule_attestation_report.json"
                    if job == "attest"
                    else attempt / "command-output.json"
                )
                summary = _read_json(summary_path, {})
                flags = (
                    _partial(summary) if isinstance(summary, Mapping) else ["summary_unreadable"]
                )
                if not summary:
                    flags.append("summary_missing")
                status = "success" if completed.returncode == 0 and not flags else "blocked"
                detail: dict[str, Any] = {"returncode": completed.returncode, "flags": flags}
            except subprocess.TimeoutExpired as exc:
                for name, value in (("stdout", exc.stdout), ("stderr", exc.stderr)):
                    text = (
                        value.decode("utf-8", errors="replace")
                        if isinstance(value, bytes)
                        else value or ""
                    )
                    (attempt / f"{name}.log").write_text(text, encoding="utf-8")
                summary, status, detail = {}, "blocked", {"reason": "timeout", "flags": ["timeout"]}
            except (OSError, ValueError) as exc:
                summary, status, detail = (
                    {},
                    "blocked",
                    {"reason": str(exc), "flags": ["command_or_summary_failure"]},
                )
            try:
                manifest = self._write_manifest(
                    job,
                    attempt,
                    summary if isinstance(summary, Mapping) else {},
                    due_at,
                    window_start,
                )
            except (OSError, ValueError, KeyError) as exc:
                manifest, status = None, "blocked"
                detail = {
                    **detail,
                    "manifest_error": str(exc),
                    "flags": [*detail.get("flags", []), "manifest_verification_failed"],
                }
            receipt = {
                "receipt_version": 1,
                "job": job,
                "slot": slot,
                "status": status,
                "started_at": started.isoformat(),
                "finished_at": dt.datetime.now(tz=UTC).isoformat(),
                "command": command,
                "attempt_directory": str(attempt),
                "manifest": str(manifest) if manifest else None,
                **detail,
            }
            receipt_path = (
                self.receipts_root
                / job
                / slot
                / f"{receipt['finished_at'].replace(':', '')}-{uuid.uuid4().hex}.json"
            )
            _atomic_json(receipt_path, receipt)
            if status == "success":
                state["successful_slots"][key] = {
                    "receipt": str(receipt_path),
                    "finished_at": receipt["finished_at"],
                }
                state["failed_slots"].pop(key, None)
            else:
                attempts = int(prior_failure.get("attempts", 0)) + 1
                state["failed_slots"][key] = {
                    "attempts": attempts,
                    "retry_at": (
                        now + dt.timedelta(seconds=self.settings.backoff_seconds)
                    ).isoformat(),
                    "receipt": str(receipt_path),
                }
            _atomic_json(self.state_path, state)
            results.append(
                {"job": job, "slot": slot, "status": status, "receipt": str(receipt_path), **detail}
            )
        cycle = {"now": now.isoformat(), "results": results, "state_path": str(self.state_path)}
        _atomic_json(self.settings.root / "cycles" / f"{uuid.uuid4().hex}.json", cycle)
        return cycle

    def status(self, now: dt.datetime | None = None) -> dict[str, Any]:
        now = now or dt.datetime.now(tz=UTC)
        state = self._state()
        state.setdefault("failed_slots", {})
        due = [
            {
                "job": job,
                "slot": slot,
                "due_at": due_at.isoformat(),
                "successful": self._completed(
                    state["successful_slots"].get(f"{job}:{slot}", {}), job, due_at, _window
                ),
            }
            for job, slot, due_at, _window in due_slots(self.settings, now)
        ]
        local = (
            now.astimezone()
            if self.settings.timezone == "local"
            else now.astimezone(ZoneInfo(self.settings.timezone))
        )
        next_rules = local.replace(
            hour=self.settings.rules_time.hour,
            minute=self.settings.rules_time.minute,
            second=self.settings.rules_time.second,
            microsecond=0,
        )
        if next_rules <= local:
            next_rules += dt.timedelta(days=1)
        next_markets = local.replace(
            day=1,
            hour=self.settings.markets_time.hour,
            minute=self.settings.markets_time.minute,
            second=self.settings.markets_time.second,
            microsecond=0,
        )
        if next_markets <= local:
            month = next_markets.month % 12 + 1
            year = next_markets.year + (next_markets.month == 12)
            next_markets = next_markets.replace(year=year, month=month)
        return {
            "now": now.isoformat(),
            "due": due,
            "next_due": {
                "rules": next_rules.isoformat(),
                "attest": next_rules.isoformat(),
                "metadata": next_rules.isoformat(),
                "markets": next_markets.isoformat(),
            },
            "successful_slots": state["successful_slots"],
            "failed_slots": state["failed_slots"],
        }

    def locked_run(self, now: dt.datetime | None = None) -> dict[str, Any]:
        lock_path = self.settings.root / ".collector.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return {"status": "blocked", "reason": "duplicate_writer"}
            return self.run_due(now)


def loop(coordinator: Coordinator) -> None:
    while True:
        print(json.dumps(coordinator.locked_run()), flush=True)
        time.sleep(coordinator.settings.loop_seconds)
