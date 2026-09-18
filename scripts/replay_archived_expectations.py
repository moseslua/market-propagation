"""Verify archived forecast quotations and replay descriptive first-print surprises."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit

from research_evidence_report import PublicationMetadata

from market_propagation.ingest.expectations import (
    ExpectationRecord,
    declared_unit,
    load_expectations,
    load_release_facts,
    surprise,
    validate_expectation,
)
from market_propagation.ingest.rule_attestation import visible_text
from market_propagation.storage import RawStore, hash_file

STATISTICS = {
    "cpi": "cpi_headline_sa_mom_pct",
    "employment": "payrolls_change_thousands",
}
REVIEWED_CPI_DEFINITIONS = {
    "cpi_2025_01": (
        "2024-12",
        "Dow Jones",
        "https://www.cnbc.com/2025/01/15/cpi-inflation-december-2024-.html",
        "927c27aedc5f622837ffef2049eefba464426c184394fbe253632ed4f186dc7d",
    ),
    "cpi_2025_02": (
        "2025-01",
        "Dow Jones",
        "https://www.cnbc.com/2025/02/12/cpi-january-2025.html",
        "46a22676b957c13816014588597356568d16928ef06aa02d9557ad1aec7b0cb2",
    ),
    "cpi_2025_03": (
        "2025-02",
        "Dow Jones",
        "https://www.cnbc.com/2025/03/12/cpi-inflation-report-february-2025.html",
        "1ad1c1b0917f09057e67a446256e3bea8e34dce6252057bfb1e62f4ea10a06cc",
    ),
}


def publication_instants(body: bytes) -> tuple[dt.datetime | None, dt.datetime | None]:
    metadata = PublicationMetadata()
    metadata.feed(body.decode("utf-8"))
    result = []
    for primary, fallback in (
        ("article:published_time", "datePublished"),
        ("article:modified_time", "dateModified"),
    ):
        instants = set()
        for raw in metadata.values.get(primary) or metadata.values.get(fallback, []):
            parsed = dt.datetime.fromisoformat(raw)
            if parsed.tzinfo is not None:
                instants.add(parsed.astimezone(dt.UTC))
        if len(instants) > 1:
            raise ValueError("publication metadata names conflicting instants")
        result.append(instants.pop() if instants else None)
    return result[0], result[1]


def archived_record(candidate: dict, facts, evidence_root: Path) -> dict:
    if candidate["reference_period"] != facts.reference_period:
        raise ValueError("reference_period_does_not_match_the_release")
    if not candidate["consensus_id"].strip():
        raise ValueError("consensus_identity_is_not_named")
    source_root = (evidence_root / candidate["source_root"]).resolve()
    if not source_root.is_relative_to(evidence_root.resolve()):
        raise ValueError("source root escapes the evidence root")
    store = RawStore(source_root / "raw")
    index_body = store.get(candidate["index_raw_hash"]).decode("utf-8")
    index = json.loads(index_body.partition("Markdown Content:\n")[2].strip())
    if index["url"] != candidate["original_url"]:
        raise ValueError("archive index names a different source")
    snapshot = index["archived_snapshots"]["closest"]
    if snapshot["status"] != "200" or snapshot["available"] is not True:
        raise ValueError("archive index does not identify an available response")
    parts = urlsplit(snapshot["url"])
    if (
        parts.hostname != "web.archive.org"
        or parts.path != f"/web/{snapshot['timestamp']}/{candidate['original_url']}"
    ):
        raise ValueError("archive URL does not bind the original URL and capture time")
    observed = dt.datetime.strptime(snapshot["timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=dt.UTC)
    if observed >= facts.release_time:
        raise ValueError("archive capture is not strictly before the release")
    body = store.get(candidate["raw_hash"])
    wrapper = (
        r"__wm\.wombat\("
        + re.escape(json.dumps(candidate["original_url"]))
        + r"\s*,\s*"
        + re.escape(json.dumps(snapshot["timestamp"]))
    )
    if not re.search(wrapper, body.decode("utf-8")):
        raise ValueError("served archive wrapper does not match the indexed capture")
    text = " ".join((visible_text(body, content_type="text/html") or "").split())
    quote = " ".join(candidate["quote"].split())
    attribution = " ".join(candidate.get("consensus_quote", quote).split())
    if (
        quote not in text
        or attribution not in text
        or candidate["consensus_token"] not in attribution
    ):
        raise ValueError("named consensus quotation is absent from the archived text")
    try:
        match = re.fullmatch(candidate["value_pattern"], quote)
        if match is None:
            raise ValueError("forecast value cannot be extracted from the exact quotation")
        value = Decimal(match["value"].replace(",", ""))
    except (re.error, IndexError, InvalidOperation) as error:
        raise ValueError(f"invalid forecast value extraction: {error}") from error
    if not value.is_finite():
        raise ValueError("forecast value must be finite")
    publication, modified = publication_instants(body)
    if (publication and publication > observed) or (modified and modified > observed):
        raise ValueError("publication metadata postdates the archive capture")
    if publication and modified and publication > modified:
        raise ValueError("publication metadata postdates the modification instant")
    statistic = STATISTICS[facts.family]
    if facts.family == "employment":
        value /= 1000
    evidence_path = (
        source_root / "raw/blobs" / candidate["raw_hash"][:2] / f"{candidate['raw_hash']}.bin"
    )
    record = {
        "event_id": facts.event_id,
        "statistic": statistic,
        "unit": declared_unit(statistic),
        "reference_period": candidate["reference_period"],
        "value": str(value),
        "published_at": (publication or observed).isoformat(),
        "source_kind": "archived_forecast",
        "revision_status": "initial",
        "consensus_id": candidate["consensus_id"],
        "verified_by": "pre-release Wayback index and wrapper, exact attributed forecast quotation, and source hash; published_at uses source metadata when timezone-qualified, otherwise a conservative archive bound for descriptive surprise only",
        "source_url": snapshot["url"],
        "evidence_path": str(evidence_path.resolve().relative_to(evidence_root.resolve())),
        "evidence_sha256": candidate["raw_hash"],
        "archive_observed_at": observed.isoformat(),
        "original_publication_instant": publication.isoformat() if publication else None,
        "source_modified_at": modified.isoformat() if modified else None,
        "archive_index_sha256": candidate["index_raw_hash"],
        "original_url": candidate["original_url"],
        "archived_quote": quote,
        "consensus_quote": attribution,
        "statistic_mapping_basis": candidate["statistic_mapping_basis"],
        "timing_basis": "source_publication_metadata_with_independent_pre_release_archive"
        if publication
        else "archive_capture_upper_bound",
        "historical_live_receipt_claimed": False,
        "publication_or_latency_measurement_eligible": False,
    }
    if facts.family == "cpi":
        definition = candidate.get("seasonal_adjustment_evidence")
        if definition is None:
            return {
                "parsed_forecast": {
                    **record,
                    "statistic": None,
                    "required_statistic": statistic,
                    "seasonal_adjustment": "unknown",
                },
                "reason": "forecast_seasonal_adjustment_unverified",
            }
        if (
            definition["event_id"] != facts.event_id
            or definition["reference_period"] != facts.reference_period
        ):
            raise ValueError("statistic definition names a different release")
        binding = (
            definition["reference_period"],
            candidate["consensus_token"],
            definition["url"],
            definition["raw_hash"],
        )
        if REVIEWED_CPI_DEFINITIONS.get(facts.event_id) != binding:
            raise ValueError(
                "statistic definition does not match the reviewed event/poll/source binding"
            )
        definition_root = (evidence_root / definition["source_root"]).resolve()
        if not definition_root.is_relative_to(evidence_root.resolve()):
            raise ValueError("statistic definition root escapes the evidence root")
        definition_store = RawStore(definition_root / "raw")
        definition_body = definition_store.get(definition["raw_hash"])
        receipts = [
            receipt
            for receipt in definition_store.receipts(raw_hash=definition["raw_hash"])
            if receipt.get("metadata", {}).get("request_url") == definition["url"]
            and receipt.get("metadata", {}).get("http_status") == 200
        ]
        if not receipts:
            raise ValueError("statistic definition has no successful receipt for its source URL")
        definition_text = " ".join(
            (visible_text(definition_body, content_type="text/html") or "").split()
        )
        if (
            definition["quote"] not in definition_text
            or "seasonally adjusted" not in definition["quote"]
            or "not seasonally adjusted" in definition["quote"]
            or candidate["consensus_token"] not in definition["quote"]
        ):
            raise ValueError(
                "statistic definition does not bind seasonal adjustment and the named poll"
            )
        record["statistic_mapping_evidence"] = definition
    validate_expectation(
        ExpectationRecord.from_mapping(record, where=facts.event_id),
        facts,
        evidence_root=evidence_root,
    )
    return {"record": record}


def replay(registry: Path, releases: Path, evidence_root: Path, output: Path) -> dict:
    facts = load_release_facts(releases)
    release_store = RawStore(releases.parent / "raw")
    for fact in facts.values():
        release_store.get(fact.raw_hash)
    candidates = json.loads(registry.read_text())
    event_ids = [row["event_id"] for row in candidates]
    if len(event_ids) != len(set(event_ids)) or set(event_ids) - facts.keys():
        raise ValueError("registry contains duplicate or unknown release identities")
    selected = {row["event_id"]: row for row in candidates}
    records = []
    outcomes = []
    for event_id, fact in sorted(facts.items()):
        row = {"event_id": event_id, "reference_period": fact.reference_period}
        candidate = selected.get(event_id)
        if candidate is None:
            row.update(state="uncovered", reason="no_selected_archived_forecast")
        else:
            try:
                result = archived_record(candidate, fact, evidence_root)
            except (ValueError, KeyError, OSError) as error:
                row.update(state="refused", reason=str(error), candidate=candidate)
            else:
                if "record" in result:
                    records.append(result["record"])
                    row.update(state="validated", **result)
                else:
                    row.update(state="semantic_refusal", **result)
        outcomes.append(row)
    output.mkdir(parents=True, exist_ok=True)
    for family, statistic in STATISTICS.items():
        family_records = [r for r in records if facts[r["event_id"]].family == family]
        path = output / f"{family}-expectations.json"
        path.write_text(json.dumps({"records": family_records}, indent=2) + "\n")
        loaded = load_expectations(
            path, facts=facts, statistics=[statistic], evidence_root=evidence_root
        )
        for row in outcomes:
            if row["event_id"] in loaded:
                fact = facts[row["event_id"]]
                expected = loaded[row["event_id"]][statistic]
                row.update(
                    first_print=str(fact.first_print(statistic)),
                    surprise=str(surprise(expected, fact)),
                    first_print_raw_hash=fact.raw_hash,
                )
    report = {
        "registry": str(registry),
        "registry_sha256": hash_file(registry),
        "releases": str(releases),
        "releases_sha256": hash_file(releases),
        "evidence_root": str(evidence_root.resolve()),
        "declared_releases": len(facts),
        "validated_expectations": len(records),
        "archived_forecasts_recovered": sum(
            row["state"] in {"validated", "semantic_refusal"} for row in outcomes
        ),
        "all_release_consensus_complete": len(records) == len(facts),
        "outcomes": outcomes,
        "scope": "source_specific_descriptive_first_print_surprises",
        "selection": "first verified reachable source per release; heterogeneous polls and vintages; not a fixed-provider latest-consensus series",
        "frozen_protocol_modified": False,
        "confirmatory_claim_eligible": False,
        "propagation_fit_performed": False,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument(
        "--releases", type=Path, default=Path("data/public/bls-normalized/releases.parquet")
    )
    parser.add_argument("--evidence-root", type=Path, default=Path("."))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    report = replay(args.registry, args.releases, args.evidence_root, args.out)
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "declared_releases",
                    "validated_expectations",
                    "all_release_consensus_complete",
                )
            },
            indent=2,
        )
    )
    return 0 if report["all_release_consensus_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
