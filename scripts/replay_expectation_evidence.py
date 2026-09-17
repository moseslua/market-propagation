"""Verify held release vintages separately from candidate consensus evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from market_propagation.ingest.expectations import (
    ExpectationRecord,
    ExpectationSourceError,
    load_release_facts,
)
from market_propagation.ingest.macro_releases import parse_release_payload
from market_propagation.storage import RawStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--releases", type=Path, default=Path("data/public/bls-normalized/releases.parquet")
    )
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    release_store = RawStore(args.releases.parent / "raw")
    facts = load_release_facts(args.releases)
    vintages = []
    for event_id, fact in sorted(facts.items()):
        receipt = release_store.receipts(raw_hash=fact.raw_hash)[0]
        payload = release_store.get(fact.raw_hash)
        source = receipt["source"]
        _, period, embargo, values, revisions, _, _, agreement = parse_release_payload(
            payload.decode("utf-8"),
            family_slug="cpi" if fact.family == "cpi" else "empsit",
            source_url=source,
            scheduled_at=fact.release_time,
        )
        normalized_period = dt.datetime.strptime(period.title(), "%B %Y").strftime("%Y-%m")
        if (
            normalized_period != fact.reference_period
            or agreement != "agrees_with_calendar"
            or values != fact.statistics
        ):
            raise ValueError(f"held release no longer replays to its sealed facts: {event_id}")
        vintages.append(
            {
                "event_id": event_id,
                "source_url": source,
                "raw_hash": fact.raw_hash,
                "raw_store": str(release_store.root),
                "reference_period": normalized_period,
                "source_reference_period_text": period,
                "release_time": fact.release_time.isoformat(),
                "embargo_time": embargo.isoformat() if embargo else None,
                "retrieved_at": receipt["received_time"],
                "first_print_values": values,
                "revision_values": revisions,
                "revision_scope": "revisions_explicitly_reported_in_this_original_release_only",
                "historical_live_receipt_claimed": False,
            }
        )
    source_manifest = json.loads((args.sources / "manifest.json").read_text())
    source_store = RawStore(args.sources / "raw")
    providers = []
    for provider in ("tradingeconomics_cpi", "investing_payroll", "econoday", "spf"):
        source = source_manifest[provider]
        if source["status"] != "fetched":
            providers.append({"provider": provider, **source})
            continue
        source_store.get(source["raw_hash"])
        # A public calendar page alone does not supply a dated historical poll.
        candidate = {
            "source_url": source["url"],
            "evidence_sha256": source["raw_hash"],
            "evidence_path": f"blobs/{source['raw_hash'][:2]}/{source['raw_hash']}.bin",
        }
        try:
            ExpectationRecord.from_mapping(candidate, where=provider)
        except ExpectationSourceError as error:
            validation = {"accepted": False, "reason": str(error)}
        else:
            raise AssertionError("an unbound calendar page must not validate as a forecast")
        providers.append(
            {
                "provider": provider,
                **source,
                "admissible_records_extracted": 0,
                "status_detail": "dated_historical_monthly_consensus_not_established_by_this_probe",
                "validator": validation,
                "requires": [
                    "exact_event_and_statistic",
                    "units",
                    "reference_month",
                    "forecast_value",
                    "named_consensus",
                    "demonstrable_pre_release_publication",
                ],
                "frequency_caveat": "quarterly_SPF_cannot_substitute_for_monthly_release_consensus"
                if provider == "spf"
                else None,
            }
        )
    result = {
        "d3a": {
            "state": "original_release_vintages_verified",
            "releases_verified": len(vintages),
            "releases_reporting_revisions": sum(bool(row["revision_values"]) for row in vintages),
            "vintages": vintages,
            "alfred_download_state": "not_acquired_direct_transport_timeout_form_only_visible_via_alternate_reader",
            "fresh_revision_source_attempts": {
                name: source_manifest[name] for name in ("alfred_form", "fred_cpi", "fred_payroll")
            },
        },
        "d3b": {
            "state": "pre_release_consensus_unverified",
            "providers": providers,
            "validated_expectations": 0,
        },
        "surprise_claimed": False,
        "news_or_network_gate_satisfied": False,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(
        json.dumps(
            {
                "d3a_state": result["d3a"]["state"],
                "verified_releases": len(vintages),
                "revisions": result["d3a"]["releases_reporting_revisions"],
                "d3b_state": result["d3b"]["state"],
                "validated_expectations": 0,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
