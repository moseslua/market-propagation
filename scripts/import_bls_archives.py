import argparse
import json
from datetime import datetime
from pathlib import Path

from market_propagation.domain import Availability, Clock, Release
from market_propagation.ingest.macro_releases import parse_release_payload
from market_propagation.storage import RawStore, write_parquet

parser = argparse.ArgumentParser(
    description="Import complete BLS browser response captures with provenance."
)
parser.add_argument("source", type=Path)
parser.add_argument("output", type=Path)
args = parser.parse_args()
store = RawStore(args.output / "raw")
releases = []
for receipt_path in sorted(args.source.glob("*.json")):
    receipt = json.loads(receipt_path.read_text())
    body = (args.source / (receipt["event_id"] + ".html")).read_bytes()
    if (
        receipt["status"] != 200
        or not receipt["payload_complete"]
        or body.rstrip()[-7:].lower() != b"</html>"
    ):
        raise ValueError(f"Incomplete original payload: {receipt_path}")
    received = datetime.fromisoformat(receipt["received_time"])
    scheduled = datetime.fromisoformat(receipt["scheduled_at"])
    provenance = store.put(
        body,
        source=receipt["source_url"],
        received_time=received,
        record_id=receipt["event_id"],
        metadata=receipt,
    )
    title, period, embargo, values, revisions, statements, usdl, agreement = parse_release_payload(
        body.decode("utf-8"),
        family_slug="cpi" if receipt["family"] == "cpi" else "empsit",
        source_url=receipt["source_url"],
        scheduled_at=scheduled,
        provenance=provenance,
    )
    if not values or agreement != "agrees_with_calendar":
        raise ValueError(f"Unverified release values or schedule: {receipt_path}: {agreement}")
    clock = Clock(embargo, received, Availability.unknown(basis="late_archived_browser_capture"))
    releases.append(
        Release(
            receipt["event_id"],
            receipt["family"],
            scheduled,
            receipt["reference_period"],
            values,
            clock,
            provenance,
            revisions,
        )
    )
    print(
        json.dumps(
            {
                "event_id": receipt["event_id"],
                "values": values,
                "revisions": revisions,
                "raw_hash": provenance.raw_hash,
            },
            default=str,
        ),
        flush=True,
    )
write_parquet(
    releases,
    args.output / "releases.parquet",
    table="releases",
    coverage_epoch="original_bls_browser_captures",
)
