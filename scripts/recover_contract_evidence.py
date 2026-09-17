"""Build a complete historical provenance gap queue and probe discovered sources.

Current metadata is retained as a discovery lead, never as a historical rule
version. Raw responses are kept by the existing content-addressed store. Replay
rebuilds the queue from the saved attempts without issuing requests.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path
from urllib.parse import quote, urlparse

from market_propagation.ingest.audit import RuleVersionEvidence, series_of
from market_propagation.ingest.kalshi_rest import KALSHI_BASE_URL
from market_propagation.ingest.transport import HttpTransport, RetryPolicy, TransportError
from market_propagation.storage import RawStore, hash_file, read_parquet

FIELDS = ("reference_period", "settlement_criterion", "historical_validity_interval")


def urls_in(value: object) -> set[str]:
    if isinstance(value, dict):
        return set().union(*(urls_in(child) for child in value.values()), set())
    if isinstance(value, list):
        return set().union(*(urls_in(child) for child in value), set())
    if isinstance(value, str) and value.startswith("https://") and " " not in value:
        return {value}
    return set()


def probe(transport: HttpTransport, url: str, *, kind: str, parent: str) -> dict:
    result = {"url": url, "kind": kind, "discovered_from": parent}
    try:
        envelope = transport.get(url, source="evidence_recovery")
    except TransportError as error:
        return {
            **result,
            "status": "blocked",
            "reason": error.reason,
            "detail": str(error),
            "http_status": error.status_code,
            "raw_hash": error.payload_hash,
        }
    result.update(
        status="fetched",
        raw_hash=envelope.provenance.raw_hash,
        retrieved_at=envelope.received_time.isoformat(),
        source_observed_at=envelope.server_date.isoformat() if envelope.server_date else None,
        effective_at=None,
        effective_at_reason="response_date_is_not_a_rule_effective_date",
        content_type=envelope.content_type,
    )
    return result


def acquire(root: Path, contracts: list[str], count: int, archive_limit: int) -> dict:
    store = RawStore(root / "raw")
    attempts: list[dict] = []
    selected = []
    # Cover distinct declared series before taking a second contract from one.
    for series in sorted({series_of(ticker) for ticker in contracts}):
        selected.extend(ticker for ticker in contracts if series_of(ticker) == series)
    representatives = [
        next(ticker for ticker in selected if series_of(ticker) == series)
        for series in sorted({series_of(ticker) for ticker in selected})
    ]
    selected = (representatives + [x for x in selected if x not in representatives])[:count]
    links: dict[str, str] = {}
    with HttpTransport(
        store,
        timeout_seconds=15,
        policy=RetryPolicy(attempts=1, min_interval_seconds=0.3),
    ) as transport:
        for ticker in selected:
            url = f"{KALSHI_BASE_URL}/historical/markets/{quote(ticker, safe='')}"
            attempt = probe(
                transport, url, kind="market", parent="documented_historical_market_endpoint"
            )
            attempt["contract_id"] = ticker
            attempts.append(attempt)
            if attempt["status"] != "fetched":
                continue
            payload = json.loads(store.get(attempt["raw_hash"]))
            market = payload.get("market", {})
            if market.get("ticker") != ticker:
                attempt.update(status="refused", reason="response_contract_identity_mismatch")
                continue
            attempt["rule_text_present"] = bool(market.get("rules_primary"))
            attempt["semantic_fields"] = {field: market.get(field) for field in FIELDS[:2]}
            links.update(dict.fromkeys(urls_in(market), url))
            event = market.get("event_ticker")
            if event:
                event_url = f"{KALSHI_BASE_URL}/events/{quote(event, safe='')}"
                event_attempt = probe(transport, event_url, kind="event", parent=url)
                attempts.append(event_attempt)
                if event_attempt["status"] == "fetched":
                    links.update(
                        dict.fromkeys(
                            urls_in(json.loads(store.get(event_attempt["raw_hash"]))), event_url
                        )
                    )
        for series in sorted({series_of(ticker) for ticker in selected}):
            url = f"{KALSHI_BASE_URL}/series/{quote(series, safe='')}"
            attempt = probe(transport, url, kind="series", parent="declared_policy_series")
            attempts.append(attempt)
            if attempt["status"] == "fetched":
                links.update(
                    dict.fromkeys(urls_in(json.loads(store.get(attempt["raw_hash"]))), url)
                )
        official = {
            url: parent
            for url, parent in links.items()
            if (urlparse(url).hostname or "").endswith((".kalshi.com", ".federalreserve.gov"))
            or urlparse(url).hostname in {"kalshi.com", "www.federalreserve.gov"}
        }
        for url, parent in sorted(official.items())[:12]:
            attempts.append(probe(transport, url, kind="linked_document", parent=parent))
        # Archive only exact URLs discovered in primary metadata, plus the exact
        # documented endpoints actually fetched above; never invent page slugs.
        archive_urls = list(
            dict.fromkeys(
                [attempt["url"] for attempt in attempts if attempt["kind"] in {"market", "event"}]
                + sorted(official)
            )
        )
        for url in archive_urls[:archive_limit]:
            archive_url = "https://web.archive.org/cdx/search/cdx?" + (
                f"url={quote(url, safe='')}&output=json&filter=statuscode:200"
                "&fl=timestamp,original,statuscode&limit=100"
            )
            attempt = probe(transport, archive_url, kind="archive_index", parent=url)
            if attempt["status"] == "fetched":
                try:
                    records = json.loads(store.get(attempt["raw_hash"]))
                    if not isinstance(records, list):
                        raise ValueError("archive response is not an array")
                    attempt["archive_rows"] = max(0, len(records) - 1)
                    attempt["search_complete"] = len(records) < 101
                except (ValueError, TypeError) as error:
                    attempt.update(status="refused", reason=f"archive_shape_invalid: {error}")
            attempts.append(attempt)
    return {
        "acquired_at": dt.datetime.now(dt.UTC).isoformat(),
        "contracts_probed": selected,
        "discovered_links": official,
        "attempts": attempts,
        "historical_attestations_created": 0,
        "limit": "bounded_discovery_not_an_exhaustive_historical_absence_claim",
    }


def build_queue(panel_path: Path, rule_path: Path, acquisition: dict) -> tuple[list[dict], dict]:
    panel = read_parquet(panel_path, table="trade_panel")
    pairs = panel.drop_duplicates(["event_id", "venue", "contract_id"])
    rules: dict[str, list[RuleVersionEvidence]] = {}
    if rule_path.exists():
        for item in json.loads(rule_path.read_text())["contract_rules"]:
            record = RuleVersionEvidence(
                **{
                    key: (dt.datetime.fromisoformat(value) if value is not None else None)
                    if key in {"in_force_from", "in_force_to", "observed_at"}
                    else value
                    for key, value in item.items()
                    if key in RuleVersionEvidence.__dataclass_fields__
                }
            )
            rules.setdefault(record.contract_id, []).append(record)
    attempts_by_contract = {
        a["contract_id"]: a for a in acquisition.get("attempts", []) if a.get("contract_id")
    }
    rows = []
    time_covered = 0
    for pair in pairs.sort_values(["event_id", "contract_id"]).to_dict("records"):
        ticker = pair["contract_id"]
        at = pair["event_time"].to_pydatetime()
        horizon = float(
            panel.loc[
                (panel.event_id == pair["event_id"]) & (panel.contract_id == ticker),
                "horizon_seconds",
            ].max()
        )
        # This is a temporal prerequisite, not hash certification: the historical
        # row must additionally bind the rule hash that was in force at the time.
        covers = any(
            r.applies_to(rule_hash=r.rule_hash, at=at - dt.timedelta(seconds=1800))
            and r.applies_to(rule_hash=r.rule_hash, at=at + dt.timedelta(seconds=horizon))
            for r in rules.get(ticker, [])
        )
        time_covered += covers
        attempt = attempts_by_contract.get(ticker, {})
        for field in FIELDS:
            rows.append(
                {
                    "contract_id": ticker,
                    "event_id": pair["event_id"],
                    "release_time": at.isoformat(),
                    "missing_field": field,
                    "required_evidence": "contract_bound_primary_text_and_dated_version_covering_window",
                    "candidate_source": attempt.get("url"),
                    "attempted_retrieval": attempt.get("status", "not_probed_in_bounded_run"),
                    "raw_hash": attempt.get("raw_hash"),
                    "value_in_current_payload": attempt.get("semantic_fields", {}).get(field),
                    "temporal_prerequisite_met": covers,
                    "status": "unverified_historical_rule_version",
                    "reason": "current_metadata_does_not_attest_its_past_version",
                }
            )
    return rows, {
        "panel_path": str(panel_path),
        "panel_sha256": hash_file(panel_path),
        "pairs": len(pairs),
        "contracts": int(pairs.contract_id.nunique()),
        "releases": int(pairs.event_id.nunique()),
        "gap_rows": len(rows),
        "pairs_with_temporal_prerequisite": time_covered,
        "historical_claim_eligible": False,
        "historical_estimand": "unidentified_from_available_provenance",
        "d4_d7_input_change": False,
        "d4_d7_reason": "no_new_historical_rule_version_attested",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--panel", type=Path, default=Path(".audit/e2e/studypanel/trade_panel.parquet")
    )
    parser.add_argument("--rules", type=Path, default=Path("rules/attested_contract_rules.json"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--probe-contracts", type=int, default=4)
    parser.add_argument("--archive-limit", type=int, default=6)
    args = parser.parse_args()
    if not 1 <= args.probe_contracts <= 20 or not 0 <= args.archive_limit <= 20:
        parser.error("probe-contracts must be 1..20 and archive-limit must be 0..20")
    args.out.mkdir(parents=True, exist_ok=True)
    acquisition_path = args.out / "acquisition.json"
    if args.fetch:
        if acquisition_path.exists():
            parser.error(
                "use a fresh output directory for a new acquisition; omit --fetch to replay"
            )
        panel = read_parquet(args.panel, table="trade_panel")
        acquisition = acquire(
            args.out, sorted(set(panel.contract_id)), args.probe_contracts, args.archive_limit
        )
        acquisition_path.write_text(json.dumps(acquisition, indent=2) + "\n")
    else:
        acquisition = json.loads(acquisition_path.read_text()) if acquisition_path.exists() else {}
        store = RawStore(args.out / "raw")
        for attempt in acquisition.get("attempts", []):
            if attempt.get("raw_hash"):
                store.get(attempt["raw_hash"])
    rows, summary = build_queue(args.panel, args.rules, acquisition)
    with (args.out / "gap_queue.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
