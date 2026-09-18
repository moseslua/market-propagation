"""Replay research evidence without admitting unverified inputs to the study."""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import hashlib
import json
import re
from collections import defaultdict
from decimal import Decimal
from html.parser import HTMLParser
from itertools import pairwise
from pathlib import Path

import pyarrow.parquet as pq

from market_propagation.ingest.expectations import load_release_facts
from market_propagation.ingest.rule_attestation import visible_text
from market_propagation.storage import RawStore, hash_file, read_parquet


class PublicationMetadata(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.values: dict[str, list[str]] = defaultdict(list)
        self.json_text: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta":
            key = attrs.get("property") or attrs.get("name")
            if key in {"article:published_time", "article:modified_time"}:
                self.values[key].append(attrs.get("content", ""))
        if tag == "script" and attrs.get("type") == "application/ld+json":
            self.json_text = []

    def handle_data(self, text):
        if self.json_text is not None:
            self.json_text.append(text)

    def handle_endtag(self, tag):
        if tag != "script" or self.json_text is None:
            return
        try:
            self.collect(json.loads("".join(self.json_text)))
        except json.JSONDecodeError:
            self.values["parse_refusals"].append("invalid_json_ld")
        self.json_text = None

    def collect(self, item):
        if isinstance(item, list):
            for value in item:
                self.collect(value)
        elif isinstance(item, dict):
            for key, value in item.items():
                if key in {"datePublished", "dateModified"}:
                    self.values[key].append(str(value))
                elif isinstance(value, (dict, list)):
                    self.collect(value)


def source_rows(root: Path) -> tuple[dict, dict[str, bytes]]:
    manifest = json.loads((root / "sources.json").read_text())
    raw = RawStore(root / "raw")
    bodies = {key: raw.get(row["raw_hash"]) for key, row in manifest.items() if row.get("raw_hash")}
    return manifest, bodies


def historical_rules(root: Path, panel) -> dict:
    manifest, bodies = source_rows(root / "rules")
    candidates = sorted(set(panel.contract_id))
    records = []
    for ticker in candidates:
        name = f"market:{ticker}"
        source = manifest.get(name)
        market = (
            json.loads(bodies[name]).get("market")
            if source and source["status"] == "fetched"
            else None
        )
        if market and market.get("ticker") != ticker:
            raise ValueError(f"source identity mismatch for {ticker}")
        primary = market.get("rules_primary") if market else None
        dates = re.findall(
            r"(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec) \d{1,2}, \d{4}",
            primary or "",
        )
        records.append(
            {
                "contract_id": ticker,
                "source": source,
                "raw_store": str(root / "rules/raw"),
                "rules_primary": primary,
                "rules_secondary": market.get("rules_secondary") if market else None,
                "reference_date_texts": dates,
                "semantic_evidence_state": "current_contract_text" if primary else "missing",
                "historical_version_state": "unverified",
                "reason": "no_independent_dated_interval_covering_analysis_window",
            }
        )
    pairs = panel.drop_duplicates(["event_id", "venue", "contract_id"])
    return {
        "contracts": len(candidates),
        "pairs": len(pairs),
        "primary_payloads": sum(bool(r["rules_primary"]) for r in records),
        "contract_bound_primary_text": records,
        "historical_intervals_admitted": 0,
        "archive_attempts": {k: v for k, v in manifest.items() if k.startswith("archive:")},
        "historical_absence_proved": False,
    }


def definition_changes(roots: list[Path]) -> dict:
    versions: dict[str, dict[str, dict]] = defaultdict(dict)
    verified_blobs = set()
    for root in roots:
        if not (root / "captures").is_dir():
            raise FileNotFoundError(f"missing capture directory: {root / 'captures'}")
        store = RawStore(root / "raw")
        pages = {}
        for path in sorted((root / "captures").glob("*/*.json")):
            capture = json.loads(path.read_text())
            digest = capture["raw_hash"]
            if digest not in pages:
                pages[digest] = {m["ticker"]: m for m in json.loads(store.get(digest))["markets"]}
                verified_blobs.add((str(root), digest))
            market = pages[digest][capture["contract_id"]]
            text = {key: market.get(key) for key in ("rules_primary", "rules_secondary")}
            rule_digest = hashlib.sha256(json.dumps(text, sort_keys=True).encode()).hexdigest()
            observed = capture.get("source_observed_at")
            row = versions[capture["contract_id"]].setdefault(
                rule_digest,
                {
                    "text_digest": rule_digest,
                    **text,
                    "observations": [],
                },
            )
            row["observations"].append(
                {
                    "source_observed_at": observed,
                    "retrieved_at": capture["captured_at"],
                    "raw_hash": digest,
                    "raw_store": str(store.root),
                    "capture": str(path),
                }
            )
    changed = []
    for ticker, observed_versions in sorted(versions.items()):
        if len(observed_versions) < 2:
            continue
        variants = sorted(
            observed_versions.values(),
            key=lambda r: min(o["retrieved_at"] for o in r["observations"]),
        )
        changed.append(
            {
                "contract_id": ticker,
                "versions": variants,
                "effective_change_time": None,
                "effective_change_time_reason": "only_observation_times_are_known",
            }
        )
    return {
        "contracts": len(versions),
        "verified_blobs": len(verified_blobs),
        "changed_contracts": changed,
        "substantive_change_requires_review": True,
    }


def forecast_candidates(root: Path, facts: dict) -> dict:
    manifest, bodies = source_rows(root / "expectations")
    rows = []
    by_period = {
        fact.reference_period: fact for fact in facts.values() if fact.family == "employment"
    }
    for name, body in bodies.items():
        if "factset" not in name or name.startswith("archive:"):
            continue
        metadata = PublicationMetadata()
        metadata.feed(body.decode("utf-8"))
        text = visible_text(body, content_type="text/html")
        matches = re.findall(
            r"The median estimate for total nonfarm payroll employment for the month of ([A-Za-z]+ \d{4}) is ([\d,]+)\.",
            text,
        )
        for month, value in matches:
            period = dt.datetime.strptime(month, "%B %Y").strftime("%Y-%m")
            if period not in by_period:
                continue
            fact = by_period[period]
            rows.append(
                {
                    "event_id": fact.event_id,
                    "reference_period": period,
                    "statistic": "payrolls_change_thousands",
                    "unit": "thousands_of_count",
                    "value": str(Decimal(value.replace(",", "")) / 1000),
                    "consensus_id": "FactSet",
                    "publication_metadata": metadata.values,
                    "source": manifest[name],
                    "raw_store": str(root / "expectations/raw"),
                    "quote": f"The median estimate for total nonfarm payroll employment for the month of {month} is {value}.",
                    "admitted": False,
                    "refusal": "pre_event_archive_or_licensed_point_in_time_route_not_established",
                }
            )
        cpi = re.search(
            r"The median estimate \(year-over-year, not seasonally adjusted\) for the consumer price index \(CPI\) for the month of ([A-Za-z]+ \d{4}) is ([\d.]+)%",
            text,
        )
        if cpi:
            period = dt.datetime.strptime(cpi[1], "%B %Y").strftime("%Y-%m")
            events = [
                f.event_id
                for f in facts.values()
                if f.family == "cpi" and f.reference_period == period
            ]
            if events:
                rows.append(
                    {
                        "event_id": events[0],
                        "reference_period": period,
                        "statistic": "cpi_headline_nsa_yoy_pct",
                        "unit": "percent_change",
                        "value": cpi[2],
                        "consensus_id": "FactSet",
                        "publication_metadata": metadata.values,
                        "source": manifest[name],
                        "raw_store": str(root / "expectations/raw"),
                        "quote": cpi[0],
                        "admitted": False,
                        "refusal": "wrong_statistic_for_declared_monthly_seasonally_adjusted_news_vector",
                    }
                )
    coverage = [
        {
            "event_id": key,
            "declared_statistic": "cpi_headline_sa_mom_pct"
            if f.family == "cpi"
            else "payrolls_change_thousands",
            "candidate_documents": sum(r["event_id"] == key for r in rows),
            "admitted_records": 0,
            "state": "unverified_source_route"
            if any(r["event_id"] == key for r in rows)
            else "no_admissible_record_found_in_bounded_discovery",
        }
        for key, f in sorted(facts.items())
    ]
    return {
        "scope": "initial_current_page_probe_only; independently_recovered_archives_are_replayed_separately",
        "events": coverage,
        "candidate_records": rows,
        "admitted_expectations": 0,
        "source_attempts": manifest,
        "surprise_estimated": False,
    }


def revision_diagnostics(path: Path) -> dict:
    dataset = read_parquet(path, table="releases")
    rows = []
    release_outcomes = []
    for row in dataset.to_dict("records"):
        if row["family"] != "employment":
            continue
        revisions = row["revisions_json"]
        revisions = json.loads(revisions) if isinstance(revisions, str) else revisions
        release = dt.datetime.fromisoformat(str(row["scheduled_at"]))
        before = len(rows)
        for month in calendar.month_name[1:]:
            old = revisions.get(f"payrolls_change_thousands_prior_{month}")
            new = revisions.get(f"payrolls_change_thousands_revised_{month}")
            if (old is None) != (new is None):
                raise ValueError(f"unmatched prior/revised {month} values for {row['event_id']}")
            if old is None:
                continue
            month_number = list(calendar.month_name).index(month)
            year = release.year - (month_number >= release.month)
            rows.append(
                {
                    "event_id": row["event_id"],
                    "revised_reference_period": f"{year:04d}-{month_number:02d}",
                    "prior_thousands": str(old),
                    "revised_thousands": str(new),
                    "revision_thousands": str(Decimal(str(new)) - Decimal(str(old))),
                    "release_raw_hash": row["raw_hash"],
                }
            )
        steps = len(rows) - before
        release_outcomes.append(
            {
                "event_id": row["event_id"],
                "revision_steps": steps,
                "state": "reported_revision_pairs" if steps else "no_revision_pairs_reported",
            }
        )
    changes = [Decimal(row["revision_thousands"]) for row in rows]
    return {
        "revision_events": len(rows),
        "input_employment_releases": len(release_outcomes),
        "releases_with_revision_steps": len({r["event_id"] for r in rows}),
        "release_outcomes": release_outcomes,
        "rows": rows,
        "mean_absolute_revision_thousands": str(sum(map(abs, changes)) / len(changes))
        if changes
        else None,
        "largest_absolute_revision_thousands": str(max(map(abs, changes))) if changes else None,
        "scope": "reported_revision_steps_not_independent_errors_or_complete_vintage_history",
    }


def coverage_diagnostics(panel) -> dict:
    rows = []
    for (event, horizon), group in panel.groupby(["event_id", "horizon_seconds"], sort=True):
        baseline, endpoint = group.baseline.notna(), group.endpoint.notna()
        rows.append(
            {
                "event_id": event,
                "horizon_seconds": int(horizon),
                "pairs": len(group),
                "observed_baselines": int(baseline.sum()),
                "observed_endpoints": int(endpoint.sum()),
                "both_observed": int((baseline & endpoint).sum()),
                "valid_rows": int(group.valid.sum()),
            }
        )
    return {"rows": rows, "scope": "observation_attrition_only_no_masked_response_estimation"}


def signed_pair_funding(root: Path) -> dict:
    if not root.is_dir():
        raise FileNotFoundError(f"missing perp snapshot directory: {root}")
    observations = []
    missing = []
    for path in sorted(root.glob("*.parquet")):
        selected = [
            row
            for row in pq.read_table(path).to_pylist()
            if row["symbol"] == "BTCUSDT" and row["venue"] in {"binance", "bybit"}
        ]
        quotes = {row["venue"]: row for row in selected}
        if len(quotes) != len(selected):
            raise ValueError(f"duplicate venue/instrument identity in {path}")
        if len(quotes) != 2 or any(row["funding_apr"] is None for row in quotes.values()):
            missing.append(
                {"path": str(path), "sha256": hash_file(path), "reason": "both_named_APRs_required"}
            )
            continue
        if quotes["binance"]["build_time"] != quotes["bybit"]["build_time"]:
            raise ValueError(f"pair is not from one source build in {path}")
        spread = quotes["bybit"]["funding_apr"] - quotes["binance"]["funding_apr"]
        observations.append(
            {
                "path": str(path),
                "sha256": hash_file(path),
                "build_time": quotes["binance"]["build_time"].isoformat(),
                "binance_apr": str(quotes["binance"]["funding_apr"]),
                "bybit_apr": str(quotes["bybit"]["funding_apr"]),
                "signed_apr_spread": str(spread),
            }
        )
    signs = [
        1
        if Decimal(r["signed_apr_spread"]) > 0
        else -1
        if Decimal(r["signed_apr_spread"]) < 0
        else 0
        for r in observations
    ]
    return {
        "instrument": "BTCUSDT",
        "orientation": "bybit_apr_minus_binance_apr",
        "observations": observations,
        "missing": missing,
        "positive": signs.count(1),
        "negative": signs.count(-1),
        "zero": signs.count(0),
        "observed_adjacent_sign_reversals": sum(a * b < 0 for a, b in pairwise(signs))
        if len(signs) >= 2
        else None,
        "scope": "signed_quotes_at_observed_builds_not_funding_earned_between_builds",
        "legacy_sign_stability_is_informative": False,
        "legacy_reason": "funding_differentials_reorders_long_and_short_each_build_so_spread_is_nonnegative_by_construction",
        "net_return_or_capacity_claimed": False,
    }


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument(
        "--panel", type=Path, default=repo / ".audit/e2e/studypanel/trade_panel.parquet"
    )
    parser.add_argument(
        "--releases", type=Path, default=repo / "data/public/bls-normalized/releases.parquet"
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--perp-root", type=Path, default=repo / "data/perp/market/crypto/BTC")
    parser.add_argument("--rule-root", type=Path, action="append")
    args = parser.parse_args()
    panel = read_parquet(args.panel, table="trade_panel")
    facts = load_release_facts(args.releases)
    report = {
        "report_version": "research_evidence_v1",
        "input_hashes": {str(path): hash_file(path) for path in (args.panel, args.releases)},
        "rules": historical_rules(args.root, panel),
        "definition_changes": definition_changes(
            args.rule_root or [repo / "rules", repo / "data/prospective/rules"]
        ),
        "expectations": forecast_candidates(args.root, facts),
        "revisions": revision_diagnostics(args.releases),
        "coverage": coverage_diagnostics(panel),
        "fixed_pair_funding": signed_pair_funding(args.perp_root),
        "confirmatory_claim_eligible": False,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(
        json.dumps(
            {
                "historical_contracts": report["rules"]["contracts"],
                "primary_payloads": report["rules"]["primary_payloads"],
                "historical_intervals_admitted": 0,
                "changed_rule_contracts": len(report["definition_changes"]["changed_contracts"]),
                "forecast_candidates": len(report["expectations"]["candidate_records"]),
                "current_page_candidates_admitted": 0,
                "revision_events": report["revisions"]["revision_events"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
