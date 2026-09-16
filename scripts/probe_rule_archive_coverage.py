"""Measure whether free dated captures can attest a rule vintage for the candidates.

The primary graph is blocked because no record attests a contract's rule *version*.
The venue's historical endpoint returns the rule text, but it returns it *now*, and
an undated fetch cannot bound which version a 2025 contract carried. A capture with
a timestamp can. This script asks one question and answers it with counts: for how
many of the declared candidate contracts does a dated capture exist that would
include the contract's own rule text, and how many of those captures predate the
contract's decision date.

Three distinctions the measurement keeps apart, because collapsing them would
overstate the coverage:

* **Shape.** ``kalshi.com/markets/<ticker>`` is a client-rendered shell with no
  contract text in it. ``api.elections.kalshi.com/trade-api/v2/markets/<ticker>`` is
  JSON that carries ``rules_primary``. Only the second shape can attest anything, so
  captures are counted by shape and a page capture is never counted as text.
* **Date.** A capture dated after the decision date can attest the *terminal* text of
  a settled market, which is a weaker claim than text in force during the release
  window. A capture dated before it can bound the earlier side. Both counts are
  reported separately, and neither is silently promoted to "in force across the
  window", which is what the graph actually requires.
* **Reachability.** CDX rate-limits, times out, and returns 503s. A failed query is
  recorded as a failure, never as zero coverage, because the two imply opposite
  conclusions.

Run::

    uv run --no-sync python scripts/probe_rule_archive_coverage.py
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

CDX = "http://web.archive.org/cdx/search/cdx"

#: The two hosts that have ever served the market API, and the page host. The API
#: host moved, so a capture search that knows only the current name misses whatever
#: predates the move.
API_HOSTS = ("api.elections.kalshi.com", "trading-api.kalshi.com")
PAGE_HOSTS = ("kalshi.com", "www.kalshi.com")
API_PATH = "/trade-api/v2/markets"
PAGE_PATH = "/markets"

SERIES = ("FED", "FEDDECISION", "KXFED", "KXFEDDECISION")

#: The window worth searching. A capture has to fall between the earliest declared
#: contract's listing and now to be able to carry its text.
SEARCH_FROM = "20241101"
SEARCH_TO = "20260916"


def _load_panel_helpers():
    spec = importlib.util.spec_from_file_location(
        "build_forecast_panel", Path(__file__).with_name("build_forecast_panel.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def candidates() -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Every declared candidate with a readable predicate and its decision date."""
    module = _load_panel_helpers()
    graph_config = module._load(module.GRAPH_CONFIG)
    cohort_config = module._load(module.COHORT_CONFIG)
    months, _calendar = module.declared_calendar(graph_config)
    series = tuple(str(name) for name in cohort_config["policy_series"])
    markets = module.candidate_rows(series)
    known, refused = module.predicates(markets, months=months, rules={})
    rows = [
        {
            "ticker": ticker,
            "event_ticker": next(
                (m["event_ticker"] for m in markets if m["ticker"] == ticker), None
            ),
            "decision_date": predicate.decision_date.isoformat(),
        }
        for ticker, predicate in sorted(known.items())
    ]
    return rows, refused


def cdx(prefix: str, *, attempts: int = 3, pause: float = 4.0) -> dict[str, Any]:
    """Every capture under one URL prefix, or a recorded failure.

    A failure is returned as a failure. Reporting an exhausted retry budget as an
    empty capture list would turn "could not reach the archive" into "the archive has
    nothing", which is the opposite fact.
    """
    query = "?" + urllib.parse.urlencode(
        {
            "output": "json",
            "url": prefix,
            "fl": "timestamp,original,statuscode",
            "collapse": "urlkey",
            "from": SEARCH_FROM,
            "to": SEARCH_TO,
            "limit": "5000",
        }
    )
    last: str | None = None
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(
                CDX + query, headers={"User-Agent": "research/rule-vintage-coverage"}
            )
            with urllib.request.urlopen(request, timeout=60) as response:
                body = json.loads(response.read().decode("utf-8") or "[]")
            rows = body[1:] if body else []
            return {
                "prefix": prefix,
                "ok": True,
                "error": None,
                "captures": [
                    {"timestamp": row[0], "original": row[1], "status": row[2]} for row in rows
                ],
            }
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as error:
            last = f"{type(error).__name__}: {error}"
            time.sleep(pause * (attempt + 1))
    return {"prefix": prefix, "ok": False, "error": last, "captures": []}


def _ticker_from_url(url: str) -> str | None:
    """The contract ticker a capture URL names, when it names one."""
    tail = url.rstrip("/").split("/")[-1]
    if "?" in tail:
        return None
    if not tail or "." in tail or "=" in tail:
        return None
    upper = tail.upper()
    return upper if upper.startswith(SERIES) else None


def run(out_path: Path) -> dict[str, Any]:
    rows, refused = candidates()
    prefixes: list[dict[str, Any]] = []
    # Every prefix carries its scheme, and the series is suffixed with "-" so a
    # prefix match for ``KXFED`` cannot silently return ``KXFEDDECISION`` captures
    # and report them as a second series' coverage.
    for series in SERIES:
        for host in API_HOSTS:
            prefixes.append(cdx(f"https://{host}{API_PATH}/{series}-*"))
        for host in PAGE_HOSTS:
            prefixes.append(cdx(f"https://{host}{PAGE_PATH}/{series}-*"))

    api_captures: dict[str, list[dict[str, str]]] = {}
    page_captures: dict[str, list[dict[str, str]]] = {}
    for block in prefixes:
        if not block["ok"]:
            continue
        is_api = any(host in block["prefix"] for host in API_HOSTS)
        table = api_captures if is_api else page_captures
        for capture in block["captures"]:
            ticker = _ticker_from_url(str(capture["original"]))
            if ticker is None:
                continue
            record = {
                "timestamp": str(capture["timestamp"]),
                "url": str(capture["original"]),
                # The archived status matters: a capture of a 401 or a 404 is an
                # archived error page, not archived content, and counting it as
                # coverage would credit the archive with text it never stored.
                "status": str(capture.get("status") or ""),
            }
            bucket = table.setdefault(ticker, [])
            # ``kalshi.com`` and ``www.kalshi.com`` collapse to one urlkey, so the
            # same capture comes back from both prefixes. Recording it twice would
            # inflate every per-contract capture count.
            if record not in bucket:
                bucket.append(record)

    declared = {row["ticker"] for row in rows}
    # Captures that exist but belong to no declared candidate are reported rather
    # than dropped, so the artifact cannot be misread as "the archive holds nothing"
    # when what it holds is captures of other contracts.
    near_misses = sorted(
        [
            {
                "ticker": ticker,
                "shape": "market_api" if table is api_captures else "market_page",
                "captures": sorted(captures, key=lambda capture: capture["timestamp"]),
            }
            for table in (api_captures, page_captures)
            for ticker, captures in table.items()
            if ticker not in declared
        ],
        key=lambda item: (item["ticker"], item["shape"]),
    )

    coverage = []
    for row in rows:
        ticker = row["ticker"]
        decision = dt.date.fromisoformat(row["decision_date"])
        api = sorted(api_captures.get(ticker, []), key=lambda c: c["timestamp"])
        page = sorted(page_captures.get(ticker, []), key=lambda c: c["timestamp"])
        before = [c for c in api if c["timestamp"][:8] <= decision.strftime("%Y%m%d")]
        coverage.append(
            {
                **row,
                "api_captures": len(api),
                "api_captures_before_decision_date": len(before),
                "api_earliest": api[0]["timestamp"] if api else None,
                "api_latest": api[-1]["timestamp"] if api else None,
                "page_captures": len(page),
                "attests_text_before_decision": bool(before),
                "attests_text_at_all": bool(api),
            }
        )

    failed = [
        {"prefix": block["prefix"], "error": block["error"]}
        for block in prefixes
        if not block["ok"]
    ]
    payload = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "search_window": {"from": SEARCH_FROM, "to": SEARCH_TO},
        "queries": [
            {"prefix": b["prefix"], "ok": b["ok"], "captures": len(b["captures"])} for b in prefixes
        ],
        "failed_queries": failed,
        "candidate_contracts": len(rows),
        "predicates_refused": len(refused),
        "near_miss_captures": near_misses,
        "summary": {
            "with_any_api_capture": sum(1 for c in coverage if c["attests_text_at_all"]),
            "with_api_capture_before_decision_date": sum(
                1 for c in coverage if c["attests_text_before_decision"]
            ),
            "with_page_capture_only": sum(
                1 for c in coverage if c["page_captures"] and not c["api_captures"]
            ),
            "with_no_capture": sum(
                1 for c in coverage if not c["api_captures"] and not c["page_captures"]
            ),
            "queries_failed": len(failed),
            "near_miss_contracts": len(near_misses),
        },
        "coverage": coverage,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=".audit/study-v3/rule_archive_coverage.json")
    args = parser.parse_args()
    payload = run(Path(args.out))
    summary = payload["summary"]
    print(f"candidate contracts measured : {payload['candidate_contracts']}")
    print(f"CDX queries failed           : {summary['queries_failed']}")
    print(f"with any market-API capture  : {summary['with_any_api_capture']}")
    print(f"  ... dated before decision  : {summary['with_api_capture_before_decision_date']}")
    print(f"page captures only (no text) : {summary['with_page_capture_only']}")
    print(f"no capture at all            : {summary['with_no_capture']}")
    print(f"near-miss contracts recorded : {summary['near_miss_contracts']}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
