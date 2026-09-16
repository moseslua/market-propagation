"""Build the complete declared study panel across every development release.

The saved acceptance run bounded its extraction at 20,000 rows, so it reached one
release and produced a single-event panel that no paired comparison can be fitted
on. This script is the cohort-targeted path the execution plan asks for: it
selects a declared candidate population before the scan and carries every
declared event into the panel, so an event with no observation appears as a
missing cell rather than as an absence of evidence.

The candidate population is selected from **pre-event information only**. A
contract is a candidate for one release when it belongs to a declared policy
series and its own recorded listing interval covers the release instant. Activity
after the release never enters this test, because a universe chosen by what
traded afterwards is the selection the plan prohibits.

Run::

    uv run --no-sync python scripts/build_study_panel.py --out .audit/study-v2
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import duckdb
import yaml

from market_propagation.ingest.audit import series_of
from market_propagation.ingest.external_history import (
    KALSHI_VENUE,
    extract_trades,
    write_trades,
)
from market_propagation.ingest.external_inventory import load_inventory
from market_propagation.trade_panel import build_trade_panel, load_event_specs, load_panel_settings

CONFIG_PATH = "configs/external_history_v1.yaml"
DEFAULT_INVENTORY = ".audit/acceptance/first/inventory/inventory.json"
MARKETS_GLOB = "data/external/kalshi-trades/markets-*.parquet"
KALSHI_LAYER = "kalshi_trades"


def _config() -> dict:
    return yaml.safe_load(Path(CONFIG_PATH).read_text(encoding="utf-8"))


def _candidate_markets(policy_series: set[str]) -> list[dict]:
    """Every declared-series contract with the listing interval its archive records.

    The scan is bounded by the declared series in SQL and then filtered with
    :func:`market_propagation.ingest.audit.series_of`, so the series parser is the
    one the coverage audit already owns instead of a second copy here. Filtering
    on ``LIKE 'FED-%'`` alone would drop the sibling ``FEDDECISION`` series, which
    is a declared policy series and whose contracts are candidates in their own
    right.
    """
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    placeholders = ", ".join("?" for _ in policy_series)
    rows = con.execute(
        f"""
        SELECT ticker, open_time::VARCHAR AS open_time, close_time::VARCHAR AS close_time,
               status, title
        FROM read_parquet('{MARKETS_GLOB}')
        WHERE regexp_extract(ticker, '^[A-Z]+') IN ({placeholders})
        """,
        sorted(policy_series),
    ).fetchall()
    con.close()
    out: list[dict] = []
    for ticker, opened, closed, status, title in rows:
        if series_of(ticker) not in policy_series:
            continue
        out.append(
            {
                "ticker": ticker,
                "open_time": opened,
                "close_time": closed,
                "status": status,
                "title": title,
            }
        )
    return out


def _instant(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=".audit/study-v2")
    parser.add_argument("--inventory", default=DEFAULT_INVENTORY)
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    config = _config()
    policy_series = {str(name) for name in config["policy_series"]}
    window_start = dt.datetime.fromisoformat(config["extraction"]["window_start"])
    window_end = dt.datetime.fromisoformat(config["extraction"]["window_end"])

    events = load_event_specs(
        config["inputs"]["release_dataset"],
        rule_evidence_path=config["inputs"].get("rule_evidence_source"),
    )
    markets = _candidate_markets(policy_series)

    # Candidate selection, per release, from listing intervals alone. The grid is
    # keyed by release and reaches ``build_trade_panel`` as the denominator, so a
    # declared contract that never traded is a missing cell in the panel rather
    # than an absent pair.
    per_event: dict[str, tuple[tuple[str, str], ...]] = {}
    for event in events:
        selected = []
        for market in markets:
            opened = _instant(market["open_time"])
            closed = _instant(market["close_time"])
            if opened is None or closed is None:
                continue
            if opened <= event.event_time < closed:
                selected.append(market["ticker"])
        per_event[event.event_id] = tuple(
            (KALSHI_VENUE, ticker) for ticker in sorted(set(selected))
        )

    candidates = sorted({ticker for names in per_event.values() for _, ticker in names})
    inventory = load_inventory(args.inventory)
    extraction = extract_trades(
        config["inputs"]["root"],
        inventory,
        layer=KALSHI_LAYER,
        window_start=window_start,
        window_end=window_end,
        tickers=candidates,
    )
    trades_path = out_dir / "historical_trades.parquet"
    trades_ref = write_trades(extraction.trades, trades_path)

    panel = build_trade_panel(
        list(extraction.trades),
        list(events),
        settings=load_panel_settings(CONFIG_PATH),
        clock_mode=config["clock"]["mode"],
        candidates=per_event,
    )
    panel_path = out_dir / "trade_panel.parquet"
    panel_ref = panel.write(panel_path)

    summary = {
        "policy_series": sorted(policy_series),
        "candidate_markets_declared_series": len(markets),
        "candidates_per_event": {k: len(v) for k, v in sorted(per_event.items())},
        "candidate_grid_declared_pairs": sum(len(v) for v in per_event.values()),
        "candidate_union": len(candidates),
        "events_declared": len(events),
        "extraction": {
            "rows_scanned": extraction.rows_scanned,
            "trades": len(extraction.trades),
            "bounded": extraction.bounded,
            "max_rows_applied": extraction.max_rows_applied,
            "shards_read": len(extraction.shards_read),
            "flags": list(extraction.flags),
        },
        "panel": {
            "path": str(panel_path),
            "content_hash": panel_ref.content_hash,
            "rows": len(panel.rows),
            "counts": dict(panel.counts),
            "flags": list(panel.flags),
        },
        "trades": {"path": str(trades_path), "content_hash": trades_ref.content_hash},
    }
    (out_dir / "study_panel_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    print(json.dumps({k: summary[k] for k in ("candidate_union", "events_declared")}, indent=1))
    print(json.dumps(summary["extraction"], indent=1))
    print(json.dumps(summary["panel"]["counts"], indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
