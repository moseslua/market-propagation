"""Build the source-time forecast panel and the exposures behind every row.

The forecast table is the propagation study's own input, and three things decide
whether it means anything:

* which contracts are receivers, decided per release from the declared candidate
  grid before the release and never from what traded afterwards;
* which donor each receiver has, decided by the declared decision calendar and the
  contracts' own published predicates;
* which rule version each contract carried, which no record on this checkout
  attests.

This script builds all three and writes down every refusal. The graph is built over
real contracts with real predicates read from the venue's own archived text, so the
receivers it refuses are refused for a reason a reader can check, and the reason is
recorded per contract rather than collapsed into an absent graph.

Run::

    uv run --no-sync python scripts/build_forecast_panel.py --out .audit/study-v3
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import duckdb
import yaml

from market_propagation.historical_forecast import (
    ForecastSettings,
    build_forecast_rows,
)
from market_propagation.ingest.external_history import load_trades
from market_propagation.ingest.policy_predicates import (
    PredicateError,
    decision_date_for,
    parse_predicate,
)
from market_propagation.neighbors import ContractPredicate, build_neighbor_graph, graph_digest
from market_propagation.storage import hash_file, read_parquet, write_parquet

GRAPH_CONFIG = "configs/neighbor_graph_v2.yaml"
COHORT_CONFIG = "configs/cohort_v2.yaml"
STUDY_CONFIG = "configs/study_v2.yaml"
PIPELINE_CONFIG = "configs/external_history_v1.yaml"
MARKETS_GLOB = "data/external/kalshi-trades/markets-*.parquet"
KALSHI_VENUE = "kalshi"


def _load(path: str) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def _instant(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    parsed = dt.datetime.fromisoformat(str(text))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def declared_calendar(
    graph_config: dict[str, Any],
) -> tuple[dict[tuple[int, int], dt.date], list[dt.date]]:
    """The declared decision calendar, as a month map and as the ordered dates."""
    block = graph_config["decision_calendar"]
    dates = [dt.date.fromisoformat(str(name)) for name in block["dates"]]
    if len(set(dates)) != len(dates):
        raise ValueError("the declared calendar names a meeting date twice")
    return {(date.year, date.month): date for date in dates}, sorted(dates)


#: Record fields that must be *stated* and may be null. ``in_force_to`` is the only
#: one: a null states that the version was still in force at its last confirming dated
#: observation, which is how a run no later observation has closed is written. The
#: field is required — its absence means the record is incomplete — but its value may
#: be open, and dropping an open record here would discard every capture of a rule
#: version that has not yet been superseded, which is every capture of a rule text that
#: has not changed.
OPEN_ENDED_RECORD_FIELDS: tuple[str, ...] = ("in_force_to",)


def rule_records(graph_config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Per-contract rule-vintage records, or an empty mapping with the reason.

    A record is admitted only with every field the graph's rule requirement names. A
    field named in :data:`OPEN_ENDED_RECORD_FIELDS` must be present and may be null;
    every other required field must be present and non-empty. A document that carries
    no per-contract records yields nothing, which leaves every contract without a
    verified vintage rather than silently inheriting one.
    """
    source = Path(str(graph_config["rule_vintage"]["evidence_source"]))
    if not source.exists():
        return {}
    document = json.loads(source.read_text(encoding="utf-8"))
    entries = document.get("contract_rules")
    if not isinstance(entries, list):
        return {}
    required = tuple(graph_config["rule_vintage"]["required_record_fields"])
    open_ended = set(OPEN_ENDED_RECORD_FIELDS)
    records: dict[str, dict[str, Any]] = {}
    for entry in entries:
        absent = [name for name in required if name not in entry]
        blank = [
            name for name in required if name not in open_ended and entry.get(name) in (None, "")
        ]
        if absent or blank:
            continue
        records[str(entry["contract_id"])] = dict(entry)
    return records


def candidate_rows(policy_series: tuple[str, ...]) -> list[dict[str, Any]]:
    """Every declared-series contract with the text its predicate is read from."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    placeholders = ", ".join("?" for _ in policy_series)
    rows = con.execute(
        f"""
        SELECT ticker, event_ticker, yes_sub_title, no_sub_title, title, status,
               open_time::VARCHAR AS open_time, close_time::VARCHAR AS close_time
        FROM read_parquet('{MARKETS_GLOB}')
        WHERE regexp_extract(ticker, '^[A-Z]+') IN ({placeholders})
        """,
        list(policy_series),
    ).fetchall()
    con.close()
    return [
        {
            "ticker": row[0],
            "event_ticker": row[1],
            "yes_sub_title": row[2],
            "no_sub_title": row[3],
            "title": row[4],
            "status": row[5],
            "open_time": row[6],
            "close_time": row[7],
        }
        for row in rows
    ]


def predicates(
    markets: list[dict[str, Any]],
    *,
    months: dict[tuple[int, int], dt.date],
    rules: dict[str, dict[str, Any]],
) -> tuple[dict[str, ContractPredicate], dict[str, str]]:
    """The predicates the archived text states, and the refusal for each contract it does not."""
    out: dict[str, ContractPredicate] = {}
    refused: dict[str, str] = {}
    for market in markets:
        ticker = str(market["ticker"])
        try:
            parsed = parse_predicate(
                yes_sub_title=str(market["yes_sub_title"] or ""),
                title=str(market["title"] or ""),
            )
            decision_date = decision_date_for(str(market["event_ticker"] or ""), months)
        except PredicateError as error:
            refused[ticker] = f"{error.reason}: {error.detail}"
            continue
        record = rules.get(ticker)
        out[ticker] = ContractPredicate(
            contract_id=ticker,
            venue=KALSHI_VENUE,
            series=str(market["event_ticker"] or "").split("-")[0] or ticker.split("-")[0],
            decision_date=decision_date,
            rate_definition=parsed.rate_definition,
            threshold=parsed.threshold,
            inequality=parsed.inequality,
            yes_axis=parsed.yes_axis,
            orientation=parsed.orientation,
            open_time=_instant(market["open_time"]),
            close_time=_instant(market["close_time"]),
            rule_hash=(str(record["rule_hash"]) if record else None),
            rule_in_force_from=(_instant(record.get("in_force_from")) if record else None),
            rule_in_force_to=(_instant(record.get("in_force_to")) if record else None),
            rule_verified_by=(str(record["verified_by"]) if record else None),
        )
    return out, refused


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=".audit/study-v3")
    parser.add_argument("--panel", default=".audit/study-v3/trade_panel.parquet")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    graph_config = _load(GRAPH_CONFIG)
    cohort_config = _load(COHORT_CONFIG)
    months, calendar = declared_calendar(graph_config)
    rules = rule_records(graph_config)
    policy_series = tuple(str(name) for name in cohort_config["policy_series"])

    panel = read_parquet(args.panel, table="trade_panel")
    study_config = _load(STUDY_CONFIG)
    clock = study_config["clock"]
    caps = study_config["caps"]
    forecast_settings = ForecastSettings(
        forecast_origin_seconds=int(clock["forecast_origin_seconds"]),
        lag_guard_seconds=int(clock["lag_guard_seconds"]),
        future_horizon_seconds=int(clock["future_horizon_seconds"]),
        anchor_max_age_seconds=int(caps["anchor_max_age_seconds"]),
        target_max_age_seconds=int(caps["target_max_age_seconds"]),
        donor_max_age_seconds=int(caps["donor_max_age_seconds"]),
    )

    releases = (
        panel.groupby("event_id", sort=True)["event_time"]
        .agg(lambda values: min(_instant(str(value)) for value in values))
        .to_dict()
    )
    declared_receivers = {
        str(event_id): sorted(
            {str(contract) for contract in group["contract_id"].astype(str).unique().tolist()}
        )
        for event_id, group in panel.groupby("event_id", sort=True)
    }

    markets = candidate_rows(policy_series)
    known, refused = predicates(markets, months=months, rules=rules)

    trades_path = out_dir / "historical_trades.parquet"
    trade_records = load_trades(trades_path) if trades_path.exists() else []

    decisions: dict[str, Any] = {}
    graphs: dict[str, Any] = {}
    all_rows: list[dict[str, Any]] = []
    for event_id, release in sorted(releases.items(), key=lambda item: item[1]):
        tau = release + dt.timedelta(seconds=forecast_settings.forecast_origin_seconds)
        window_end = tau + dt.timedelta(seconds=forecast_settings.future_horizon_seconds)
        graph = build_neighbor_graph(
            list(known.values()),
            at=tau,
            calendar=calendar,
            window_end=window_end,
        )
        graphs[event_id] = {
            "digest": graph_digest(graph),
            "edges": len(graph.edges),
            "blocked": graph.blocked_count,
        }
        decisions[event_id] = {
            "release_time": release.isoformat(),
            "forecast_origin": tau.isoformat(),
            "window_end": window_end.isoformat(),
            "edges": [edge.as_dict() for edge in graph.edges],
            "counts": graph.as_dict()["counts"],
            "decisions": [decision.as_dict() for decision in graph.decisions],
        }
        rows = build_forecast_rows(
            trade_records,
            [
                type(
                    "Event",
                    (),
                    {
                        "event_id": event_id,
                        "cluster_id": event_id,
                        "family": ("cpi" if str(event_id).startswith("cpi") else "employment"),
                        "event_time": release,
                    },
                )()
            ],
            graph,
            receivers={event_id: declared_receivers[event_id]},
            settings=forecast_settings,
        )
        all_rows.extend(rows.rows)

    forecast_path = out_dir / "forecast_panel.parquet"
    reference = write_parquet(
        [dict(row) for row in all_rows],
        forecast_path,
        table="historical_forecast",
        coverage_epoch="historical_forecast_v2",
    )
    (out_dir / "graph_decisions.json").write_text(
        json.dumps(
            {
                "calendar": [date.isoformat() for date in calendar],
                "predicate_refused": refused,
                "by_release": decisions,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )
    summary = {
        "policy_series": list(policy_series),
        "input_panel": {
            "path": str(args.panel),
            "content_hash": hash_file(Path(args.panel)),
            "rows": len(panel),
        },
        "candidate_markets": len(markets),
        "predicates_read": len(known),
        "predicates_refused": len(refused),
        "rule_records_available": len(rules),
        "releases": len(releases),
        "graphs": graphs,
        "forecast": {
            "path": str(forecast_path),
            "content_hash": reference.content_hash,
            "rows": len(all_rows),
        },
    }
    (out_dir / "forecast_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=1, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
