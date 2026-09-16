"""Measure whether the primary policy-rate neighbour graph is estimable.

This is a feasibility probe, not an analysis. It answers one bounded question
against the real archive: for each development release, and for each matched
strike in the primary graph, do the four observations the network estimand
requires actually exist in the source-time trade record?

The four observations, using the plan's clock definitions with
``tau = release_time + 300s`` and lag guard ``L = 60s``:

* donor baseline   last trade strictly before ``release_time``
* donor signal     last trade at or before ``release_time + 240s``
* receiver anchor  last trade at or before ``release_time + 300s``
* receiver target  last trade after ``release_time + 300s`` and at or before
  ``release_time + 600s``

Every count is a count of real rows in the sealed archive. Nothing here
estimates, imputes or fills a missing observation, and an absent observation is
reported as a zero count rather than as an imputed value.

Run::

    uv run --no-sync python scripts/probe_graph_support.py --out .audit/study-v2
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

import duckdb

#: The ten development releases, as UTC scheduled instants.
RELEASES: tuple[tuple[str, str, str, str], ...] = (
    ("empsit_2025_01", "2025-01-10 13:30:00", "25JAN", "25MAR"),
    ("cpi_2025_01", "2025-01-15 13:30:00", "25JAN", "25MAR"),
    ("empsit_2025_02", "2025-02-07 13:30:00", "25MAR", "25MAY"),
    ("cpi_2025_02", "2025-02-12 13:30:00", "25MAR", "25MAY"),
    ("empsit_2025_03", "2025-03-07 13:30:00", "25MAR", "25MAY"),
    ("cpi_2025_03", "2025-03-12 12:30:00", "25MAR", "25MAY"),
    ("empsit_2025_04", "2025-04-04 12:30:00", "25MAY", "25JUN"),
    ("cpi_2025_04", "2025-04-10 12:30:00", "25MAY", "25JUN"),
    ("empsit_2025_05", "2025-05-02 12:30:00", "25MAY", "25JUN"),
    ("cpi_2025_05", "2025-05-13 12:30:00", "25JUN", "25JUL"),
)

#: The matched strike vocabulary every KXFEDDECISION expiry carries.
STRIKES: tuple[str, ...] = ("C25", "C26", "H0", "H25", "H26")

TRADES_GLOB = "data/external/kalshi-trades/trades-*.parquet"

#: Observation offsets in seconds, relative to the scheduled release instant.
BASELINE_OPEN = -1800
DONOR_SIGNAL_CLOSE = 240
RECEIVER_ANCHOR_CLOSE = 300
RECEIVER_TARGET_CLOSE = 600

_TICKER_SQL = """
    SELECT
      count(*) FILTER (WHERE created_time >= CAST(? AS TIMESTAMPTZ)
                         AND created_time <  CAST(? AS TIMESTAMPTZ)) AS baseline,
      count(*) FILTER (WHERE created_time >= CAST(? AS TIMESTAMPTZ)
                         AND created_time <= CAST(? AS TIMESTAMPTZ)) AS donor_signal,
      count(*) FILTER (WHERE created_time >= CAST(? AS TIMESTAMPTZ)
                         AND created_time <= CAST(? AS TIMESTAMPTZ)) AS anchor,
      count(*) FILTER (WHERE created_time >  CAST(? AS TIMESTAMPTZ)
                         AND created_time <= CAST(? AS TIMESTAMPTZ)) AS target
    FROM read_parquet(?)
    WHERE ticker = ?
      AND created_time >= CAST(? AS TIMESTAMPTZ)
      AND created_time <= CAST(? AS TIMESTAMPTZ)
"""


def _iso(instant: dt.datetime) -> str:
    return instant.strftime("%Y-%m-%d %H:%M:%S+00")


def _strike_counts(con: duckdb.DuckDBPyConnection, release_utc: str, ticker: str) -> dict:
    """Count the four required observations for one contract around one release."""
    base = dt.datetime.strptime(release_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=dt.UTC)
    offsets = {
        "baseline_open": BASELINE_OPEN,
        "release": 0,
        "donor_signal_close": DONOR_SIGNAL_CLOSE,
        "anchor_close": RECEIVER_ANCHOR_CLOSE,
        "target_close": RECEIVER_TARGET_CLOSE,
    }
    stamps = {k: _iso(base + dt.timedelta(seconds=v)) for k, v in offsets.items()}
    row = con.execute(
        _TICKER_SQL,
        [
            stamps["baseline_open"],
            stamps["release"],
            stamps["release"],
            stamps["donor_signal_close"],
            stamps["release"],
            stamps["anchor_close"],
            stamps["anchor_close"],
            stamps["target_close"],
            TRADES_GLOB,
            ticker,
            stamps["baseline_open"],
            stamps["target_close"],
        ],
    ).fetchone()
    return {
        "baseline": int(row[0] or 0),
        "donor_signal": int(row[1] or 0),
        "anchor": int(row[2] or 0),
        "target": int(row[3] or 0),
    }


def probe() -> dict:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    events = []
    for event_id, release_utc, donor_expiry, receiver_expiry in RELEASES:
        per_strike = {}
        for strike in STRIKES:
            donor = _strike_counts(con, release_utc, f"KXFEDDECISION-{donor_expiry}-{strike}")
            receiver = _strike_counts(con, release_utc, f"KXFEDDECISION-{receiver_expiry}-{strike}")
            per_strike[strike] = {"donor": donor, "receiver": receiver}
        complete = sum(
            1
            for v in per_strike.values()
            if v["donor"]["donor_signal"] > 0
            and v["receiver"]["anchor"] > 0
            and v["receiver"]["target"] > 0
        )
        events.append(
            {
                "event_id": event_id,
                "release_utc": release_utc,
                "donor_expiry": donor_expiry,
                "receiver_expiry": receiver_expiry,
                "per_strike": per_strike,
                "complete_strike_observations": complete,
            }
        )
    con.close()
    return {
        "graph": "earlier_to_next_decision_matched_strike",
        "series": "KXFEDDECISION",
        "strikes": list(STRIKES),
        "offsets_seconds": {
            "baseline_open": BASELINE_OPEN,
            "donor_signal_close": DONOR_SIGNAL_CLOSE,
            "receiver_anchor_close": RECEIVER_ANCHOR_CLOSE,
            "receiver_target_close": RECEIVER_TARGET_CLOSE,
        },
        "events": events,
        "totals": {
            "events": len(events),
            "complete_strike_observations": sum(e["complete_strike_observations"] for e in events),
            "events_with_at_least_one_complete_strike": sum(
                1 for e in events if e["complete_strike_observations"] > 0
            ),
            "distinct_receiver_contracts": sum(
                sum(
                    1
                    for v in e["per_strike"].values()
                    if v["receiver"]["anchor"] > 0 and v["receiver"]["target"] > 0
                )
                for e in events
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=".audit/study-v2")
    args = parser.parse_args()
    payload = probe()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "graph_support.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps(payload["totals"], indent=2))
    for e in payload["events"]:
        detail = " ".join(
            f"{s}:d{v['donor']['donor_signal']}/a{v['receiver']['anchor']}/t{v['receiver']['target']}"
            for s, v in e["per_strike"].items()
        )
        print(f"  {e['event_id']:16s} complete={e['complete_strike_observations']} {detail}")
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
