"""Append-only store for PerpDexList sweeps, idempotent by build stamp.

The store's central property is that **the source's build stamp, not our clock,
names an observation**. A sweep writes one file per ``(asset, build)`` and skips
the write when that file already exists. Two consequences follow, and both are
load-bearing for a collector meant to run unattended for months:

* Re-running a sweep inside one source build writes nothing and costs nothing
  but the fetch, so a crash-and-restart cannot duplicate a build. That is what
  makes this idempotent rather than merely repeated.
* Because the whole site rebuilds as one unit, every file sharing a build stamp
  is a genuine cross-section. Differentials computed across files of one build
  are simultaneous; across builds they are not, and the layout makes the
  distinction visible instead of implicit.

The local receive time is stored alongside the build stamp rather than instead
of it. The difference between them is our fetch latency, which is measurable
here and would be unknowable if either were discarded.

Refusals are written as nulls with their code in a sibling column. A venue whose
figure the source did not publish is therefore present in the file with a named
reason, which is the opposite of absent and can be counted.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .parse import (
    FundingHistorySnapshot,
    MarketSnapshot,
    VenueFundingSummary,
    VenueQuote,
)

DECIMAL_TYPE = pa.decimal128(38, 12)

QUOTE_COLUMNS = pa.schema(
    [
        ("asset_class", pa.string()),
        ("asset", pa.string()),
        ("venue", pa.string()),
        ("symbol", pa.string()),
        ("build_time", pa.timestamp("us", tz="UTC")),
        ("received_at", pa.timestamp("us", tz="UTC")),
        ("price", DECIMAL_TYPE),
        ("volume_24h_usd", DECIMAL_TYPE),
        ("open_interest_usd", DECIMAL_TYPE),
        ("funding_rate", DECIMAL_TYPE),
        ("funding_apr", DECIMAL_TYPE),
        ("paid_24h", DECIMAL_TYPE),
        ("paid_7d", DECIMAL_TYPE),
        ("paid_30d", DECIMAL_TYPE),
        ("refusals", pa.string()),
    ]
)

FUNDING_COLUMNS = pa.schema(
    [
        ("asset_class", pa.string()),
        ("asset", pa.string()),
        ("venue", pa.string()),
        ("build_time", pa.timestamp("us", tz="UTC")),
        ("received_at", pa.timestamp("us", tz="UTC")),
        ("window_days", pa.int64()),
        ("settlements", pa.int64()),
        ("total_paid", DECIMAL_TYPE),
        ("average_apr", DECIMAL_TYPE),
        ("largest", DECIMAL_TYPE),
        ("smallest", DECIMAL_TYPE),
        ("last_settled", pa.timestamp("us", tz="UTC")),
        ("refusals", pa.string()),
    ]
)

_DECIMAL_FIELDS = (
    "price",
    "volume_24h_usd",
    "open_interest_usd",
    "funding_rate",
    "funding_apr",
    "paid_24h",
    "paid_7d",
    "paid_30d",
)

DEFAULT_ROOT = "data/perp"


def build_slug(build_time: Any) -> str:
    """A filename-safe form of a build stamp, so one build is one filename."""
    return build_time.strftime("%Y%m%dT%H%M%SZ")


@dataclass(frozen=True, slots=True)
class SnapshotStore:
    """Rooted storage for market and funding snapshots plus a sweep log."""

    root: Path

    @classmethod
    def open(cls, root: str | Path = DEFAULT_ROOT) -> SnapshotStore:
        path = Path(root)
        (path / "market").mkdir(parents=True, exist_ok=True)
        (path / "funding").mkdir(parents=True, exist_ok=True)
        return cls(root=path)

    def path_for(self, kind: str, asset_class: str, asset: str, build_time: Any) -> Path:
        return self.root / kind / asset_class / asset / f"{build_slug(build_time)}.parquet"

    def has_build(self, kind: str, asset_class: str, asset: str, build_time: Any) -> bool:
        return self.path_for(kind, asset_class, asset, build_time).exists()

    def _write(self, kind: str, path: Path, table: pa.Table) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return path
        # An atomic rename keeps a killed process from leaving a half-written
        # file that a later sweep would read as a complete build.
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(path)
        return path

    def write_market(self, snapshot: MarketSnapshot) -> Path:
        """Persist one market snapshot, keyed by its build stamp."""
        rows = [_quote_row(q, snapshot) for q in snapshot.quotes]
        table = pa.Table.from_pylist(rows, schema=QUOTE_COLUMNS)
        return self._write(
            "market",
            self.path_for("market", snapshot.asset_class, snapshot.asset, snapshot.build_time),
            table,
        )

    def write_funding(self, snapshot: FundingHistorySnapshot) -> Path:
        """Persist one settled-funding snapshot, keyed by its build stamp."""
        rows = [_funding_row(s, snapshot) for s in snapshot.summaries]
        table = pa.Table.from_pylist(rows, schema=FUNDING_COLUMNS)
        return self._write(
            "funding",
            self.path_for("funding", snapshot.asset_class, snapshot.asset, snapshot.build_time),
            table,
        )

    def market_builds(self, asset_class: str, asset: str) -> list[str]:
        """Build slugs already held for one asset, oldest first."""
        directory = self.root / "market" / asset_class / asset
        if not directory.is_dir():
            return []
        return sorted(p.stem for p in directory.glob("*.parquet"))

    def log_sweep(self, record: dict[str, Any]) -> None:
        """Append one sweep record. Append-only, so a sweep is never rewritten."""
        log = self.root / "sweeps.jsonl"
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")

    def sweeps(self) -> list[dict[str, Any]]:
        log = self.root / "sweeps.jsonl"
        if not log.is_file():
            return []
        return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


def _decimal(value: Any) -> Any:
    return value


def _quote_row(quote: VenueQuote, snapshot: MarketSnapshot) -> dict[str, Any]:
    row: dict[str, Any] = {
        "asset_class": quote.asset_class,
        "asset": quote.asset,
        "venue": quote.venue,
        "symbol": quote.symbol,
        "build_time": snapshot.build_time,
        "received_at": snapshot.received_at,
        "refusals": "|".join(quote.refusals),
    }
    for field in _DECIMAL_FIELDS:
        row[field] = getattr(quote, field)
    return row


def _funding_row(summary: VenueFundingSummary, snapshot: FundingHistorySnapshot) -> dict[str, Any]:
    return {
        "asset_class": snapshot.asset_class,
        "asset": snapshot.asset,
        "venue": summary.venue,
        "build_time": snapshot.build_time,
        "received_at": snapshot.received_at,
        "window_days": snapshot.window_days,
        "settlements": summary.settlements,
        "total_paid": summary.total_paid,
        "average_apr": summary.average_apr,
        "largest": summary.largest,
        "smallest": summary.smallest,
        "last_settled": summary.last_settled,
        "refusals": "|".join(summary.refusals),
    }
