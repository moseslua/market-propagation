"""Collect the cross-venue perpetual-futures cross-section and archive it.

This is the data system for the fourth study. It runs before the analysis exists
on purpose: the source publishes rolling windows and no per-episode history, so
nothing it serves today can be recovered tomorrow.

Five decisions shape it, and each answers something measured on the live pages
rather than assumed:

* **One build, one observation, per asset class.** Every page carries a
  ``Data generated at`` stamp, so the stamp names the observation and the local
  clock only measures our latency. Both are stored. The source does *not* rebuild
  every section at the same instant: measured across two builds, the rwa pages
  ran 8 seconds behind the crypto pages both times. One canary per declared asset
  class is therefore read, and the sweep is skipped only when every class's stamp
  is unchanged.
* **Check the canaries before sweeping.** The index page carries no build stamp,
  but every asset page does and its class shares one, so one asset page dates its
  class. Reading those pages replaces fetching every asset page, which is what
  keeps an unattended hourly loop from becoming a crawl.
* **Refuse rather than guess.** A page that does not match the declared table
  header raises, and a figure the source marked unobserved is stored as a null
  with a named reason code. A venue the source did not look at is not a venue
  that traded nothing.
* **Store the cross-section, derive the spreads.** Differentials and basis are
  pure functions of one stored snapshot, so they are computed on demand from the
  Parquet rather than written alongside it. Storing a derived table would
  duplicate the quotes and let the two drift.
* **A quoted spread is not an arbitrage.** No differential carries the cost
  layer, because the source's execution-cost page ships an empty
  client-rendered shell. Every stored record is labelled accordingly and no
  quoted spread may be promoted to an executable opportunity on this data.

The sweep is a plain function taking a transport and a store, so it is exercised
in tests with neither the network nor the clock. The loop around it is a thin
wrapper, which is what lets an unattended process keep collecting across a
failure instead of stopping silently.
"""

from __future__ import annotations

import datetime as dt
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import yaml

from ..ingest.transport import HttpTransport, RetryPolicy, TransportError
from ..storage import RawStore
from .differentials import REFUSAL_EXECUTION_COST_UNAVAILABLE
from .parse import (
    PageShapeError,
    parse_build_stamp,
    parse_funding_page,
    parse_market_page,
)
from .store import DEFAULT_ROOT, SnapshotStore
from .universe import AssetRef, parse_priority, parse_universe

#: The declared source configuration. It holds the endpoint shapes, cadence and
#: politeness, so no page path or interval is hard-coded here.
CONFIG_PATH = "configs/perp_arbitrage_v1.yaml"

#: Archived response bytes, kept separately from the parsed snapshots so a page
#: can be re-parsed after the parser changes without re-fetching it.
RAW_ROOT = "data/perp/raw"

#: The index page carries no build stamp; asset pages carry one and share it
#: across the whole build. One asset page therefore dates every other page, and
#: this is the page that is read to find out whether a sweep is worth starting.
CANARY_PATH = "/markets/crypto/BTC"

#: Per-page outcomes. Each names a distinct way a page failed to become an
#: observation, so a partial sweep is auditable rather than merely short.
OUTCOME_STORED = "stored"
OUTCOME_ALREADY_HELD = "already_held_for_this_build"
OUTCOME_BLOCKED = "transport_blocked"
OUTCOME_UNSHAPED = "page_shape_refused"

#: A sweep that raised before it could produce a record. Kept distinct from the
#: page-level outcomes because it says nothing about any one page.
OUTCOME_SWEEP_FAILED = "sweep_failed"


#: The key under which a configuration that declares a single canary is recorded.
#: Such a configuration asserts that the source rebuilds as one unit; the key is
#: then nominal, and is only ever compared with itself.
SITE_CANARY_KEY = "*"


def canary_paths(config: dict[str, Any]) -> dict[str, str]:
    """The page read per asset class to date a build, keyed by asset class.

    The source stamps every page but does not rebuild every section at the same
    instant: measured across two builds, the rwa pages carried 10:30:26Z and
    11:36:11Z while the crypto pages carried 10:30:34Z and 11:36:19Z, an
    8-second skew that repeated. One canary therefore cannot date the site. With
    a single canary, a sweep in which only the slower class had advanced would be
    skipped, and that class would lapse by one build for no reason.
    """
    cadence = config.get("cadence") or {}
    declared = cadence.get("build_stamp_canaries")
    if isinstance(declared, Mapping) and declared:
        return {str(key): str(path) for key, path in declared.items()}
    return {SITE_CANARY_KEY: str(cadence.get("build_stamp_canary", CANARY_PATH))}


def load_config(path: str | Path = CONFIG_PATH) -> dict[str, Any]:
    """Read the declared source configuration."""
    return yaml.safe_load(Path(path).read_text())


def build_transport(config: dict[str, Any], *, raw_root: str | Path = RAW_ROOT) -> HttpTransport:
    """A GET-only archiving transport, paced by the declared politeness policy."""
    politeness = config.get("politeness") or {}
    policy = RetryPolicy(
        attempts=int(politeness.get("max_attempts", 4)),
        initial_backoff_seconds=float(politeness.get("initial_backoff_seconds", 1.0)),
        max_backoff_seconds=float(politeness.get("max_backoff_seconds", 30.0)),
        min_interval_seconds=float(politeness.get("min_interval_seconds", 0.25)),
    )
    transport = HttpTransport(
        RawStore(raw_root),
        timeout_seconds=float(politeness.get("timeout_seconds", 30.0)),
        policy=policy,
    )
    return transport


def fetch_text(transport: HttpTransport, base_url: str, path: str, accept: str) -> str:
    """Fetch one page as text. A blocked endpoint raises, never returns empty."""
    envelope = transport.get(f"{base_url}{path}", source="perpdexlist", accept=accept)
    return envelope.text


def select_assets(
    universe_text: str,
    config: dict[str, Any],
    *,
    limit: int | None,
    use_all: bool,
) -> tuple[list[AssetRef], frozenset[tuple[str, str]]]:
    """The assets to sweep, and which of them have a funding-history page.

    The priority set follows the source's own liquidity ranking, so it tracks
    what is liquid instead of freezing whatever was liquid on the day this was
    written. Ranking misses are appended rather than dropped, so a ``--limit``
    above the ranking length still fills.
    """
    universe = parse_universe(universe_text)
    allowed = set((config.get("universe") or {}).get("include_asset_classes") or [])
    candidates = [ref for ref in universe.assets if not allowed or ref.asset_class in allowed]

    if use_all:
        chosen = candidates
    else:
        ranked = parse_priority(universe_text)
        order = {ref.key: index for index, ref in enumerate(ranked)}
        in_ranking = sorted(
            (ref for ref in candidates if ref.key in order), key=lambda ref: order[ref.key]
        )
        rest = [ref for ref in candidates if ref.key not in order]
        effective = (
            limit
            if limit is not None
            else int((config.get("universe") or {}).get("priority_limit", 40))
        )
        chosen = (in_ranking + rest)[:effective]

    return chosen, universe.with_funding_history


def _funding_pass(
    transport: HttpTransport,
    store: SnapshotStore,
    base_url: str,
    accept: str,
    chosen: list[AssetRef],
    with_funding: frozenset[tuple[str, str]],
) -> tuple[dict[tuple[str, str], dict[str, int]], list[dict[str, Any]]]:
    """Fetch settled-funding pages. Returns per-asset venue counts and outcomes.

    Counts are keyed by ``(asset, venue)`` and never pooled across assets: a
    venue's settlement count belongs to one contract on one asset, and carrying
    it to another asset's interval derivation would attribute one market's
    schedule to a different market.
    """
    counts: dict[tuple[str, str], dict[str, int]] = {}
    outcomes: list[dict[str, Any]] = []

    for ref in chosen:
        if ref.key not in with_funding:
            continue
        try:
            text = fetch_text(transport, base_url, ref.funding_path, accept)
            snapshot = parse_funding_page(
                text,
                asset_class=ref.asset_class,
                asset=ref.asset,
                received_at=dt.datetime.now(dt.UTC),
            )
            held = store.has_build("funding", ref.asset_class, ref.asset, snapshot.build_time)
            store.write_funding(snapshot)
            counts[ref.key] = {
                summary.venue: summary.settlements
                for summary in snapshot.summaries
                if summary.settlements
            }
            outcomes.append(
                {
                    "asset": ref.key,
                    "kind": "funding",
                    "outcome": OUTCOME_ALREADY_HELD if held else OUTCOME_STORED,
                }
            )
        except (TransportError, PageShapeError) as exc:
            outcomes.append(
                {
                    "asset": ref.key,
                    "kind": "funding",
                    "outcome": (
                        OUTCOME_BLOCKED if isinstance(exc, TransportError) else OUTCOME_UNSHAPED
                    ),
                    "detail": str(exc)[:200],
                }
            )
    return counts, outcomes


def _market_pass(
    transport: HttpTransport,
    store: SnapshotStore,
    base_url: str,
    accept: str,
    chosen: list[AssetRef],
) -> tuple[int, list[dict[str, Any]]]:
    """Fetch market pages. Returns the count newly stored and the outcomes."""
    stored = 0
    outcomes: list[dict[str, Any]] = []

    for ref in chosen:
        try:
            text = fetch_text(transport, base_url, ref.market_path, accept)
            snapshot = parse_market_page(
                text,
                asset_class=ref.asset_class,
                asset=ref.asset,
                received_at=dt.datetime.now(dt.UTC),
            )
            # Checked before the write, because the store is idempotent by build
            # and would otherwise report a re-fetch as a new observation.
            held = store.has_build("market", ref.asset_class, ref.asset, snapshot.build_time)
            store.write_market(snapshot)
            if held:
                outcomes.append(
                    {"asset": ref.key, "kind": "market", "outcome": OUTCOME_ALREADY_HELD}
                )
            else:
                stored += 1
                outcomes.append({"asset": ref.key, "kind": "market", "outcome": OUTCOME_STORED})
        except (TransportError, PageShapeError) as exc:
            outcomes.append(
                {
                    "asset": ref.key,
                    "kind": "market",
                    "outcome": (
                        OUTCOME_BLOCKED if isinstance(exc, TransportError) else OUTCOME_UNSHAPED
                    ),
                    "detail": str(exc)[:200],
                }
            )
    return stored, outcomes


def sweep(
    transport: HttpTransport,
    store: SnapshotStore,
    config: dict[str, Any],
    *,
    limit: int | None,
    use_all: bool,
    include_funding: bool,
    force: bool,
) -> dict[str, Any]:
    """One sweep of the source. Returns the record written to the sweep log.

    The canary decides whether the sweep is worth starting, so an hour in which
    the source has not rebuilt costs one request rather than one per asset.
    """
    source = config.get("source") or {}
    base_url = str(source.get("base_url", "https://perpdexlist.com"))
    accept = str(source.get("accept", "text/markdown"))
    started = dt.datetime.now(dt.UTC)

    # Every declared canary is read, not just one: the sections rebuild at slightly
    # different instants, so a single stamp cannot say whether the site advanced.
    stamps = {
        key: parse_build_stamp(fetch_text(transport, base_url, path, accept)).isoformat()
        for key, path in canary_paths(config).items()
    }
    build_time = dt.datetime.fromisoformat(next(iter(stamps.values())))

    prior = store.sweeps()
    if not force and prior and prior[-1].get("build_times") == stamps:
        record = {
            "started_at": started.isoformat(),
            "build_time": build_time.isoformat(),
            "build_times": stamps,
            "skipped": True,
            "skip_reason": "source_build_unchanged",
            "assets_attempted": 0,
            "assets_stored": 0,
        }
        store.log_sweep(record)
        return record
    index_text = fetch_text(transport, base_url, "/markets", accept)
    chosen, with_funding = select_assets(index_text, config, limit=limit, use_all=use_all)

    funding_counts: dict[tuple[str, str], dict[str, int]] = {}
    outcomes: list[dict[str, Any]] = []
    if include_funding:
        funding_counts, outcomes = _funding_pass(
            transport, store, base_url, accept, chosen, with_funding
        )

    stored, market_outcomes = _market_pass(transport, store, base_url, accept, chosen)
    outcomes.extend(market_outcomes)

    record = {
        "started_at": started.isoformat(),
        "finished_at": dt.datetime.now(dt.UTC).isoformat(),
        "build_time": build_time.isoformat(),
        "build_times": stamps,
        "skipped": False,
        "assets_attempted": len(chosen),
        "assets_stored": stored,
        "with_funding_history": len([r for r in chosen if r.key in with_funding]),
        "funding_assets_collected": len(funding_counts),
        "blocked": len([o for o in outcomes if o["outcome"] == OUTCOME_BLOCKED]),
        "unshaped": len([o for o in outcomes if o["outcome"] == OUTCOME_UNSHAPED]),
        "outcomes": outcomes,
        "cost_layer": REFUSAL_EXECUTION_COST_UNAVAILABLE,
    }
    store.log_sweep(record)
    return record


def _recorded_failure(started: dt.datetime, exc: Exception) -> dict[str, Any]:
    """A sweep that raised, recorded as itself rather than as a page outcome."""
    return {
        "started_at": started.isoformat(),
        "finished_at": dt.datetime.now(dt.UTC).isoformat(),
        "build_time": None,
        "skipped": False,
        "sweep_failed": True,
        "error": f"{type(exc).__name__}: {exc}"[:500],
        "assets_attempted": 0,
        "assets_stored": 0,
    }


def collect(
    transport: HttpTransport,
    store: SnapshotStore,
    config: dict[str, Any],
    *,
    limit: int | None,
    use_all: bool,
    include_funding: bool,
    force: bool,
    once: bool,
    interval_minutes: int,
    on_record: Callable[[dict[str, Any]], None] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> int:
    """Sweep now, and keep sweeping until interrupted unless ``once``.

    A sweep that raises is recorded and the loop continues. An unattended
    collector that dies on one unexpected response loses every later build, and
    the source serves no history, so the failure is logged as a failure and the
    next hour is still attempted. Nothing is swallowed: the record carries the
    exception.
    """
    while True:
        started = dt.datetime.now(dt.UTC)
        try:
            record = sweep(
                transport,
                store,
                config,
                limit=limit,
                use_all=use_all,
                include_funding=include_funding,
                force=force,
            )
        except Exception as exc:
            record = _recorded_failure(started, exc)
            store.log_sweep(record)
        if on_record is not None:
            on_record(record)
        if once:
            return 0
        sleeper(float(max(60, interval_minutes * 60)))


def report(store: SnapshotStore) -> dict[str, Any]:
    """What is held, and what each sweep could not collect."""
    sweeps = store.sweeps()
    # A failed sweep observed no build, so it must not contribute one: counting the
    # null would report a build the collector never saw.
    builds = sorted(
        {
            str(sweep["build_time"])
            for sweep in sweeps
            if not sweep.get("skipped")
            and not sweep.get("sweep_failed")
            and sweep.get("build_time")
        }
    )
    # One entry per asset class, because the sections are rebuilt at slightly
    # different instants: a single count would hide that one class had lapsed.
    per_class: dict[str, list[str]] = {}
    for sweep in sweeps:
        if sweep.get("skipped") or sweep.get("sweep_failed"):
            continue
        for key, stamp in (sweep.get("build_times") or {}).items():
            per_class.setdefault(key, []).append(str(stamp))
    return {
        "sweeps_logged": len(sweeps),
        "sweeps_performed": len([s for s in sweeps if not s.get("skipped")]),
        "sweeps_skipped": len([s for s in sweeps if s.get("skipped")]),
        "sweeps_failed": len([s for s in sweeps if s.get("sweep_failed")]),
        "distinct_builds": len(builds),
        "distinct_builds_by_class": {
            key: len(sorted(set(values))) for key, values in sorted(per_class.items())
        },
        "first_build": builds[0] if builds else None,
        "last_build": builds[-1] if builds else None,
        "last_sweep": sweeps[-1] if sweeps else None,
        "cost_layer_observable": False,
        "note": (
            "Differentials and basis are derived from the stored cross-sections "
            "with market_propagation.perp.differentials, not stored."
        ),
    }


def interval_minutes(config: dict[str, Any]) -> int:
    """The declared hourly interval, floored so a typo cannot become a crawl."""
    return int((config.get("cadence") or {}).get("priority_hourly_minutes", 60))


__all__ = [
    "CANARY_PATH",
    "CONFIG_PATH",
    "DEFAULT_ROOT",
    "OUTCOME_ALREADY_HELD",
    "OUTCOME_BLOCKED",
    "OUTCOME_STORED",
    "OUTCOME_SWEEP_FAILED",
    "OUTCOME_UNSHAPED",
    "RAW_ROOT",
    "SITE_CANARY_KEY",
    "build_transport",
    "canary_paths",
    "collect",
    "fetch_text",
    "interval_minutes",
    "load_config",
    "report",
    "select_assets",
    "sweep",
]
