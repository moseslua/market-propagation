"""Bounded runtime operations the CLI calls: coverage audit, snapshot capture,
collector-quality report and event card.

Each function does the work it names against the real store and the real public
read-only endpoints, and each returns a plain ``dict`` so the CLI owns
presentation while this module owns behaviour. Four rules hold throughout:

1. **A successful HTTP response is never study eligibility.** The audit reports
   attempted, acquired and eligible sizes separately and carries its own coverage
   gates. ``eligibility.study_eligibility_established`` reads the cohort
   configuration's verified market ids, never the request outcomes.
2. **Nothing is invented where an observation is missing.** An unobserved value is
   ``null`` with a reason, and JSON is written with ``allow_nan=False`` so a
   non-finite number cannot silently become a data point.
3. **Every call is bounded.** Request attempts, pacing, pages, contracts, capture
   duration and capture request count each have an explicit ceiling, and the
   bounds actually applied are returned with the result.
4. **The archive is the evidence.** Capture keeps the receipt instant and the
   monotonic reading from the response envelope rather than deriving a time, and
   the audit, quality and card paths verify referenced payload hashes instead of
   trusting a receipt's existence.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import pathlib
import statistics
import time
from collections.abc import Mapping, Sequence
from decimal import Decimal
from itertools import pairwise
from typing import Any, NamedTuple
from urllib.parse import quote

import yaml

from .domain import Clock, Provenance
from .ingest.audit import REQUESTED_INTERVALS, CohortAuditor
from .ingest.kalshi_rest import KALSHI_BASE_URL, KalshiClient
from .ingest.macro_releases import (
    ACQUISITION_NETWORK,
    ACQUISITION_SEALED_DATASET,
    MacroReleaseClient,
)
from .ingest.normalize import (
    KALSHI_ASKS_DERIVED_FROM_NO_BIDS,
    normalize_kalshi_orderbook_snapshot,
    stable_record_id,
)
from .ingest.polymarket_public import (
    POLYMARKET_CLOB_URL,
    PolymarketPublicClient,
    PolymarketUnreachable,
    normalize_book_snapshot,
)
from .ingest.transport import (
    HttpTransport,
    ResponseEnvelope,
    RetryPolicy,
    TransportError,
    WireShapeError,
)
from .replay import ORDER_USABLE, BookEvent, BookState, Quote
from .storage import RawStore, write_parquet

__all__ = [
    "CAPTURE_MODE",
    "DEFAULT_COHORT_PATH",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_WINDOWS_PATH",
    "MAX_CAPTURE_REQUESTS",
    "MAX_CAPTURE_SECONDS",
    "capture_snapshots",
    "event_card",
    "quality_report",
    "run_audit",
]

#: Default study inputs. Both are paths to the study's own configuration, and the
#: audit refuses to run its measurement window without reading one of them.
DEFAULT_COHORT_PATH = "configs/cohort.yaml"
DEFAULT_WINDOWS_PATH = "configs/event_windows.yaml"

DEFAULT_TIMEOUT_SECONDS = 8.0
MAX_TIMEOUT_SECONDS = 120.0

#: The artifact recording which path supplied this run's first releases. It is a
#: separate document rather than a field inside ``coverage.json``, so a run that
#: reads an archive is distinguishable from an older audit without rewriting it.
RELEASE_SOURCE_NAME = "release_source.json"
RELEASE_SOURCE_SCOPE = "release_source"

#: Bounded retry and pacing configuration. ``REQUEST_PACING_SECONDS`` is a
#: configured conservative floor for one sequential client, not a claim about a
#: documented quota.
RETRY_ATTEMPTS = 3
INITIAL_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 8.0
MAX_RETRY_AFTER_SECONDS = 60.0
REQUEST_PACING_SECONDS = 0.2

#: Capture bounds. Duration, interval and total requests are each capped, so a
#: caller cannot turn a public snapshot read into an unbounded pull.
MAX_CAPTURE_SECONDS = 3600.0
MIN_CAPTURE_INTERVAL_SECONDS = 0.1
MAX_CAPTURE_REQUESTS = 1800
CAPTURE_MODE = "snapshots"

SOURCE_TIME_UNAVAILABLE = "public_snapshot_carries_no_verified_source_clock"

#: What a capture can honestly say about its own clock. The receipt instant and
#: the monotonic reading are real observations on this process's time axis, which
#: keeps the record usable there; neither one audits synchronization or measures
#: a feed delay, so the quality stays ``unknown`` and the physical uncertainty
#: stays unmeasured rather than reported as a zero-width bound.
UNMEASURED_CLOCK_QUALITY = "unknown"

#: Names the missing audit in the policy itself; a basis that stopped at "receipt
#: window" would leave it implicit.
RECEIPT_WINDOW_BASIS = "receipt_window_public_snapshot_no_clock_sync_audit"

TIMING_UNCERTAINTY_UNMEASURED = (
    "no clock synchronization audit and no source-feed delay measurement were "
    "performed, so absolute and source-clock timing uncertainty is unmeasured"
)

#: The documented public snapshot surfaces, one per venue. ``param`` names the
#: query parameter carrying the market identity, or ``None`` when the identity is
#: a path segment.
_SNAPSHOT_SOURCES: Mapping[str, Mapping[str, Any]] = {
    "kalshi": {
        "base_url": KALSHI_BASE_URL,
        "path": "/markets/{contract_id}/orderbook",
        "source": "kalshi.orderbook",
        "param": None,
        "ask_levels_basis": KALSHI_ASKS_DERIVED_FROM_NO_BIDS,
    },
    "polymarket": {
        "base_url": POLYMARKET_CLOB_URL,
        "path": "/book",
        "source": "polymarket.clob.book",
        "param": "token_id",
        "ask_levels_basis": "quoted_ask_levels_from_the_public_book_response",
    },
}


def _load_yaml(path: pathlib.Path, *, purpose: str) -> Mapping[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{purpose} configuration not found: {path}")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(f"{purpose} configuration at {path} is not valid YAML: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"{purpose} configuration at {path} must be a mapping, got {type(payload).__name__}"
        )
    return payload


def _window_seconds(windows: Mapping[str, Any]) -> dict[str, Any]:
    """Read the measurement window from the study's own event-window config.

    A missing or non-integer bound raises instead of falling back to a value
    defined here: a second window definition would be a second source of truth for
    the study's measurement window.
    """
    block = windows.get("windows")
    main = block.get("main") if isinstance(block, Mapping) else None
    if not isinstance(main, Mapping):
        raise ValueError(
            "event-window configuration carries no windows.main mapping; the audit "
            "window must come from the study's own configuration and this module "
            "defines no fallback window"
        )
    before = main.get("pre_event_seconds")
    after = main.get("post_event_seconds")
    for name, value in (("pre_event_seconds", before), ("post_event_seconds", after)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(
                "event-window configuration windows.main."
                f"{name} must be a non-negative whole number of seconds, got {value!r}"
            )
    alignment = windows.get("alignment")
    alignment = alignment if isinstance(alignment, Mapping) else {}
    return {
        "main_window_id": str(main.get("window_id") or "main"),
        "pre_event_seconds": int(before),
        "post_event_seconds": int(after),
        "pre_event_role": main.get("pre_event_role"),
        "post_event_role": main.get("post_event_role"),
        "reference_point": alignment.get("reference_point"),
        "reference_precision": alignment.get("reference_precision"),
        "source_calendar_timezone": alignment.get("source_calendar_timezone"),
        "storage_timezone": alignment.get("storage_timezone"),
        "secondary_horizons_seconds": list(block.get("secondary_horizons_seconds") or []),
        "contamination_stop": list(block.get("contamination_stop") or []),
        "stop_action": block.get("stop_action"),
        "restrictions": list(block.get("restrictions") or []),
    }


def _bounded_seconds(
    value: Any,
    *,
    name: str,
    minimum: float = 0.0,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if number < minimum:
        raise ValueError(f"{name} must be at least {minimum}s, got {number}s")
    if maximum is not None and number > maximum:
        raise ValueError(
            f"{name} must be at most {maximum}s; this operation is deliberately "
            f"bounded, got {number}s"
        )
    return number


def _positive_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive int, got {value!r}")
    return value


def _retry_policy(*, min_interval_seconds: float) -> RetryPolicy:
    return RetryPolicy(
        attempts=RETRY_ATTEMPTS,
        initial_backoff_seconds=INITIAL_BACKOFF_SECONDS,
        max_backoff_seconds=MAX_BACKOFF_SECONDS,
        max_retry_after_seconds=MAX_RETRY_AFTER_SECONDS,
        min_interval_seconds=min_interval_seconds,
    )


def _pacing(min_interval_seconds: float) -> dict[str, Any]:
    return {
        "attempts_per_request": RETRY_ATTEMPTS,
        "initial_backoff_seconds": INITIAL_BACKOFF_SECONDS,
        "max_backoff_seconds": MAX_BACKOFF_SECONDS,
        "max_retry_after_seconds": MAX_RETRY_AFTER_SECONDS,
        "min_interval_seconds": min_interval_seconds,
        "basis": (
            "configured bound for one sequential public read-only client; not a claim "
            "about a documented quota"
        ),
    }


def _json_safe(value: Any) -> Any:
    """Recursively replace values JSON cannot carry as data with ``None``.

    A non-finite float is the case that matters: writing it would emit a token no
    JSON consumer should accept, so it becomes an explicit null here.
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.UTC).isoformat()
    return str(value)


def _write_json(path: pathlib.Path, payload: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(_json_safe(payload), indent=2, sort_keys=True, allow_nan=False)
    path.write_text(text + "\n", encoding="utf-8")
    return str(path)


def _read_json(path: pathlib.Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}") from exc


def _decimal_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _is_hash(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(ch in "0123456789abcdef" for ch in value)
    )


def _cohort_verified_ids(cohort: Mapping[str, Any]) -> dict[str, Any]:
    block = cohort.get("cohort")
    value = block.get("verified_eligible_market_ids") if isinstance(block, Mapping) else None
    if value is None:
        return {
            "ids": None,
            "count": None,
            "status": "not_recorded_in_supplied_cohort_configuration",
        }
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(
            "cohort.verified_eligible_market_ids must be a sequence of market "
            f"identifiers, got {type(value).__name__}"
        )
    ids = [str(item) for item in value]
    return {"ids": ids, "count": len(ids), "status": "empty" if not ids else "recorded"}


def run_audit(
    output_dir: str | pathlib.Path,
    *,
    cohort_path: str | pathlib.Path = DEFAULT_COHORT_PATH,
    windows_path: str | pathlib.Path = DEFAULT_WINDOWS_PATH,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_pages: int = 1,
    max_contracts: int = 12,
    max_candle_contracts: int = 2,
    release_dataset: str | pathlib.Path | None = None,
    archive_raw_root: str | pathlib.Path | None = None,
) -> dict[str, Any]:
    """Run the bounded G0 coverage audit and write its outputs through the auditor.

    The event cohort and the measurement window are read from the study's own
    configuration files (``configs/cohort.yaml`` and ``configs/event_windows.yaml``
    by default); neither has a fallback defined in this module. One
    :class:`~market_propagation.ingest.transport.HttpTransport` serves both public
    clients with bounded attempts, backoff and pacing, and it is closed even when
    the audit raises.

    ``release_dataset`` names a sealed archived-release dataset explicitly. When it
    is given, first releases are read from it and its sibling raw store and the
    network is not asked for a release at all; every selected payload is verified
    against the record that cites it before it counts, and a record that is absent
    or disagreeing is a blocked result for that event. The archive is opened once,
    up front, so a missing or malformed dataset raises here instead of being
    reported as a per-event block. When it is absent, the archived-release fetch
    path is unchanged.

    Returns a dict carrying the auditor's own result under ``coverage`` (its
    ``as_dict``), the window and bounds actually applied, aggregated access
    blockers with ``empty_result`` preserved as ``False``, the written artifact
    paths, an ``eligibility`` block that reads the cohort configuration's
    verified market ids rather than any request outcome, and a
    ``release_source`` block naming which path supplied the first releases.
    """
    timeout = _bounded_seconds(
        timeout_seconds, name="timeout_seconds", minimum=0.1, maximum=MAX_TIMEOUT_SECONDS
    )
    pages = _positive_int(max_pages, name="max_pages")
    contracts = _positive_int(max_contracts, name="max_contracts")
    candle_contracts = _positive_int(max_candle_contracts, name="max_candle_contracts")

    cohort_config = _load_yaml(pathlib.Path(cohort_path), purpose="cohort")
    windows_config = _load_yaml(pathlib.Path(windows_path), purpose="event window")
    window = _window_seconds(windows_config)
    verified = _cohort_verified_ids(cohort_config)

    out = pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    store = RawStore(out / "raw")
    transport = HttpTransport(
        store,
        timeout_seconds=timeout,
        policy=_retry_policy(min_interval_seconds=REQUEST_PACING_SECONDS),
    )
    try:
        bls = MacroReleaseClient(
            store,
            transport=transport,
            release_dataset=release_dataset,
            archive_raw_root=archive_raw_root,
        )
        auditor = CohortAuditor(
            store,
            kalshi=KalshiClient(store, transport=transport),
            bls=bls,
            max_contracts_per_event=contracts,
            max_candle_contracts_per_event=candle_contracts,
            max_pages=pages,
        )
        audit = auditor.audit_cohort(
            out,
            config=cohort_config,
            before_seconds=window["pre_event_seconds"],
            after_seconds=window["post_event_seconds"],
            candle_intervals=REQUESTED_INTERVALS,
        )
    finally:
        transport.close()

    event_blockers = [
        {"event_id": event.event_id, "scope": "event", **dict(record)}
        for event in audit.events
        for record in event.blocked
    ]
    run_blockers = [
        {"event_id": None, "scope": "series_discovery", **dict(record)} for record in audit.blocked
    ]
    blockers = event_blockers + run_blockers
    release_source = {
        "kind": (ACQUISITION_SEALED_DATASET if bls.archive is not None else ACQUISITION_NETWORK),
        "explicitly_selected": release_dataset is not None,
        "dataset": bls.archive.dataset if bls.archive is not None else None,
        "archive": bls.archive.as_dict() if bls.archive is not None else None,
        "network_release_requests_issued": bls.archive is None,
        "fallback_to_network_used": False,
        "basis": (
            "the caller named an archived release dataset explicitly, so every first release "
            "was read from it and its sibling raw store and verified against the record that "
            "cites it; a record that is absent or disagreeing is a blocked event, never a "
            "live fetch"
            if bls.archive is not None
            else "no archived release dataset was named, so first releases were fetched from "
            "the public archive through the normal transport path"
        ),
        "historical_market_rule_versions_certified": False,
        "note": (
            "original BLS first-release bytes are evidence about the release alone. They do "
            "not certify which market rule version was in force at the release, nor the quote "
            "coverage of any contract, so no empirical eligibility follows from loading them"
        ),
    }
    result = {
        "operation": "run_audit",
        "gate": "G0",
        "venue": audit.venue,
        "status": audit.status,
        "complete": audit.complete,
        "cohort_definition_hash": audit.cohort_definition_hash,
        "cohort_path": str(cohort_path),
        "windows_path": str(windows_path),
        "window_basis": {
            **window,
            "source_config": str(windows_path),
            "candle_intervals_minutes": list(REQUESTED_INTERVALS),
            "candle_intervals_basis": (
                "market_propagation.ingest.audit.REQUESTED_INTERVALS, the module that "
                "declares which candle resolutions are requested"
            ),
        },
        "bounds": {
            "timeout_seconds": timeout,
            "max_pages": pages,
            "max_contracts_per_event": contracts,
            "max_candle_contracts_per_event": candle_contracts,
            "discover_series": True,
        },
        "pacing": _pacing(REQUEST_PACING_SECONDS),
        "cohort_size": dict(audit.cohort_size),
        "unsatisfied_gates": [
            {"event_id": event_id, "gate": gate} for event_id, gate in audit.unsatisfied_gates
        ],
        "access": {
            "blocked_records": blockers,
            "blocked_count": len(blockers),
            "empty_result_substitution_used": False,
            "http_success_is_not_study_eligibility": True,
        },
        "eligibility": {
            "source": "cohort configuration cohort.verified_eligible_market_ids",
            "verified_eligible_market_ids": verified["ids"],
            "verified_eligible_market_id_count": verified["count"],
            "verified_eligible_market_ids_status": verified["status"],
            "audit_eligible_downstream_candidates": audit.cohort_size.get("eligible_downstream"),
            "study_eligibility_established": bool(verified["ids"]),
            "basis": (
                "eligibility is established by verified market identity in the study's "
                "own cohort configuration; a 2xx response, a returned market row or a "
                "returned candle establishes coverage of a request, never eligibility"
            ),
        },
        "limitations": list(audit.limitations),
        "release_source": release_source,
        "outputs": {"raw_store": str(store.root), **dict(audit.outputs)},
        "coverage": audit.as_dict(),
    }
    # The release source is written as its own document rather than folded into
    # coverage.json, so this run's archive identity sits beside the old audit
    # artifacts without editing them, and the event card can cite which path
    # supplied the first releases it reports.
    written = _write_json(out / RELEASE_SOURCE_NAME, release_source)
    result["outputs"][RELEASE_SOURCE_SCOPE] = written
    return result


def _snapshot_request(
    *,
    index: int,
    run_id: str,
    contract_id: str,
    spec: Mapping[str, Any],
) -> tuple[str, dict[str, Any] | None, str]:
    """The documented public snapshot request for one poll.

    Each poll gets its own occurrence identity. The venues publish no snapshot
    identifier, so identity is derived from the run, the market and the poll
    ordinal rather than from a content hash, because two identical books are two
    observations of a quiet market.
    """
    url = f"{spec['base_url']}{spec['path'].format(contract_id=quote(contract_id, safe=''))}"
    param = spec["param"]
    params = {param: contract_id} if param else None
    record_id = f"snapshot-{stable_record_id(run_id, contract_id, f'{index:05d}')}"
    return url, params, record_id


def _normalize_snapshot(
    venue_key: str,
    payload: Mapping[str, Any],
    *,
    contract_id: str,
    clock: Clock,
    provenance: Provenance,
) -> list[BookEvent]:
    if venue_key == "kalshi":
        return normalize_kalshi_orderbook_snapshot(
            payload, contract_id=contract_id, clock=clock, provenance=provenance
        )
    return normalize_book_snapshot(
        payload, contract_id=contract_id, clock=clock, provenance=provenance
    )


def _observation(
    *,
    index: int,
    venue_key: str,
    contract_id: str,
    request_record_id: str,
    envelope: ResponseEnvelope,
    clock: Clock,
    get_spec: Mapping[str, Any],
    event: BookEvent | None,
    quote: Quote | None,
) -> dict[str, Any]:
    return {
        "index": index,
        "venue": venue_key,
        "contract_id": contract_id,
        "request_record_id": request_record_id,
        "http_status": envelope.status_code,
        "attempts": envelope.attempts,
        "raw_hash": envelope.provenance.raw_hash,
        "received_time": envelope.received_time.isoformat(),
        "usable_time": clock.usable_time.isoformat() if clock.usable_time else None,
        "availability_lower": (
            clock.availability.lower.isoformat() if clock.availability.lower else None
        ),
        "availability_upper": (
            clock.availability.upper.isoformat() if clock.availability.upper else None
        ),
        "availability_quality": clock.availability.quality,
        "availability_basis": clock.availability.basis,
        # The interval is built from the receipt instant this process really
        # recorded, so its width is a local quantity, not a physical timing
        # uncertainty: the synchronization audit and feed-delay measurement were
        # never performed, so that stays null with its reason rather than zero.
        "availability_width_seconds": clock.availability.width_seconds,
        "timing_uncertainty_seconds": None,
        "timing_uncertainty_status": TIMING_UNCERTAINTY_UNMEASURED,
        "monotonic_ns": envelope.monotonic_ns,
        "source_time": None,
        "source_time_status": SOURCE_TIME_UNAVAILABLE,
        "source_time_precision": "unknown",
        "kind": event.kind.value if event is not None else None,
        "sequence": event.sequence if event is not None else None,
        "sequence_scope": event.sequence_scope if event is not None else None,
        "bid_levels": len(event.bids) if event is not None else None,
        "ask_levels": len(event.asks) if event is not None else None,
        "ask_levels_basis": get_spec["ask_levels_basis"],
        "bid": _decimal_text(quote.bid) if quote is not None else None,
        "ask": _decimal_text(quote.ask) if quote is not None else None,
        "bid_size": _decimal_text(quote.bid_size) if quote is not None else None,
        "ask_size": _decimal_text(quote.ask_size) if quote is not None else None,
        "spread": _decimal_text(quote.spread) if quote is not None else None,
        "midpoint": _decimal_text(quote.midpoint) if quote is not None else None,
        "validity": quote.validity.value if quote is not None else "no_quote_emitted",
        "replay_order": quote.replay_order if quote is not None else None,
        "observation_kind": "snapshot",
        "tick_complete": False,
    }


def _persist_normalized(
    store: RawStore,
    directory: pathlib.Path,
    *,
    events: Sequence[BookEvent],
    quotes: Sequence[Quote],
    run_id: str,
) -> dict[str, Any]:
    """Seal the normalized snapshot stream, or state why nothing was sealed.

    ``write_parquet`` refuses a row missing a required identifier, so an empty
    capture writes no dataset at all rather than an empty one that could be read
    as an observed absence of quotes.
    """
    if not events:
        return {
            "written": False,
            "reason": "no snapshot was archived, so there is nothing to normalize",
            "book_events": None,
            "quotes": None,
        }
    directory.mkdir(parents=True, exist_ok=True)
    events_ref = write_parquet(
        list(events),
        directory / "book_events.parquet",
        table="book_events",
        coverage_epoch=run_id,
        metadata={"capture_mode": CAPTURE_MODE, "tick_complete": "false"},
    )
    quotes_ref = (
        write_parquet(
            list(quotes),
            directory / "quotes.parquet",
            table="quotes",
            coverage_epoch=run_id,
            metadata={"capture_mode": CAPTURE_MODE, "tick_complete": "false"},
        )
        if quotes
        else None
    )
    return {
        "written": True,
        "reason": None,
        "book_events": {
            "path": events_ref.path,
            "table": events_ref.table,
            "row_count": events_ref.row_count,
            "content_hash": events_ref.content_hash,
            "coverage_epoch": events_ref.coverage_epoch,
        },
        "quotes": (
            {
                "path": quotes_ref.path,
                "table": quotes_ref.table,
                "row_count": quotes_ref.row_count,
                "content_hash": quotes_ref.content_hash,
                "coverage_epoch": quotes_ref.coverage_epoch,
            }
            if quotes_ref is not None
            else None
        ),
    }


def capture_snapshots(
    output_dir: str | pathlib.Path,
    *,
    venue: str = "kalshi",
    contract_id: str,
    duration_seconds: float = 10,
    interval_seconds: float = 2,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Capture bounded public book snapshots for one market and persist them.

    ``contract_id`` is mandatory and the caller chooses a legitimate public
    market; an unknown identifier surfaces as a recorded access failure rather
    than as an empty capture. Only documented public snapshot GETs are issued,
    through :class:`~market_propagation.ingest.transport.HttpTransport`, so each
    response keeps its own receipt instant, monotonic reading and raw payload hash.
    The receipt instant is the observation's usable time and no source time is
    claimed: these responses publish no verified clock. No authenticated channel is
    used, no order route exists, and ``tick_complete`` is ``False`` because polling
    cannot observe the messages between two polls.

    Polls are bounded by duration, by interval, and by a request budget derived
    from them; the budget never falls below one, so an immediate stop still yields
    a single observation instead of nothing. A ``KeyboardInterrupt`` stops the
    loop, keeps everything already archived, and is reported through
    ``interrupted`` and ``stopped_reason``.

    Normalized output is sealed as ``book_events`` and ``quotes`` datasets under
    ``<output_dir>/normalized/<run_id>/``; the exact paths are returned in
    ``outputs``.
    """
    venue_key = str(venue).strip().lower()
    get_spec = _SNAPSHOT_SOURCES.get(venue_key)
    if get_spec is None:
        raise ValueError(f"venue must be one of {sorted(_SNAPSHOT_SOURCES)}, got {venue!r}")
    if not isinstance(contract_id, str) or not contract_id.strip():
        raise ValueError(
            "capture_snapshots requires contract_id: the caller chooses which "
            "legitimate public market to read"
        )
    timeout = _bounded_seconds(
        timeout_seconds, name="timeout_seconds", minimum=0.1, maximum=MAX_TIMEOUT_SECONDS
    )
    duration = _bounded_seconds(
        duration_seconds, name="duration_seconds", minimum=0.0, maximum=MAX_CAPTURE_SECONDS
    )
    interval = _bounded_seconds(
        interval_seconds,
        name="interval_seconds",
        minimum=MIN_CAPTURE_INTERVAL_SECONDS,
        maximum=MAX_CAPTURE_SECONDS,
    )

    out = pathlib.Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    store = RawStore(out / "raw")
    transport = HttpTransport(
        store, timeout_seconds=timeout, policy=_retry_policy(min_interval_seconds=interval)
    )

    started = dt.datetime.now(dt.UTC)
    run_id = started.strftime("%Y%m%dT%H%M%S%fZ")
    observations: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    events: list[BookEvent] = []
    quotes: list[Quote] = []
    reachability: dict[str, Any] | None = None
    stopped_reason = "duration_elapsed"
    interrupted = False
    request_budget = max(1, min(MAX_CAPTURE_REQUESTS, int(duration // interval) + 1))

    state = BookState(order=ORDER_USABLE)
    deadline = time.monotonic() + duration
    try:
        for index in range(request_budget):
            if venue_key == "polymarket" and reachability is None:
                try:
                    reachability = _require_polymarket(
                        store, transport, max_attempts=min(RETRY_ATTEMPTS, 2)
                    )
                except PolymarketUnreachable as exc:
                    reachability = {
                        "venue": "polymarket",
                        "reachable": False,
                        "attempts": None,
                        "blocked": dict(exc.blocked),
                        "reason": (
                            "no documented Polymarket endpoint answered; refusing to "
                            "treat an unreachable source as an empty capture"
                        ),
                    }
                    blocked.append(
                        {
                            "index": index,
                            "contract_id": contract_id,
                            "scope": "reachability",
                            **dict(exc.blocked),
                        }
                    )
                    stopped_reason = "unreachable"
                    break
            url, params, record_id = _snapshot_request(
                index=index, run_id=run_id, contract_id=contract_id, spec=get_spec
            )
            try:
                envelope = transport.get(
                    url,
                    params=params,
                    source=get_spec["source"],
                    record_id=record_id,
                    accept="application/json",
                )
                payload = envelope.json()
                if not isinstance(payload, Mapping):
                    raise WireShapeError(
                        f"snapshot for {contract_id} is a JSON "
                        f"{type(payload).__name__}, not an object"
                    )
            except (TransportError, WireShapeError) as exc:
                record = (
                    exc.as_blocked_record()
                    if isinstance(exc, TransportError)
                    else {
                        "recorded": True,
                        "empty_result": False,
                        "url": url,
                        "status_code": None,
                        "reason": "wire_shape",
                        "attempts": 1,
                        "payload_hash": None,
                        "observed_at": dt.datetime.now(dt.UTC).isoformat(),
                        "detail": str(exc),
                    }
                )
                blocked.append({"index": index, "contract_id": contract_id, **record})
                stopped_reason = "blocked"
                break

            clock = Clock.captured(
                source_time=None,
                received_time=envelope.received_time,
                monotonic_ns=envelope.monotonic_ns,
                uncertainty_seconds=0.0,
                quality=UNMEASURED_CLOCK_QUALITY,
                basis=RECEIPT_WINDOW_BASIS,
            )
            snapshot_events = _normalize_snapshot(
                venue_key,
                payload,
                contract_id=contract_id,
                clock=clock,
                provenance=envelope.provenance,
            )
            quote = state.apply(snapshot_events[0])
            events.extend(snapshot_events)
            if quote is not None:
                quotes.append(quote)
            observations.append(
                _observation(
                    index=index,
                    venue_key=venue_key,
                    contract_id=contract_id,
                    request_record_id=record_id,
                    envelope=envelope,
                    clock=clock,
                    get_spec=get_spec,
                    event=snapshot_events[0],
                    quote=quote,
                )
            )
            if time.monotonic() >= deadline:
                stopped_reason = "duration_elapsed"
                break
        else:
            stopped_reason = "request_budget"
    except KeyboardInterrupt:
        interrupted = True
        stopped_reason = "keyboard_interrupt"
    finally:
        transport.close()

    normalized_dir = out / "normalized" / run_id
    persisted = _persist_normalized(
        store, normalized_dir, events=events, quotes=quotes, run_id=run_id
    )
    capture_path = out / "capture.json"
    summary: dict[str, Any] = {
        "operation": "capture_snapshots",
        "venue": venue_key,
        "contract_id": contract_id,
        "run_id": run_id,
        "mode": CAPTURE_MODE,
        "tick_complete": False,
        "tick_complete_reason": (
            "each poll is an independent public snapshot read; messages between two "
            "polls were not observed"
        ),
        "orders_submitted": 0,
        "authenticated_channels_used": False,
        "orderbook_requests_used": "documented public snapshot GET only",
        "started_at": started.isoformat(),
        "stopped_reason": stopped_reason,
        "interrupted": interrupted,
        "duration_seconds_requested": duration,
        "interval_seconds_requested": interval,
        "timeout_seconds": timeout,
        "request_budget": request_budget,
        "requests_completed": len(observations),
        "pacing": _pacing(interval),
        "reachability": reachability,
        "blocked": blocked,
        "blocked_count": len(blocked),
        "observations": observations,
        "observation_count": len(observations),
        "normalized": persisted,
        "source_clock_policy": {
            "source_time": None,
            "source_time_status": SOURCE_TIME_UNAVAILABLE,
            "source_precision": "unknown",
            "availability_quality": UNMEASURED_CLOCK_QUALITY,
            "availability_quality_basis": (
                "no clock synchronization audit was performed, so the record is "
                "usable on this process's local time axis without any claim that "
                "the axis was synchronized to a source clock"
            ),
            "timing_uncertainty_seconds": None,
            "timing_uncertainty_status": TIMING_UNCERTAINTY_UNMEASURED,
            "usable_time_basis": "availability.upper from each response's receipt instant",
            "received_time_basis": "the moment this process read the response",
            "monotonic_basis": "int monotonic_ns read alongside the receipt; no "
            "sub-second source precision is claimed",
        },
        "outputs": {
            "raw_store": str(store.root),
            "capture": str(capture_path),
            "normalized_dir": str(normalized_dir),
        },
    }
    summary["outputs"]["capture"] = _write_json(capture_path, summary)
    return summary


def _require_polymarket(
    store: RawStore, transport: HttpTransport, *, max_attempts: int
) -> dict[str, Any]:
    """Fail-closed reachability gate before a Polymarket capture.

    The documented hosts were unreachable from this environment, so the existing
    public polling client's own probe is used and an unreachable source raises
    :class:`~market_propagation.ingest.polymarket_public.PolymarketUnreachable`
    rather than producing an empty capture. The caller turns that raise into an
    explicit blocked outcome.
    """
    client = PolymarketPublicClient(store, transport=transport)
    return client.require_reachable(max_attempts=max_attempts).as_dict()


#: Receipt metadata keys that would carry an observed source clock or a stated
#: timestamp uncertainty. The public transport records none of them, and the report
#: says so rather than scoring a clock it never observed.
_CLOCK_METADATA_KEYS = (
    "clock_quality",
    "source_time",
    "timing_uncertainty_seconds",
    "usable_time",
)


class _PayloadCheck(NamedTuple):
    """One blob's verification outcome: verified size, or why it failed.

    The payload bytes are deliberately not part of this record. Identical bytes
    legitimately back several receipts, so one read and one re-hash answers every
    receipt that cites the same hash, and a report over a large store holds no
    payload in memory beyond the check that produced it.
    """

    byte_count: int | None
    reason: str | None
    detail: str | None


def _check_payload(store: RawStore, raw_hash: str) -> _PayloadCheck:
    """Read and re-hash one stored blob once, reporting size or failure reason."""
    try:
        payload = store.get(raw_hash)
    except FileNotFoundError as exc:
        return _PayloadCheck(None, "payload_missing", str(exc))
    except ValueError as exc:
        return _PayloadCheck(None, "payload_hash_mismatch", str(exc))
    return _PayloadCheck(len(payload), None, None)


def quality_report(raw_dir: str | pathlib.Path, output_path: str | pathlib.Path) -> dict[str, Any]:
    """Audit what one raw store actually holds, and what that can support.

    Every receipt is read back from disk and its referenced payload is fetched
    through :meth:`~market_propagation.storage.RawStore.get`, which re-hashes the
    bytes, so a receipt whose blob is missing or altered is reported as a
    verification failure rather than counted as a good record. Payloads are also
    listed independently of the receipt manifest, so bytes with no receipt are
    visible as orphans instead of passing unnoticed.

    The report states what the receipts contain: counts by source and by HTTP
    status, non-2xx and transport failures, the observed spacing between receipt
    instants, whether any per-record source clock or timing uncertainty was
    recorded at all, and the limits of what a receipt can establish. It computes no
    clock-quality score: receipt spacing measures when this process wrote records,
    which is not the accuracy of any source clock and is not treated as one.
    """
    root = pathlib.Path(raw_dir)
    if not root.exists():
        raise FileNotFoundError(f"raw store directory not found: {root}")
    store = RawStore(root)
    receipts = store.receipts()
    stored = store.stored_hashes()
    referenced = {str(receipt.get("raw_hash")) for receipt in receipts}

    by_source: dict[str, int] = {}
    by_status: dict[str, int] = {}
    by_reason: dict[str, int] = {}
    verified: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    non_2xx: list[dict[str, Any]] = []
    clock_evidence: dict[str, int] = dict.fromkeys(_CLOCK_METADATA_KEYS, 0)
    metadata_keys: dict[str, int] = {}
    byte_total = 0

    # One read and one re-hash per distinct blob, reused by every receipt that
    # cites it: bytes may legitimately back several occurrences, and only the
    # status, size and error are kept rather than the payload. The check is made
    # fresh per call, so bytes corrupted after an earlier report still fail here.
    checks = {
        raw_hash: _check_payload(store, raw_hash)
        for raw_hash in sorted(h for h in referenced if _is_hash(h))
    }

    for receipt in receipts:
        source = str(receipt.get("source") or "unknown")
        by_source[source] = by_source.get(source, 0) + 1
        raw_hash = receipt.get("raw_hash")
        metadata = receipt.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        for key in metadata:
            metadata_keys[str(key)] = metadata_keys.get(str(key), 0) + 1
        for key in _CLOCK_METADATA_KEYS:
            if metadata.get(key) is not None:
                clock_evidence[key] += 1
        status = metadata.get("http_status")
        bucket = str(status) if status is not None else "unrecorded"
        by_status[bucket] = by_status.get(bucket, 0) + 1
        if isinstance(status, int) and not 200 <= status < 300:
            non_2xx.append(
                {
                    "receipt_id": receipt.get("receipt_id"),
                    "source": source,
                    "record_id": receipt.get("record_id"),
                    "raw_hash": raw_hash,
                    "http_status": status,
                }
            )
        note = metadata.get("note")
        if note:
            by_reason[str(note)] = by_reason.get(str(note), 0) + 1

        if not _is_hash(raw_hash):
            failures.append(
                {
                    "receipt_id": receipt.get("receipt_id"),
                    "reason": "receipt_carries_no_usable_raw_hash",
                    "raw_hash": raw_hash,
                }
            )
            continue
        check = checks[raw_hash]
        if check.reason is not None:
            failures.append(
                {
                    "receipt_id": receipt.get("receipt_id"),
                    "reason": check.reason,
                    "detail": check.detail,
                    "raw_hash": raw_hash,
                }
            )
            continue
        byte_total += check.byte_count or 0
        verified.append(
            {
                "receipt_id": receipt.get("receipt_id"),
                "source": source,
                "record_id": receipt.get("record_id"),
                "raw_hash": raw_hash,
                "byte_count": check.byte_count,
                "received_time": receipt.get("received_time"),
                "http_status": status,
            }
        )

    orphans = sorted(set(stored) - referenced, key=str)
    spacings, spacing_note = _receipt_spacing(receipts)
    clock_evidence_present = {key: value for key, value in clock_evidence.items() if value}
    report: dict[str, Any] = {
        "operation": "quality_report",
        "raw_dir": str(root),
        "receipts": {
            "count": len(receipts),
            "verified_payloads": len(verified),
            "failed_verifications": len(failures),
            "verification_failures": failures,
            "by_source": dict(sorted(by_source.items())),
            "byte_total": byte_total,
        },
        "blobs": {
            "count": len(stored),
            "referenced_by_a_receipt": len(referenced & set(stored)),
            "orphan_count": len(orphans),
            "orphan_hashes": orphans,
            "orphan_note": (
                "a blob with no receipt has no occurrence identity; identical bytes "
                "may legitimately back several receipts, so one blob per receipt is "
                "not expected"
            ),
        },
        "http": {
            "by_status": dict(sorted(by_status.items())),
            "non_2xx_count": len(non_2xx),
            "non_2xx": non_2xx,
            "note_by_receipt": dict(sorted(by_reason.items())),
            "empty_result_substitution_used": False,
        },
        "receipt_spacing_seconds": spacings,
        "receipt_spacing_note": spacing_note,
        "source_clock_evidence": {
            "metadata_keys_observed": dict(sorted(metadata_keys.items())),
            "records_carrying_each_key": clock_evidence,
            "clock_metadata_keys_present": sorted(clock_evidence_present),
            "records_carrying_any_clock_metadata": sum(clock_evidence_present.values()),
            "source_time_recorded": clock_evidence["source_time"] > 0,
            "timing_uncertainty_recorded": clock_evidence["timing_uncertainty_seconds"] > 0,
            "basis": (
                "the public transport records the receipt instant and an int "
                "monotonic_ns; it states no source clock and no timestamp "
                "uncertainty, so none is scored here"
            ),
        },
        "clock_quality_score": None,
        "clock_quality_score_reason": (
            "no clock accuracy was measured, so no score is computed; receipt "
            "spacing describes when records were written, not the accuracy of any "
            "source clock"
        ),
        "missingness": {
            "receipts_without_http_status": by_status.get("unrecorded", 0),
            "receipts_without_schema_version": sum(
                1 for receipt in receipts if not receipt.get("schema_version")
            ),
            "distinct_schema_versions": sorted(
                {str(receipt.get("schema_version")) for receipt in receipts}
            ),
            "note": (
                "an absent optional field stays absent; it is never filled with a default value"
            ),
        },
        "completeness_limits": [
            "receipts establish that this process stored these bytes at these "
            "instants; they establish nothing about when a source published them",
            "no payload content was parsed here, so a stored body that is an error "
            "page is counted as stored bytes",
            "a missing receipt cannot be detected from the store alone: the report "
            "shows what is present, never that an expected record never arrived",
            "receipt spacing mixes sources and is reported as observed, without any "
            "claim about a documented cadence",
            "blob verification confirms integrity against the content hash, not that "
            "the bytes came from the venue named in the receipt",
        ],
        "outputs": {"report": str(pathlib.Path(output_path))},
    }
    report["outputs"]["report"] = _write_json(pathlib.Path(output_path), report)
    return report


def _receipt_spacing(receipts: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], str]:
    """Observed spacing between consecutive receipt instants, or explicit absence."""
    moments: list[dt.datetime] = []
    unparsed = 0
    for receipt in receipts:
        value = receipt.get("received_time")
        if not isinstance(value, str):
            unparsed += 1
            continue
        try:
            parsed = dt.datetime.fromisoformat(value)
        except ValueError:
            unparsed += 1
            continue
        if parsed.tzinfo is None:
            unparsed += 1
            continue
        moments.append(parsed.astimezone(dt.UTC))
    if len(moments) < 2:
        return (
            {
                "available": False,
                "reason": ("fewer than two parsed receipt instants, so no interval is defined"),
                "unparsed_receipt_times": unparsed,
            },
            "no spacing computed; this is an absence of observations, not a zero interval",
        )
    ordered = sorted(moments)
    deltas = [(later - earlier).total_seconds() for earlier, later in pairwise(ordered)]
    return (
        {
            "available": True,
            "interval_count": len(deltas),
            "min_seconds": min(deltas),
            "median_seconds": statistics.median(deltas),
            "max_seconds": max(deltas),
            "unparsed_receipt_times": unparsed,
        },
        (
            "observed intervals between receipt instants across all sources; a short "
            "interval is when this process wrote records and is not evidence about a "
            "source clock or about a feed's real cadence"
        ),
    )


#: Artifacts the card is assembled from, and whether each is required.
_AUDIT_ARTIFACTS = {
    "coverage": "coverage.json",
    "raw_hashes": "raw_hashes.json",
    "series_discovery": "series_discovery.json",
    "event_card": "event_card.json",
    "release_source": RELEASE_SOURCE_NAME,
}


def event_card(
    audit_dir: str | pathlib.Path,
    output_path: str | pathlib.Path,
    *,
    event_id: str | None = None,
) -> dict[str, Any]:
    """Assemble one event card from an audit's own artifacts, verifying its evidence.

    Reads the audit directory written by :func:`run_audit` — ``coverage.json``
    above all — and reports only what those artifacts record. Every stored hash the
    chosen event cites is re-read through
    :class:`~market_propagation.storage.RawStore`, so a cited payload that is
    missing or altered appears as a verification failure instead of being carried
    forward on the strength of the citation.

    The card reports its own artifact outcome separately from what it found.
    ``status`` is ``created`` when a card was assembled from the audit's readable
    artifacts, and ``event_not_in_audit`` when the audit records no such event.
    Neither implies empirical standing: the event's own status, the audit's
    completeness, the unsatisfied coverage gates and the cited-hash verification
    are reported beside the outcome, so a created card can still carry an
    unsatisfied gate or a citation nobody can re-read.

    Three absences are kept explicit rather than smoothed over: a first release
    whose payload was never retrieved, markets the venue closed before the release
    (direct-resolution material, excluded from the post-release cohort), and every
    recorded access failure. This function issues no request.
    """
    root = pathlib.Path(audit_dir)
    if not root.exists():
        raise FileNotFoundError(f"audit directory not found: {root}")

    artifacts: dict[str, str | None] = {}
    loaded: dict[str, Any] = {}
    for key, name in _AUDIT_ARTIFACTS.items():
        path = root / name
        artifacts[key] = str(path) if path.exists() else None
        if path.exists():
            loaded[key] = _read_json(path)

    coverage = loaded.get("coverage")
    if not isinstance(coverage, Mapping):
        raise FileNotFoundError(
            f"{root} carries no readable coverage.json; an event card cannot be "
            "produced without the audit result it describes"
        )
    events = coverage.get("events")
    events = events if isinstance(events, list) else []

    requested = event_id
    if requested is None:
        prior = loaded.get("event_card")
        if isinstance(prior, Mapping) and prior.get("event_id"):
            requested = str(prior["event_id"])
    chosen: Mapping[str, Any] | None = None
    if requested is not None:
        chosen = next(
            (
                event
                for event in events
                if isinstance(event, Mapping) and str(event.get("event_id")) == str(requested)
            ),
            None,
        )
        if chosen is None:
            return _no_such_event(root, requested, coverage, artifacts, output_path)
    else:
        chosen = next((event for event in events if isinstance(event, Mapping)), None)
    if chosen is None:
        return _no_such_event(root, None, coverage, artifacts, output_path)

    raw_store_dir = root / "raw"
    store_available = raw_store_dir.exists()
    store = RawStore(raw_store_dir) if store_available else None
    cited = [str(value) for value in (chosen.get("raw_hashes") or []) if _is_hash(value)]
    hash_report = _verify_hashes(store, cited)

    manifest = loaded.get("raw_hashes")
    manifest_hashes: list[str] = []
    if isinstance(manifest, Mapping):
        for entry in manifest.get("raw_hashes") or []:
            if isinstance(entry, Mapping) and _is_hash(entry.get("raw_hash")):
                manifest_hashes.append(str(entry["raw_hash"]))
            elif _is_hash(entry):
                manifest_hashes.append(str(entry))

    gates = [
        dict(gate) for gate in (chosen.get("coverage_gates") or []) if isinstance(gate, Mapping)
    ]
    unsatisfied = [str(name) for name in (chosen.get("unsatisfied_gates") or [])]
    release = chosen.get("release")
    release_source = loaded.get("release_source")
    release_source = release_source if isinstance(release_source, Mapping) else None
    archived_record = None
    if release_source is not None and release is not None:
        archived_record = next(
            (
                record
                for record in (release_source.get("archive") or {}).get("records") or ()
                if isinstance(record, Mapping)
                and str(record.get("event_id")) == str(chosen.get("event_id"))
            ),
            None,
        )
    release_gate = next(
        (gate for gate in gates if gate.get("gate") == "release_payload_archived"), None
    )
    release_blockers = [
        dict(record)
        for record in (chosen.get("blocked") or [])
        if isinstance(record, Mapping) and "release" in str(record.get("url", ""))
    ]
    closed_markets = [
        dict(candidate)
        for candidate in (chosen.get("candidates") or [])
        if isinstance(candidate, Mapping) and candidate.get("cohort") == "direct_closed_pre_release"
    ]
    downstream = [
        dict(candidate)
        for candidate in (chosen.get("candidates") or [])
        if isinstance(candidate, Mapping) and candidate.get("cohort") != "direct_closed_pre_release"
    ]

    card: dict[str, Any] = {
        "operation": "event_card",
        "status": "created",
        "gate": "G0",
        "event_id": chosen.get("event_id"),
        "family": chosen.get("family"),
        "reference_period": chosen.get("reference_period"),
        "scheduled_at": chosen.get("scheduled_at"),
        "window_start": chosen.get("window_start"),
        "window_end": chosen.get("window_end"),
        "window_basis": (
            "the measurement window recorded by the audit run, read from the study's "
            "event-window configuration"
        ),
        "event_status": chosen.get("status"),
        "source_urls": dict(chosen.get("source_urls") or {}),
        "audit_status": coverage.get("status"),
        "audit_complete": coverage.get("complete"),
        "cohort_definition_hash": coverage.get("cohort_definition_hash"),
        "candidate_counts": {
            "downstream": len(downstream),
            "direct_closed_pre_release": len(closed_markets),
            "eligible": int(chosen.get("eligible_count") or 0),
            "reported_candidate_count": chosen.get("candidate_count"),
        },
        "first_release": {
            "retrieved": release is not None,
            "values": (release or {}).get("values"),
            "revisions": (release or {}).get("revisions"),
            "values_are_first_release": True if release is not None else None,
            "revisions_kept_separate": True if release is not None else None,
            "raw_hash": (release or {}).get("raw_hash"),
            "schedule_agreement": (release or {}).get("schedule_agreement"),
            "embargo_time_from_payload": (release or {}).get("embargo_time_from_payload"),
            "embargo_is_not_observed_publication": True,
            "observed_publication_at": None,
            "observed_publication_unavailable_reason": (
                "the payload was fetched after the event, so reading it cannot "
                "establish when the material first became public"
            ),
            "absent_reason": (
                None
                if release is not None
                else (
                    "no archived first-release payload was retrieved for this event; "
                    "the absence is recorded rather than replaced with a value"
                )
            ),
            "gate_satisfied": (release_gate or {}).get("satisfied"),
            "gate_detail": (release_gate or {}).get("detail"),
            "access_failures": release_blockers,
            "source": {
                "kind": (release_source or {}).get("kind"),
                "explicitly_selected": (release_source or {}).get("explicitly_selected"),
                "network_release_requests_issued": (release_source or {}).get(
                    "network_release_requests_issued"
                ),
                "fallback_to_network_used": (release_source or {}).get("fallback_to_network_used"),
                "dataset": (release_source or {}).get("dataset"),
                "acquisition_method": (release or {}).get("acquisition_method"),
                "input_dataset_hash": (release or {}).get("input_dataset_hash"),
                "archived_raw_hash": (archived_record or {}).get("archived_raw_hash"),
                "values_verified_against_original_bytes": (archived_record or {}).get(
                    "values_verified_against_original_bytes"
                ),
                "dataset_artifact": artifacts.get("release_source"),
                # The note describes what happened to *this* release, so it is
                # chosen on whether a release was actually read and, when one was,
                # on that release's own acquisition method. Choosing it on the
                # presence of the sibling archive artifact instead let a run whose
                # release was never retrieved print a verification claim while its
                # own values, hash and gate said otherwise.
                "provenance_note": (
                    "no first-release payload was retrieved for this event, so there "
                    "are no first-release values to attribute; the absence is recorded "
                    "rather than replaced"
                    if release is None
                    else (
                        "these first-release values were read from the named sealed "
                        "dataset and verified against the original archived bytes it "
                        "cites; the failed network path an earlier audit recorded is a "
                        "separate record and does not describe this payload"
                        if (release or {}).get("acquisition_method") == ACQUISITION_SEALED_DATASET
                        else "these first-release values came from the public archive "
                        "through the transport, which archived the bytes before parsing "
                        "them"
                    )
                ),
                "not_evidence_of": [
                    "the market rule version in force at this release",
                    "the quote coverage of any contract at this release",
                ],
            },
        },
        "closed_markets": {
            "count": len(closed_markets),
            "markets": closed_markets,
            "treatment": (
                "kept in the direct_closed_pre_release cohort; excluded from the "
                "post-release quote response because the venue's own lifecycle closed "
                "them first"
            ),
            "post_release_response_measured": False,
            "filled_forward": False,
        },
        "downstream_markets": downstream,
        "access_failures": [dict(record) for record in (chosen.get("blocked") or [])],
        "errors": [str(value) for value in (chosen.get("errors") or [])],
        "coverage_gates": gates,
        "unsatisfied_gates": unsatisfied,
        "claims": {
            "supported": [
                claim
                for claim, ok in (
                    (
                        "the candidate contract set the venue listed at audit time",
                        int(chosen.get("candidate_count") or 0) > 0,
                    ),
                    (
                        "the archived first-release values and the payload hash they came from",
                        release is not None,
                    ),
                    (
                        "the requested versus observed candle resolution for the "
                        "contracts attempted",
                        bool(chosen.get("candle_audits")),
                    ),
                )
                if ok
            ],
            "blocked_by_unsatisfied_gates": {
                name: next(
                    (list(gate.get("blocks") or []) for gate in gates if gate.get("gate") == name),
                    [],
                )
                for name in unsatisfied
            },
            "not_supported": [
                "any expectation-relative statement: no point-in-time expectation "
                "source is available to this project",
                "any order-book depth or intra-candle quote reconstruction from candles",
                "any statement about messages that were not observed",
                "causal attribution of any price movement to this release",
            ],
        },
        "evidence_verification": {
            "raw_store": str(raw_store_dir) if store_available else None,
            "raw_store_available": store_available,
            "cited_hash_count": len(cited),
            "verified_hash_count": hash_report["verified_count"],
            "failed_hash_count": hash_report["failed_count"],
            "verified": hash_report["verified"],
            "failures": hash_report["failures"],
            "raw_hashes_manifest_count": len(manifest_hashes),
            "manifest_hashes_not_cited_by_this_event": sorted(set(manifest_hashes) - set(cited)),
            "release_source": {
                "kind": (release_source or {}).get("kind"),
                "dataset_content_hash": ((release_source or {}).get("dataset") or {}).get(
                    "content_hash"
                ),
                "dataset_coverage_epoch": ((release_source or {}).get("dataset") or {}).get(
                    "coverage_epoch"
                ),
                "dataset_row_count": ((release_source or {}).get("dataset") or {}).get("row_count"),
                "records_loaded": ((release_source or {}).get("archive") or {}).get(
                    "records_loaded"
                ),
                "artifact": artifacts.get("release_source"),
                "note": (
                    "the release payload this card cites was copied into the audit's own raw "
                    "store from the named archive, so the hash above re-reads out of this "
                    "audit rather than out of the archive it came from"
                    if release_source is not None
                    else "this audit recorded no release-source artifact"
                ),
            },
            "note": (
                "every cited hash was re-read through RawStore, which re-hashes the "
                "bytes; a failure here is a missing or altered payload"
                if store_available
                else "the audit's raw store is not present beside the audit artifacts, "
                "so cited hashes could not be independently re-read"
            ),
        },
        "source_artifacts": artifacts,
        "data_limits": [
            "candles are candle-frequency observations and are not order-book depth",
            "the candidate universe is bounded by the audit's page and contract caps, "
            "so its completeness is not established",
            "no point-in-time expectation source exists, so no surprise is estimable",
        ],
        "outputs": {"card": str(pathlib.Path(output_path))},
    }
    card["outputs"]["card"] = _write_json(pathlib.Path(output_path), card)
    return card


def _verify_hashes(store: RawStore | None, hashes: Sequence[str]) -> dict[str, Any]:
    """Re-read every cited payload, reporting each distinct failure reason."""
    verified: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for raw_hash in hashes:
        if store is None:
            failures.append({"raw_hash": raw_hash, "reason": "raw_store_unavailable"})
            continue
        try:
            payload = store.get(raw_hash)
        except FileNotFoundError as exc:
            failures.append({"raw_hash": raw_hash, "reason": "payload_missing", "detail": str(exc)})
            continue
        except ValueError as exc:
            failures.append(
                {
                    "raw_hash": raw_hash,
                    "reason": "payload_hash_mismatch",
                    "detail": str(exc),
                }
            )
            continue
        verified.append({"raw_hash": raw_hash, "byte_count": len(payload)})
    return {
        "verified": verified,
        "failures": failures,
        "verified_count": len(verified),
        "failed_count": len(failures),
    }


def _no_such_event(
    root: pathlib.Path,
    requested: str | None,
    coverage: Mapping[str, Any],
    artifacts: Mapping[str, str | None],
    output_path: str | pathlib.Path,
) -> dict[str, Any]:
    """An explicit card for an event the audit does not hold.

    A requested event with no audit record is a real outcome, and it is reported
    as one rather than as an empty card that could be read as an event with no
    movement.
    """
    events = coverage.get("events")
    available = [
        str(event.get("event_id"))
        for event in (events if isinstance(events, list) else [])
        if isinstance(event, Mapping)
    ]
    card: dict[str, Any] = {
        "operation": "event_card",
        "status": "event_not_in_audit",
        "event_id": requested,
        "status_reason": (
            "the audit's coverage artifact carries no record of this event, so no "
            "evidence about it exists to report"
        ),
        "requested_event_id": requested,
        "audited_event_ids": available,
        "audited_event_count": len(available),
        "cohort_definition_hash": coverage.get("cohort_definition_hash"),
        "audit_status": coverage.get("status"),
        "audit_complete": coverage.get("complete"),
        "first_release": None,
        "closed_markets": None,
        "access_failures": [],
        "claims": {"supported": [], "not_supported": ["any claim about this event"]},
        "source_artifacts": dict(artifacts),
        "outputs": {"card": str(pathlib.Path(output_path))},
    }
    card["outputs"]["card"] = _write_json(pathlib.Path(output_path), card)
    return card
