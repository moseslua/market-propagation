"""Command-line surface for the market-propagation study.

Eleven read-only commands. Each prints one JSON document to stdout, so the exit code
and the parsed payload agree:

* ``0`` — the command ran and its own result is complete for what it claims.
* ``1`` — invalid arguments or a technical failure (an unreadable file, a bad
  configuration, a refused request shape). Nothing is claimed either way.
* ``2`` — the command ran, and its own result reports blocked or absent evidence:
  an audit with unsatisfied coverage gates, a capture that recorded access
  failures, a quality report that verified no payload or failed a payload hash, a
  reproduction with a blocked stage, an event card whose cited payloads cannot be
  re-read, or a requested event the audit holds no record of. A card that was
  assembled and whose citations verify exits ``0`` even when it reports an
  unsatisfied empirical gate, the way ``reproduce`` completes with unpromoted
  results. Exit ``2`` is a successful computation of a blocked or negative
  finding, never an exception.

Argument errors exit ``1`` rather than argparse's customary ``2``, so ``2`` means
exactly one thing and a scheduler can act on it. Every command only reads public
data or local files: no command places an order, holds a credential, or spends.

Each handler calls the owning module directly and forwards only the options the
caller supplied, so a flag left off keeps the library's own default instead of a
number duplicated here.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import pathlib
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any, NoReturn

import yaml

from .ingest.macro_releases import ACQUISITION_SEALED_DATASET
from .operations import (
    capture_snapshots,
    event_card,
    quality_report,
    run_audit,
)

PROGRAM = "market-propagation"

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_BLOCKED = 2

#: The two public snapshot surfaces `capture` can read.
VENUES = ("kalshi", "polymarket")

#: The pipeline configuration the external-history commands read by default.
DEFAULT_EXTERNAL_CONFIG = "configs/external_history_v1.yaml"

#: The declared perpetual-futures source configuration and its snapshot store root.
#: Both mirror the package defaults; ``tests/test_perp.py`` pins them together so
#: the two copies cannot drift apart unnoticed.
DEFAULT_PERP_CONFIG = "configs/perp_arbitrage_v1.yaml"
DEFAULT_PERP_ROOT = "data/perp"

#: The declared evidence standard for rule intervals. It mirrors the package
#: default ``rule_attestation.CONFIG_PATH``; the equality is pinned by a test so
#: the two copies cannot drift apart unnoticed.
DEFAULT_ATTESTATION_CONFIG = "configs/rule_attestation_v1.yaml"

#: The declared inputs the cross-venue candidate universe is drawn from.
DEFAULT_MATCH_CONFIG = "configs/matching_v1.yaml"
DEFAULT_COHORT_CONFIG = "configs/cohort_v2.yaml"
DEFAULT_GRAPH_CONFIG = "configs/neighbor_graph_v2.yaml"

#: The two local layers the cross-venue candidate universe is drawn from.
DEFAULT_MARKETS_GLOB = "data/external/kalshi-trades/markets-*.parquet"
DEFAULT_SECOND_VENUE_GLOB = "data/external/polymarket-v1/daily_aligned_multi/*.parquet"


def _jsonable(value: Any) -> Any:
    """Recursively make a result JSON-representable without inventing a value.

    A non-finite float becomes an explicit ``null`` rather than a token no JSON
    consumer should accept, and an unknown object is described by its text form
    instead of being dropped from the document.
    """
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.UTC).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_jsonable(item) for item in value), key=str)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    scalar = getattr(value, "item", None)
    if callable(scalar):
        try:
            return _jsonable(scalar())
        except (TypeError, ValueError):
            pass
    return str(value)


def _print_json(payload: Any) -> None:
    json.dump(_jsonable(payload), sys.stdout, indent=2, sort_keys=True, allow_nan=False)
    sys.stdout.write("\n")


def _note(message: str) -> None:
    print(f"{PROGRAM}: {message}", file=sys.stderr)


def _write_json(path: pathlib.Path, payload: Any) -> None:
    """Write an artifact under the output directory a command was given.

    Every external command's ``--output`` must actually be used, and the coverage
    builder is the one stage whose entry point takes no output path of its own: it
    returns the report and leaves persisting it to the caller. A required flag that
    nothing reads would be an inert flag, so this is where the coverage artifact lands.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(_jsonable(payload), indent=2, sort_keys=True, allow_nan=False)
    path.write_text(text + "\n", encoding="utf-8")


def _dig(mapping: Mapping[str, Any], dotted: str, *, purpose: str) -> Any:
    """Read one dotted key out of a parsed configuration or report.

    A required key that is absent raises instead of falling back to a value defined
    here: a second copy of the pipeline's own bound, layer or window would be a second
    source of truth for it.
    """
    current: Any = mapping
    for key in dotted.split("."):
        if not isinstance(current, Mapping) or key not in current:
            raise ValueError(
                f"{purpose} carries no {dotted!r}; the pipeline configuration names it and "
                "this command defines no fallback for it"
            )
        current = current[key]
    return current


def _pipeline_config(path: str) -> Mapping[str, Any]:
    """Read one external-history pipeline configuration the caller named."""
    config_path = pathlib.Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"pipeline configuration not found: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ValueError(
            f"pipeline configuration at {config_path} is not valid YAML: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise ValueError(
            f"pipeline configuration at {config_path} must be a mapping, "
            f"got {type(payload).__name__}"
        )
    return payload


def _repo_relative(candidate: Any, *, config_path: str) -> pathlib.Path:
    """Resolve a configured input path against the repository root, not the working directory.

    The configuration names its archives and datasets relative to the checkout, so a run
    started from any directory reads the same bytes. An absolute path is taken as given.
    """
    path = pathlib.Path(str(candidate))
    if path.is_absolute():
        return path
    anchor = pathlib.Path(config_path).resolve().parent
    root = anchor.parent if anchor.name == "configs" else anchor
    return root / path


def _iso_instant(text: str) -> str:
    """Validate an ISO-8601 instant and keep it as text for the library to parse.

    An unparseable bound is an argument error rather than a failure partway through a
    read, and the text is passed on unchanged so a caller-supplied bound is parsed by the
    same code that parses the configured one.
    """
    try:
        dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not an ISO-8601 timestamp: {text!r}") from exc
    return text


def _candidate_grid(path: str) -> dict[str, list[tuple[str, str]]]:
    """Read a declared per-release candidate grid, chosen from pre-event information.

    The grid is what makes the denominator declared rather than activity-selected: each
    release names its candidate ``(venue, contract_id)`` pairs, and every named pair keeps
    its rows in the panel even when it never traded. A malformed grid is an argument error,
    because a partly-read grid would silently shrink the universe it is supposed to freeze.
    """
    grid_path = pathlib.Path(path)
    if not grid_path.exists():
        raise argparse.ArgumentTypeError(f"candidate grid not found: {grid_path}")
    try:
        payload = json.loads(grid_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise argparse.ArgumentTypeError(
            f"candidate grid at {grid_path} is not readable JSON: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise argparse.ArgumentTypeError(
            f"candidate grid at {grid_path} must map release ids to candidate lists, "
            f"got {type(payload).__name__}"
        )
    grid: dict[str, list[tuple[str, str]]] = {}
    for event_id, entries in payload.items():
        if not isinstance(event_id, str) or not event_id.strip():
            raise argparse.ArgumentTypeError(
                f"candidate grid release id {event_id!r} is not a name"
            )
        if isinstance(entries, (str, bytes)) or not isinstance(entries, (list, tuple)):
            raise argparse.ArgumentTypeError(
                f"candidate grid entry for {event_id!r} must be a list of (venue, contract_id) "
                f"pairs, got {type(entries).__name__}"
            )
        pairs: list[tuple[str, str]] = []
        for entry in entries:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                raise argparse.ArgumentTypeError(
                    f"candidate grid entry for {event_id!r} is not a (venue, contract_id) pair: "
                    f"{entry!r}"
                )
            venue, contract_id = entry
            if not isinstance(venue, str) or not isinstance(contract_id, str):
                raise argparse.ArgumentTypeError(
                    f"candidate grid entry for {event_id!r} must name strings, got {entry!r}"
                )
            pairs.append((venue, contract_id))
        grid[event_id] = pairs
    return grid


def _dataset_record(reference: Any) -> dict[str, Any]:
    """The identity fields of one sealed dataset, as a caller records them."""
    return {
        "path": reference.path,
        "table": reference.table,
        "schema_version": reference.schema_version,
        "coverage_epoch": reference.coverage_epoch,
        "content_hash": reference.content_hash,
        "row_count": reference.row_count,
    }


def _gate_is_blocked(gate: Any) -> bool:
    """Whether a coverage gate is recorded as blocked.

    The contract freezes the blocked literal and leaves each satisfying value to the
    module that owns the gate, so this tests for the blocked word rather than enumerating
    passing values it would have to guess at.
    """
    return str(gate).strip().lower() == "blocked"


def _run_reproduce(args: argparse.Namespace) -> int:
    """Rebuild the offline reproduction from the packaged synthetic fixture."""
    from . import reporting

    options: dict[str, Any] = {}
    if args.spec is not None:
        options["spec_path"] = args.spec
    if args.events is not None:
        options["n_events"] = args.events
    if args.repetitions is not None:
        options["repetitions"] = args.repetitions
    if args.bootstrap is not None:
        options["bootstrap_samples"] = args.bootstrap
    if args.real_audit is not None:
        options["real_audit_dir"] = args.real_audit
    if args.release_dataset is not None:
        options["release_dataset"] = args.release_dataset

    record = reporting.reproduce(args.output, **options)
    _print_json(record)
    if record.get("complete") is True:
        return EXIT_OK
    for stage in record.get("blocked_stages") or ():
        _note(f"blocked stage: {stage}")
    _note(
        f"reproduction status is {record.get('status')!r}; its artifacts are under "
        f"{args.output}. A blocked or incomplete stage means the reproduction does "
        "not support its claims."
    )
    return EXIT_BLOCKED


def _run_audit(args: argparse.Namespace) -> int:
    """Run the bounded G0 coverage audit and print its report."""
    options: dict[str, Any] = {}
    if args.cohort is not None:
        options["cohort_path"] = args.cohort
    if args.windows is not None:
        options["windows_path"] = args.windows
    if args.timeout is not None:
        options["timeout_seconds"] = args.timeout
    if args.max_pages is not None:
        options["max_pages"] = args.max_pages
    if args.max_contracts is not None:
        options["max_contracts"] = args.max_contracts
    if args.max_candle_contracts is not None:
        options["max_candle_contracts"] = args.max_candle_contracts
    if args.archive_raw_root is not None and args.release_dataset is None:
        # Accepting this would leave the flag inert and quietly run the live network
        # path, while the operator believes the run is reading an offline archive.
        _note(
            "--archive-raw-root names the raw store of a --release-dataset, and no "
            "--release-dataset was given; refusing to fall back to the network for a "
            "run that named an archived raw store"
        )
        return EXIT_ERROR
    if args.release_dataset is not None:
        options["release_dataset"] = args.release_dataset
    if args.archive_raw_root is not None:
        options["archive_raw_root"] = args.archive_raw_root

    report = run_audit(args.output, **options)
    _print_json(report)
    if report.get("complete") is True:
        return EXIT_OK
    access = report.get("access")
    blocked = access.get("blocked_count") if isinstance(access, Mapping) else None
    eligibility = report.get("eligibility")
    established = (
        eligibility.get("study_eligibility_established")
        if isinstance(eligibility, Mapping)
        else None
    )
    source = report.get("release_source")
    source_kind = source.get("kind") if isinstance(source, Mapping) else None
    _note(
        f"audit status is {report.get('status')!r} with {blocked!r} blocked access "
        "record(s); its coverage gates are unsatisfied, so G0 is not established "
        f"by this run (study eligibility established: {established!r}). The "
        "artifacts record what was read, not what was eligible."
    )
    if source_kind == ACQUISITION_SEALED_DATASET:
        _note(
            f"first releases were read from the sealed dataset "
            f"{(source.get('dataset') or {}).get('path')!r} and verified against it, and no "
            "release was fetched. Loading them does not certify the market rule versions or "
            "quote coverage a study-eligible cohort would need."
        )
    return EXIT_BLOCKED


def _run_capture(args: argparse.Namespace) -> int:
    """Capture bounded public book snapshots for one contract."""
    options: dict[str, Any] = {}
    if args.duration is not None:
        options["duration_seconds"] = args.duration
    if args.interval is not None:
        options["interval_seconds"] = args.interval
    if args.timeout is not None:
        options["timeout_seconds"] = args.timeout

    summary = capture_snapshots(args.output, venue=args.venue, contract_id=args.contract, **options)
    _print_json(summary)
    blocked = summary.get("blocked_count")
    if isinstance(blocked, int) and blocked > 0:
        _note(
            f"capture recorded {blocked} blocked request(s); stopped_reason="
            f"{summary.get('stopped_reason')!r}. The archived observations are under "
            f"{args.output} and exclude every failed poll."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_quality(args: argparse.Namespace) -> int:
    """Report what one raw store actually holds and what it can support."""
    report = quality_report(args.raw_dir, args.output)
    _print_json(report)
    receipts = report.get("receipts")
    count = receipts.get("count") if isinstance(receipts, Mapping) else None
    failures = receipts.get("failed_verifications") if isinstance(receipts, Mapping) else None
    if isinstance(failures, int) and failures > 0:
        _note(
            f"{failures} archived payload(s) failed verification against their content "
            f"hash; the store is not intact. Report: {report.get('outputs', {}).get('report')}"
        )
        return EXIT_BLOCKED
    if count == 0:
        _note(
            "the raw store holds no receipt, so this report verifies nothing; an empty "
            "store is reported as empty rather than as a clean one"
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_registry_review(args: argparse.Namespace) -> int:
    """Print the durable runs, locked-test reservations and event claims."""
    from .registry import ExperimentRegistry

    target = args.registry_path
    if target != ":memory:" and not pathlib.Path(target).expanduser().exists():
        raise FileNotFoundError(
            f"registry not found: {target}; a review reads durable state and never "
            "creates a registry to look at an empty one"
        )
    with ExperimentRegistry(target) as registry:
        state = registry.snapshot()
    _print_json({"registry_path": target, **state})
    return EXIT_OK


def _run_event_card(args: argparse.Namespace) -> int:
    """Assemble one event card from an audit directory's own artifacts."""
    card = event_card(args.audit_dir, args.output, event_id=args.event_id)
    _print_json(card)
    # ``status`` reports whether the artifact was assembled, which is a different
    # question from what the audit behind it establishes. ``reproduce`` completes
    # with unpromoted results, and a card assembled from a partial audit is the
    # same shape of outcome: the file exists and the gates that fail stay in it.
    status = card.get("status")
    if status == "created":
        verification = card.get("evidence_verification")
        verification = verification if isinstance(verification, Mapping) else {}
        failures = [
            failure
            for failure in (verification.get("failures") or [])
            if isinstance(failure, Mapping)
        ]
        if failures:
            listed = "; ".join(
                f"{failure.get('raw_hash')} ({failure.get('reason')})" for failure in failures
            )
            _note(
                f"the card was written to {card.get('outputs', {}).get('card')}, but "
                f"{len(failures)} cited payload(s) could not be re-read against their "
                f"content hash: {listed}. A card whose evidence does not verify is not "
                "a usable result."
            )
            return EXIT_BLOCKED
        _note_empirical_standing(card)
        return EXIT_OK
    if status == "event_not_in_audit":
        # The command ran correctly and the evidence for this event is absent,
        # whether the audit is incomplete or complete and simply does not hold it.
        # The outcome record itself is a written artifact, and the note says so
        # rather than reading as "nothing was produced".
        _note(
            f"no card describes event {args.event_id!r}. The audit's coverage artifact "
            f"records no such event, so no evidence about it exists to report. Events "
            f"it does hold: {card.get('audited_event_ids')} (audit_complete="
            f"{card.get('audit_complete')!r}). An outcome record with that finding was "
            f"written to {card.get('outputs', {}).get('card')}."
        )
        return EXIT_BLOCKED
    _note(
        f"the card reports status {status!r}, which this command does not recognize "
        "as a produced card or a known absence; a technical failure is reported "
        "rather than a result"
    )
    return EXIT_ERROR


def _run_inventory_external(args: argparse.Namespace) -> int:
    """Inventory the external archive layers without reading their rows into memory.

    The inventory is a manifest, so a missing layer or a shard whose footer cannot be
    read is a recorded quality flag on a still-produced artifact rather than a failed
    run. Either one means the archive does not hold what the layer claims, which is a
    blocked finding and not an error.
    """
    from .ingest import external_inventory

    inventory = external_inventory.build_inventory(
        args.root,
        config_path=args.config,
        verify_hashes=not args.no_hashes,
    )
    result = external_inventory.write_inventory(inventory, args.output)
    _print_json(result)

    absent = list(result.get("absent_evidence") or ())
    if absent:
        missing = list(result.get("layers_missing") or ())
        unreadable = list(result.get("shards_unreadable") or ())
        _note(
            f"the inventory at {result.get('path')} records {len(absent)} absent-evidence "
            f"finding(s) {absent}: missing layer(s) {missing or 'none'}, unreadable "
            f"shard(s) {unreadable or 'none'}. A layer or shard the archive does not "
            "actually hold cannot support a measurement, and the manifest says so rather "
            "than reading as a complete inventory."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_normalize_external(args: argparse.Namespace) -> int:
    """Normalize one external archive layer into sealed historical trade records.

    The archive root, the layer and the window come from the pipeline configuration when
    the caller does not name them, because the CLI does not carry a second copy of a
    configured bound. A read that stopped at its own cap is reported as incomplete for
    the window it claims rather than silently truncated.
    """
    from .ingest import external_history, external_inventory

    config = _pipeline_config(args.config)
    root = _repo_relative(
        _dig(config, "inputs.root", purpose="pipeline configuration"),
        config_path=args.config,
    )
    layer = args.layer
    if layer is None:
        layer = _dig(config, "normalization.kalshi_layer", purpose="pipeline configuration")
    declared = external_inventory.load_layer_specs(args.config)
    known = tuple(spec.name for spec in declared)
    if layer not in known:
        _note(
            f"unknown layer {layer!r}; the pipeline configuration declares {list(known)}. "
            "An unconfigured layer is refused rather than replaced by the default one, "
            "because reading a different layer than the one asked for would misdescribe "
            "the result."
        )
        return EXIT_ERROR
    window_start = args.window_start
    if window_start is None:
        window_start = _dig(config, "extraction.window_start", purpose="pipeline configuration")
    window_end = args.window_end
    if window_end is None:
        window_end = _dig(config, "extraction.window_end", purpose="pipeline configuration")

    options: dict[str, Any] = {}
    if args.max_rows is not None:
        options["max_rows"] = args.max_rows
    # One layer is read, so only that layer is inventoried: extracting one layer does not
    # require the footer of every other shard in a 58 GB archive to be parsed first.
    inventory = external_inventory.build_inventory(root, layers=[layer], config_path=args.config)
    extraction = external_history.extract_trades(
        root,
        inventory,
        layer=layer,
        window_start=window_start,
        window_end=window_end,
        **options,
    )
    reference = external_history.write_trades(
        extraction.trades, pathlib.Path(args.output) / "historical_trades.parquet"
    )
    summary = extraction.as_dict()
    record = {
        "operation": "normalize_external",
        "root": str(root),
        "output": _dataset_record(reference),
        **summary,
    }
    _print_json(record)

    count = record.get("trade_count")
    if count == 0:
        _note(
            f"the archive layer {layer!r} holds no trade inside the window "
            f"[{window_start}, {window_end}], so the extraction at "
            f"{record['output']['path']} is empty. Nothing was normalized because there "
            "was nothing in the window to normalize."
        )
        return EXIT_BLOCKED
    if record.get("bounded") is True:
        _note(
            f"the extraction stopped at max_rows={record.get('max_rows_applied')!r} after "
            f"{count} row(s), so it is not complete for the window it claims; the declared "
            "bound was applied rather than the read finishing. The applied bound is "
            "recorded in the artifact."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_coverage_external(args: argparse.Namespace) -> int:
    """Build the release-linked external coverage grid for the configured cohort.

    The coverage inputs and the rule join belong to the module that owns the semantics,
    so they are loaded rather than rebuilt here. G0 stays blocked unless rules, cohort
    and supported frequency pass, and this command reports that blocking instead of
    estimating around it.
    """
    from .ingest import audit

    options: dict[str, Any] = {}
    if args.release_dataset is not None:
        options["release_dataset"] = args.release_dataset
    else:
        # The configured dataset is the one the study names. Resolving it here keeps the
        # CLI from fabricating a default: an omitted flag means the configuration decides,
        # and a configured path that does not exist raises rather than being ignored.
        options["release_dataset"] = _repo_relative(
            _dig(
                _pipeline_config(args.config),
                "inputs.release_dataset",
                purpose="pipeline configuration",
            ),
            config_path=args.config,
        )
    if args.max_contracts is not None:
        options["max_contracts"] = args.max_contracts
    inputs = audit.load_coverage_inputs(args.config, **options)
    report = audit.build_external_coverage(
        releases=inputs["releases"],
        pairs=inputs["pairs"],
        settings=inputs["settings"],
        config=inputs["config"],
        # What the run actually read, so a grid built without rule evidence says so in
        # its own artifact instead of looking like a grid with nothing to report.
        inputs=inputs["inputs"],
    )
    payload = report.as_dict()
    record = {"operation": "coverage_external", **payload}
    coverage_path = pathlib.Path(args.output) / "coverage_external.json"
    _write_json(coverage_path, record)
    _print_json(record)

    gate = record.get("gate_g0")
    if _gate_is_blocked(gate):
        blocked = [entry for entry in (record.get("blocked") or ()) if isinstance(entry, Mapping)]
        reasons = sorted(
            {str(entry.get("reason")) for entry in blocked if entry.get("reason") is not None}
        )
        _note(
            f"G0 is {gate!r}: the coverage grid is written to {coverage_path} and reports "
            f"{len(blocked)} blocked candidate(s) with reason(s) {reasons or 'unnamed'}. "
            "A grid whose rules, cohort or supported frequency do not pass establishes no "
            "eligible cohort, and the missing cells stay in the artifact."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_build_trade_panel(args: argparse.Namespace) -> int:
    """Build the transaction response panel from sealed trades and the configured cohort.

    An event whose rule evidence is absent is masked with its reason rather than admitted,
    and a horizon the configuration does not already carry is refused, so the panel never
    silently covers a window the study did not freeze.
    """
    from . import trade_panel

    settings = trade_panel.load_panel_settings(args.config)
    narrowing: dict[str, Any] = {}
    if args.horizon is not None:
        configured = tuple(settings.horizons_seconds)
        if args.horizon not in configured:
            _note(
                f"--horizon {args.horizon} is not one of the configured horizons "
                f"{list(configured)}; a requested horizon narrows the configured set and "
                "never introduces one the study did not freeze"
            )
            return EXIT_ERROR
        # Narrow and re-primary together: a narrowed set whose primary still names an
        # absent horizon would report a zero-count primary instead of the requested one.
        settings = replace(
            settings,
            horizons_seconds=(args.horizon,),
            primary_horizon_seconds=args.horizon,
        )
        narrowing = {
            "horizon_narrowed": True,
            "requested_horizon_seconds": args.horizon,
            "configured_horizons_seconds": list(configured),
        }
    clock_mode = args.clock_mode
    if clock_mode is None:
        clock_mode = trade_panel.CLOCK_MODES[0]
    if clock_mode not in trade_panel.CLOCK_MODES:
        _note(
            f"unknown clock mode {clock_mode!r}; permitted modes are "
            f"{list(trade_panel.CLOCK_MODES)}"
        )
        return EXIT_ERROR

    config = _pipeline_config(args.config)
    root = _repo_relative(
        _dig(config, "inputs.release_dataset", purpose="pipeline configuration"),
        config_path=args.config,
    )
    rule_evidence_path = _repo_relative(
        _dig(config, "inputs.rule_evidence_source", purpose="pipeline configuration"),
        config_path=args.config,
    )
    audit_coverage_path = _repo_relative(
        _dig(config, "inputs.audit_coverage", purpose="pipeline configuration"),
        config_path=args.config,
    )
    events = trade_panel.load_event_specs(
        root,
        rule_evidence_path=rule_evidence_path,
        audit_coverage_path=audit_coverage_path,
    )

    from .ingest import external_history

    candidates = args.candidate_grid
    trades = external_history.load_trades(args.trades)
    panel = trade_panel.build_trade_panel(
        trades, events, settings=settings, clock_mode=clock_mode, candidates=candidates
    )
    reference = panel.write(pathlib.Path(args.output) / "trade_panel.parquet")
    summary = panel.as_dict()
    # Every row is in the sealed dataset, so the JSON keeps the summary rather than a
    # second copy of the panel.
    summary.pop("rows", None)
    record = {
        "operation": "build_trade_panel",
        "output": _dataset_record(reference),
        **summary,
        **narrowing,
    }
    _print_json(record)

    counts = record["counts"]
    overall = counts.get("overall") if isinstance(counts, Mapping) else None
    valid = overall.get("valid_rows") if isinstance(overall, Mapping) else None
    if not valid:
        _note(
            f"no panel row is valid: the panel at {record['output']['path']} holds "
            f"{record['row_count']} row(s) with none valid, so it supports no response "
            f"measurement. Counts: {counts!r}."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_report_external(args: argparse.Namespace) -> int:
    """Assemble the external report from an existing panel and coverage artifact.

    A blocked panel produces a report whose gate is recorded as blocked, and that is a
    result rather than a failure: the artifact says what could not be established instead
    of substituting an estimate for it.
    """
    from . import external_report

    options: dict[str, Any] = {}
    if args.run_id is not None:
        options["run_id"] = args.run_id
    payload = external_report.run_external_report(args.config, args.panel, args.output, **options)
    _print_json(payload)
    if payload.get("blocked") is True:
        detail = payload.get("gate_detail")
        reason = detail.get("reason") if isinstance(detail, Mapping) else None
        _note(
            f"the report's gate is {payload.get('gate')!r} (status "
            f"{payload.get('status')!r}): {reason or 'no reason recorded'}. The "
            "artifacts are written and the blocked gate is preserved in them; a blocked "
            "gate is reported rather than estimated around."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_study_external(args: argparse.Namespace) -> int:
    """Fit the declared ladder on a sealed panel and record the run durably.

    The absorption rung is fit and the propagation rung is audited. A panel whose
    rows are all masked produces a blocked absorption result and a blocked
    propagation audit, and both are results rather than failures: the artifact
    names the missing input instead of fitting a substitute and calling it the
    study's claim.
    """
    from . import study

    payload = study.run_study(
        args.panel,
        output_dir=args.output,
        run_id=args.run_id,
        forecast_panel_path=getattr(args, "forecast_panel", None),
        registry_path=getattr(args, "registry", None),
    )
    _print_json(payload)
    blocked = sorted(
        name
        for name, fit in (payload.get("families") or {}).items()
        if fit.get("status") != study.STATUS_COMPLETE
    )
    propagation = payload.get("propagation") or {}
    if blocked or not propagation.get("supported"):
        missing = (
            ", ".join(
                str(name)
                for name in (
                    list(propagation.get("missing_neighbor_columns", []))
                    + list(propagation.get("missing_news_columns", []))
                )
            )
            or "none"
        )
        detail = "; ".join(
            f"{name}: {(payload['families'][name] or {}).get('reason')}" for name in blocked
        )
        _note(
            f"the propagation rung is blocked (missing: {missing}) and blocked famil(ies): "
            f"{blocked or 'none'}. {detail or 'no family reason recorded'}. The result and "
            "the registry record are written; a blocked rung is reported rather than "
            "estimated around."
        )
        return EXIT_BLOCKED
    return EXIT_OK


def _run_calibrate_rule(args: argparse.Namespace) -> int:
    """Calibrate the network-promotion decision rule on simulated tapes.

    This measures the size and the recovery of the rule the study ships, on a process
    whose truth is known, because the archive still has no eligible contracts to run it
    on. The verdict is ``pass``, ``fail`` or ``inconclusive``: a rule that the repetition
    count cannot certify is reported inconclusive, never promoted to a pass.
    """
    from . import calibration

    # Only the options the caller actually supplied are passed, so the module's own
    # declared defaults stay the single place the calibration's settings are written.
    options: dict[str, Any] = {}
    if args.seed is not None:
        options["seed"] = args.seed
    if args.repetitions is not None:
        options["repetitions"] = args.repetitions
    if args.releases is not None:
        options["n_releases"] = args.releases
    if args.bootstrap is not None:
        options["bootstrap_samples"] = args.bootstrap
    if args.workers is not None:
        options["workers"] = args.workers
    if args.ceiling is not None:
        options["false_positive_ceiling"] = args.ceiling
    if args.target_power is not None:
        options["target_power"] = args.target_power
    if args.registry is not None:
        options["registry_path"] = args.registry
    if args.run_id is not None:
        options["run_id"] = args.run_id

    certificate = calibration.calibrate(**options)
    output = pathlib.Path(args.output)
    if output.parent != pathlib.Path(""):
        output.parent.mkdir(parents=True, exist_ok=True)
    written = calibration.write_certificate(certificate, output)
    payload = certificate.as_dict()
    payload["certificate_path"] = str(written)
    _print_json(payload)
    _note(
        f"verdict={certificate.verdict} over {certificate.repetitions} repetition(s) per "
        f"scenario, {len(certificate.estimable_nulls)} of "
        f"{len(certificate.null_scenarios)} null scenario(s) estimable; "
        f"certificate written to {written}"
    )
    for reason in certificate.verdict_reasons:
        _note(reason)
    registry = certificate.registry or {}
    if registry.get("recorded"):
        _note(
            f"recorded run {registry.get('run_id')} in {registry.get('path')} "
            f"(synthetic, so it is not readable as an empirical estimate)"
        )
    elif registry:
        _note(str(registry.get("reason")))
    _note(
        "this certifies the behaviour of the decision rule on a declared synthetic process "
        "and is not an empirical finding about any venue, release or contract"
    )
    return EXIT_OK if certificate.verdict == calibration.VERDICT_PASS else EXIT_BLOCKED


def _declared_policy_series() -> tuple[str, ...]:
    """The declared policy series, so the capture universe is not chosen by this command."""
    payload = yaml.safe_load(pathlib.Path(DEFAULT_COHORT_CONFIG).read_text(encoding="utf-8"))
    series = tuple(str(name) for name in (payload.get("policy_series") or ()))
    if not series:
        raise ValueError(
            f"{DEFAULT_COHORT_CONFIG} declares no policy_series; the capture universe is a "
            "declared cohort decision and this command keeps no fallback list of its own"
        )
    return series


def _rule_text_in(body_text: str, value: str) -> str:
    """``value`` in the exact form the archived bytes carry it.

    ``RuleCaptureStore.verify`` requires the recorded rule text to occur in the bytes
    the record cites. A payout text carrying a character JSON escapes occurs in the
    body only in its escaped form, so the escaped form is what is recorded when the
    plain one is absent. A text in neither form means the payload and the record
    disagree, which is refused rather than recorded.
    """
    if value in body_text:
        return value
    escaped = json.dumps(value)[1:-1]
    if escaped in body_text:
        return escaped
    raise ValueError(
        "the rule text read from the listing does not occur in the listing's own bytes; "
        "the archived payload and the record would disagree about the same contract"
    )


def _run_capture_rules(args: argparse.Namespace) -> int:
    """Capture the venue's live rule text for every contract in the declared series.

    One GET per page of the venue's live listing, fanned out into one immutable
    capture per contract on that page. Every capture cites the page's own bytes and
    carries the page's own stated instant, so no bound is taken from this run's clock.
    """
    from .ingest import rule_attestation
    from .ingest.kalshi_rest import KALSHI_BASE_URL
    from .ingest.transport import HttpTransport, RetryPolicy, TransportError
    from .operations import REQUEST_PACING_SECONDS

    settings = rule_attestation.load_attestation_settings(args.config)
    store = rule_attestation.RuleCaptureStore(args.root, settings=settings)
    series = list(args.series or _declared_policy_series())
    captured: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    transport = HttpTransport(
        store.raw_store,
        timeout_seconds=args.timeout,
        policy=RetryPolicy(min_interval_seconds=REQUEST_PACING_SECONDS),
    )
    with transport:
        for series_ticker in series:
            cursor = None
            for page in range(args.max_pages):
                try:
                    envelope = transport.get(
                        f"{KALSHI_BASE_URL}/markets",
                        params={
                            "series_ticker": series_ticker,
                            "limit": args.limit,
                            "cursor": cursor,
                        },
                        source=f"rule_capture:{rule_attestation.SOURCE_KIND_LIVE_RULE_TEXT}",
                        record_id=f"live-markets-{series_ticker}-{page:05d}",
                    )
                except TransportError as error:
                    blocked.append(
                        {
                            "series_ticker": series_ticker,
                            "page": page,
                            "reason": error.reason,
                            "detail": str(error),
                        }
                    )
                    break
                payload = envelope.json()
                markets = payload.get("markets") if isinstance(payload, Mapping) else None
                if not isinstance(markets, Sequence) or isinstance(markets, (str, bytes)):
                    raise ValueError(
                        f"the live listing for {series_ticker} page {page} carries no "
                        "'markets' list; the wire shape changed and this command refuses "
                        "rather than recording an empty success"
                    )
                for market in markets:
                    contract_id = str(market.get("ticker") or "")
                    rules = str(market.get("rules_primary") or "").strip()
                    if not contract_id or not rules:
                        skipped.append({"series_ticker": series_ticker, "market": contract_id})
                        continue
                    store.capture(
                        envelope,
                        contract_id=contract_id,
                        source_kind=rule_attestation.SOURCE_KIND_LIVE_RULE_TEXT,
                        rule_text=_rule_text_in(envelope.text, rules),
                        settlement_semantics=rules,
                        source_observed_at=envelope.server_date,
                        source_names_contract=True,
                        note=(
                            f"live listing page {page} of series {series_ticker}; "
                            f"market status {market.get('status')!r}; "
                            f"{len(markets)} contract(s) on this page"
                        ),
                    )
                    captured.append(
                        {
                            "contract_id": contract_id,
                            "series_ticker": series_ticker,
                            "source_observed_at": (
                                envelope.server_date.isoformat() if envelope.server_date else None
                            ),
                            "raw_hash": envelope.provenance.raw_hash,
                        }
                    )
                cursor = payload.get("cursor") or None
                if not cursor:
                    break
    document = {
        "produced_by": f"{PROGRAM}.capture-rules",
        "config_version": settings.config_version,
        "capture_root": str(store.root),
        "series": series,
        "captures_written": len(captured),
        "contracts_skipped_no_rule_text": len(skipped),
        "pages_blocked": len(blocked),
        "captures": captured,
        "skipped": skipped,
        "blocked": blocked,
    }
    if args.output:
        path = pathlib.Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(_jsonable(document), indent=2, sort_keys=True) + "\n")
        document["output_path"] = str(path)
    _print_json(document)
    with_instant = sum(1 for item in captured if item["source_observed_at"])
    _note(
        f"{len(captured)} capture(s) written under {store.root}: {with_instant} carry an "
        f"instant the serving system states, {len(captured) - with_instant} carry none and "
        "are admissible and unattested until one is observed"
    )
    for item in blocked:
        _note(
            f"blocked page: series {item['series_ticker']} page {item['page']} "
            f"({item['reason']}) {item['detail']}"
        )
    return EXIT_OK if captured and not blocked else EXIT_BLOCKED


def _run_attest_rules(args: argparse.Namespace) -> int:
    """Report which declared contracts carry an attested in-force rule interval.

    This is the evidence gate the exposure graph waits on. It reads captures that
    are already held and issues no request: coverage is a finding, and the point
    of the command is to state it per contract rather than to imply that a
    contract with no capture is a contract with no rule.
    """
    from .ingest import rule_attestation

    settings = rule_attestation.load_attestation_settings(args.config)
    store = rule_attestation.RuleCaptureStore(args.root, settings=settings)
    report = rule_attestation.RuleAttestor(store).report(args.contract_id or None)

    payload = report.as_dict()
    records = [record.as_dict() for contract in report.contracts for record in contract.evidence]
    if args.emit_graph_records:
        document = {
            "document_version": "1",
            "produced_by": f"{PROGRAM}.attest-rules",
            "config_version": settings.config_version,
            "capture_root": str(settings.capture_root),
            "contract_rules": records,
        }
        target = pathlib.Path(args.emit_graph_records)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(_jsonable(document), indent=2, sort_keys=True) + "\n")
        payload["graph_records_path"] = str(target)
        payload["graph_records"] = len(records)
    if args.output:
        written = report.write(args.output)
        payload["report_path"] = str(written)
    _print_json(payload)

    totals = report.totals()
    attested = int(totals.get("contracts_attested") or 0)
    examined = int(totals.get("contracts_examined") or 0)
    _note(
        f"{examined} contract(s) read from {settings.capture_root}: {attested} attested, "
        f"{int(totals.get('contracts_unattested') or 0)} unattested, "
        f"{int(totals.get('captures_held') or 0)} capture(s) held"
    )
    for reason, count in sorted(report.refusals_by_reason.items(), key=lambda kv: -kv[1]):
        _note(f"  {count:>6}  {reason}")
    unattested = list(report.unattested_contract_ids)
    if unattested:
        _note(f"unattested contracts: {', '.join(unattested[:12])}")
    _note(
        "an unattested contract has no admissibly dated source for its rule interval, which "
        "withholds every edge that depends on the interval and is not evidence that no rule "
        "governs the contract"
    )
    return EXIT_OK if attested else EXIT_BLOCKED


def _run_absorption_panel(args: argparse.Namespace) -> int:
    """Estimate the absorption curve on a transaction response panel.

    The terminal horizon is required rather than defaulted because it defines the
    reaction the absorption time is a fraction of: the same path returns a
    different h50 under a different horizon, so a default would silently choose
    the estimand.
    """
    import pandas as pd

    from . import absorption

    frame = pd.read_parquet(args.panel)
    # Only the options the caller supplied are passed, so the module's own declared
    # defaults stay the single place the estimator's settings are written.
    options: dict[str, Any] = {"terminal_horizon_seconds": args.terminal_horizon_seconds}
    if args.seed is not None:
        options["seed"] = args.seed
    if args.samples is not None:
        options["samples"] = args.samples
    if args.coverage is not None:
        options["coverage"] = args.coverage
    result = absorption.summarise_absorption_panel(frame, **options)
    if args.output:
        output = pathlib.Path(args.output)
        if output.parent != pathlib.Path(""):
            output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(_jsonable(result), indent=2, sort_keys=True), encoding="utf-8")
        result = {**result, "output_path": str(output)}
    _print_json(result)

    overall = result.get("overall") or {}
    _note(
        f"terminal horizon {args.terminal_horizon_seconds}s: "
        f"{overall.get('estimateable_pairs')} of {overall.get('pairs')} pair(s) estimateable, "
        f"{overall.get('refused_pairs')} refused"
    )
    for reason, count in sorted(
        (overall.get("refusals_by_reason") or {}).items(), key=lambda kv: -kv[1]
    ):
        _note(f"  {count:>6}  {reason}")
    _note(
        "a refused pair contributes no value and is never imputed, so a median over few "
        "pairs is reported beside the refusals rather than instead of them"
    )
    return EXIT_OK if overall.get("estimateable_pairs") else EXIT_BLOCKED


def _run_match_cross_venue(args: argparse.Namespace) -> int:
    """Grade every cross-venue candidate pair and write the registry.

    The candidate filter decides the denominator, so the selection that produced it
    is printed beside the counts: what the layer held, what the declared pattern
    matched, what was supplied, and whether a cap hid anything. A count from a
    bounded universe is never presented as a count from the whole layer.
    """
    from . import cross_venue

    result = cross_venue.run_cross_venue_matching(
        match_config_path=args.config,
        cohort_config_path=args.cohort,
        graph_config_path=args.graph,
        markets_glob=args.markets_glob,
        second_venue_glob=args.second_venue_glob,
        second_venue_limit=args.second_venue_limit,
        slug_pattern=args.slug_pattern,
    )
    summary = result.summary()
    if args.output:
        output = pathlib.Path(args.output)
        if output.parent != pathlib.Path(""):
            output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(_jsonable(result.as_dict()), indent=2, sort_keys=True), encoding="utf-8"
        )
        summary = {**summary, "registry_path": str(output), "registry_bytes": output.stat().st_size}
    _print_json(summary)

    counts = summary["counts"]
    selection = summary["candidate_selection"]
    _note(
        f"candidate universe: {counts['candidate_pairs']} pair(s) from "
        f"{summary['reads_by_venue']}; the second venue held "
        f"{selection['records_available']} record(s), the declared pattern matched "
        f"{selection['records_matching_the_declared_pattern']}, and "
        f"{selection['candidates_supplied']} were supplied"
    )
    if selection["cap_hid_records"]:
        _note(
            "the declared cap hid records: the counts below describe the supplied "
            "candidates and not the whole layer"
        )
    for grade, count in sorted(counts["by_grade"].items()):
        _note(f"  {count:>7}  {grade}")
    for reason, count in list(summary["pair_refusals"].items())[:8]:
        _note(f"  {count:>7}  {reason}")
    for reason, count in list(summary["read_refusals"].items())[:8]:
        _note(f"  {count:>7}  {reason} (per record)")
    if args.output:
        _note(
            f"the full registry was written to {summary['registry_path']} "
            f"({summary['registry_bytes'] / 1_048_576:.1f} MiB); every refused pair is kept with "
            "its reasons, so the file grows with the candidate universe rather than with the "
            "match count"
        )
    _note(
        "a pair graded here is identical on the parsed predicate and nothing more; "
        "whether it is a *verified* identical claim is answered by the exposure graph's "
        "rule-vintage requirement, which is a separate gate"
    )
    if not counts["primary_analysis"]["pairs"]:
        _note(
            "no pair reached the primary analysis, so this run establishes no cross-venue "
            "match. An empty match set on these inputs states that no supplied pair graded "
            "EXACT, not that no matching contract exists"
        )
    return EXIT_OK if counts["primary_analysis"]["pairs"] else EXIT_BLOCKED


def _run_perp_collect(args: argparse.Namespace) -> int:
    """Collect the cross-venue perpetual-futures cross-section into the store.

    The source publishes rolling windows and no per-episode history, so a sweep
    that is not taken is a cross-section that no longer exists. This command
    collects; it analyses nothing. Every stored record is a quoted cross-section,
    and no quoted spread may be promoted to an executable opportunity on this
    data because the cost layer is unobservable from the source.
    """
    from .perp import collector
    from .perp.store import SnapshotStore

    config = collector.load_config(args.config)
    store = SnapshotStore.open(args.root or DEFAULT_PERP_ROOT)

    if args.report:
        held = collector.report(store)
        _print_json(held)
        _note(
            f"{held['distinct_builds']} distinct build(s) held over "
            f"{held['sweeps_performed']} performed sweep(s); differentials and basis are "
            "derived from the stored cross-sections, not stored"
        )
        return EXIT_OK

    captured: list[dict[str, Any]] = []

    def emit(record: dict[str, Any]) -> None:
        _print_json(record)
        captured.append(record)
        if record.get("skipped"):
            _note("source build unchanged: nothing fetched beyond the canary page")
        elif record.get("sweep_failed"):
            _note(f"sweep failed and was recorded as a failure: {record.get('error')}")
        else:
            _note(
                f"build {record.get('build_time')}: {record.get('assets_stored')} of "
                f"{record.get('assets_attempted')} asset page(s) newly stored, "
                f"{record.get('blocked')} blocked, {record.get('unshaped')} refused on shape"
            )

    with collector.build_transport(config) as transport:
        collector.collect(
            transport,
            store,
            config,
            limit=args.limit,
            use_all=args.all,
            include_funding=not args.no_funding,
            force=args.force,
            # An unattended loop has to be asked for explicitly; one sweep is the default.
            once=not args.loop,
            interval_minutes=collector.interval_minutes(config),
            on_record=emit,
        )

    _note(
        "stored cross-sections are quoted prices and rates: the source's execution-cost "
        "surface is client-rendered and empty, so no differential is an executable "
        "arbitrage and none is recorded as one"
    )
    final = captured[-1] if captured else {}
    if final.get("sweep_failed"):
        return EXIT_BLOCKED
    if int(final.get("blocked") or 0) or int(final.get("unshaped") or 0):
        return EXIT_BLOCKED
    return EXIT_OK


def _note_empirical_standing(card: Mapping[str, Any]) -> None:
    """Say plainly that a produced card establishes no empirical result.

    A card that exists is not a card whose cohort can be used. The verification
    outcome and every unsatisfied gate are reported on stderr while the artifact
    itself keeps them in JSON.
    """
    unsatisfied = [str(name) for name in (card.get("unsatisfied_gates") or [])]
    if card.get("audit_complete") is not True or unsatisfied:
        gates = ", ".join(unsatisfied) or "none named"
        _note(
            f"the card was written to {card.get('outputs', {}).get('card')} and is a "
            f"real artifact, but empirical use remains blocked: audit_complete="
            f"{card.get('audit_complete')!r} with unsatisfied gate(s): {gates}. The "
            "gates and the blocked claims are preserved in the JSON."
        )


class _ArgumentParser(argparse.ArgumentParser):
    """Argument errors exit 1, leaving exit 2 to mean 'blocked result'."""

    def error(self, message: str) -> NoReturn:
        self.print_usage(sys.stderr)
        self.exit(EXIT_ERROR, f"{self.prog}: error: {message}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog=PROGRAM,
        description=(
            "Point-in-time measurement of information propagation across prediction "
            "markets. Market access is read-only; commands write local research "
            "artifacts under the output directory they are given."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    reproduce = commands.add_parser(
        "reproduce",
        help="rebuild the synthetic offline reproduction and write its artifacts",
        description=(
            "Run the offline methods reproduction from the packaged synthetic fixture. "
            "Every panel it builds is generated data, so the result is a software and "
            "methods check, not empirical evidence about any release."
        ),
    )
    reproduce.add_argument(
        "--output", required=True, help="directory to write the reproduction into"
    )
    reproduce.add_argument(
        "--spec",
        help=(
            "frozen study specification to read (default: the reporting module's own "
            "default, configs/study_v1.yaml)"
        ),
    )
    reproduce.add_argument(
        "--events",
        type=int,
        help="number of synthetic events, passed as n_events (default: the library default)",
    )
    reproduce.add_argument(
        "--repetitions",
        type=int,
        help="falsification repetitions (default: the library default)",
    )
    reproduce.add_argument(
        "--bootstrap",
        type=int,
        help="bootstrap resamples, passed as bootstrap_samples (default: the library default)",
    )
    reproduce.add_argument(
        "--real-audit",
        help=(
            "a real acquisition audit directory to cite explicitly; no directory is "
            "selected for you, and omitting this cites no real audit at all"
        ),
    )
    reproduce.add_argument(
        "--release-dataset",
        help=(
            "sealed archived-release dataset to cite explicitly; it is verified and "
            "recorded, and is never fit as if it were a synthetic panel"
        ),
    )
    reproduce.set_defaults(handler=_run_reproduce)

    audit = commands.add_parser(
        "audit",
        help="run the bounded public G0 coverage audit",
        description=(
            "Run the bounded read-only coverage audit against the public venue and the "
            "archived release payloads. Every axis is capped, and the caps actually "
            "applied are returned in the result."
        ),
    )
    audit.add_argument("--output", required=True, help="directory to write the audit into")
    audit.add_argument("--cohort", help="cohort configuration (default: configs/cohort.yaml)")
    audit.add_argument(
        "--windows", help="event-window configuration (default: configs/event_windows.yaml)"
    )
    audit.add_argument("--timeout", type=float, help="per-request timeout in seconds")
    audit.add_argument("--max-pages", type=int, help="pagination pages per listing walk")
    audit.add_argument("--max-contracts", type=int, help="candidate contracts per event")
    audit.add_argument(
        "--max-candle-contracts", type=int, help="contracts candle queries are issued for"
    )
    audit.add_argument(
        "--release-dataset",
        help=(
            "sealed archived-release Parquet to read first releases from instead of "
            "fetching them; the dataset and every selected payload are verified, and a "
            "missing or disagreeing record blocks that event rather than falling back "
            "to the network"
        ),
    )
    audit.add_argument(
        "--archive-raw-root",
        help=(
            "raw store holding the named dataset's original payloads "
            "(default: the 'raw' directory beside the dataset)"
        ),
    )
    audit.set_defaults(handler=_run_audit)

    capture = commands.add_parser(
        "capture",
        help="capture bounded public book snapshots for one contract",
        description=(
            "Poll one documented public snapshot surface. The contract is required "
            "because the caller chooses which legitimate public market to read; no "
            "ticker is guessed. No authenticated channel is used and no order route "
            "exists."
        ),
    )
    capture.add_argument("--output", required=True, help="directory to capture into")
    capture.add_argument(
        "--venue",
        choices=VENUES,
        default="kalshi",
        help="documented public snapshot surface to read (default: kalshi)",
    )
    capture.add_argument(
        "--contract",
        required=True,
        help="public market identity to read; an unknown id is recorded as a failure",
    )
    capture.add_argument("--duration", type=float, help="capture duration in seconds")
    capture.add_argument("--interval", type=float, help="seconds between polls")
    capture.add_argument("--timeout", type=float, help="per-request timeout in seconds")
    capture.set_defaults(handler=_run_capture)

    quality = commands.add_parser(
        "quality",
        help="report what one raw store holds and what it can support",
        description=(
            "Re-read every receipt and verify each referenced payload against its "
            "content hash. This parses no payload content and claims no source clock."
        ),
    )
    quality.add_argument("raw_dir", help="raw store directory to audit")
    quality.add_argument("--output", required=True, help="path of the JSON report to write")
    quality.set_defaults(handler=_run_quality)

    inventory = commands.add_parser(
        "inventory-external",
        help="manifest the external archive layers without reading their rows",
        description=(
            "Build a manifest of the configured local archive layers: file sizes, row "
            "counts, schema fingerprints, timestamp bounds and content hashes. The raw "
            "files are read only, and repeating a run over unchanged bytes yields the "
            "same identity."
        ),
    )
    inventory.add_argument("--root", required=True, help="archive root to inventory")
    inventory.add_argument("--output", required=True, help="directory to write the inventory into")
    inventory.add_argument(
        "--config",
        default=DEFAULT_EXTERNAL_CONFIG,
        help=(f"pipeline configuration naming the layers (default: {DEFAULT_EXTERNAL_CONFIG})"),
    )
    inventory.add_argument(
        "--no-hashes",
        action="store_true",
        help=(
            "skip content hashing; the inventory then records no hash and its identity "
            "cannot detect a changed byte"
        ),
    )
    inventory.set_defaults(handler=_run_inventory_external)

    normalize = commands.add_parser(
        "normalize-external",
        help="normalize one external archive layer into sealed historical trades",
        description=(
            "Read one configured archive layer inside the configured window through "
            "DuckDB projection and normalize its rows into sealed historical trade "
            "records. Exact venue price units and a null quantity are preserved, and a "
            "read that stops at its own cap is reported as incomplete."
        ),
    )
    normalize.add_argument(
        "--config",
        required=True,
        help="pipeline configuration naming the archive root, layer and window",
    )
    normalize.add_argument("--output", required=True, help="directory to write the trades into")
    normalize.add_argument(
        "--layer",
        help=(
            "archive layer to read (default: the configured kalshi layer); an "
            "unconfigured layer is refused"
        ),
    )
    normalize.add_argument(
        "--max-rows", type=int, help="row cap for the read (default: the library default)"
    )
    normalize.add_argument("--window-start", type=_iso_instant, help="window start instant")
    normalize.add_argument("--window-end", type=_iso_instant, help="window end instant")
    normalize.set_defaults(handler=_run_normalize_external)

    coverage = commands.add_parser(
        "coverage-external",
        help="build the release-linked external coverage grid",
        description=(
            "Build the event and contract coverage grid for the configured releases from "
            "the sealed release dataset and the normalized trades. Missing cells stay in "
            "the grid, and G0 remains blocked unless rules, cohort and supported frequency "
            "pass."
        ),
    )
    coverage.add_argument(
        "--config", required=True, help="pipeline configuration naming the coverage inputs"
    )
    coverage.add_argument("--output", required=True, help="directory to write the coverage into")
    coverage.add_argument(
        "--release-dataset",
        help="sealed archived-release Parquet to read the releases from",
    )
    coverage.add_argument(
        "--max-contracts",
        type=int,
        help="contracts considered per event (default: the library default)",
    )
    coverage.set_defaults(handler=_run_coverage_external)

    panel = commands.add_parser(
        "build-trade-panel",
        help="build the transaction response panel from sealed trades",
        description=(
            "Build the source-time transaction panel around each configured release. A "
            "response requires a new post-release trade, no post-close zero is carried "
            "forward, and an event without verified rule evidence is masked with its "
            "reason rather than admitted."
        ),
    )
    panel.add_argument(
        "--config", required=True, help="pipeline configuration naming the panel rules"
    )
    panel.add_argument("--trades", required=True, help="sealed historical-trades dataset to read")
    panel.add_argument("--output", required=True, help="directory to write the panel into")
    panel.add_argument(
        "--clock-mode",
        help="panel clock mode (default: the library's own first permitted mode)",
    )
    panel.add_argument(
        "--horizon",
        type=int,
        help=(
            "narrow the panel to one configured horizon and make it the primary "
            "(default: the configured horizon set)"
        ),
    )
    panel.add_argument(
        "--candidate-grid",
        type=_candidate_grid,
        help=(
            "JSON mapping each release id to its declared (venue, contract_id) candidate "
            "pairs, selected before the release (default: the window-activity universe, "
            "which the counts label as such)"
        ),
    )
    panel.set_defaults(handler=_run_build_trade_panel)

    external_report = commands.add_parser(
        "report-external",
        help="assemble the external report from an existing panel",
        description=(
            "Build the coverage report, event cards, response figures, baseline summary, "
            "lineage and capability table from an existing panel and coverage artifact. "
            "Missing results stay visibly missing and a blocked gate is reported as "
            "blocked rather than estimated around."
        ),
    )
    external_report.add_argument(
        "--config", required=True, help="pipeline configuration naming the report inputs"
    )
    external_report.add_argument(
        "--panel", required=True, help="existing sealed panel to report on"
    )
    external_report.add_argument(
        "--output", required=True, help="directory to write the report into"
    )
    external_report.add_argument(
        "--run-id",
        help=(
            "run identity to record (default: derived from the specification digest and "
            "panel hash, so a rerun over identical inputs lands identical bytes)"
        ),
    )
    external_report.set_defaults(handler=_run_report_external)

    study_command = commands.add_parser(
        "study-external",
        help="fit the declared model ladder on a sealed transaction panel",
        description=(
            "Fit the declared timing-only ladder on a sealed trade_panel with whole-release "
            "chronological splits and release-clustered uncertainty, audit whether the "
            "propagation rung is estimable, write the result and record the run in the "
            "durable empirical registry. The propagation rung is reported blocked rather "
            "than fitted when a neighbour return or a verified surprise is absent."
        ),
    )
    study_command.add_argument("--panel", required=True, help="sealed trade_panel Parquet path")
    study_command.add_argument("--output", required=True, help="output directory for the result")
    study_command.add_argument("--run-id", help="run id; a stable id is derived when omitted")
    study_command.add_argument(
        "--forecast-panel",
        help=(
            "sealed source-time historical_forecast Parquet to fit the declared nested "
            "ladder on (default: the absorption ladder alone)"
        ),
    )
    study_command.add_argument(
        "--registry",
        help=(
            "registry SQLite path (default: the shared data/registry/empirical_study.sqlite3, "
            "so runs of one study do not each write a store of their own)"
        ),
    )
    study_command.set_defaults(handler=_run_study_external)

    calibrate_command = commands.add_parser(
        "calibrate-rule",
        help="calibrate the network-promotion decision rule on simulated tapes",
        description=(
            "Measure the size and the recovery of the study's complete promotion decision "
            "rule on simulated transaction tapes, which are the only process available "
            "while the archive has no eligible contracts. Tapes pass through the same "
            "exposure graph, source-time forecast panel, ladder design, nested comparison "
            "and release-clustered uncertainty as real data. The one-sided bounds are held "
            "simultaneously across the null scenarios. The verdict is pass, fail or "
            "inconclusive, and an under-powered run is reported inconclusive."
        ),
    )
    calibrate_command.add_argument("--output", required=True, help="path of the certificate")
    calibrate_command.add_argument("--seed", type=int, help="base seed for the repetition list")
    calibrate_command.add_argument(
        "--repetitions", type=int, help="repetitions per scenario (the plan declares 200)"
    )
    calibrate_command.add_argument("--releases", type=int, help="releases per repetition")
    calibrate_command.add_argument("--bootstrap", type=int, help="release-clustered draws")
    calibrate_command.add_argument(
        "--workers", type=int, help="processes to spread repetitions over (default: 1)"
    )
    calibrate_command.add_argument(
        "--ceiling", type=float, help="ceiling the null's one-sided upper bound must clear"
    )
    calibrate_command.add_argument("--target-power", type=float, help="recovery power target")
    calibrate_command.add_argument(
        "--registry",
        help=(
            "registry SQLite path (default: the shared data/registry/rule_calibration.sqlite3, "
            "a separate ledger from the empirical study's so a calibration is never read as "
            "an empirical run)"
        ),
    )
    calibrate_command.add_argument("--run-id", help="run id; a stable id is derived when omitted")
    calibrate_command.set_defaults(handler=_run_calibrate_rule)

    registry = commands.add_parser(
        "registry-review",
        help="print an experiment registry's durable state",
        description=(
            "Print the runs, locked-test reservations and event claims a registry file "
            "actually holds. An absent registry is an error; this command never creates "
            "one."
        ),
    )
    registry.add_argument("registry_path", help="registry SQLite path, or :memory:")
    registry.set_defaults(handler=_run_registry_review)

    card = commands.add_parser(
        "event-card",
        help="assemble one event card from an audit directory",
        description=(
            "Assemble one event card from an audit's own artifacts and re-verify every "
            "payload hash the chosen event cites. This command issues no request."
        ),
    )
    card.add_argument("audit_dir", help="audit directory to read")
    card.add_argument("--output", required=True, help="path of the JSON card to write")
    card.add_argument(
        "--event-id",
        help="event to describe (default: the audit's own prior card, else the first event)",
    )
    card.set_defaults(handler=_run_event_card)

    rules_capture = commands.add_parser(
        "capture-rules",
        help="capture the venue's live rule text for the declared policy series",
        description=(
            "GET the venue's live market listing for each declared policy series, archive "
            "every page, and write one immutable rule capture per live contract carrying "
            "the instant the serving system states. Issues GET requests only, uses no "
            "credentials, and refuses to derive a bound from this run's clock."
        ),
    )
    rules_capture.add_argument("--config", default=DEFAULT_ATTESTATION_CONFIG)
    rules_capture.add_argument("--root", help="capture root; defaults to the configured root")
    rules_capture.add_argument(
        "--series", action="append", help="series ticker; repeatable; defaults to cohort_v2"
    )
    rules_capture.add_argument("--limit", type=int, default=200, help="contracts per page")
    rules_capture.add_argument("--max-pages", type=int, default=25, help="page bound per series")
    rules_capture.add_argument("--timeout", type=float, default=30.0)
    rules_capture.add_argument("--output", help="path of the JSON summary to write")
    rules_capture.set_defaults(handler=_run_capture_rules)

    attest = commands.add_parser(
        "attest-rules",
        help="report which contracts carry an attested in-force rule interval",
        description=(
            "Read the held rule captures and report, per contract, whether an admissibly "
            "dated source bounds its rule interval. This is the evidence gate the "
            "exposure graph waits on: an unattested contract has no interval, so every "
            "edge that depends on one is withheld. The command issues no request and "
            "imputes no interval; coverage is stated as a finding, so an absent capture "
            "is reported as absent rather than as a contract with no governing rule."
        ),
    )
    attest.add_argument(
        "--config", default=DEFAULT_ATTESTATION_CONFIG, help="declared evidence standard"
    )
    attest.add_argument("--root", help="capture store root (default: the configured root)")
    attest.add_argument(
        "--contract-id",
        action="append",
        help="contract to report; repeatable (default: every contract the store holds)",
    )
    attest.add_argument("--output", help="directory to write the JSON report into")
    attest.add_argument(
        "--emit-graph-records",
        help="write the attested records as the document rule_vintage.evidence_source names",
    )
    attest.set_defaults(handler=_run_attest_rules)

    absorption_command = commands.add_parser(
        "absorption-panel",
        help="estimate the absorption curve on a sealed transaction response panel",
        description=(
            "Estimate the fraction of a release's terminal reaction that is absorbed by "
            "each declared horizon, with a release-clustered interval. The terminal "
            "horizon is required rather than defaulted, because it defines the reaction "
            "the absorption time is a fraction of: the same path returns a different h50 "
            "under a different horizon, so a default would choose the estimand silently. "
            "A pair with no observed pre-release baseline, or no observed response at the "
            "terminal horizon, is refused by name and never imputed."
        ),
    )
    absorption_command.add_argument("panel", help="transaction response panel (Parquet)")
    absorption_command.add_argument(
        "--terminal-horizon-seconds",
        type=int,
        required=True,
        help="the horizon whose reaction defines R (no default: it defines the estimand)",
    )
    absorption_command.add_argument("--seed", type=int, help="bootstrap seed")
    absorption_command.add_argument("--samples", type=int, help="bootstrap draws")
    absorption_command.add_argument("--coverage", type=float, help="interval coverage")
    absorption_command.add_argument("--output", help="path of the JSON result to write")
    absorption_command.set_defaults(handler=_run_absorption_panel)

    match = commands.add_parser(
        "match-cross-venue",
        help="grade every cross-venue candidate pair into a match registry",
        description=(
            "Form a candidate universe from two venues' own records, read each record's "
            "payout predicate through the declared parser, and grade every cross-venue "
            "pair into EXACT, ECONOMICALLY_EQUIVALENT, APPROXIMATE or REJECT. No "
            "similarity score is computed anywhere: the candidate filter decides the "
            "denominator and is recorded, never used as evidence, and a pair's grade "
            "rests only on the components the declared parsers read. A component a "
            "record does not publish is recorded as unobserved and refuses the pair "
            "rather than being inferred from a ticker or a title. A pair graded here is "
            "identical on the parsed predicate and nothing more; whether it is a "
            "verified identical claim is the exposure graph's separate rule-vintage gate."
        ),
    )
    match.add_argument("--config", default=DEFAULT_MATCH_CONFIG, help="declared match rules")
    match.add_argument("--cohort", default=DEFAULT_COHORT_CONFIG, help="declared candidate series")
    match.add_argument("--graph", default=DEFAULT_GRAPH_CONFIG, help="declared decision calendar")
    match.add_argument(
        "--markets-glob", default=DEFAULT_MARKETS_GLOB, help="first venue's market records"
    )
    match.add_argument(
        "--second-venue-glob",
        default=DEFAULT_SECOND_VENUE_GLOB,
        help="second venue's cleaned local layer",
    )
    match.add_argument(
        "--second-venue-limit",
        type=int,
        help="cap the second venue's candidates (default: the declared max_candidates)",
    )
    match.add_argument(
        "--slug-pattern",
        help="override the declared candidate selection pattern (changing the denominator)",
    )
    match.add_argument("--output", help="path of the full registry JSON to write")
    match.set_defaults(handler=_run_match_cross_venue)

    perp = commands.add_parser(
        "perp-collect",
        help="collect the cross-venue perpetual-futures cross-section",
        description=(
            "Collect one cross-section of perpetual-futures quotes and funding rates "
            "from a declared public source and archive it. The source publishes "
            "rolling windows and no per-episode history, so a sweep not taken is a "
            "cross-section that cannot be recovered. Every page is archived raw "
            "before it is parsed, an unchanged source build is skipped after one "
            "request, and a figure the source left unobserved is stored as a null "
            "with a named reason code rather than as a zero. The execution-cost "
            "layer is unobservable from this source, so a stored differential is a "
            "quoted spread and never an executable opportunity."
        ),
    )
    perp.add_argument("--config", default=DEFAULT_PERP_CONFIG, help="declared source config")
    perp.add_argument("--root", default=DEFAULT_PERP_ROOT, help="snapshot store root")
    perp.add_argument("--once", action="store_true", help="one sweep, then exit (the default)")
    perp.add_argument(
        "--loop", action="store_true", help="sweep repeatedly at the declared interval"
    )
    perp.add_argument(
        "--all", action="store_true", help="sweep the whole universe instead of the priority set"
    )
    perp.add_argument("--limit", type=int, help="cap the number of assets swept")
    perp.add_argument(
        "--no-funding", action="store_true", help="skip the settled-funding history pages"
    )
    perp.add_argument(
        "--force", action="store_true", help="sweep even when the source build is unchanged"
    )
    perp.add_argument(
        "--report", action="store_true", help="summarise what is held, fetching nothing"
    )
    perp.set_defaults(handler=_run_perp_collect)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one command and return its exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except (ValueError, FileNotFoundError, OSError, ImportError) as exc:
        _note(f"{type(exc).__name__}: {exc}")
        return EXIT_ERROR
