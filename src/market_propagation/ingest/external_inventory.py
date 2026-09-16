"""Immutable, hash-verified inventory of the external Parquet layers.

A run over the external archives has to be able to state which bytes it read and to
notice when those bytes have changed, without trusting a filename or a directory
listing. This module produces that statement: one record per shard carrying its size,
its footer row count and row-group count, its schema, the timestamp bounds its footer
statistics report, and its SHA-256, plus one identity over the whole set.

Four decisions decide what a downstream claim can mean, and each is deliberate.

The identity is taken over relative paths and content hashes, never over the absolute
root, so the same tree copied elsewhere is the same input version. It is also
independent of any wall clock, so two runs over unchanged bytes agree exactly and a
result can name the version it was produced from. A run with hashing disabled has no
content hash to take an identity over, so it records ``hash_scope`` as ``none`` rather
than presenting a hash-free identity as a verified one.

Failure is recorded, not raised. A shard whose footer cannot be parsed becomes an
``unreadable`` record with its error text, a layer whose glob matches nothing becomes
``missing`` with its pattern, and a filename shared by two layers is flagged. An
inventory that raised on the first unreadable shard would leave the caller with
nothing to report, and one that stopped there would hide the rest of the tree, which
is exactly the state a reader most needs described.

Footer statistics are reported, never trusted. A shard whose footer carries no
statistics, or carries them for only some row groups, says so by name in
``missing_statistics`` instead of returning a null bound that reads as "nothing in
range". Selecting shards by their timestamp bounds therefore rests on a statistic
whose absence is stated rather than inferred.

Nothing here writes to, moves, or deletes an archive file. The inventory is derived
from the archives and is the only thing this module produces.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import yaml

from ..storage import hash_bytes, hash_file

__all__ = [
    "ABSENT_EVIDENCE_FLAGS",
    "CONFIG_PATH",
    "DEFAULT_INVENTORY_VERSION",
    "FLAG_DUPLICATE_SHARD_FILENAME",
    "FLAG_HASH_UNAVAILABLE",
    "FLAG_LAYER_MISSING",
    "FLAG_SHARD_UNREADABLE",
    "FLAG_SIZE_UNMEASURED",
    "HASH_SCOPE_FULL",
    "HASH_SCOPE_NONE",
    "INPUT_CLASSES",
    "INVENTORY_FILENAME",
    "LAYER_STATUS_MISSING",
    "LAYER_STATUS_PRESENT",
    "MISSING_STATISTICS_COLUMN_ABSENT",
    "MISSING_STATISTICS_NO_FOOTER_STATISTICS",
    "MISSING_STATISTICS_PARTIAL_ROW_GROUPS",
    "MISSING_STATISTICS_UNCONVERTED_BOUND",
    "MISSING_STATISTICS_UNREADABLE_FOOTER",
    "SCHEMA_FINGERPRINT_UNAVAILABLE",
    "SHARD_STATUS_READ",
    "SHARD_STATUS_UNREADABLE",
    "ExternalInventory",
    "LayerRecord",
    "LayerSpec",
    "ShardRecord",
    "build_inventory",
    "load_inventory",
    "load_layer_specs",
    "verify_inventory",
    "write_inventory",
]

#: Path to the pipeline configuration, relative to the repository root.
CONFIG_PATH = "configs/external_history_v1.yaml"

#: Version stamped into an inventory so a run manifest can name the inventory scheme.
DEFAULT_INVENTORY_VERSION = "external_history_v1"

#: The four input classes every output states it used.
#:
#: ``external_historical_archive`` is the literal the pipeline configuration writes for
#: the named archive layers. The other three are the plan's remaining classes in the
#: same snake-case singular form, so a layer's declaration is checked against this
#: tuple rather than accepted as free text.
INPUT_CLASSES: tuple[str, ...] = (
    "external_historical_archive",
    "locally_captured_public_data",
    "derived_dataset",
    "synthetic_fixture",
)

SHARD_STATUS_READ = "read"
SHARD_STATUS_UNREADABLE = "unreadable"
LAYER_STATUS_PRESENT = "present"
LAYER_STATUS_MISSING = "missing"

#: Written under an output directory by :func:`write_inventory`.
INVENTORY_FILENAME = "inventory.json"

#: Content hashes were taken for every shard.
HASH_SCOPE_FULL = "all_shards_sha256"

#: Hashing was skipped, so no shard carries a digest to verify against.
HASH_SCOPE_NONE = "none"

#: A layer's glob matched no shard file.
FLAG_LAYER_MISSING = "layer_missing"

#: A shard's Parquet footer could not be parsed.
FLAG_SHARD_UNREADABLE = "shard_unreadable"

#: A shard's bytes could not be digested, so no content identity exists for it.
FLAG_HASH_UNAVAILABLE = "hash_unavailable"

#: A shard's size could not be measured, so its ``bytes`` is zero because it is unknown.
FLAG_SIZE_UNMEASURED = "size_unmeasured"

#: One filename is used by more than one layer.
FLAG_DUPLICATE_SHARD_FILENAME = "duplicate_shard_filename"

#: The flags that mean a named input is absent or unverified rather than merely unusual.
#:
#: These are the flags a caller acts on: an archive named by the configuration could
#: not be read, or could not be verified. A duplicate filename is a naming problem with
#: two readable shards behind it, and absent footer statistics are reported per shard
#: in ``missing_statistics``, so neither belongs here.
ABSENT_EVIDENCE_FLAGS: tuple[str, ...] = (
    FLAG_LAYER_MISSING,
    FLAG_SHARD_UNREADABLE,
    FLAG_HASH_UNAVAILABLE,
)

#: No schema could be read, so no fingerprint exists. The empty string is deliberately
#: not a digest: an empty hash is not a hash, and a reader must not mistake it for one.
SCHEMA_FINGERPRINT_UNAVAILABLE = ""

MISSING_STATISTICS_NO_FOOTER_STATISTICS = "footer_statistics_absent"
MISSING_STATISTICS_PARTIAL_ROW_GROUPS = "footer_statistics_partially_absent"
MISSING_STATISTICS_COLUMN_ABSENT = "time_column_absent_from_schema"
MISSING_STATISTICS_UNREADABLE_FOOTER = "footer_unreadable"
MISSING_STATISTICS_UNCONVERTED_BOUND = "bound_not_expressible_as_instant"

_EPOCH_SECONDS = "epoch_seconds"


@dataclass(frozen=True, slots=True)
class LayerSpec:
    """One named archive layer as the configuration declares it.

    ``path_pattern`` is relative to the configured root and is the only route to a
    shard, so a run cannot reach a file the configuration does not name. ``time_unit``
    states how the layer's ``time_column`` is encoded, which is what lets a numeric
    footer bound be reported as an instant instead of a bare integer.
    """

    name: str
    path_pattern: str
    input_class: str
    role: str
    producer: str
    license: str
    venue: str | None = None
    time_column: str | None = None
    time_unit: str | None = None


@dataclass(frozen=True, slots=True)
class ShardRecord:
    """One Parquet shard, described only from its bytes and its own footer.

    ``row_count`` and ``row_group_count`` are read from the footer, so they are null
    rather than zero for a shard whose footer could not be parsed: an unreadable file
    has an unknown row count, and reporting zero would invent an empty one.
    ``timestamp_stats`` always holds one entry for the layer's declared ``time_column``
    when one is declared, and ``missing_statistics`` names every reason those bounds
    are not the complete truth.
    """

    layer: str
    relative_path: str
    bytes: int
    row_count: int | None
    row_group_count: int | None
    schema_fingerprint: str
    columns: tuple[tuple[str, str], ...]
    timestamp_stats: tuple[tuple[str, str | None, str | None], ...]
    missing_statistics: tuple[str, ...]
    sha256: str | None
    status: str
    flags: tuple[str, ...] = ()
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """This record as plain JSON values, identical to its persisted form.

        The audit view and the persisted form are the same mapping on purpose: a
        record cannot then be written one way and read another.
        """
        return self.to_json()

    def to_json(self) -> dict[str, Any]:
        """The mapping :func:`load_inventory` reads back into a ``ShardRecord``."""
        return {
            "bytes": self.bytes,
            "columns": [[name, dtype] for name, dtype in self.columns],
            "error": self.error,
            "flags": list(self.flags),
            "layer": self.layer,
            "missing_statistics": list(self.missing_statistics),
            "relative_path": self.relative_path,
            "row_count": self.row_count,
            "row_group_count": self.row_group_count,
            "schema_fingerprint": self.schema_fingerprint,
            "sha256": self.sha256,
            "status": self.status,
            "timestamp_stats": [[column, low, high] for column, low, high in self.timestamp_stats],
        }


@dataclass(frozen=True, slots=True)
class LayerRecord:
    """One layer's totals over its shards, kept even when the layer is missing.

    A missing layer is a record with status ``missing`` and a reason, not an absence,
    so a caller reading the inventory learns which configured input produced nothing
    instead of inferring it from a short list.
    """

    name: str
    path_pattern: str
    input_class: str
    role: str
    producer: str
    license: str
    venue: str | None
    shard_count: int
    total_rows: int
    total_bytes: int
    status: str
    flags: tuple[str, ...]
    error: str | None


@dataclass(frozen=True, slots=True)
class ExternalInventory:
    """The whole inventory: what was found, and one identity over it.

    ``total_rows`` and ``total_bytes`` sum over shards whose footer was read, so an
    unreadable shard contributes nothing and is flagged in its own record and in its
    layer's flags. ``created_at`` is the run instant and is deliberately not part of
    the identity, which is why two runs over unchanged bytes agree.
    """

    root: str
    inventory_version: str
    layers: tuple[LayerRecord, ...]
    shards: tuple[ShardRecord, ...]
    identity: str
    flags: tuple[str, ...]
    created_at: dt.datetime
    hash_scope: str

    def layer(self, name: str) -> LayerRecord:
        """The record for ``name``, or a ``KeyError`` naming the layers that exist."""
        for record in self.layers:
            if record.name == name:
                return record
        known = ", ".join(record.name for record in self.layers)
        raise KeyError(f"no layer {name!r} in this inventory; it holds {known}")

    def shards_for(self, name: str) -> tuple[ShardRecord, ...]:
        """Every shard of ``name``, refusing a name the inventory does not hold."""
        self.layer(name)
        return tuple(shard for shard in self.shards if shard.layer == name)

    def as_dict(self) -> dict[str, Any]:
        """The inventory as plain JSON values, which is also its persisted form."""
        missing = [record.name for record in self.layers if record.status == LAYER_STATUS_MISSING]
        unreadable = [
            shard.relative_path for shard in self.shards if shard.status == SHARD_STATUS_UNREADABLE
        ]
        return {
            "absent_evidence": list(_absent_evidence(self.flags)),
            "created_at": self.created_at.astimezone(dt.UTC).isoformat(),
            "flags": list(self.flags),
            "hash_scope": self.hash_scope,
            "identity": self.identity,
            "inventory_version": self.inventory_version,
            "layer_count": len(self.layers),
            "layers": [_layer_to_json(record) for record in self.layers],
            "layers_missing": sorted(missing),
            "root": self.root,
            "shard_count": len(self.shards),
            "shards": [shard.to_json() for shard in self.shards],
            "shards_unreadable": sorted(unreadable),
            "total_bytes": sum(record.total_bytes for record in self.layers),
            "total_rows": sum(record.total_rows for record in self.layers),
        }


def load_layer_specs(config_path: str | Path = CONFIG_PATH) -> tuple[LayerSpec, ...]:
    """Read the configured archive layers, in the order the file declares them.

    Every field a layer declares is required rather than defaulted, because a layer
    that reaches the inventory without a producer, a license or an input class cannot
    be reported on. An unrecognized input class is refused here instead of being
    carried into an output that then claims a class the pipeline does not have.
    """
    path = Path(config_path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"external-history configuration {path} could not be read: {exc}") from exc
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"external-history configuration {path} is not valid YAML: {exc}") from exc
    if not isinstance(document, Mapping):
        raise ValueError(f"external-history configuration {path} is not a YAML mapping")
    inputs = document.get("inputs")
    where = f"{path}: inputs"
    if not isinstance(inputs, Mapping):
        raise ValueError(f"{where} must be a YAML mapping")
    declared = inputs.get("layers")
    if isinstance(declared, (str, bytes)) or not isinstance(declared, Sequence):
        raise ValueError(f"{where}.layers must be a YAML sequence")
    if not declared:
        raise ValueError(f"{where}.layers is empty; a run names its inputs explicitly")
    specs: list[LayerSpec] = []
    seen: set[str] = set()
    for index, entry in enumerate(declared):
        entry_where = f"{where}.layers[{index}]"
        source = _require_mapping(entry, where=entry_where)
        spec = LayerSpec(
            name=_require_text(source, "name", where=entry_where),
            path_pattern=_require_text(source, "path_pattern", where=entry_where),
            input_class=_require_text(source, "input_class", where=entry_where),
            role=_require_text(source, "role", where=entry_where),
            producer=_require_text(source, "producer", where=entry_where),
            license=_require_text(source, "license", where=entry_where),
            venue=_optional_text(source, "venue", where=entry_where),
            time_column=_optional_text(source, "time_column", where=entry_where),
            time_unit=_optional_text(source, "time_unit", where=entry_where),
        )
        if spec.input_class not in INPUT_CLASSES:
            allowed = ", ".join(INPUT_CLASSES)
            raise ValueError(
                f"{entry_where}.input_class is {spec.input_class!r}; the pipeline reports on "
                f"these classes: {allowed}"
            )
        if spec.time_column is None and spec.time_unit is not None:
            raise ValueError(
                f"{entry_where}.time_unit is {spec.time_unit!r} while time_column is null; a "
                "unit without a column describes nothing"
            )
        if spec.name in seen:
            raise ValueError(f"{entry_where}.name repeats layer {spec.name!r}")
        seen.add(spec.name)
        _check_spec(spec)
        specs.append(spec)
    return tuple(specs)


def build_inventory(
    root: str | Path,
    *,
    layers: Sequence[LayerSpec | str] | None = None,
    config_path: str | Path = CONFIG_PATH,
    inventory_version: str = DEFAULT_INVENTORY_VERSION,
    verify_hashes: bool = True,
) -> ExternalInventory:
    """Inventory every shard of the named layers under ``root``.

    ``layers`` is ``None`` for every layer the configuration declares, or a selection
    of ``LayerSpec`` values or configured layer names. Nothing outside ``root`` is
    read: a layer's pattern is relative and is refused if it climbs out of the root.

    ``verify_hashes=False`` skips the digests and records ``hash_scope`` as ``none``,
    which is what a caller who only needs sizes and footer facts pays instead of
    reading every byte of a multi-gigabyte archive.
    """
    base = Path(root)
    root_text = str(root)
    specs = _resolve_layer_specs(layers, config_path)
    shards: list[ShardRecord] = []
    for spec in specs:
        for path in _matching_files(base, spec):
            shards.append(
                _shard_record(
                    spec,
                    relative_path=str(path.relative_to(base)),
                    path=path,
                    verify_hash=verify_hashes,
                )
            )
    flagged = _flag_duplicate_filenames(shards)
    records = tuple(_layer_record(spec, flagged, root_text=root_text) for spec in specs)
    flags = tuple(
        sorted(
            {flag for record in records for flag in record.flags}
            | {flag for shard in flagged for flag in shard.flags}
        )
    )
    ordered = tuple(flagged)
    return ExternalInventory(
        root=root_text,
        inventory_version=inventory_version,
        layers=records,
        shards=ordered,
        identity=_inventory_identity(ordered),
        flags=flags,
        created_at=dt.datetime.now(dt.UTC),
        hash_scope=HASH_SCOPE_FULL if verify_hashes else HASH_SCOPE_NONE,
    )


def write_inventory(inventory: ExternalInventory, output_dir: str | Path) -> dict[str, Any]:
    """Write ``inventory.json`` under ``output_dir`` and describe what was written.

    The returned mapping is the inventory document itself plus ``path`` and the file's
    own ``sha256``, so a caller reports the same facts it seals. The write overwrites:
    ``created_at`` moves on every run, so the document is never byte-identical between
    runs even when the shards are, and refusing to overwrite would make a second
    inventory of an unchanged archive fail. What is stable across those runs is the
    ``identity``, which is what a result cites.
    """
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / INVENTORY_FILENAME
    document = inventory.as_dict()
    path.write_text(
        json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8"
    )
    return {**document, "path": str(path), "sha256": hash_file(path)}


def load_inventory(path: str | Path) -> ExternalInventory:
    """Read an inventory back from a file or from the directory holding it.

    The recorded identity is recomputed from the shard records and a disagreement is
    refused, so an inventory that was hand-edited after it was written cannot pass as
    the run's own statement of its inputs.
    """
    target = Path(path)
    if target.is_dir():
        target = target / INVENTORY_FILENAME
    try:
        text = target.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"inventory {target} could not be read: {exc}") from exc
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"inventory {target} is not valid JSON: {exc}") from exc
    where = str(target)
    source = _require_mapping(document, where=where)
    shards = tuple(
        _shard_from_json(entry, where=f"{where}: shards[{index}]")
        for index, entry in enumerate(
            _require_sequence(source.get("shards"), where=f"{where}: shards")
        )
    )
    layers = tuple(
        _layer_from_json(entry, where=f"{where}: layers[{index}]")
        for index, entry in enumerate(
            _require_sequence(source.get("layers"), where=f"{where}: layers")
        )
    )
    inventory = ExternalInventory(
        root=_require_text(source, "root", where=where),
        inventory_version=_require_text(source, "inventory_version", where=where),
        layers=layers,
        shards=shards,
        identity=_require_text(source, "identity", where=where),
        flags=_text_tuple(source, "flags", where=where),
        created_at=_instant(source, "created_at", where=where),
        hash_scope=_require_text(source, "hash_scope", where=where),
    )
    recomputed = _inventory_identity(shards)
    if recomputed != inventory.identity:
        raise ValueError(
            f"inventory {target} records identity {inventory.identity} but its shard records "
            f"hash to {recomputed}"
        )
    return inventory


def verify_inventory(inventory: ExternalInventory, root: str | Path) -> dict[str, Any]:
    """Re-hash every named shard under ``root`` and report what moved.

    ``identity`` is the identity being verified, taken from the inventory, because that
    is the version a result cites. ``unchanged`` is true only when every recorded shard
    was found and digested to its recorded digest, so an inventory with no digest, or
    with none of its shards present, reports ``False`` rather than passing vacuously.
    Shards that record no digest are listed under ``not_verified`` and cannot be
    verified at all; that is the state a no-hash inventory is in.
    """
    base = Path(root)
    changed: list[str] = []
    missing: list[str] = []
    unreadable: list[str] = []
    not_verified: list[str] = []
    verified = 0
    for shard in inventory.shards:
        path = base / shard.relative_path
        if shard.sha256 is None:
            not_verified.append(shard.relative_path)
            continue
        try:
            actual = hash_file(path)
        except FileNotFoundError:
            missing.append(shard.relative_path)
            continue
        except OSError:
            unreadable.append(shard.relative_path)
            continue
        verified += 1
        if actual != shard.sha256:
            changed.append(shard.relative_path)
    blocked = bool(changed or missing or unreadable or not_verified)
    return {
        "changed": sorted(changed),
        "hash_scope": inventory.hash_scope,
        "identity": inventory.identity,
        "missing": sorted(missing),
        "not_verified": sorted(not_verified),
        "shards_verified": verified,
        "unchanged": verified > 0 and not blocked,
        "unreadable": sorted(unreadable),
    }


def _canonical_json(value: Any) -> str:
    """One deterministic JSON spelling, so an identity cannot depend on key order."""
    return json.dumps(
        value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    )


def _schema_fingerprint(columns: Sequence[tuple[str, str]]) -> str:
    """A digest of the shard's own column names and Arrow types.

    The fingerprint is over the Arrow schema, not over the Parquet physical types, so
    two shards agree when a consumer reading them through Arrow would see the same
    types.
    """
    return hash_bytes(_canonical_json([[name, dtype] for name, dtype in columns]).encode("utf-8"))


def _inventory_identity(shards: Sequence[ShardRecord]) -> str:
    """The SHA-256 over the canonically ordered facts a re-run must reproduce.

    Each row holds the layer, the relative path, the size, the footer row count, the
    schema fingerprint and the content digest. Sorting by layer and relative path
    fixes the order, and both of those are unique per shard, so the identity does not
    depend on the order the file system returned the glob in.
    """
    rows = [
        [
            shard.layer,
            shard.relative_path,
            shard.bytes,
            shard.row_count,
            shard.schema_fingerprint,
            shard.sha256,
        ]
        for shard in shards
    ]
    rows.sort(key=lambda row: (row[0], row[1]))
    return hash_bytes(_canonical_json(rows).encode("utf-8"))


def _absent_evidence(flags: Sequence[str]) -> tuple[str, ...]:
    """Which of ``flags`` say a named input is absent or unverified."""
    present = set(flags)
    return tuple(sorted(flag for flag in ABSENT_EVIDENCE_FLAGS if flag in present))


def _check_spec(spec: LayerSpec) -> None:
    """Refuse a layer pattern that is absolute or climbs out of the archive root."""
    pattern = Path(spec.path_pattern)
    if not spec.path_pattern or pattern.is_absolute() or ".." in pattern.parts:
        raise ValueError(
            f"layer {spec.name!r} declares path_pattern {spec.path_pattern!r}; a layer pattern is "
            "relative to the archive root and never escapes it"
        )


def _resolve_layer_specs(
    layers: Sequence[LayerSpec | str] | None, config_path: str | Path
) -> tuple[LayerSpec, ...]:
    """The selected layer specifications, defaulting to every configured layer."""
    if layers is None:
        return load_layer_specs(config_path)
    configured: dict[str, LayerSpec] | None = None
    resolved: list[LayerSpec] = []
    for item in layers:
        if isinstance(item, LayerSpec):
            spec = item
        elif isinstance(item, str):
            if configured is None:
                configured = {known.name: known for known in load_layer_specs(config_path)}
            if item not in configured:
                raise ValueError(
                    f"unknown layer {item!r}; the configuration declares {', '.join(sorted(configured))}"
                )
            spec = configured[item]
        else:
            raise TypeError(f"layers entries must be LayerSpec or str, got {type(item).__name__}")
        _check_spec(spec)
        resolved.append(spec)
    return tuple(resolved)


def _matching_files(root: Path, spec: LayerSpec) -> tuple[Path, ...]:
    """The shard files one layer's pattern matches, in a stable order."""
    matches = (path for path in root.glob(spec.path_pattern) if path.is_file())
    return tuple(sorted(matches, key=str))


def _column_statistics(group: Any, column: str) -> Any:
    """The footer statistics of one column in one row group, or ``None``."""
    for index in range(group.num_columns):
        candidate = group.column(index)
        if candidate.path_in_schema == column:
            return candidate.statistics
    return None


def _instant_text(value: Any, time_unit: str | None) -> str | None:
    """One instant as ISO-8601 text, or ``None`` when no instant can be stated.

    A value is rendered only when it already carries a zone or when the layer declares
    that its numbers are Unix epoch seconds. A naive datetime and a number in an
    undeclared unit are left unreported rather than assigned a zone or a unit, because
    a reported bound decides which shards cover a window.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(dt.UTC).isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if time_unit != _EPOCH_SECONDS:
            return None
        try:
            return dt.datetime.fromtimestamp(value, tz=dt.UTC).isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    return None


def _timestamp_coverage(
    spec: LayerSpec, metadata: Any, columns: Sequence[tuple[str, str]]
) -> tuple[tuple[tuple[str, str | None, str | None], ...], tuple[str, ...]]:
    """The declared time column's bounds and every reason they are incomplete.

    One entry is always reported when the layer declares a time column, so a consumer
    reads one place to learn the column's coverage. The bounds are combined over the
    row groups that carry statistics; when only some row groups do, the combination is
    reported and named as partial, because a partial minimum is not the shard's
    minimum.
    """
    column = spec.time_column
    if column is None:
        return (), ()
    if column not in {name for name, _ in columns}:
        return ((column, None, None),), (f"{column}:{MISSING_STATISTICS_COLUMN_ABSENT}",)
    minimums: list[Any] = []
    maximums: list[Any] = []
    absent_groups = 0
    for index in range(metadata.num_row_groups):
        statistics = _column_statistics(metadata.row_group(index), column)
        if (
            statistics is None
            or not statistics.has_min_max
            or statistics.min is None
            or statistics.max is None
        ):
            absent_groups += 1
            continue
        minimums.append(statistics.min)
        maximums.append(statistics.max)
    missing: list[str] = []
    if not minimums:
        missing.append(f"{column}:{MISSING_STATISTICS_NO_FOOTER_STATISTICS}")
    elif absent_groups:
        missing.append(f"{column}:{MISSING_STATISTICS_PARTIAL_ROW_GROUPS}")
    # The shard's low bound is the minimum of the row groups' minimums and its high
    # bound is the maximum of their maximums. Taking one operation for both would
    # report a low bound where a high bound belongs.
    low = _instant_text(min(minimums), spec.time_unit) if minimums else None
    high = _instant_text(max(maximums), spec.time_unit) if maximums else None
    if (minimums and low is None) or (maximums and high is None):
        missing.append(f"{column}:{MISSING_STATISTICS_UNCONVERTED_BOUND}")
    return ((column, low, high),), tuple(missing)


def _shard_record(
    spec: LayerSpec, *, relative_path: str, path: Path, verify_hash: bool
) -> ShardRecord:
    """Describe one shard from its bytes and its footer, recording every failure."""
    flags: list[str] = []
    errors: list[str] = []
    size = 0
    try:
        size = path.stat().st_size
    except OSError as exc:
        flags.append(FLAG_SIZE_UNMEASURED)
        errors.append(f"size could not be measured: {exc}")
    sha256: str | None = None
    if verify_hash:
        try:
            sha256 = hash_file(path)
        except OSError as exc:
            flags.append(FLAG_HASH_UNAVAILABLE)
            errors.append(f"sha256 could not be computed: {exc}")
    row_count: int | None = None
    row_group_count: int | None = None
    columns: tuple[tuple[str, str], ...] = ()
    fingerprint = SCHEMA_FINGERPRINT_UNAVAILABLE
    stats: tuple[tuple[str, str | None, str | None], ...] = ()
    missing: tuple[str, ...] = ()
    status = SHARD_STATUS_READ
    try:
        handle = pq.ParquetFile(path)
        metadata = handle.metadata
        schema = handle.schema_arrow
    except (OSError, ValueError) as exc:
        # ``OSError`` covers a footer that deserialized to nothing, and ``ValueError``
        # covers pyarrow's own invalid-file rejection. Both are this shard's problem,
        # not the run's: the record states it and the inventory continues.
        status = SHARD_STATUS_UNREADABLE
        errors.append(f"parquet footer could not be read: {exc}")
        if spec.time_column is not None:
            stats = ((spec.time_column, None, None),)
            missing = (f"{spec.time_column}:{MISSING_STATISTICS_UNREADABLE_FOOTER}",)
    else:
        columns = tuple((field.name, str(field.type)) for field in schema)
        row_count = metadata.num_rows
        row_group_count = metadata.num_row_groups
        fingerprint = _schema_fingerprint(columns)
        stats, missing = _timestamp_coverage(spec, metadata, columns)
    return ShardRecord(
        layer=spec.name,
        relative_path=relative_path,
        bytes=size,
        row_count=row_count,
        row_group_count=row_group_count,
        schema_fingerprint=fingerprint,
        columns=columns,
        timestamp_stats=stats,
        missing_statistics=missing,
        sha256=sha256,
        status=status,
        flags=tuple(sorted(flags)),
        error="; ".join(errors) if errors else None,
    )


def _flag_duplicate_filenames(shards: Sequence[ShardRecord]) -> list[ShardRecord]:
    """Flag every shard whose filename another layer also uses.

    The archive really does reuse filenames across layers, so a bare name is not a key
    for anything. The flag is recorded on the shards and carried into their layers
    rather than being resolved by picking a winner, because the inventory's job is to
    state the collision and leave the choice to the reader.
    """
    owners: dict[str, set[str]] = {}
    for shard in shards:
        owners.setdefault(Path(shard.relative_path).name, set()).add(shard.layer)
    shared = {name for name, layers in owners.items() if len(layers) > 1}
    if not shared:
        return list(shards)
    flagged: list[ShardRecord] = []
    for shard in shards:
        if Path(shard.relative_path).name in shared:
            shard = replace(
                shard, flags=tuple(sorted({*shard.flags, FLAG_DUPLICATE_SHARD_FILENAME}))
            )
        flagged.append(shard)
    return flagged


def _layer_record(spec: LayerSpec, shards: Sequence[ShardRecord], *, root_text: str) -> LayerRecord:
    """Summarize one layer, reporting a glob that matched nothing as missing."""
    owned = [shard for shard in shards if shard.layer == spec.name]
    if not owned:
        return LayerRecord(
            name=spec.name,
            path_pattern=spec.path_pattern,
            input_class=spec.input_class,
            role=spec.role,
            producer=spec.producer,
            license=spec.license,
            venue=spec.venue,
            shard_count=0,
            total_rows=0,
            total_bytes=0,
            status=LAYER_STATUS_MISSING,
            flags=(FLAG_LAYER_MISSING,),
            error=f"no shard file matches {spec.path_pattern!r} under {root_text}",
        )
    flags: set[str] = set()
    total_rows = 0
    total_bytes = 0
    unreadable = False
    for shard in owned:
        flags.update(shard.flags)
        if shard.row_count is not None:
            total_rows += shard.row_count
        total_bytes += shard.bytes
        unreadable = unreadable or shard.status == SHARD_STATUS_UNREADABLE
    if unreadable:
        flags.add(FLAG_SHARD_UNREADABLE)
    return LayerRecord(
        name=spec.name,
        path_pattern=spec.path_pattern,
        input_class=spec.input_class,
        role=spec.role,
        producer=spec.producer,
        license=spec.license,
        venue=spec.venue,
        shard_count=len(owned),
        total_rows=total_rows,
        total_bytes=total_bytes,
        status=LAYER_STATUS_PRESENT,
        flags=tuple(sorted(flags)),
        error=None,
    )


def _layer_to_json(record: LayerRecord) -> dict[str, Any]:
    return {
        "error": record.error,
        "flags": list(record.flags),
        "input_class": record.input_class,
        "license": record.license,
        "name": record.name,
        "path_pattern": record.path_pattern,
        "producer": record.producer,
        "role": record.role,
        "shard_count": record.shard_count,
        "status": record.status,
        "total_bytes": record.total_bytes,
        "total_rows": record.total_rows,
        "venue": record.venue,
    }


def _layer_from_json(value: Any, *, where: str) -> LayerRecord:
    source = _require_mapping(value, where=where)
    return LayerRecord(
        name=_require_text(source, "name", where=where),
        path_pattern=_require_text(source, "path_pattern", where=where),
        input_class=_require_text(source, "input_class", where=where),
        role=_require_text(source, "role", where=where),
        producer=_require_text(source, "producer", where=where),
        license=_require_text(source, "license", where=where),
        venue=_optional_text(source, "venue", where=where),
        shard_count=_require_int(source, "shard_count", where=where),
        total_rows=_require_int(source, "total_rows", where=where),
        total_bytes=_require_int(source, "total_bytes", where=where),
        status=_require_text(source, "status", where=where),
        flags=_text_tuple(source, "flags", where=where),
        error=_optional_text(source, "error", where=where),
    )


def _shard_from_json(value: Any, *, where: str) -> ShardRecord:
    source = _require_mapping(value, where=where)
    return ShardRecord(
        layer=_require_text(source, "layer", where=where),
        relative_path=_require_text(source, "relative_path", where=where),
        bytes=_require_int(source, "bytes", where=where),
        row_count=_optional_int(source, "row_count", where=where),
        row_group_count=_optional_int(source, "row_group_count", where=where),
        schema_fingerprint=_require_string(source, "schema_fingerprint", where=where),
        columns=_column_pairs(source.get("columns"), where=f"{where}.columns"),
        timestamp_stats=_timestamp_pairs(
            source.get("timestamp_stats"), where=f"{where}.timestamp_stats"
        ),
        missing_statistics=_text_tuple(source, "missing_statistics", where=where),
        sha256=_optional_text(source, "sha256", where=where),
        status=_require_text(source, "status", where=where),
        flags=_text_tuple(source, "flags", where=where),
        error=_optional_text(source, "error", where=where),
    )


def _column_pairs(value: Any, *, where: str) -> tuple[tuple[str, str], ...]:
    entries = _require_sequence(value, where=where)
    pairs: list[tuple[str, str]] = []
    for entry in entries:
        pair = _require_sequence(entry, where=where)
        if len(pair) != 2 or not all(isinstance(item, str) for item in pair):
            raise ValueError(f"{where} holds [name, type] string pairs, got {entry!r}")
        pairs.append((pair[0], pair[1]))
    return tuple(pairs)


def _timestamp_pairs(value: Any, *, where: str) -> tuple[tuple[str, str | None, str | None], ...]:
    entries = _require_sequence(value, where=where)
    triples: list[tuple[str, str | None, str | None]] = []
    for entry in entries:
        triple = _require_sequence(entry, where=where)
        if len(triple) != 3 or not isinstance(triple[0], str):
            raise ValueError(f"{where} holds [column, min, max] triples, got {entry!r}")
        column, low, high = triple
        if (low is not None and not isinstance(low, str)) or (
            high is not None and not isinstance(high, str)
        ):
            raise ValueError(f"{where} bounds are strings or null, got {entry!r}")
        triples.append((column, low, high))
    return tuple(triples)


def _require_mapping(value: Any, *, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{where} must be an object, got {type(value).__name__}")
    return value


def _require_sequence(value: Any, *, where: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{where} must be an array, got {type(value).__name__}")
    return value


def _require_string(source: Mapping[str, Any], key: str, *, where: str) -> str:
    value = source.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{where}.{key} must be a string, got {value!r}")
    return value


def _require_text(source: Mapping[str, Any], key: str, *, where: str) -> str:
    value = _require_string(source, key, where=where)
    if not value:
        raise ValueError(f"{where}.{key} must not be empty")
    return value


def _optional_text(source: Mapping[str, Any], key: str, *, where: str) -> str | None:
    value = source.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{where}.{key} must be a non-empty string or null, got {value!r}")
    return value


def _require_int(source: Mapping[str, Any], key: str, *, where: str) -> int:
    value = source.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{where}.{key} must be an integer, got {value!r}")
    return value


def _optional_int(source: Mapping[str, Any], key: str, *, where: str) -> int | None:
    if source.get(key) is None:
        return None
    return _require_int(source, key, where=where)


def _text_tuple(source: Mapping[str, Any], key: str, *, where: str) -> tuple[str, ...]:
    entries = _require_sequence(source.get(key), where=f"{where}.{key}")
    if not all(isinstance(entry, str) for entry in entries):
        raise ValueError(f"{where}.{key} must hold only strings")
    return tuple(entries)


def _instant(source: Mapping[str, Any], key: str, *, where: str) -> dt.datetime:
    text = _require_text(source, key, where=where)
    try:
        moment = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{where}.{key} is not an ISO-8601 instant: {text!r}") from exc
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{where}.{key} must carry a UTC offset: {text!r}")
    return moment.astimezone(dt.UTC)
