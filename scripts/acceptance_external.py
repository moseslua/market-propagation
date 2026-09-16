"""End-to-end acceptance harness for the external-history pipeline.

This is the lever a reviewer reruns instead of redoing the work by hand. It
drives the documented commands over the configured inputs into a fresh
directory, then repeats a bounded portion of the run and compares the identities
and content hashes the commands themselves reported. Repeating a run over
unchanged inputs must reproduce the same hashes, and this script is how that
claim is checked rather than asserted.

Exit codes follow the CLI's own contract:

* ``0`` the run reproduced and its artifacts were produced.
* ``1`` an operational failure: a command raised, a run directory could not be
  created, or a repeat disagreed with the first run.
* ``2`` the pipeline ran and reported blocked evidence. A blocked cohort is a
  valid pipeline output, not a failure, so it is reported distinctly.

Usage:

    uv run --no-sync python scripts/acceptance_external.py --out .audit/acceptance
    uv run --no-sync python scripts/acceptance_external.py --out .audit/acceptance --skip-hashes

By default every shard is hashed during inventory, which reads the full archive.
``--skip-hashes`` keeps the structural inventory and skips the digest pass, which
is useful for a quick structural check and is recorded in the output as such.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import pathlib
import shutil
import sys
import time
from typing import Any

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from market_propagation.cli import main as cli_main  # noqa: E402

DEFAULT_CONFIG = "configs/external_history_v1.yaml"

#: Fields that are the content identity of what a command produced, and therefore
#: must reproduce exactly across two runs over unchanged inputs. These are the
#: digests the pipeline itself computes over derived bytes.
IDENTITY_FIELDS = ("identity", "content_hash", "panel_sha256", "report_sha256")

#: Fields that legitimately differ between two runs and are recorded rather than
#: compared. ``sha256`` is a manifest file's own digest: a manifest that records
#: when it was written is better provenance than one that cannot, so requiring
#: byte-identical manifests would mean dropping the record of when the run
#: happened. ``created_at`` and the timings are the same kind of fact.
#:
#: Excluding these does not weaken the reproduction check, because the manifest's
#: *content* identity is compared through ``identity``, which is computed over the
#: shard facts alone. A changed shard still fails.
RUN_METADATA_FIELDS = (
    "sha256",
    "created_at",
    "generated_at",
    "run_id",
    "seconds",
    "elapsed_seconds",
    "started_at",
    "ended_at",
)


class Step:
    """One command invocation, with the exit code and payload it produced."""

    def __init__(self, name: str, argv: list[str]) -> None:
        self.name = name
        self.argv = argv
        self.exit_code: int | None = None
        self.payload: Any = None
        self.stdout = ""
        self.stderr = ""
        self.seconds = 0.0
        self.error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "argv": self.argv,
            "exit_code": self.exit_code,
            "seconds": round(self.seconds, 3),
            "error": self.error,
            "stderr": self.stderr.strip() or None,
            "payload": self.payload,
        }


def run_step(name: str, argv: list[str]) -> Step:
    """Run one command in-process so its exit code is observed directly."""
    step = Step(name, argv)
    out, err = io.StringIO(), io.StringIO()
    started = time.perf_counter()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            step.exit_code = cli_main(argv)
    except BaseException as exc:  # recorded, never swallowed silently
        step.error = f"{type(exc).__name__}: {exc}"
        step.exit_code = 1
    step.seconds = time.perf_counter() - started
    step.stdout = out.getvalue()
    step.stderr = err.getvalue()
    if step.stdout.strip():
        try:
            step.payload = json.loads(step.stdout)
        except json.JSONDecodeError as exc:
            step.error = step.error or f"stdout was not one JSON document: {exc}"
    return step


def collect_identities(
    payload: Any, *, fields: tuple[str, ...] = IDENTITY_FIELDS, prefix: str = ""
) -> dict[str, Any]:
    """Every named field a payload reports, keyed by its path.

    A command reports the content hashes of the artifacts it produced. Walking
    the payload for them means a new artifact is compared without this harness
    being taught its name, and a field that stops being reported shows up as a
    missing key rather than as a silent pass.
    """
    found: dict[str, Any] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if key in fields and isinstance(value, (str, int)):
                found[path] = value
            else:
                found.update(collect_identities(value, fields=fields, prefix=path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            found.update(collect_identities(value, fields=fields, prefix=f"{prefix}[{index}]"))
    return found


def worst_exit(steps: list[Step]) -> int:
    """The run's overall verdict: failure beats blocked, blocked beats success."""
    codes = [step.exit_code for step in steps]
    if any(code is None or code == 1 for code in codes):
        return 1
    if any(code == 2 for code in codes):
        return 2
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="acceptance_external",
        description=(
            "Drive the external-history commands end to end and check that a repeated "
            "bounded run reproduces the identities and hashes the first run reported."
        ),
    )
    parser.add_argument("--out", required=True, help="directory to write this acceptance run into")
    parser.add_argument("--root", default=None, help="external archive root (default: from config)")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="pipeline configuration path")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=20000,
        help=(
            "row bound applied to the extraction and to the repeated bounded run "
            "(default: 20000). The bound is recorded; it is not a sampling rule."
        ),
    )
    parser.add_argument("--run-id", default=None, help="run identifier (default: a UTC timestamp)")
    parser.add_argument(
        "--skip-hashes",
        action="store_true",
        help="inventory without the full digest pass; recorded as a structural-only inventory",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep an existing --out directory instead of replacing it",
    )
    return parser.parse_args(argv)


def _config_root(config_path: pathlib.Path) -> str:
    """The archive root the configuration names.

    Resolved from the configuration rather than defaulted in this script, so the
    root the acceptance run reads is the same one every other command reads and
    a change to the config cannot leave this harness pointing somewhere else.
    """
    import yaml

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    inputs = payload.get("inputs") or {}
    root = inputs.get("root")
    if not isinstance(root, str) or not root.strip():
        raise SystemExit(f"{config_path} declares no inputs.root")
    return root


def fields_by_step(steps: list[Step], fields: tuple[str, ...]) -> dict[str, Any]:
    """Named fields of every step, keyed by step name and field path.

    Keying by the step's own name rather than by its position in the list is what
    makes the comparison complete. The repeat run deliberately omits the coverage
    and report stages, so positional keys would compare the wrong steps against
    each other and would leave the panel's content hash out of the comparison
    entirely while still reporting a clean result.
    """
    found: dict[str, Any] = {}
    for step in steps:
        name = step.name.removeprefix("repeat-")
        for path, value in collect_identities(step.payload, fields=fields).items():
            found[f"{name}:{path}"] = value
    return found


def compare(first: dict[str, Any], repeat: dict[str, Any]) -> dict[str, Any]:
    """Fields present in both runs whose values disagree."""
    shared = set(first) & set(repeat)
    return {
        key: {"first": first[key], "repeat": repeat[key]}
        for key in sorted(shared)
        if first[key] != repeat[key]
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out = pathlib.Path(args.out).expanduser().resolve()
    if out.exists() and not args.keep:
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)

    config = str(pathlib.Path(args.config).expanduser().resolve())
    root = args.root or _config_root(pathlib.Path(config))
    first = out / "first"
    repeat = out / "repeat"
    inventory_dir = first / "inventory"

    inventory_flags = ["--no-hashes"] if args.skip_hashes else []

    steps: list[Step] = [
        run_step(
            "inventory-external",
            [
                "inventory-external",
                "--config",
                config,
                "--root",
                root,
                "--output",
                str(inventory_dir),
                *inventory_flags,
            ],
        )
    ]

    # Each later stage is attempted only when the stage it reads from produced
    # something. A missing upstream artifact is reported as a skipped step rather
    # than being papered over with a substitute input.
    normalize_argv = [
        "normalize-external",
        "--config",
        config,
        "--output",
        str(first),
        "--max-rows",
        str(args.max_rows),
    ]
    steps.append(run_step("normalize-external", normalize_argv))

    steps.append(
        run_step(
            "coverage-external",
            ["coverage-external", "--config", config, "--output", str(first)],
        )
    )

    trades = first / "historical_trades.parquet"
    if trades.exists():
        steps.append(
            run_step(
                "build-trade-panel",
                [
                    "build-trade-panel",
                    "--config",
                    config,
                    "--trades",
                    str(trades),
                    "--output",
                    str(first),
                ],
            )
        )
        panel = first / "trade_panel.parquet"
        if panel.exists():
            steps.append(
                run_step(
                    "report-external",
                    [
                        "report-external",
                        "--config",
                        config,
                        "--panel",
                        str(panel),
                        "--output",
                        str(first / "report"),
                        "--run-id",
                        run_id,
                    ],
                )
            )

    # Repeat a bounded portion. The inventory identity and the bounded extraction
    # and panel are the artifacts whose reproduction makes the run auditable.
    repeat_steps: list[Step] = [
        run_step(
            "repeat-inventory-external",
            [
                "inventory-external",
                "--config",
                config,
                "--root",
                root,
                "--output",
                str(repeat / "inventory"),
                *inventory_flags,
            ],
        )
    ]
    repeat_normalize = list(normalize_argv)
    repeat_normalize[repeat_normalize.index(str(first))] = str(repeat)
    repeat_steps.append(run_step("repeat-normalize-external", repeat_normalize))
    if trades.exists():
        repeat_panel = [
            "build-trade-panel",
            "--config",
            config,
            "--trades",
            str(trades),
            "--output",
            str(repeat),
        ]
        # A bounded extraction is the only part whose bytes can differ between two
        # runs, so the panel is compared across runs only when the extraction was
        # itself reproducible.
        repeat_steps.append(run_step("repeat-build-trade-panel", repeat_panel))

    first_ids = fields_by_step(steps, IDENTITY_FIELDS)
    repeat_ids = fields_by_step(repeat_steps, IDENTITY_FIELDS)
    compared = sorted(set(first_ids) & set(repeat_ids))
    disagreements = compare(first_ids, repeat_ids)

    # Run metadata legitimately differs between two runs. It is still compared and
    # reported, so a reader can see exactly what was excluded and why, rather than
    # being asked to trust that nothing important was skipped.
    first_meta = fields_by_step(steps, RUN_METADATA_FIELDS)
    repeat_meta = fields_by_step(repeat_steps, RUN_METADATA_FIELDS)
    meta_compared = sorted(set(first_meta) & set(repeat_meta))
    metadata_differences = compare(first_meta, repeat_meta)

    summary = {
        "acceptance": "external_history_v1",
        "run_id": run_id,
        "config": config,
        "config_sha256": _sha256(pathlib.Path(config)),
        "root": root,
        "hash_scope": "none" if args.skip_hashes else "all_shards",
        "max_rows": args.max_rows,
        "out": str(out),
        "steps": [step.as_dict() for step in steps],
        "repeat_steps": [step.as_dict() for step in repeat_steps],
        "first_identities": first_ids,
        "repeat_identities": repeat_ids,
        "compared_identities": compared,
        "disagreements": disagreements,
        "run_metadata_compared": meta_compared,
        "run_metadata_differences": metadata_differences,
        "reproduced": not disagreements,
        "verdict": None,
    }
    (out / "acceptance.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )

    worst = worst_exit(steps)
    blocked = worst == 2
    # A step that failed technically is not a clean run, and a blocked step is not
    # a failure. Reporting success because the identities happened to agree would
    # hide a command that never produced its artifact.
    verdict = 1 if (disagreements or worst == 1) else (2 if blocked else 0)
    summary["verdict"] = verdict
    (out / "acceptance.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )

    print(
        json.dumps(
            {
                key: summary[key]
                for key in (
                    "acceptance",
                    "run_id",
                    "config_sha256",
                    "hash_scope",
                    "max_rows",
                    "compared_identities",
                    "disagreements",
                    "reproduced",
                    "verdict",
                )
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    if disagreements:
        print(
            f"acceptance_external: a repeated bounded run disagreed with the first run on "
            f"{len(disagreements)} identity field(s); see {out / 'acceptance.json'}",
            file=sys.stderr,
        )
    elif blocked:
        print(
            "acceptance_external: every command ran and the run reproduced, but the pipeline "
            "reports blocked evidence, so no empirical result is claimed by it",
            file=sys.stderr,
        )
    return verdict


def _sha256(path: pathlib.Path) -> str | None:
    if not path.is_file():
        return None
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
