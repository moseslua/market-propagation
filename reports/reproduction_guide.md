# Reproduction guide

This guide rebuilds this repository from a checkout, runs the offline synthetic
reproduction, checks the artifacts it wrote, imports the separately captured
original BLS releases from the browser archive, and runs the real coverage audit
and event card. It ends with what a clean, non-editable rebuild should verify.

All commands run from the repository root. The documented output directory for the
reproducible synthetic rebuild is `data/synthetic/final`. `data/` is git-ignored,
so every path under it is a local artifact, never a packaged file.

Nothing here places an order, holds a credential, or spends. The public commands
issue documented GET requests only.

## What this guide covers

Three paths exist and they are not interchangeable.

| Path | Input | Needs network | Establishes |
| --- | --- | --- | --- |
| Synthetic reproduction | The packaged fixture and the seeded simulator | No | That the software and methods run end to end |
| Real acquisition | Public endpoints and the captured BLS archive | Only for `audit` and `capture` | What those endpoints returned at the recorded moments |
| Live cross-section collection | The public perpetual-futures source, read now | Yes, on every sweep | What that source served at the recorded build stamp |

The synthetic path is deterministic and offline. The acquisition path archives
whatever the endpoints actually returned, including failures. Collection in Step 10
is the only path that reads a source which keeps no history: what it does not take
now is gone, while Steps 1-9 can be rerun against archives already held.

The first two can be combined. `reproduce --real-audit ... --release-dataset ...` runs the
synthetic rebuild and cites the real evidence at the status it recorded, without
fitting either real input. Naming an input is what makes it cited: with neither flag,
the run cites nothing real and stays a synthetic-only reproduction.

The real evidence this guide checks lives in two directories. `data/public/final-audit`
is the primary audit, produced by a bounded CLI run that reached the public venue and
read first releases from the named archived dataset. `data/public/delivery-audit` is
an earlier offline replay of previously archived responses, kept as that replay.
`reports/data_audit.md` reports both.

## Prerequisites

- Python 3.11 or later. `pyproject.toml` sets `requires-python = ">=3.11"`.
- `uv`, which installs the locked dependencies and runs every command.
- A full clone, not an archive of just the package. The commands read `configs/`.

## Step 1: Check out the source and install the locked dependencies

Clone the repository and enter it, then install from the lock file.

```bash
uv sync --extra dev --frozen
```

`--frozen` installs exactly the versions recorded in `uv.lock` and never rewrites
the lock. `--extra dev` adds `pytest`, which runs the test suite in Step 7. Omit
`--extra dev` if you only intend to run the commands.

Confirm the lock matches the declared dependencies:

```bash
uv lock --check
```

Both entry points run the same code and return the same exit codes:

```bash
uv run market-propagation --help
uv run python -m market_propagation --help
```

## Step 2: Run the synthetic reproduction

```bash
uv run market-propagation reproduce --output data/synthetic/final \
  --spec configs/study_v1.yaml --events 120 --repetitions 40 --bootstrap 200
```

Run from the repository root. The default `--spec` is `configs/study_v1.yaml`,
resolved relative to the working directory, so a run from elsewhere must pass an
explicit path. Each omitted option keeps the library's own default.

That command cites no real input. To cite the real evidence alongside the synthetic
rebuild, name each input explicitly:

```bash
uv run market-propagation reproduce --output data/synthetic/final \
  --events 120 --repetitions 40 --bootstrap 200 \
  --real-audit data/public/final-audit \
  --release-dataset data/public/bls-normalized/releases.parquet
```

`--real-audit` names a real acquisition audit directory to cite, and
`--release-dataset` names a sealed archived-release dataset to cite. No directory is
selected for you: omit either and this run cites no input of that kind.

Both are reference only. The dataset is verified against its own original bytes,
recorded in the manifest, and never fitted; it does not enter the synthetic sample or
any model. `manifest.json` records the path each input came from under `inputs`, plus
`real_inputs_used_as_fitted_inputs: false`, so a reader can see which real inputs a
run cited and confirm none of them was fitted.

The reproduced run's own result records the blocking: `empirical.status` is
`blocked`, `empirical.referenced_real_audit` names
`data/public/final-audit/coverage.json`, and
`empirical.referenced_release_dataset` names
`data/public/bls-normalized/releases.parquet`. The record also states that a
published release value certifies neither the market rule version in force at the
release nor the quote coverage of any contract, so the named inputs still establish
no eligible cohort.

The command writes one JSON result to stdout and a set of artifacts under
`--output`. The artifacts are:

| Artifact | What it is |
| --- | --- |
| `metrics.json` | The run's full result record |
| `manifest.json` | Inputs, hashes, seeds, dependency versions and the file inventory |
| `source_panel.parquet`, `usable_panel.parquet`, `forecast_panel.parquet` | The sealed panels, one `.manifest.json` sidecar each |
| `replay_disagreements.json` | The comparison between the source-time and usable-time replay folds |
| `experiment_registry.sqlite3` | The run registry, written into the output directory |
| `experiment_registry.jsonl` | The registry's JSON Lines export |
| `baseline_report.md`, `conditional_propagation_report.md`, `data_card.md`, `paper.md` | The generated reports |
| `figures/` | The five figures named in the manifest |
| `raw/` | The archived fixture records the panels were built from |

Exit `0` means the run completed. Exit `2` means it ran and reported a blocked
stage; read `blocked_stages` in `metrics.json` and the notes on stderr.

The panels are sealed. A second identical build leaves every byte unchanged, and a
fixture whose content changed raises instead of replacing a sealed panel. To
rebuild from a changed fixture, write to a new output directory.

## Step 3: Inspect the raw hashes and the store quality

`quality` re-reads every receipt and verifies each referenced payload against its
content hash. It parses no payload content and claims no source clock.

```bash
uv run market-propagation quality data/synthetic/final/raw \
  --output reports/generated/quality-synthetic.json
```

The report counts receipts, verified payloads, failed verifications, blobs, and
orphan blobs. A failed verification, or a store with no receipt, exits `2`. An
empty store is reported as empty rather than as clean.

The reproduction's own chain is recorded in `data/synthetic/final/data_card.md`
and in `data/synthetic/final/manifest.json`, which names the sha256 of every input
it hashed and of every file it wrote. Read the two together: `manifest.json`
carries the machine-readable hashes, and `data_card.md` states what they were read
from, including the git revision, whether the working tree was dirty, and the
dependency versions actually installed.

The same command checks an acquired store:

```bash
uv run market-propagation quality data/public/final-audit/raw \
  --output reports/generated/quality-final-audit.json
```

A receipt proves that this process stored these bytes at these instants. It proves
nothing about when a source published them.

## Step 4: Import the captured original BLS releases

`data/public/bls-browser/` holds complete browser-response captures of the ten
original BLS releases, one `.json` receipt and one `.html` payload per release.
`scripts/import_bls_archives.py` normalizes them offline.

```bash
uv run python scripts/import_bls_archives.py \
  data/public/bls-browser data/public/bls-normalized
```

The two positional arguments are the source directory and the output directory.
The script reads every `*.json` receipt in the source directory in sorted order and
expects the payload beside it, named `<event_id>.html`. It writes
`<output>/releases.parquet`, its `.manifest.json` sidecar, and the archived bytes
and receipts under `<output>/raw/`.

The script fails rather than importing a questionable payload. It raises when the
receipt status is not `200`, when `payload_complete` is not true, when the body
does not end with `</html>`, when no values parse out of the release, or when the
payload's own embargo line does not agree with the cohort calendar. Each parsed
release is printed to stdout as one JSON object, with its `event_id`, `values`,
`revisions` and `raw_hash`.

The output is sealed. Re-running against an output directory that already holds a
`releases.parquet` with different content raises; use a fresh output directory when
the captures have changed.

**The audit reads this dataset when you name it.** Pass
`--release-dataset data/public/bls-normalized/releases.parquet` and the audit reads
first releases from it and its sibling `raw/` store instead of fetching them. The
dataset is not discovered for you: with no `--release-dataset` the network path is
unchanged.

The distinction matters because the two paths leave different evidence behind. A
release read from the dataset carries `acquisition_method: sealed_release_dataset`
and requires the dataset's own `releases.parquet.manifest.json`, without which the
source refuses to read it. A release fetched over the network carries the status of
the response that produced it.

The live GET to the BLS archive is answered with HTTP 403, and a store counts those
refusals as recorded non-2xx receipts rather than replacing a release with an empty
one. An audit run without `--release-dataset` therefore leaves its
`release_payload_archived` gate unsatisfied and its card's `release` field null. The
earlier live audit under `data/public/g0-policy/` shows exactly that, with 20
refusals and a null release per event. An audit run with `--release-dataset` reads
the archived original payloads and verifies each one against the record that cites
it, which is why `data/public/final-audit/` carries ten release records with
`values_verified_against_original_bytes: true`.

Naming the dataset changes where a release comes from. It changes nothing about
market rule versions, candle coverage, or study eligibility, which stay governed by
their own gates.

### What the imported values do and do not establish

The parquet holds first-release statistics, parsed from the publishers' own payloads
and hash-verified against the archived bytes. That is all it holds.

- **They do not establish market-rule provenance.** The values say what the
  statistics were. Nothing in this dataset records which version of a market's
  settlement rules was in force on the release date, so they cannot show that a
  contract's payoff depended on the statistic as published.
- **They do not establish eligible candles.** A candle is a frequency observation,
  not evidence of complete order-book depth. Some direct-resolution contracts also
  close before the release time, so their post-release quotes cannot be read as a
  response. `configs/cohort.yaml` records observed pre-release close offsets for
  that reason.
- **They do not promote a model.** No software pass promotes a network model or any
  other candidate. Promotion is a gate decision recorded in the run's own results,
  and the falsification audit's status is reported as it stands, including an
  inconclusive one.

## Step 5: Run the coverage audit and assemble an event card

`audit` reads the public venue and the archived release payloads. Every axis is
capped, and the caps actually applied are returned in the result.

```bash
uv run market-propagation audit --output data/public/final-audit \
  --cohort configs/cohort.yaml --windows configs/event_windows.yaml \
  --timeout 8 --max-pages 2 --max-contracts 40 --max-candle-contracts 10 \
  --release-dataset data/public/bls-normalized/releases.parquet
```

The audit reaches the public venue read-only and reads its first releases from the
named dataset. `--release-dataset` is explicit and never inferred: with no
`--release-dataset` the run requests each release over the network instead.

It writes `raw/` with content-addressed payloads and one receipt per occurrence,
plus `coverage.json`, `raw_hashes.json`, `series_discovery.json`,
`release_source.json` and `event_card.json`. `release_source.json` records
`kind: sealed_release_dataset`, `explicitly_selected: true` and
`network_release_requests_issued: false` when the dataset supplied the releases.

A successful HTTP response is never study eligibility. Eligibility is established
only by a verified market identity in the cohort configuration, and an audit with
unsatisfied coverage gates exits `2` while still writing its artifacts. That exit is
the run reporting a blocked G0, not a failed request: the run's own verification
records zero access blockers.

Assemble a card for one event. This issues no request and re-verifies every payload
hash the chosen event cites.

```bash
uv run market-propagation event-card data/public/final-audit \
  --output reports/event_card.json --event-id cpi_2025_01
```

`event-card` reads the audit directory and the event identifier you name. The
`cpi_2025_01` identifier is one of the ten cohort events in `configs/cohort.yaml`;
any identifier the audit holds works the same way. A card built from readable
artifacts exits `0` even when the audit behind it is partial, and its
`unsatisfied_gates`, `audit_complete` and `claims.blocked_by_unsatisfied_gates`
still say that empirical use stays blocked. Exit `2` covers a requested event the
audit holds no record of, and a card whose cited payloads cannot be re-read
against their content hashes.

The card this command writes is not the same artifact as the `event_card.json`
that `audit` writes into its own output directory. The audit's card records
`status: "audited"` and lists its gates under `unsatisfied_gate_names`. Read
whichever one you are looking at by its own field names.

## Step 6: Read a registry

```bash
uv run market-propagation registry-review \
  data/synthetic/final/experiment_registry.sqlite3
```

Point this at a registry a run already wrote. `reproduce` writes
`experiment_registry.sqlite3` into its own output directory, so no registry lives
in `reports/`. The command never creates a registry, so a path that does not exist
exits `1`. Pass `:memory:` to read an empty in-process registry.

## Step 7: Verify a clean, non-editable rebuild

A reproduction produced from a dirty working tree proves nothing about the
committed source. `data_card.md` and `manifest.json` record `git.dirty` and the
dirty source-tree digest, so read them before you quote any run as a rebuild.

To verify one, build a source distribution, unpack it into a fresh directory, and
install from that tree. The unpacked tree is a clean checkout by construction: it
contains only what the archive ships.

```bash
uv build --sdist
mkdir -p /tmp/rebuild
tar -xzf dist/market_propagation-0.1.0.tar.gz -C /tmp/rebuild
cd /tmp/rebuild/market_propagation-0.1.0
uv sync --extra dev --frozen --no-editable
```

The build backend normalizes the project name for archive paths, so the
distribution name `market-propagation` becomes `market_propagation` in the
filename and in the archive's top-level directory. `uv sync` creates `.venv` in the
current directory and installs the project into it from the tree it sits in.
`--frozen` resolves against the `uv.lock` the archive ships and never rewrites it.
`--no-editable` installs the package rather than linking it back to the source
directory, which is what makes the fixture lookup exercise the packaged resource
instead of a checkout path.

A clean rebuild should confirm all of the following, and each item is checkable
with the tools in this guide.

1. **The source distribution ships every file the guide and the specification
   name.** List the archive and confirm it contains `README.md`, `pyproject.toml`,
   `uv.lock`, `configs/`, `scripts/`, `tests/`, the seven `reports/` files named in
   `pyproject.toml`, each `src/market_propagation/` file, and
   `src/market_propagation/fixtures/replay.json`.

   ```bash
   tar -tzf dist/market_propagation-0.1.0.tar.gz
   ```

   It must not contain `data/`, `.audit/`, `.venv/`, `logs/`, `__pycache__/`, or any
   generated run artifact: no run output such as `metrics.json` or
   `quality-<date>.json` from the quality job, no registry SQLite file, no panel
   Parquet, no figure PNG, and no `event_card.json`. Those paths hold acquired data,
   audit scratch, generated reports and build output, and they ship with nothing.

2. **The specification resolves its own referenced paths.** `configs/study_v1.yaml`
   names `reports/preregistration.md`, and the reporting module resolves that path
   against the repository root. If the archive omits that file, the packaged
   specification points at a path that does not exist. The same applies to
   `reports/contract_rule_registry.json` and `reports/literature_matrix.csv`, which
   the specification names as its rule registry and literature records.

   The static evidence documents ship alongside them: `reports/data_audit.md`,
   `reports/data_card.md` and `reports/paper.md` are named in the same allowlist, so
   an unpacked tree can be read without a checkout. No generated counterpart ships
   with them. `data/synthetic/final/paper.md` and `data/synthetic/final/data_card.md`
   are run outputs from the simulator, and the archive carries neither.

3. **The lock file travels with the source.** `manifest.json` records the absolute
   path and sha256 of `uv.lock` as provenance. An archive without it cannot be
   resolved to the recorded versions. `uv lock --check` should pass in the
   unpacked tree.

4. **The packaged fixture resolves without a source tree.** The wheel ships
   `src/market_propagation/fixtures/replay.json` and the code locates it through
   `importlib.resources`. A non-editable install finds it inside `site-packages`
   rather than in a checkout, which is what `--no-editable` exercises.

5. **The same inputs produce the same artifact bytes.** Compare the `hashes`, the
   `files` inventory and `environment_lock_hash` in the run's `manifest.json`
   against another run made with identical `--events`, `--repetitions` and
   `--bootstrap` values. Those three are caller settings, and `manifest.json`
   records them under `inputs` precisely because a different value changes the
   output. `seeds.network_falsification_seeds` and `settings.bootstrap_samples_used`
   differ between two runs whose settings differ, so compare only runs that match.
   `uv.lock` pins the dependency versions, and the panels are deterministic
   because rows are sorted before they are sealed, so a matching pair should agree
   on `hashes` and on `environment_lock_hash`.

6. **The registry records the run.** Confirm the run appears in
   `experiment_registry.sqlite3` with its own identifiers and that a synthetic run
   holds no locked-test reservation.

7. **The suite passes in the installed tree.**

   ```bash
   uv run pytest
   ```

   This is the `--extra dev` dependency in Step 1. The suite replaces the HTTP
   boundary only, so it needs no network and reads no acquired data.

## Step 8: Run the external-history pipeline

Steps 1-7 need only the packaged fixture and the captured releases. This step needs
the acquired archives under `data/external/`, which are local and not redistributed.

The five commands run in order, and `scripts/acceptance_external.py` drives all of
them into a fresh directory and then repeats the reproducible part:

```bash
uv run python scripts/acceptance_external.py --out .audit/acceptance --max-rows 20000
```

It writes `acceptance.json` with every command's argv, exit code, elapsed time,
stdout payload and stderr note, the content identities the commands reported, and a
per-field comparison between the first and repeated runs. Measured locally over the
ten development releases with a 20000-row extraction bound, it reports:

```text
compared_identities: inventory-external:identity
                     normalize-external:output.content_hash
                     build-trade-panel:output.content_hash
disagreements:       {}          reproduced: true          verdict: 2
```

Exit `2` is the correct outcome here and not a failure. Every command ran and the
content identities reproduced exactly, and the primary panel is blocked because no
candidate carries a rule version verified against its own rule document. Section 2
of `prediction_market_information_diffusion_final_plan.md` calls a verified blocked
cohort report a valid pipeline output rather than an empirical result, and this run
is exactly that. No estimate is produced and none is substituted.

The individual commands, if you want to inspect a stage on its own:

```bash
uv run market-propagation inventory-external --root data/external \
  --output data/derived/external/<run_id>/inventory
uv run market-propagation normalize-external --config configs/external_history_v1.yaml \
  --output data/derived/external/<run_id> --max-rows 20000
uv run market-propagation coverage-external --config configs/external_history_v1.yaml \
  --output data/derived/external/<run_id>
uv run market-propagation build-trade-panel --config configs/external_history_v1.yaml \
  --trades data/derived/external/<run_id>/historical_trades.parquet \
  --output data/derived/external/<run_id>
uv run market-propagation report-external --config configs/external_history_v1.yaml \
  --panel data/derived/external/<run_id>/trade_panel.parquet \
  --output data/derived/external/<run_id>/report
```

`inventory-external` alone reads every byte of every archive to hash it. On the
local archives that is 2,173 shards and 58,407,016,383 bytes, and it dominates the
run time. Pass `--no-hashes` for a structural inventory that is much faster and
whose identity cannot detect a changed byte; the artifact records `hash_scope`.

Three things to check in the output rather than take on trust:

- The coverage grid's `counts.overall` separates candidate, lifecycle-eligible,
  rule-verified, baseline-observed and endpoint-observed pairs, and reports an
  unmeasured class as `null` with a reason rather than as `0`.
- The panel masks rows in place. A row with `valid: false` keeps the prices it
  observed and carries its `exclusion_reason`; its `response` is null. No row
  carries a quote-only column, because this table has none.
- The report's `gate` and `blocked` always agree, a blocked report carries
  `blocked_reason`, and an unestimable quantity is null with its reason rather than
  a number carried forward from another horizon.

Interrupted runs: each command writes its own artifact under the `--output` it was
given, so a stage that failed can be rerun on its own. Sealed datasets refuse to be
overwritten with different content, so a rerun either reproduces identical bytes or
fails rather than silently replacing a frozen run.

## Step 9: Build the complete study cohort and fit it

Step 8's harness bounds its extraction at 20,000 rows, which reaches one release.
This step selects a declared candidate population before the scan and carries
every declared release into the panel.

```bash
uv run python scripts/build_study_panel.py --out .audit/study-v3
uv run python scripts/build_forecast_panel.py --out .audit/study-v3

# The same panel through the CLI, from the declared grid the first command wrote
uv run market-propagation build-trade-panel \
  --config configs/external_history_v1.yaml \
  --trades .audit/study-v3/historical_trades.parquet \
  --output .audit/acceptance/v2/panel \
  --candidate-grid .audit/acceptance/v2/candidate_grid.json

uv run market-propagation study-external \
  --panel .audit/acceptance/v2/panel/trade_panel.parquet \
  --forecast-panel .audit/study-v3/forecast_panel.parquet \
  --registry .audit/acceptance/v2/registry.sqlite3 \
  --output .audit/acceptance/v2/study
```

The first builds the declared grid, the panel and `study_panel_summary.json`; the
second reads the declared decision calendar, reads each contract's payout from the
venue's archived text, builds the exposure graph at each release origin, and writes
`forecast_panel.parquet` and `graph_decisions.json`; the third rebuilds the same
panel through the CLI from the declared grid, and the fourth fits the declared
ladder, audits the propagation rung and the rungs on the forecast panel, writes
`study_result.json` and records a durable run.

`candidate_grid.json` is written by the first command's own per-release selection;
the reproduction script for it is in `reports/empirical_study/paper.md` §9.

Measured locally, over 163,795 trades and 107 candidate contracts:

```text
panel:     3,925 rows, 785 declared pairs, 10 of 10 releases, bounded: false
           universe declared_listing_grid; 697 declared pairs never traded
rule-verified pairs: 0 of 785     valid rows: 0
graph:     155 readable predicates, 0 edges
           785 rule_vintage_unverified, 480 resolved_before_forecast_origin,
           285 receiver_not_open_at_origin
forecast:  785 rows, 107 receivers, 12 valid, 0 with a neighbour signal
ladder:    all four rungs blocked; missing delayed_shock, neighbor_lag,
           neighbor_lag_control, shock
```

The panel built by the script and the panel built through the CLI are byte-identical
(`0df1db6a97af472a3a9679551cf4b086317cbb5941a79aec98ceb9d45d2abcae`).

`study-external` exits `2` here, and that is the correct outcome: the absorption
ladder has no valid row to fit and the propagation rung is blocked on named inputs.
Calibration is **not** claimed from this archive run: it is measured on simulated
tapes, recorded in `data/calibration/calibration_certificate.json` with verdict
`inconclusive`, and `configs/study_v2.yaml` carries the rates. Four things to check
rather than take on trust:

- `propagation.missing_neighbor_columns` and `missing_news_columns` name exactly
  what is absent, and `propagation.supported` is `false`, so no substituted model
  was fitted in place of the propagation rung.
- `forecast_ladder.rungs` reports each rung's own status and the columns it lacks;
  a rung blocked on the common comparison sample says so rather than reporting a
  fit on fewer rows.
- `exploratory.label` is `exploratory_unverified_rule_semantics` and
  `exploratory.caveat` states the limit, so an exploratory number cannot be read
  as the primary estimand.
- The registry record exists at the path named by `--registry`, or `registry.reason`
  says why not, and `registry.source_hash` is computed over the actual bytes of the
  modules and configurations that produced the run. A run that could not be recorded
  says so instead of implying it was.

To confirm the sealed inputs are actually verified, copy the panel and its
`.manifest.json` to a scratch directory, flip one byte in the copy, and rerun: the
run fails with `content hash … does not match its manifest` and exit 1.

## Step 10: Collect the perpetual-futures cross-section

Steps 1-9 rebuild sealed history from archives that are already held. This step is
the opposite and the two must not be confused: it reads a public source **now**,
because the source publishes rolling windows and no per-episode history. A
cross-section not collected this hour does not exist afterwards and cannot be
back-filled.

This step needs the network and issues live requests. It is the only step here that
does.

```bash
# One sweep of the source's own liquidity ranking (40 assets by default)
uv run market-propagation perp-collect --once

# The same sweep from the script entry point
uv run --no-sync python scripts/perp_collector.py --once

# What is held, fetching nothing
uv run market-propagation perp-collect --report

# The unattended loop: one sweep per source build
uv run market-propagation perp-collect --loop
```

Five properties of the collection are deliberate, and each is checked rather than
assumed:

- **One build, one observation.** Every page carries the same `Data generated at`
  stamp. The stamp is the observation time and is stored beside the receive time,
  which only measures local latency. A page with no stamp raises instead of being
  dated by the local clock.
- **A canary gates the sweep.** The index page carries no stamp, but asset pages do
  and share one, so one asset page dates the whole build. When the build is
  unchanged the sweep costs a single request instead of one per asset, which is what
  keeps an hourly loop from becoming a crawl. `--force` overrides the skip.
- **Unobserved is not zero.** A figure the source marked unobserved is stored as a
  null with a named reason code in `refusals`; a venue that reported no volume is
  stored as a genuine zero with no code. The two are distinguishable after the fact,
  which is the point.
- **The interval is derived, and a disagreement refuses.** The source prints a
  funding interval column but no interval value. The interval is derived twice,
  from the annualised rate and from the settled count over the funding page's
  30-day window, and `resolve_interval` returns a refusal rather than choosing when
  the two disagree, because the interval sets the per-hour normalisation every
  differential depends on.
- **A quoted spread is not an arbitrage.** The source's execution-cost surface is a
  client-rendered shell with no data, so the cost layer is unobservable. Every
  differential carries
  `execution_cost_not_observable_from_this_source` and no stored record is an
  executable opportunity.

The cross-section is stored once per build and the derived tables are not stored at
all: differentials and basis are pure functions of one snapshot, computed on demand
with `market_propagation.perp.differentials`, so a stored copy cannot drift from the
quotes it came from. Raw response bytes are archived before parsing, so a parser
change re-parses held pages instead of re-fetching them.

To confirm the skip works, run `--once` twice inside one source build: the second
run reports `skipped: true` with `source_build_unchanged` and fetches only the canary.

## Step 11: Grade the cross-venue candidate universe

Step 9 fits the declared ladder within one venue. This step asks the other question:
which contracts on two venues state the *same claim*, so that a follower-repricing
analysis has pairs to work with. It reads the held archives and issues no request.

```bash
# Grade every pair and write the full registry
uv run market-propagation match-cross-venue --output data/match_registry.json

# A bounded candidate universe, and the run says so
uv run market-propagation match-cross-venue --second-venue-limit 25
```

The command's output is a funnel, and each stage is a measured fact rather than a
gap in the search:

- **The candidate universe is declared.** The first venue's candidates are the
  contracts whose series `configs/cohort_v2.yaml` declares, matched through
  `series_of` rather than by substring, because a `LIKE 'FED-%'` test drops the
  sibling `FEDDECISION` series. The second venue's are the records whose own
  identity column matches the declared pattern in `configs/matching_v1.yaml`.
- **The filter is a selection rule and never evidence.** It decides the denominator
  and nothing else. No similarity score is computed anywhere, no grade or refusal is
  expressed in terms of the pattern, and a record the filter excludes is outside the
  candidate universe rather than refused for resembling anything. The result carries
  the pattern, the totals available and the cap actually applied, so a count from a
  bounded universe cannot be read as a count from the whole layer.
- **A component the record does not publish is passed as absent.** The venue's own
  market record publishes no reference period, and the rule records that would carry
  settlement semantics hold no per-contract entry on this checkout, so both are
  recorded as unobserved and refuse the pair. Deriving a reference period from a
  contract's own ticker would invent the component.
- **A refused pair keeps its place.** The registry is the universe that was searched,
  so it grows with the candidate set rather than with the match count. On the current
  checkout that is why the file is large: every pair is refused and every refusal is
  recorded.

The measured funnel on the current checkout is 689 first-venue candidates, of which
155 yield a readable predicate and 534 are refused (533 because their month is not on
the declared decision calendar, 1 because the archived text states no readable
payout); 229 second-venue candidates, of which **0 are readable** because that venue's
record carries no settlement-rule text and the configuration declares no parser for
it. Every cross-venue pair is therefore `REJECT`, and the run exits `2`.

That is a state of the archive rather than a defect in the search, and it is the
reason an empty match set here must not be read as "no matching contract exists".
The command becomes informative the moment a parser is declared for a vocabulary the
second venue's record actually states — never by reading its slug, which would be the
title similarity this layer exists to refuse.

A pair graded here is identical on the parsed predicate and nothing more. Whether it
is a *verified* identical claim is answered by the exposure graph's rule-vintage
requirement, which is a separate gate; the registry records that distinction rather
than folding the two together.

## Exit codes

Every command prints one JSON document to stdout. Diagnostic notes go to stderr.

| Code | Meaning |
| --- | --- |
| `0` | The command ran and its own result is complete for what it claims. |
| `1` | Invalid arguments, or a technical failure: a missing file, an unreadable configuration, a refused request shape. |
| `2` | The command ran, and its result reports blocked or absent evidence. |

Exit `2` is a successful computation of a blocked or negative finding, not a crash.
Argument errors exit `1`, so `2` means exactly one thing to a scheduler.

## What a passing build does not establish

A complete synthetic reproduction shows that the software and methods run end to
end from the packaged fixture. It is not evidence about any real release, any real
venue, or any real contract.

A passing test suite validates the properties it tests. It establishes neither
economic identification nor profitability, and no empirical claim follows from it.
Real-cohort claims stay blocked until a real eligible cohort, panel and outcome are
consumed, and the reproduction reports that blocking separately in its gates.
