# market-propagation

Point-in-time measurement of information propagation across prediction markets.

The [final pipeline plan](prediction_market_information_diffusion_final_plan.md)
consolidates both research plans against the acquired historical data and prioritizes
a reusable pipeline. It describes proposed external-history work; the commands below
document the current implementation.

The [empirical study execution plan](prediction_market_information_diffusion_study_execution_plan.md)
extends that pipeline into a news-conditioned, neighbour-based study, with explicit
source, graph, falsification, held-out evaluation, and reporting gates. It is an
execution plan, not a completed empirical result or an external preregistration.

The study asks which parts of a prediction-market response to a public information
release reflect direct updating, transmission between contracts, and mechanical
delay in observation or quoting. US CPI and Employment Situation releases are the
initial domain. Hypotheses, thresholds, cohorts, gates and interpretation limits are
frozen in `configs/study_v1.yaml`, with `configs/cohort.yaml` and
`configs/event_windows.yaml` supplying the event cohort and the window rules.

Market access is read-only. No command places an order, holds a funded account,
authenticates to a venue, or spends money. Paid trading and any execution or
commercial-feasibility work are out of scope and are not implemented here. The
frozen specification caps paid spending at zero, and the public clients issue
documented GET requests only.

Read-only describes the market access, not the whole repository. Every command
except `registry-review` writes files under the output path you pass it, and a
successful `audit` or `capture` archives the payloads it read under that path.
Nothing writes anywhere else, and no command writes to a venue.

## Install

```bash
uv sync --extra dev
```

Run commands through `uv run`, from the repository root:

```bash
uv run market-propagation --help
uv run python -m market_propagation --help
```

Both entry points run the same code and return the same exit codes.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | The command ran and its own result is complete for what it claims. |
| `1` | Invalid arguments, or a technical failure: a missing file, an unreadable configuration, a refused request shape. |
| `2` | The command ran, and its result reports blocked or absent evidence. |

Exit `2` is a successful computation of a blocked or negative finding, not a crash.
A negative synthetic finding is a valid result and exits `0` when the reproduction
itself completed. Argument errors exit `1`, not argparse's customary `2`, so `2`
means exactly one thing to a scheduler.

Every command prints one JSON document to stdout. Diagnostic notes go to stderr.

## Commands

### `reproduce`

Rebuilds the offline methods reproduction from the packaged synthetic fixture.

```bash
uv run market-propagation reproduce --output data/synthetic/final
uv run market-propagation reproduce --output data/synthetic/final \
  --spec configs/study_v1.yaml --events 120 --repetitions 40 --bootstrap 200
```

Options: `--output` (required), `--spec`, `--events`, `--repetitions`,
`--bootstrap`, `--real-audit`, `--release-dataset`. Each omitted option keeps the
library's own default, so the numbers are declared in one place rather than repeated
in the CLI.

Default reproduction cites no real input. The synthetic rebuild and the real
evidence are separate, and neither is selected for you.

```bash
uv run market-propagation reproduce --output data/synthetic/final \
  --real-audit data/public/final-audit \
  --release-dataset data/public/bls-normalized/releases.parquet
```

`--real-audit` takes a real acquisition audit directory to cite. `--release-dataset`
takes a sealed archived-release dataset to cite. Omitting either cites no input of
that kind, and nothing is discovered from the checkout on your behalf.

A named input must exist and verify. An audit is cited at the status it recorded,
and a named release dataset has every selected payload re-read from its own raw store
and reparsed, so a record that disagrees with the bytes it cites is reported as
unverified and counts for nothing.

Neither input is fitted. The dataset is a reference: it does not enter the synthetic
sample or any model, and `manifest.json` records
`inputs.real_inputs_used_as_fitted_inputs: false` together with the path each input
came from.

Output under `--output`: `metrics.json`, `manifest.json`, `source_panel.parquet`,
`usable_panel.parquet`, `forecast_panel.parquet`, one `.manifest.json` sidecar per
panel, `replay_disagreements.json`, `experiment_registry.jsonl`,
`experiment_registry.sqlite3`, `baseline_report.md`,
`conditional_propagation_report.md`, `data_card.md`, `paper.md`, `figures/`, and
`raw/`, which holds the archived fixture records the panels were built from.
`experiment_registry.sqlite3` is the registry file itself and
`experiment_registry.jsonl` is its export; both are written into the output
directory, so a registry lives with the run that produced it rather than in
`reports/`.

**The reproduction is synthetic.** Every panel is generated by the simulator from
the packaged fixture at `src/market_propagation/fixtures/replay.json`, which ships
inside the wheel and is located through `importlib.resources`, so an installed
package finds it without a source tree. A complete reproduction shows that the
software and methods run end to end; it is not evidence about any real release.
Real-cohort claims stay blocked until real data are acquired, and the reproduction
reports that blocking separately in its gates.

The default `--spec` is `configs/study_v1.yaml`, resolved relative to the working
directory. Run the config-dependent commands from the repository root, or pass
`--spec` an explicit path.

### `audit`

Runs the bounded read-only G0 coverage audit against the public venue and the
archived release payloads.

```bash
uv run market-propagation audit --output data/public/final-audit \
  --cohort configs/cohort.yaml --windows configs/event_windows.yaml \
  --timeout 8 --max-pages 2 --max-contracts 40 --max-candle-contracts 10 \
  --release-dataset data/public/bls-normalized/releases.parquet
```

Options: `--output` (required), `--cohort`, `--windows`, `--timeout`,
`--max-pages`, `--max-contracts`, `--max-candle-contracts`, `--release-dataset`,
`--archive-raw-root`. Every axis is capped, and the caps actually applied are
returned in the result.

`--release-dataset` names a sealed archived-release dataset to read first releases
from instead of fetching them, and `--archive-raw-root` overrides the raw store
holding that dataset's original payloads. The default raw root is the `raw` directory
beside the dataset. The dataset is not discovered for you: with no
`--release-dataset` the network path is unchanged.

The dataset and every selected payload are verified before they count. A record that
is absent or disagrees with the bytes it cites blocks that event rather than falling
back to the network.

Output under `--output`: `raw/` (content-addressed payloads and one receipt per
occurrence), `coverage.json`, `raw_hashes.json`, `series_discovery.json`,
`release_source.json`, and `event_card.json`.

`release_source.json` names which path supplied the first releases and whether the
network was asked at all. It is written by every audit, so an audit that read an
archived dataset is distinguishable from one that did not.

A successful HTTP response is never study eligibility. Eligibility is established
only by a verified market identity in the cohort configuration, and an audit with
unsatisfied coverage gates exits `2` while still writing its artifacts.

### `capture`

Polls one documented public snapshot surface for one contract.

```bash
uv run market-propagation capture --output data/public/capture \
  --venue kalshi --contract <PUBLIC_MARKET_ID> --duration 10 --interval 2 --timeout 8
```

Options: `--output` (required), `--venue` (`kalshi` or `polymarket`, default
`kalshi`), `--contract` (required), `--duration`, `--interval`, `--timeout`.

The contract is required because the caller chooses which legitimate public market
to read; no ticker is guessed. An unknown identifier is recorded as an access
failure, not as an empty capture, and the command exits `2`.

Output under `--output`: `raw/`, `capture.json`, and
`normalized/<run_id>/book_events.parquet` plus `quotes.parquet` when at least one
snapshot was archived.

Polling observes independent snapshots. Messages between two polls are not
observed, so `tick_complete` stays `false` in the capture summary.

### `quality`

Reports what one raw store actually holds and what it can support.

```bash
uv run market-propagation quality data/public/final-audit/raw \
  --output reports/generated/quality.json
```

Re-reads every receipt and verifies each referenced payload against its content
hash. It parses no payload content and claims no source clock. A failed
verification or a store with no receipt exits `2`.

### `registry-review`

Prints the durable state of an experiment registry: runs, locked-test
reservations, and event claims.

An absent registry is an error; this command never creates one to inspect an empty
result. The locked test is consumed once per locally frozen specification version,
and the consumed event identifiers are recorded here so a later version cannot
reuse the same cohort.

`reproduce` is the only command that writes a registry. It writes
`experiment_registry.sqlite3` into its own output directory, so the path to review
is the one under that directory:

```bash
uv run market-propagation registry-review \
  data/synthetic/final/experiment_registry.sqlite3
```

The extension is `.sqlite3`, and `reports/` holds no registry. Pass `:memory:` to
read an empty in-process registry.

### `event-card`

Assembles one event card from an audit directory's own artifacts and re-verifies
every payload hash the chosen event cites. It issues no request.

```bash
uv run market-propagation event-card data/public/final-audit \
  --output reports/event_card.json --event-id cpi_2025_01
```

Options: `audit_dir` (required), `--output` (required), `--event-id` (default: the
audit's own prior card, else the first event).

The card's `status` reports whether the artifact was assembled, separately from the
audit's own standing. A card built from readable artifacts exits `0` even when the
audit is partial, and its `audit_complete`, `unsatisfied_gates` and
`claims.blocked_by_unsatisfied_gates` still say that empirical use stays blocked.
Exit `2` covers a requested event the audit holds no record of, and a card whose
cited payloads cannot be re-read against their content hashes; the card is still
written, and the unverifiable citations are what fail. A missing or unreadable audit
directory exits `1`.

## External-history pipeline

The commands below read the acquired historical archives under `data/external/`
and turn them into verified contract records, a release-linked coverage grid and
an explicitly masked transaction-price panel. They are read-only with respect to
the venues and with respect to the archives: nothing under `data/external/` is
written, moved or deleted, and no command reaches the network.

`configs/external_history_v1.yaml` names every input, the extraction bounds, the
clock mode, the age caps, the horizons and the masking rules. It is an operational
and measurement specification, not an empirical preregistration.

The five commands are meant to run in that order, and each writes its own artifact
under the `--output` directory it is given.

### `inventory-external`

```bash
uv run market-propagation inventory-external --root data/external \
  --output data/derived/external/<run_id>/inventory
```

Records, for every shard of every configured layer, its relative path, byte size,
row count, row-group count, schema fingerprint, timestamp-statistic coverage and
SHA-256, together with its producer and licence. It writes `inventory.json` and
reports an `identity` that is a digest over those shard facts, so repeating the run
over unchanged bytes yields the same `identity`.

`--no-hashes` skips the digest pass, which is much faster and makes the identity
unable to detect a changed byte; the artifact says `hash_scope: none` when it is
used. A corrupt shard is recorded as unreadable with its error text rather than
raising, an absent layer is reported missing, and a filename appearing in two
layers is flagged. Exit `2` when any shard is unreadable or any layer is missing.

Measured on the local archives: 7 layers, 2,173 shards, 2,958,370,301 rows,
58,407,016,383 bytes.

### `normalize-external`

```bash
uv run market-propagation normalize-external --config configs/external_history_v1.yaml \
  --output data/derived/external/<run_id> --max-rows 200000
```

Reads one configured layer inside the configured window through DuckDB projection
and row-group pushdown and seals the rows as a `historical_trades` dataset. Root,
layer and window come from the configuration unless `--layer`, `--window-start` or
`--window-end` narrow them; an unconfigured `--layer` is refused at exit `1`.

Exact venue price units are preserved: Kalshi keeps integer cents in `raw_price`
with `price = yes_price / 100`, and the NO cents are retained in `secondary_price`
for consistency checks. Polymarket keeps its float64 price and the documented
event-axis projection. A quantity the archive does not carry stays null and is
excluded from weighted flow; it is never replaced with zero. Boundary prices are
reported as flags rather than clipped: the local Kalshi archive contains 37,918
zero-cent rows and 358,480 rows where `yes_price + no_price != 100` in a single
shard, despite the dataset README documenting a 1-99 range.

Exit `2` when the read produced no trade, or when `--max-rows` stopped it early
(`bounded: true`), because a bounded read is not complete for the window it claims.

### `coverage-external`

```bash
uv run market-propagation coverage-external --config configs/external_history_v1.yaml \
  --output data/derived/external/<run_id>
```

Builds the event and contract coverage grid for the ten development releases from
the sealed release dataset, the bounded audit's candidate artifact and the rule
evidence. Missing cells stay in the grid, and zero-activity and rule-blocked
candidates are counted rather than dropped.

Series are matched on exact identity, never on a substring. This is not a
formality: the archive's market identifiers embed hexadecimal, so a substring
match on `FED` returns mostly sports markets such as
`KXMVENFLSINGLEGAME-S2025FED4B0DA5B1`. Exact identity is the leading capital run
of the ticker.

An unmeasured pair-level window count is `null` with a reason, never `0`, because
"no count was joined for this pair" and "this pair traded nothing" are different
facts. `gate_g0` is `blocked` unless the rule, cohort and supported-frequency gates
all pass, and every blocked gate names its reason. The local rule registry
publishes no eligible market ids, so rules stay unverified and G0 stays blocked;
that is the correct reading of this input, not a fault.

### `build-trade-panel`

```bash
uv run market-propagation build-trade-panel --config configs/external_history_v1.yaml \
  --trades data/derived/external/<run_id>/historical_trades.parquet \
  --output data/derived/external/<run_id>
```

Builds the masked source-time transaction panel. `s_minus` is the latest
transaction strictly before the release and `s_plus(h)` the latest at or before
`tau_e + h`. A row requires a new post-release transaction: with no post-release
print the row is masked `no_post_release_trade` and never carries a zero from a
carried-forward baseline. Two distinct valid transactions at the same price may
produce a genuine observed zero.

Masking applies to the estimand rather than to the observations. An invalid row
keeps the prices it genuinely observed, with `valid: false` and an
`exclusion_reason`; its `response` is null. A closed direct contract yields a null
response with `contract_closed_before_release`, never `0`. Prints sharing the
finest supported timestamp become one declared tie-group observation at the
unweighted mean of event-axis prices, with the group size and envelope retained.
No quote-only column exists in this table: a trade tape has no bid, ask, spread or
depth, and a column that could only hold null invites a later reader to cast a
trade price into it.

`--clock-mode` is one of `source`, `usable` or `assumed_delay`. `usable` reports
`unidentifiable` and masks the rows, because these archive rows carry no receipt
evidence and no usable interval may be invented. `--horizon` narrows the panel to
one horizon the configuration already carries and makes it the primary, and
records that it did so.

Exit `2` when no panel row is valid.

### `report-external`

```bash
uv run market-propagation report-external --config configs/external_history_v1.yaml \
  --panel data/derived/external/<run_id>/trade_panel.parquet \
  --output data/derived/external/<run_id>/report
```

Builds the coverage report, event cards, response figures, baseline summary,
lineage and capability table from an existing panel. Every reported value links to
its inputs and to the specification hash, and the panel is read through the sealed
reader, so its content hash and schema version are verified first.

A blocked gate is reported as blocked rather than estimated around. `gate` and
`blocked` are derived from one set of checks, so they cannot disagree, and a
blocked report carries `blocked_reason` plus structured `gate_detail.blocked_by`
and `gate_detail.blocked_reasons`. An unestimable quantity is null with its
reason: when no governed row carries a response at the primary horizon, `point`,
`interval` and `bootstrap` stay null with a status of `unavailable` and no
interval is substituted from another horizon.

Exit `2` when the gate is blocked. The artifacts are still written.

### Acceptance harness

```bash
uv run python scripts/acceptance_external.py --out .audit/acceptance
```

Drives all five commands over a fresh output directory, then repeats the inventory,
the bounded extraction and the panel and compares the content identities the
commands themselves reported. Exit `0` when the run reproduced and nothing was
blocked, `1` on a failure or a disagreement between runs, `2` when the pipeline ran
and reproduced but reports blocked evidence. Its full record lands in
`acceptance.json`.

The measured result on the local archives is exit `2`: every command ran, the
inventory `identity` and the `historical_trades` and `trade_panel` content hashes
reproduced exactly, and the primary panel is blocked because no candidate carries a
verified rule version. No empirical estimate is claimed by that run.

## Empirical study execution

The external-history pipeline above measures coverage. The modules below fit it
and record what the evidence can and cannot carry. Full results, including the
analyses that could not run, are in `reports/empirical_study/paper.md`; the
running checkpoint is `reports/study_execution_status.md`.

### `market_propagation.study`

`run_study(panel_path, output_dir=...)` reads a sealed `trade_panel`, builds the
declared timing-only design, fits the ladder with whole-release chronological
splits and release-clustered uncertainty, and writes `study_result.json` plus a
durable run in `empirical_study.sqlite3`.

```bash
uv run --no-sync market-propagation study-external \
  --panel .audit/study-v3/trade_panel.parquet \
  --forecast-panel .audit/study-v3/forecast_panel.parquet \
  --registry .audit/acceptance/v2/registry.sqlite3 \
  --output .audit/study-v3/cli-fit
```

Exit `0` when the absorption ladder fitted and the propagation rung is
estimable, `2` when either is blocked. The command reports the propagation rung
as **blocked** with the missing columns named rather than fitting a substitute: a
same-contract panel carries no lagged neighbour return, and with no validated
surprise a shared shock is indistinguishable from transmission. `--registry`
defaults to the shared `data/registry/empirical_study.sqlite3`, so two runs of one
study record into one ledger instead of writing a store each.

The exploratory absorption branch is separate and explicitly labelled. It is the
plan's exploratory candidate panel: a row qualifies only when both legs were
observed and every recorded reason for excluding it is a rule reason, so a row
missing its endpoint stays missing rather than being repaired.

### `market_propagation.neighbors`

`build_neighbor_graph(contracts, at=..., calendar=..., window_end=...)` joins an
earlier decision date to the immediately following one for an identical payoff
predicate, and records a `GraphDecision` for every receiver that has none. The
calendar is required and is declared independently of the contracts, so a missing
predecessor cannot be reported as no predecessor; a predecessor date the graph
holds no contract for is `calendar_date_holds_no_contract`. An edge also needs both
contracts listed from the origin through the window's end and a rule version
verified in force across that whole interval. An empty graph is a valid result, not
an error.

### Cohort-targeted panel build

```bash
uv run --no-sync python scripts/build_study_panel.py --out .audit/study-v3
uv run --no-sync python scripts/build_forecast_panel.py --out .audit/study-v3
```

The first selects the declared candidate population per release by listing interval
before the scan, passes it to the panel builder as the denominator, and carries
every declared release into the panel with no row cap. The second reads the
declared decision calendar, reads each contract's payout from the venue's own
archived text, builds the exposure graph at each release origin, and writes the
source-time forecast panel plus `graph_decisions.json` — every receiver's donor or
the reason there is none.

### Measured outcome on the local archives

| Quantity | Value |
| --- | --- |
| Declared policy series | 4 (`FED`, `FEDDECISION`, `KXFED`, `KXFEDDECISION`), 689 contracts |
| Complete study panel | 3,925 rows, **785 declared pairs**, 10 of 10 releases |
| Declared pairs that never traded | 697, kept in the denominator |
| Extraction | 163,795 trades, `bounded: false` |
| Rule-verified pairs | 0 of 785, so every row is masked |
| Graph over real contracts | 155 readable predicates, 0 edges, 785 `rule_vintage_unverified` |
| Forecast panel | 785 rows, 107 receivers, 12 valid, 0 with a neighbour signal |
| Declared ladder | all four rungs blocked, missing `shock`, `delayed_shock`, `neighbor_lag`, `neighbor_lag_control` |
| Calibration | **not run**: 200-repetition transaction-tape calibration outstanding |

The terminal outcome is **blocked**: no candidate contract carries an attested
rule-vintage interval, and without one the graph admits no edge and the panel has no
valid row. The gap has a measured size: with the rule requirement satisfied by a
diagnostic sentinel, the same declared calendar, predicates, liveness and window
rules admit **623 edges** across the ten releases, and all 623 are withheld by the
rule-vintage requirement alone. No propagation claim is made and no absorption claim
is promoted. The decision rule's false-positive rate and power are unmeasured,
because the calibration that would measure them has not run.

## Reading the data

**Source time and receipt time are different things, and neither is usable time.**

| Concept | Meaning |
| --- | --- |
| Source time | When the venue or publisher says the record occurred. Historical economic panels admit it, and it retains explicit clock-quality and uncertainty columns. |
| Receipt time | When this process read the response. Capture keeps it exactly as observed. |
| Usable time | `availability.upper`. Forecast features admit only this. |

A historical record with no receipt timestamp cannot support a latency claim. It is
given an availability interval or a documented coarse-time reading instead of
invented precision. The public snapshot surfaces publish no verified source clock,
and the capture summary says so rather than scoring one it never observed.

Two replay orders are computed and published separately: source-time order for the
economic panel, and usable-time order for the forecast panel. Disagreements between
them are reported, not resolved by picking the more attractive order.

A quote is valid, awaiting a snapshot, in a gap, disconnected, halted, closed,
crossed, or missing. Midpoint and spread are defined only for a valid two-sided
book. Staleness is read from gap detection and documented refresh behavior, with
`last_verified` as the freshness field.

**Synthetic and real data are classified separately.** The reproduction's panels
are generated. The `synthetic` flag is recorded per run in the registry, and a real
capture writes to a different output directory than a synthetic reproduction. A
synthetic software experiment can satisfy a gate whose required evidence is
generated data; it can never satisfy a gate whose required evidence is real data or
a real empirical effect estimate.

Three kinds of input reach this repository, and the difference decides what any
result can claim.

| Input | Where it lives | What produced it | What it can establish |
| --- | --- | --- | --- |
| Packaged synthetic fixture | `src/market_propagation/fixtures/replay.json`, inside the wheel | Written for the repository, opened in 2031 | That the software and methods run end to end. Nothing about a real release. |
| Acquired public market data | `data/public/`, which is git-ignored and never packaged | `audit` and `capture` reading documented public endpoints | What those endpoints returned at the recorded moments. Not eligibility, and not a market response. |
| Sealed archived release dataset | `data/public/bls-normalized/`, git-ignored and never packaged | `scripts/import_bls_archives.py` normalizing browser-captured BLS payloads offline | The published first-release values, verified against the bytes they were parsed from. Not a market rule version, and not quote coverage. |

The audit and the reproduction differ in how they use the release dataset. An `audit`
run with `--release-dataset` reads first releases from it in place of a fetch, so the
values become the release basis reported in that audit's event card. A `reproduce`
run with `--release-dataset` cites it as reference only: the dataset is verified and
recorded in the manifest, and it never enters the synthetic sample or any model.
Either way `data/public/bls-normalized/` is the same dataset and neither path writes
to it.

Real inputs are cited only when you name them. `reproduce` selects no real audit and
no release dataset on your behalf, so a synthetic-only run stays synthetic-only even
in a checkout that holds both.

The test suite uses neither real input. It drives the real implementations with an
isolated fixture at the HTTP boundary, so a test run reads no acquired data and
reaches no endpoint. Commands that would read a public endpoint, `audit` and
`capture`, only run when a caller passes them explicitly. Nothing in this repository
runs them on a schedule by default, and no test does it at all.

`reports/reproduction_guide.md` documents the install, the synthetic rebuild, the
hash and quality checks, the offline import of the captured BLS releases, and what
a clean non-editable rebuild verifies.

Three committed reports record what the real artifacts hold and what they cannot
support, and they are distinct from the generated reports a run writes into its own
output directory.

| report | what it holds |
| --- | --- |
| `reports/data_audit.md` | The ten-release cohort, the two real audits, their provenance, coverage, gates and missing rule evidence |
| `reports/data_card.md` | The inputs, their provenance chain, and the limits each carries |
| `reports/paper.md` | The methods draft, the synthetic outcomes with explicit labels, and the blocked empirical prerequisite |

**A negative result is a valid outcome.** If apparent propagation disappears once
observation clocks are aligned, that is the finding. The reproduction reports the
mechanism audit's own status, including an inconclusive one, without dressing an
inconclusive result as a positive one.

## Scheduled reports

No scheduler, cron entry, or service is installed by this repository, and no job
is deployed. `configs/scheduled-reports.cron` is a configuration artifact that is
ready to adapt: it is configured for this checkout, with the project root and the
`uv` binary at their absolute paths and the raw store set to
`data/public/final-audit/raw`. Change those path variables to reuse it elsewhere.
Read the setup steps at the top of that file before you add its entries.

Two report entries are active and the capture template at the bottom is disabled.
Merge the active entries into your own crontab rather than replacing it, so
unrelated jobs survive. Both jobs write under `logs/` and `reports/generated/`,
which the file creates. That directory holds generated output only. The committed
report sources stay in `reports/`. The quality job reports on one raw store, and
the registry-review job reads a registry that a run already wrote, because
`registry-review` never creates one.

Because exit `2` marks blocked or absent evidence, a scheduler can alert on it
directly. Capture the status first, then test it:

```bash
uv run market-propagation quality data/public/final-audit/raw \
  --output reports/generated/quality-$(date +%F).json
status=$?
if [ "$status" -eq 2 ]; then
  echo "quality blocked: inspect the artifacts"
fi
```

Do not write this as `command || [ $? -eq 2 ] && echo ...`. `||` and `&&` associate
left to right, so that form reports a blocked result on a successful run as well.

A scheduled capture across a release window needs its own job, configured
separately, because the contract identifier comes from whoever runs it. See the
capture template in `configs/scheduled-reports.cron`.

Release windows are scheduled in the source calendar's timezone, which is
`America/New_York` for BLS releases. A cron entry fires on the host's clock, so a
fixed `25 8 * * *` entry does not follow the DST transition and does not guarantee
a start 08:30 Eastern. Direct release-window scheduling must use a timezone-aware
calendar, or you must convert the release instant to the host clock yourself. See
the capture template for what each approach requires.

## Tests

```bash
uv run pytest
```

The suite exercises the real implementations: the real `RawStore`, the real book
replay through `BookState` and `replay`, the real normalizers, and the real public
client classes. It replaces one boundary only, `httpx.Client.get`, with a fixture
that answers the documented URLs, so every assertion is made against bytes that
were actually archived and read back. Each test builds its store under a pytest
temporary directory, and no test reads `data/` or writes into the working tree.

The `network` marker is declared in `pyproject.toml` for tests that would reach a
public endpoint, and no test currently carries it, so `-m "not network"` deselects
nothing today. A passing software suite validates the tested properties. It
establishes neither economic identification nor profitability.
