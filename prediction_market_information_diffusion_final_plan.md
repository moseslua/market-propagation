# Prediction-Market Information Diffusion: Final Pipeline Plan

**Version:** 3.0, consolidated 15 September 2026.  
**Priority:** reusable data and analysis pipeline first; research paper later.  
**Planning authority:** this document reconciles [v1](prediction_market_information_diffusion_plan.md) and [v2](prediction_market_information_diffusion_plan_v2.md) against the local data and implementation. The originals remain historical records.  
**Implementation status:** the existing quote/synthetic pipeline works on its tested paths. The external-history pipeline described below is proposed, not implemented. This document does not register an empirical study or claim completed economic results.

## 1. Decision and Definition of Done

Build a local, reproducible pipeline that turns the acquired historical archives into verified contract records, release-linked coverage reports, explicitly masked transaction-price panels, and auditable baseline analyses. Begin with Kalshi and the ten already archived CPI/Employment Situation releases. Extend the release history and add Polymarket through the same interfaces after the first path works.

Keep v1's contract semantics, provenance, leakage controls, event-level inference, and model promotion rules. Adopt v2's historical-trade direction, with the corrections in Section 3. The default historical observable is a transaction price on a documented outcome axis. It is not a quote midpoint, an executable price at the evaluation instant, or a measured latent belief.

The first deliverable is a user-facing workflow:

```text
external archives -> inventory and integrity -> normalized trades and contracts
                  -> release/rule validation -> event coverage and exclusions
                  -> transaction panel -> diagnostic report and baseline analysis
```

Pipeline completion means a fresh output directory can reproduce this path from named local inputs, every reported number traces to its source, and unsupported analyses return a structured reason. A successful economic hypothesis, network model, new data purchase, or paper submission is not required for pipeline completion. A verified blocked-cohort report is a valid pipeline output; it is not an empirical event-study result.

The project remains CPU-first, local, read-only with respect to venues, and within the existing zero-paid-spend scope. Reuse installed dependencies, clocks, provenance, and compatible domain types. Do not start another full-venue download or build a trading system.

## 2. Data We Actually Have

### 2.1 External archives

Counts below were read from every local Parquet footer on 15 September 2026. Byte counts are logical Parquet file sizes, not allocated disk usage. They verify the local inventory, not upstream completeness, unique economic trades, or study eligibility.

| Local input | Files | Rows | Verified time extent / role |
| --- | ---: | ---: | --- |
| `data/external/kalshi-trades/trades-*.parquet` | 16 | 154,505,005 | Source trades, 2021-06-30 20:09:14.185137 UTC through 2026-01-29 05:27:17.012139 UTC |
| `data/external/kalshi-trades/markets-*.parquet` | 4 | 17,464,713 | Market metadata snapshot; these are rows, not a newly verified distinct-market count |
| `data/external/polymarket-v1/OrderFilled/*.parquet` | 42 | 1,201,580,990 | Raw nominal fills, 2022-11-21 through 2026-04-28 |
| `data/external/polymarket-v1/daily_aligned/*.parquet` | 1,248 | 601,934,424 | Cleaned Standard Binary fills, same calendar extent |
| `data/external/polymarket-v1/daily_aligned_multi/*.parquet` | 857 | 144,175,988 | Cleaned Neg Risk fills, 2023-12-22 through 2026-04-28 |
| `data/external/polymarket-v1/CTF/*.parquet` | 5 | 838,688,922 | Lifecycle records: preparation, split, merge, resolution, redemption; not additional trades |
| `data/external/forecast-snapshots-kalshi_events-768472771c/snapshot_dataset.parquet` | 1 | 20,259 | 738 distinct markets and 102 snapshot dates, 2025-01-01 through 2025-10-31 |

The external Parquet files total **58,407,016,383 bytes, approximately 58.41 decimal GB**: Kalshi 5.69 GB, Polymarket 52.72 GB, forecasts 0.49 MB. Do not add raw and cleaned Polymarket rows as independent observations. The CSV beside the forecast Parquet is another representation of the dataset, not another cohort.

Source documentation:

- [Kalshi dataset README](data/external/kalshi-trades/README.md), lines 19-110: TrevorJS, incorporating Andrew Becker's collection and Kalshi API data; CC-BY-4.0. Zero duplicates, no null prices, and complete trade/market joins are uploader assertions pending our validation.
- [Polymarket README](data/external/polymarket-v1/README.md), lines 34-155: Boka Qin and Rui Yang, *Polymarket-v1 Database*; CC-BY-4.0. Cleaned layers remove relayer/router records according to the producer. None of these layers contains historical books, quotes, cancellations, or resting depth.
- [Forecast README](data/external/forecast-snapshots-kalshi_events-768472771c/README.md) and [configuration](data/external/forecast-snapshots-kalshi_events-768472771c/config.json): publisher namespace `chestnutforty`, source `kalshi_events`; MIT. The configuration samples every three days. Future community predictions and eventual resolutions are labels. All 20,259 `model_pred_now` values are null. These snapshots are not a macro consensus series.

The Polymarket cleaned schemas do not carry the raw `OrderFilled.id`; the Standard Binary schema also omits `token_amount`. The loader must inspect each actual schema rather than apply v2's raw-fill mapping to every layer. Preserve an occurrence identifier using shard hash and row position where no source ID exists. Preserve possible duplicates for audit instead of deduplicating identical rows as though they could not be separate fills.

### 2.2 Existing public and synthetic inputs

| Input | What is verified locally | What it does not establish |
| --- | --- | --- |
| [BLS normalized releases](data/public/bls-normalized/releases.parquet.manifest.json) | Ten rows: five CPI and five Employment Situation releases, January-May 2025; file SHA-256 matches the manifest | Additional historical release coverage or contemporaneous receipt availability; all ten `usable_time` values are null |
| [Primary audit](data/public/final-audit/verification.json) and [coverage](data/public/final-audit/coverage.json) | 52,895 acquired candidate observations; 400 deep-audited lifecycle-eligible market/event candidates across 64 tickers; **zero study-eligible candidates** | Verified historical rule versions, verified settlement semantics, or an unbounded representative candidate universe |
| [Verified snapshot capture](data/public/capture-verified/capture.json) | Six Kalshi snapshots for `KXCPI-26SEP-T0.4`, with quotes and receipt times; another separate capture also has six snapshots | Historical macro quote coverage, a synchronized source clock, or a complete tick feed; `tick_complete=false` |
| `data/synthetic/final/` and `data/synthetic/reproduction/` | Reproducible generated panels, models, simulator results, and registries | Real-market estimates or real-data power |

The live registry reads found three synthetic runs in `final` and one in `reproduction`, with no reservations or event claims in either registry. This does **not** prove that every historical event is untouched: v2 already discusses selected real activity around May 2025. Treat those events, and the ten existing audit events, as development material. A registry records declared evaluations; it cannot detect unrecorded exploratory inspection.

The existing [data card](reports/data_card.md) and [audit report](reports/data_audit.md) describe the earlier bounded acquisition. They do not yet document the external expansion. Their zero-eligible result remains valid for that audited cohort; it is neither a verdict on every external contract nor a gate cleared by downloading more files.

### 2.3 What remains unknown

No full external uniqueness/join audit, hash inventory, eligible macro event panel, or external-history model fit was executed for this consolidation. v2's CPI/policy counts, release-minute spike, and keyword totals are exploratory leads; its `/tmp/probe_density.py` reference is not a durable reproduction path. Recompute them with checked-in pipeline commands before using them as evidence. Archive size and whole-venue volume do not establish usable macro activity.

Only ten official release payloads are currently normalized. The five-year trade span is not five years of verified release packages. Build an actual release calendar and coverage matrix; do not reuse v2's approximate event count. Publication delays, missing releases, revisions, DST, and simultaneous announcements require event-specific evidence.

## 3. Resolutions of Conflicts Between the Plans

| Topic | Final decision |
| --- | --- |
| Primary output | Reusable pipeline and its documentation first. Paper and advanced modeling are later consumers. |
| Historical outcome | Freshness-qualified trade-price response; midpoint, spread, depth, and bid-ask coherence remain separate quote-only capabilities. |
| Timestamp precision | Default to 60/300/900/1800/3600-second horizons, primary 300 seconds. Do not enable v2's 1/5-second claims merely because a timestamp has seconds or microseconds. |
| Staleness | Call it `last_trade_age_seconds`. It measures transaction recency, not feed delay, quote freshness, or the time news became available. v1's quote-freshness rule remains intact. |
| Closed direct contracts | Post-release response is null with a closure reason, never zero. A deliberately bad carry-forward can illustrate a software error only in a separately labeled diagnostic. |
| Threshold geometry | Replace dimensionally invalid `abs(p-K)` with pre-event price and boundary proximity. Underlying moneyness requires an expectation and strike in the same economic units. |
| Logical nesting | Derive it from rules. For `A_K={X>K}` and `K2>K1`, `A_K2` is a subset of `A_K1`; do not infer direction from ticker order. |
| Polymarket orientation | `p_event` is documented as `price` or `1-price` using the outcome axis, not the eventual winner. Recompute/validate it from a verified outcome mapping; exclude winner-derived orientation. |
| Historical liveness | `open_time`/`close_time` are candidate lifecycle evidence, not sufficient proof of a historical rule version or absence of halts. Do not infer closure from the last trade. |
| Surprise slopes | Disabled without matching point-in-time expectations. Timing analysis remains available; missing surprise is not a numeric zero. |
| Pre-release tests and controls | Diagnostic comparisons with uncertainty, not requirements that observed changes equal zero. Mention contracts may have CPI exposure and are candidate controls only. |
| Specification | Preserve all v1 files. Create a distinct trade-study spec, cohort, window file, and preregistration after development choices are fixed and before confirmatory estimation. |
| Test integrity | Assign development dates first, disclose prior inspections, and reserve genuinely uninspected whole events in a shared registry before the locked evaluation. |
| Model promotion | Pipeline completion does not depend on a network gain. A failed gain retains the simpler baseline and is a reportable result. |

## 4. Pipeline Contracts

### 4.1 Inputs and capability flags

Every run takes explicit input paths, an inventory version, and a specification path. Never discover another dataset as an implicit fallback. Normalize all timestamps to UTC while preserving original fields and precision. Polymarket day partitions use UTC+8 according to its README; select shards by actual timestamp bounds, not filenames alone. Audit the forecast file's naive datetime strings against its epoch fields before joining it to any clock.

Introduce `configs/external_history_v1.yaml` as the pipeline configuration: named archive layers, selected release dataset, rule-evidence source, extraction bounds, clock mode, age caps, horizons, and output limits. This is an operational/measurement specification, not an empirical preregistration. An optional separate analysis-spec path is required for registered estimation. Keep both hashes in each run manifest.

Record capabilities independently: `historical_trades`, `historical_quotes`, `receipt_clock`, `rule_vintage_verified`, `initial_release_verified`, `expectation_verified`, and `economic_size_verified`. An archive's existence cannot turn all flags on. Unknown capability stays unknown with a reason.

Use four input classes: external historical archives, locally captured public data, derived datasets, and synthetic fixtures. Outputs must state which classes they used. Synthetic fixtures never fill missing real inputs.

### 4.2 Immutable inventory and lineage

Inventory every Parquet shard with relative path, bytes, row count, schema fingerprint, timestamp-stat coverage, producer, license, and SHA-256. Record when footer statistics are absent. Hash once during inventory creation and verify the named input version before execution; do not promise predicate pushdown eliminates the initial hashing cost.

Use DuckDB projection/filter pushdown and Arrow batches to extract bounded development windows. Do not materialize billion-row archives, create one Python object per full-archive row, or sort the full archive in memory. Full duplicate and join validation may need a separate disk-backed pass. Report audit scope and completeness explicitly.

Every derived row links to the original shard hash and occurrence locator, plus the mapping/specification version. Seal derived files with row counts, schema version, hashes, and input references. Write to a fresh output location or verify an identical existing result; conflicting content fails rather than overwriting a frozen run. Record interrupted partitions and support restart without duplicated output.

### 4.3 Trade normalization

**Kalshi:** use `ticker`, `trade_id`, `created_time`, `count`, `taker_side`, and exact `Decimal(yes_price)/100`. Retain original cents and NO price for consistency checks. Validate ranges according to the actual archived schema; report unexpected boundary prices instead of silently clipping. A YES-axis signed-flow measure can use verified `taker_side` and count.

**Polymarket:** choose one analysis layer explicitly. Default to cleaned Standard Binary; handle Neg Risk as a distinct cohort with parent grouping. Raw `OrderFilled` supports lineage and raw-volume reconciliation, not additional cleaned volume. Retain asset ID, condition ID, outcome sequence, raw price, normalized event-axis price, raw taker direction, and normalized `D`. Verify their relations with fixtures for both legs.

`Trade.price` represents the declared contract/token payout axis. An event-axis projection must retain the original price and mapping; do not change semantics invisibly. Venue-qualified identity stays mandatory, and token identities must not collapse incorrectly into one condition. In a verified binary mapping, projecting both legs to one reference condition is explicit and versioned.

Polymarket amounts/prices are stored as floating point in the archive. Decimal conversion does not recover lost source precision. Preserve source precision and a documented tolerance. Because cleaned rows omit token quantity, use them for counts and prices; enable quantity-weighted flow only after a defensible reconstruction or raw-fill join. Retain zero-price and ambiguous-join cases as unavailable size, not invented quantities.

The existing `Trade.size` and `trades` storage table require non-null quantity. Preserve that contract. Add a bounded `HistoricalTrade` record for the external path, reusing `Clock` and `Provenance`, with nullable `size`, a `size_quality` field, raw/event-axis price metadata, and occurrence lineage; persist it in a separate `historical_trades` table. Both external adapters emit that common record. Convert to an existing `Trade` only when quantity and price semantics are verified. Unknown size remains null through storage round trips and is excluded from quantity-weighted flow; it is never replaced with zero. This schema addition is part of P1, not an assumption that the current types already accept these rows.

No historical trade receives a fabricated receipt time. Existing `Clock` stores source and received times; `Provenance` stores the hash, occurrence ID, and source. These are distinct objects in [domain.py](src/market_propagation/domain.py), lines 388-534 and 836-881.

### 4.4 Metadata and label quarantine

Separate feature-safe fields, retrospectively fetched descriptors, and outcome labels in the schema. Enforce an allowlist at feature construction, supplemented by tests for prohibited fields.

- Kalshi fetched bids/asks, last price, lifetime volume, open interest, `status`, and `result` cannot become historical features. Do not assert a specific metadata fetch date without a receipt or revision record.
- Polymarket winning labels, resolution status/times, later category revisions, and unverified historical fee metadata cannot become historical features or determine outcome orientation.
- Forecast `community_pred_1day/3day/1week/2week/1month`, resolution flags, and outcomes are future labels. Even `community_pred_now` requires timestamp, identity, and provenance validation. Default use is a separate low-frequency forecast-example dataset, outside the macro intraday panel.
- Event-time universe selection uses rule/lifecycle evidence and pre-event information. An eventual close time may itself have changed; record the evidence quality instead of calling all fetched lifecycle fields point-in-time certified.

Rule verification must bind contract ID, raw rule hash, source, observation time, in-force interval, and settlement semantics. Missing evidence yields an exploratory candidate panel or a blocked primary panel; it is not waived by a trade-history join. See [coverage.json](data/public/final-audit/coverage.json), `study_eligibility`, and [cohort.yaml](configs/cohort.yaml), lines 306-365.

## 5. Release Coverage and Panel Semantics

### 5.1 Coverage comes before estimation

Use the ten existing releases as the first development batch. CPI and Employment Situation remain separate families. Start with the configured policy series `KXFED`, `FED`, `KXFEDDECISION`, and `FEDDECISION`, then expand with documented rules, not keywords alone. Direct CPI/payroll ladders support pre-release and settlement/lifecycle diagnostics when they close before publication.

Freeze a candidate set using admissible pre-event information. For every event/contract, report:

1. Rule/lifecycle evidence and whether the contract remains unresolved through each endpoint.
2. Pre-window, baseline-window, release-window, and endpoint-window trade counts.
3. Baseline/endpoint transaction ages, observation times, ties, gaps, and boundary/contamination flags.
4. Available outcome axes, size quality, and source-time precision.
5. Validity per horizon and every exclusion reason; keep missing cells in the coverage grid.
6. Separate counts for candidate, lifecycle-eligible, rule-verified, baseline-observed, and endpoint-observed pairs, plus distinct economic release clusters.

Post-event activity can determine whether an endpoint is observed, but cannot retrospectively select the candidate universe or relabel inactivity as a zero response. Report the missingness rate against the preselected denominator and differences in pre-event characteristics between observed and missing outcomes. Do not claim selection bias is removed by masking.

Expanding beyond the ten releases requires archived initial payloads, actual release dates/reference periods, and contamination coding. Use IANA `America/New_York`; never extrapolate the calendar mechanically. Cross-venue work uses the verified overlap, no later than Kalshi's last observed timestamp on 29 January 2026, and separately checks macro coverage within that overlap.

### 5.2 Primary transaction response

For event `e` at `tau_e`, contract `i`, and horizon `h`, let `s_minus` be the latest eligible transaction time strictly before the release and `s_plus(h)` the latest eligible transaction time at or before `tau_e+h`. Define:

\[
R^T_{i,e}(h)=p_i(s^+_{i,e,h})-p_i(s^-_{i,e}).
\]

A valid primary row requires a verified contract, no intervening closure/contamination, `s_plus > tau_e`, and both transaction ages within the configured cap. Initial development settings are a 30-minute pre-window, 60-minute post-window, 120-second baseline and endpoint caps, and horizons 60, 300, 900, 1800, 3600 seconds. These trade-age choices are provisional pipeline defaults, not inherited quote-validity evidence. Freeze them for the empirical specification after development coverage is measured.

Require a new post-release trade for a response observation. An endpoint with no such trade is `no_post_release_trade`, even if carrying the baseline forward would produce zero. Two distinct valid transactions at the same price can produce an observed zero. The resulting price estimand is conditional on observable trading; also report the fraction with any post-release trade as an activity outcome.

When multiple prints share the finest supported timestamp and execution order is unknown, create one declared tie-group observation. Default to its unweighted mean event-axis price, retaining count and min/max; this avoids assuming absent quantities or inventing chronological order. Report endpoint-envelope sensitivity. Source IDs may identify occurrences without establishing an economically meaningful ordering. Same-block Polymarket rows never establish sub-block information leadership.

Record at least: event/cluster/venue/contract IDs; release time; horizon; baseline/endpoint source times and prices; raw and event-axis price conventions; trade ages/counts; rule version; valid mask; exclusion reasons; provenance locators; clock mode; and label-time basis. Quote-only columns remain absent or null, never zero.

If caps or horizons leave too few valid endpoints, return a sparse/blocked report. A coarser or window-average study is a separately versioned estimand chosen in development, not an automatic rescue after seeing effects. No implicit widening, smoothing, interpolation, or post-close filling.

### 5.3 Clock modes

| Mode | Historical archives | Permitted interpretation |
| --- | --- | --- |
| `source` | Available at the recorded venue/block precision | Retrospective event alignment; source order is not proof of what a live participant knew |
| `usable` | Unknown for these external trade rows | Report `unidentifiable`; do not fabricate a replay or an empty disagreement result implying agreement |
| `assumed_delay` | Optional explicit scenario | Sensitivity to a declared delay assumption; never measured receipt availability |

Trade age can diagnose intermittent transactions; it does not bound an unknown feed delay. Delay perturbations provide assumption-conditional sensitivity, not an unconditional identification bound.

## 6. Analysis Capabilities, in Order

### 6.1 Required baseline capability

The first analysis report contains coverage/age maps, event cards, transaction-response curves, signed and absolute changes, activity responses, and pre-release/placebo comparisons. Equal-weight economic events are the default aggregation unit; contracts inside an event share that event's weight. Alternative volume weights change the estimand and must be separately declared.

A timing-only model uses release-family/calendar structure and admissible pre-event state. Pre-event price and `p*(1-p)` or distance to the probability boundary can model bounded-price geometry. These are price covariates, not distance to a rate/inflation strike. Keep CPI/employment coefficients separate and account for the declared family/horizon comparisons.

An optional surprise model uses initial actual minus a genuinely pre-release expectation in matching units, with scaling learned on training releases only. Keep headline/core and payroll/unemployment/earnings/revisions distinct where available. Missing expectations disable the relevant slope; they never become zero-valued surprises or disqualify timing-only analysis.

### 6.2 Optional predictive extension

Reuse the nested ladder: no change; own history/current target price; own plus shared release state and heterogeneous delayed responses; then admissible lagged neighbor information. All models use the same rows, target, and comparable tuning. The current [models.py](src/market_propagation/models.py), lines 97-111, hardcodes `shock` and `delayed_shock`, so a real timing-only feature specification must be implemented before use. Filling those fields with invented surprises is prohibited.

Initial forecast target: transaction-price change from `tau+60s` to `tau+360s`, with the same endpoint validity rules. Primary loss is event-weighted MAE; paired model gains use whole-event uncertainty. Retain v1's proposed meaningful sizes, 0.01 absolute price units for responses and 0.005 for MAE gain, as scientific choices requiring power assessment, not measured capabilities.

For external historical data, this is a **retrospective source-time prediction task**. Its rows need a distinct clock/target schema. It cannot pass through the current availability-certified forecast path by renaming a source timestamp as `max_input_available_time`. A usable-time predictive claim needs separate receipt/availability evidence. Unknown availability also limits historical label-availability certification.

A network model earns promotion only through a prespecified held-out gain beyond the common-news/own-market baseline and acceptable falsification. A predictive edge is not a causal information channel. Observation-aware state-space models are a later option; generative diffusion/flow models and GPUs have no role in the first pipeline milestone.

### 6.3 Deferred capabilities

Keep three graphs distinct: exact logical payoffs, economic exposure, and training-only predictive relationships. For a verified payoff family, v1's coherent set `C={A*pi : pi>=0, sum(pi)=1}` and bid-ask box intersection remain the right quote diagnostic. The historical tape has no contemporaneous bid-ask box. Do not substitute trades, latest metadata quotes, or projected coherent prices and call the result H4.

Thus spread/depth responses, quote coherence recovery, receipt-latency attribution, cross-venue exact-price comparisons, and usable-time forecasts are disabled unless their own inputs pass validation. Polymarket is a second adapter and a possible replication cohort, not an automatic pool. Neg Risk condition groups do not supply an exhaustive payout partition without a rule audit. Traditional assets, unscheduled news, wallet analysis, and a paper are later independent extensions.

## 7. Falsification, Splits, and Honest Results

Freeze raw-integrity and measurement rules before inspecting new outcomes. Use only development data to choose age caps, supported resolution, model complexity, and nuisance ranges. Before confirmatory fitting, create `configs/study_v2.yaml`, `configs/cohort_v2.yaml`, `configs/event_windows_v2.yaml`, and `reports/preregistration_v2.md`; all are proposed paths, currently absent. Do not point v2 at shared files and then mutate v1's frozen meaning.

Record inspected events, cohort exclusions, exact chronological split cutoffs, target windows, configuration hashes, and the reason each date is development or holdout. All markets and venues for one release stay in one split. Repeated contracts/later policy outcomes can connect releases: purge overlapping labels and use calendar blocks when dependence crosses release dates. Train transforms, graph edges, scales, and calibration only inside the training partition.

Use one explicitly selected empirical registry across run directories so copying an output path does not reset test consumption. Reserve whole locked-test events before evaluation and finalize the reservation afterward, including failures. If no uninspected adequate historical sample remains, make the historical study exploratory and reserve later prospective events. The empty existing synthetic registries do not authorize retrospective claims of preregistration.

Required falsifiers include common news with heterogeneous delays and **no communication**; sparse transaction arrivals and missing endpoints; simultaneous direct responses; omitted shared shocks; opposite payoff orientations; block-time ties; placebo release times matched on time-of-day/session/regime; pre-release changes; reversed candidate edges; endpoint/tie/age sensitivity; and leave-one-release-out analysis. A mention contract becomes a negative control only after an exposure argument is recorded. Pre-release changes are not required to equal zero.

The current synthetic registry reports network null false-positive rate 0.0, network recovery power 0.525, and response-slope false-positive rate 0.10 over 40 repetitions. These are limited simulator results; neither zero observed network errors nor passing software tests proves a calibrated inferential procedure. Extend/calibrate the simulator's transaction observation process and report Monte Carlo uncertainty. A proposed inferential gate is a one-sided 95% binomial upper bound on null false-positive rate at or below 0.05 for the prespecified nulls; inadequate repetitions are inconclusive, not a pass.

Estimate power from development-event variance and dependence under the actual observation masks, separately by family and regime. Do not infer power from archive bytes or trade count. Report numerical intervals and relevant effect bounds; a nonsignificant estimate alone is not evidence of no effect. Failed falsification blocks interpretation/promotion, while the pipeline still exports the failed diagnostic.

## 8. Implementation Backlog and Gates

All new names below are proposed deliverables, not existing CLI commands. Preserve the current `reproduce`, `audit`, `capture`, `quality`, `registry-review`, and `event-card` behavior and exit-code meanings.

| Stage | Concrete change | Output and acceptance gate |
| --- | --- | --- |
| P0: inventory | Add `configs/external_history_v1.yaml`, `ingest/external_inventory.py`, and an `inventory-external` CLI command; use existing DuckDB/Arrow tooling | Manifest for all local layers, hashes, schemas, timestamp bounds, attribution; raw files unchanged; repeat execution yields the same inventory identity |
| P1: external normalization | Add `ingest/external_history.py`, the `HistoricalTrade` record, a nullable-size `historical_trades` table in `storage.py`, and `normalize-external`; implement Kalshi first, then one explicit Polymarket layer | Bounded trade partitions with exact Kalshi cents, declared Polymarket orientation/precision, null-preserving quantity round trips, stable occurrence lineage, and explicit unknown receipt clocks; fixture and real-window reconciliation pass |
| P2: release/rule coverage | Reuse archived-release parsing; extend evidence validation in `ingest/audit.py` and add `coverage-external` | Complete event/contract coverage grid for the ten development releases, including zero-activity and blocked-rule candidates; archive-derived counts reproduce directly; **G0 remains blocked unless rules, cohort, and supported frequency pass** |
| P3: transaction panels | Add a dedicated `trade_panel.py` and `build-trade-panel`; extend `storage.py` with a separate panel table | Source-time transaction panel and exclusions, no trade-to-quote casting, no imputed availability, no post-close zeroes; one event manually reconciled to raw rows plus a missing/closed case; G2 measurement check |
| P4: report and baseline | Add `report-external` orchestration, explicit timing-only analysis configuration, and report templates using existing plotting/statistics | Coverage report, event card, response figures, baseline summary, lineage, and capability/blocker table; no synthetic substitution; unsupported requests exit 2 with usable artifacts |
| P5: harden and document | Add resume/idempotency checks, validate data bounds, update README/data card/reproduction guide and packaging allowlist only as needed | Fresh-output offline run succeeds; altered input fails verification; interrupted run resumes without duplicates; documented actual commands and example outputs; **pipeline v1 delivery milestone** |
| P6: optional empirical study | Freeze separate v2 study files, expand verified releases, calibrate transaction nulls/power, add source-time forecast schema and bounded model adaptation | G1 limitation recorded; G3 assessed with uncertainty; one registered held-out comparison only if eligible data and power permit; G4 is not a pipeline completion requirement |
| P7: optional replication/paper | Integrate a rule-matched Polymarket or later-regime cohort; refresh literature before paper drafting | Separately reported replication/G5 and evidence-matched G6 package; unmatched or insufficient cohorts stay unevaluated |

Important integration facts from the current code:

- [point_in_time.py](src/market_propagation/point_in_time.py), lines 525-588: `build_event_panel` takes `Quote` records and rejects trades. Reuse semantics where appropriate; add the trade-specific builder rather than weaken this boundary.
- [point_in_time.py](src/market_propagation/point_in_time.py), lines 1017-1050: `forecast_frame` intentionally admits availability-based feature timestamps, not source timestamps. A source-time study requires a distinct explicit path.
- [storage.py](src/market_propagation/storage.py), lines 1283-1340: `query_sealed` expects repository table metadata. Raw external Parquet needs an inventory-verified scan before conversion; passing a pathname alone does not perform the same content-hash check as supplying a `DatasetRef`.
- [domain.py](src/market_propagation/domain.py), lines 388-534 and 836-881: reuse `Clock` and `Provenance`; `Trade` requires verified non-null size. The external `HistoricalTrade` path preserves unknown size without weakening that existing contract. Attach source clocks to `Clock`, not nonexistent fields on `Provenance`.
- [models.py](src/market_propagation/models.py), lines 551, 861, and 1244: baseline fitting, nested comparison, and local projections already exist but require compatible targets/features. They are not automatically a historical trade pipeline.
- [registry.py](src/market_propagation/registry.py), lines 225-365: reuse durable runs and one-shot reservations, with one shared empirical registry path and a separate inspection ledger.

Inventory and fixture design can proceed independently. Normalization depends on inventory/schema decisions; coverage depends on normalized trades and rule/release inputs; valid panels depend on coverage; estimation depends on an explicit analysis specification. Within a stage, use bounded independent venue adapters or test work where ownership is separate. Keep a single owner for shared schemas and CLI integration.

## 9. Verification and Operational Acceptance

The new pipeline needs behavioral tests and a real-input manual run, not just schema declarations.

| Boundary | Required check |
| --- | --- |
| Inventory | Missing/changed shard, inconsistent schema, absent timestamp stats, duplicate filename, and corrupt Parquet produce explicit failures/quality flags; counts reconcile by layer |
| Normalization | Exact cents, YES/NO orientation, second-vs-millisecond errors, UTC+8 partition boundaries, repeated identical fills, and stable shard/row identity are exercised; unknown size survives a Parquet round trip as null and cannot enter weighted flow |
| Metadata | Future winner/result/status/volume/forward predictions cannot alter earlier features; altered outcome labels cannot change the reference price axis |
| Rules/releases | A missing historical rule version blocks eligibility; initial/revised releases remain distinct; DST and rescheduled publication are checked from original evidence |
| Panel | Missing baseline, no new post-release print, old endpoint, closure, halt/contamination, and ambiguous ties are masked with reasons; truly unchanged fresh prices can yield zero |
| Clocks | `source` never silently becomes `usable`; unknown availability cannot enter the certified forecast path; unavailable replay comparison reports `unidentifiable` |
| Splits/models | All rows for one release share a split; future records do not change earlier features; training transforms ignore test mutations; nested models compare identical rows; labels respect the declared clock basis |
| Operations | Named output paths only, bounded memory, raw archives unchanged, deterministic small replay, interrupted-run recovery, conflicting output refusal, and no network in offline execution |
| Reports | Every plotted/estimated row links to inputs/spec; missing results are visibly missing; evidence classes and blocked gates survive export |

At each stage run the related existing and new tests and repository lint. Before pipeline delivery, run the full offline suite and package/build verification supported by the project. No typechecker is currently configured in `pyproject.toml`; report that gap rather than claiming a typecheck ran. Adding tooling/dependencies is a separate decision, not hidden in the data adapter work.

Manual acceptance: run the documented commands on the ten development events into a fresh directory, inspect one valid real transaction calculation if one is eligible, inspect one sparse/closed/rule-blocked case, and trace both to archived rows. If no eligible event exists, demonstrate the blocked report end to end and state that empirical estimates remain unavailable. Then repeat a bounded run and compare inventory/panel hashes and key summary counts.

## 10. Deliverables and Resource Plan

The primary package consists of usable CLI commands and Python entry points; separate inventory/trade/contract/release/panel schemas; source and license attribution; row-level lineage; reusable event specifications; masked coverage/panels; diagnostics and baseline outputs; tests/fixtures; and a reproduction guide. Put generated real outputs under a run-specific directory such as `data/derived/external/<run_id>/`, separate from `data/synthetic/` and external raw inputs.

Provide a small redistributable fixture for installation/testing and acquisition instructions plus hashes for the large archives. Keep raw data local by default under the current packaging policy. Document CC-BY attribution/change notices and the MIT notice; assess permitted derived exports before enabling them. Do not promise redistribution simply because a README displays a license tag.

Record source tree/environment/specification hashes, selected shards, tool version, seeds, audit scope, elapsed time, peak memory, bytes read/written, row counts, and statistical unit counts. The current checkout is uncommitted; a source-tree hash is necessary if no commit identifies the implementation. Benchmark the ten-event path before assigning hardware or throughput guarantees. Start CPU-only with bounded extraction; the initial full hash pass and full uniqueness/join audit are separate measured costs.

Priority order is P0-P2 (trustworthy inputs and coverage), P3-P5 (working reusable pipeline), then optional P6-P7 (confirmatory models, replication, paper). Historical rule evidence and additional official release archives may be the longest dependencies. Engineering progress must not hide those scientific blockers.

## 11. Evidence for This Consolidation

Both source plans were read completely. Local Parquet metadata was inspected across all external layers; the small forecast file was queried for distinct markets/dates and missing model predictions. The ten-release dataset was checked against its manifest hash. Both existing SQLite registries were inspected with the actual CLI. No real response model, surprise model, or locked evaluation was run.

Source hashes at consolidation:

| File | SHA-256 |
| --- | --- |
| `prediction_market_information_diffusion_plan.md` | `21fe4bc100b540e7a220d77f43157188d35292cb3f9669edd2982f0fac590fac` |
| `prediction_market_information_diffusion_plan_v2.md` | `894d65411c4916e89b098b1d2a54815b58e0b0cc059a20097e7ac7534c62e9c4` |
| `configs/study_v1.yaml` | `b7fd55f3eac27db787a2ab6ee9e6348424ec197f6457d46fb3b976bc80fe98eb` |
| `data/public/bls-normalized/releases.parquet` | `bfa14ecf7247dba20c35fe349496167aab28b63589e94902add92eccaf59c191` |

Verification run for the existing boundaries: `uv run --no-sync pytest -q tests/test_core.py tests/test_study_eligibility.py tests/test_registry.py tests/test_forecast_storage.py` returned **110 passed**. The CLI `--help` and read-only `registry-review` commands worked. These checks support reuse of the tested boundaries; they do not test the proposed external adapters.

Repository lint (`uv run --no-sync ruff check src tests scripts`) passed. All 24 local Markdown links across this plan and its `.omx/plans/` pointer resolved, and the fenced code blocks were balanced. Both original plans and the three existing v1 configuration files retained their pre-edit hashes. No source code, dataset, or frozen study file was changed for this consolidation.

The inventory can be rechecked without scanning all trade values:

```python
from pathlib import Path
import pyarrow.parquet as pq

for pattern in (
    "data/external/kalshi-trades/trades-*.parquet",
    "data/external/kalshi-trades/markets-*.parquet",
    "data/external/polymarket-v1/OrderFilled/*.parquet",
    "data/external/polymarket-v1/daily_aligned/*.parquet",
    "data/external/polymarket-v1/daily_aligned_multi/*.parquet",
    "data/external/polymarket-v1/CTF/*.parquet",
    "data/external/forecast-snapshots-*/snapshot_dataset.parquet",
):
    files = sorted(Path(".").glob(pattern))
    print(
        pattern,
        len(files),
        sum(pq.read_metadata(p).num_rows for p in files),
        sum(p.stat().st_size for p in files),
    )
```

Run it with the existing `uv run --no-sync python` environment. Footer counts are not duplicate checks or source-completeness certificates. Preserve the existing dependency set; this inspection required no installation. A DuckDB timezone-aware Python result conversion initially required absent `pytz`; selecting timestamp strings with an explicit UTC session avoided that inspection-only dependency.

The [v1 source list](prediction_market_information_diffusion_plan.md#sources) remains a literature/API starting point, not a newly verified bibliography. Refresh external sources when their APIs or paper claims become implementation requirements. This final plan resolves the two documents against local evidence; it does not claim a fresh literature review.
