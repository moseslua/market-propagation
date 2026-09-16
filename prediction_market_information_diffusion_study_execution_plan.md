# Information Diffusion: Empirical Study Execution Plan

Date: 15 September 2026. Status: execution plan, not a preregistration or an empirical result.

## 1. Objective and Authority

Turn the existing external-history pipeline into a reproducible empirical study of whether lagged information from related policy-rate contracts improves prediction beyond the recipient's own history and a verified common-news baseline. Deliver the data evidence, implemented analysis, calibrated falsification, empirical results that the evidence permits, and a complete research report. A positive result is not required for completion.

This plan extends the [final pipeline plan](prediction_market_information_diffusion_final_plan.md), especially P6 and P7. It supersedes that document's pipeline-only completion target for this task. Its old implementation-status paragraph is historical: the external commands now exist. Preserve the original plans and frozen v1 configurations. The user has authorized planning followed by end-to-end execution by the existing OMP client; routine acquisition, implementation, verification, and local report generation may proceed autonomously.

Maintain CPU-first/local execution, existing dependencies, zero paid spending, public read-only market access, and immutable acquired archives. Do not place orders, purchase data, publish externally, submit a paper, or change authentication. Use bounded public-source acquisition and existing permitted data. Missing paid-source access is a documented prerequisite, not authorization to spend. Preserve the currently untracked implementation and all unrelated work.

## 2. Live Starting Point

The following were inspected while preparing this plan. Saved artifacts are prior-run evidence, not reruns performed by the planner.

| Evidence | Observed state | Implication |
| --- | --- | --- |
| `uv run --no-sync market-propagation --help` | All five external-history commands are registered | Extend the existing path; do not rebuild P0-P5 from scratch |
| `.audit/acceptance/acceptance.json` | `verdict=2`, `reproduced=true`, `hash_scope=none`, `max_rows=20000` | Repeated derived identities passed for a bounded structural run; this does not certify full archive integrity or complete cohort extraction |
| `.audit/acceptance/first/coverage_external.json` | 10 release clusters, 400 candidate event-contract pairs, zero rule-verified pairs; baseline/endpoint counts unmeasured | The audit candidates are a development starting set, not an eligible or exhaustive cohort |
| `.audit/acceptance/first/report/external_report.json` | 70 masked rows, 14 contracts, one CPI event, no estimate, no registered run | The saved panel does not cover all ten releases and is not an empirical study |
| `configs/external_history_v1.yaml` | `expectation_verified=false`, source clock, no historical quotes/receipts, confirmatory estimation disabled | Preserve these limits until specific evidence clears each one |
| `timing_model.py` versus `external_report.py` | A timing-model implementation exists; the report path computes descriptive event-weighted responses and does not call its fitter | Add a real analysis runner; reporting a configured model kind does not prove a fitted model |
| `models.py`, `evaluation.py`, `registry.py` | Nested forecast comparison, release grouping, and durable reservations exist | Reuse compatible internals with a distinct historical source-time data contract |
| `falsification.py` | Shared-news/no-communication and communication processes exist; current default FPR ceiling is 0.10 | New 0.05 study calibration must be explicit and cannot inherit an old `ok` label |
| `.audit/falsification-480.json` | 40 repetitions of 480 synthetic events; null 0/40, one-sided upper bound 0.07216; recovery 16/40, lower bound 0.26940; `inconclusive` | Neither the proposed 0.05 false-positive ceiling nor 0.80 power gate is established |

Read `.audit/external-pipeline-contract.md`, `.audit/external-measurements.md`, and current source before editing. Reconcile any newer worker changes. Historical counts must be regenerated from named inputs when used in the paper. Inspect saved manifest hashes before reusing evidence; do not treat filenames as provenance.

## 3. Research Questions and Claim Ceiling

The primary question is: among eligible policy-rate contracts following a BLS release, does a prespecified neighbour's earlier transaction-price movement improve held-out prediction of the recipient's later movement, after controlling for the recipient's history and observed common news?

Report three distinct results:

1. **Absorption:** source-time transaction responses from just before release to prespecified endpoints. No consensus is needed for this descriptive result.
2. **News-conditioned response:** associations with an independently documented pre-release forecast error and other newly released information. Only verified dimensions qualify.
3. **Conditional predictive propagation:** incremental future-price prediction from admissible neighbour history beyond the complete declared news/own-market baseline.

Even a positive third result is not proof that a trader read another market or that one market caused another to move. Forecast errors do not measure every shared shock, and archived timestamps do not establish participant availability. State the strongest permitted conclusion as conditional predictive propagation in source-time transaction data, robust to the declared falsifiers. A causal information-channel claim would require a separate identification design with credible exogenous variation; it is outside this study.

Simulation measures detection and false-positive rates under specified mechanisms. It does not identify the real-world fraction of discovered edges that are true transmissions, or `P(transmission | detected edge)`. Those require additional assumptions about mechanisms and their prevalence. Cross-venue data are not necessary for the primary within-venue study.

## 4. Design Decisions to Implement

These are explicit defaults chosen for this handoff. Freeze them in v2 after development-only feasibility checks. Any substantive replacement must be explained in a dated design decision before new evaluation; a change after looking at test outcomes requires a new cohort/version.

### Primary Cohort and Neighbour Graph

Use Kalshi contracts on a documented policy-rate level at a future decision date. Begin with the existing `FED`, `KXFED`, `FEDDECISION`, and `KXFEDDECISION` candidates, but admit contracts by verified settlement semantics, not ticker resemblance. A rate-change contract, target-range upper bound, effective rate, and point-in-time rate are different predicates until the rules establish comparability.

The primary economic-exposure graph links an earlier decision date to the immediately following decision date for the same rate definition, numerical threshold, inequality, and YES-axis orientation. Both contracts must be open and unresolved through the required windows. For receiver `i`, select the matched donor `j` at the immediately preceding decision date in the verified calendar. If that exact match is absent, mark `no_admissible_neighbor`; do not silently skip dates or substitute the most correlated strike. Resolve duplicate equivalent contracts with a fixed identifier-based rule recorded in the graph spec, not post-release liquidity.

This earlier-to-later direction is a prespecified forecasting hypothesis, not a known transmission edge. Eligibility and topology depend on rule/lifecycle information in force before the release. Store donor and receiver IDs, effective intervals, predicate/threshold/decision-date fields, rule hashes, exclusion reason, and graph version. Do not use eventual settlement, winner, post-event volume, or full-history correlations.

Keep three relations distinct: logical payoff identities, economic exposure candidates, and estimated predictive associations. Same-expiry adjacent strikes are a separate mechanical-dependence sensitivity; exact complements or duplicate claims are not independent neighbours. Reverse earlier/later direction is a prespecified diagnostic, not a required null: feedback and shared news could produce prediction in either direction. CPI-linked to FED-linked and cross-venue links are secondary extensions only after their own rule, liveness, news, and timing gates pass. Direct CPI contracts closed at release cannot supply a post-release donor signal.

### Clocks, Forecast Origin, and Target

Retain the absorption horizons `60, 300, 900, 1800, 3600` seconds, with 300 seconds primary. Define a separate primary network forecast origin `tau = release_time + 300s`, lag guard `L = 60s`, and future horizon `H = 300s`:

```text
absorption(e, i, h) = p_i(last valid trade <= release_time + h)
                     - p_i(last valid trade < release_time)

network_target(e, i) = p_i(last valid trade <= tau + H)
                      - p_i(last valid trade <= tau)

neighbor_lag(e, j) = p_j(last valid trade <= tau - L)
                    - p_j(last valid trade < release_time)
```

The primary network target is the 5-to-10-minute increment; it is not the first five minutes' absorption or the v1 quote target at origin +60s. This deliberate horizon choice tests delayed propagation at a resolution plausible for historical trades. It cannot answer whether transmission occurred entirely in the first minute. Record the change in v2. If development coverage cannot support it, report that result and make any coarser design a separately named specification before evaluation.

Require a fresh recipient anchor and a genuinely new trade after `tau` for its target. A donor signal requires a post-release observation by `tau-L`; missing or tied timing is unavailable, never an imputed zero. Start with the existing 120-second age caps for anchors/endpoints and apply an explicit cap to the donor cutoff as well. Compute observed timestamp envelopes: strict ordering must survive the documented timestamp precision. Group indistinguishable ties before constructing features; no arbitrary row-order lead/lag.

Keep `max_input_source_time`, actual anchor/endpoint times, `forecast_origin`, `label_source_time`, ages, tie envelopes, input occurrence IDs/hashes, and `clock_basis=source` on every forecast row. Receipt and availability fields remain null. Source-time chronological validation is retrospective; it must not be described as an executable backtest. Preserve the existing usable-time validation path unchanged.

### Models and Primary Estimand

Fit separate CPI and employment models. Make CPI the primary confirmatory family and employment the prespecified secondary family; unavailable CPI does not silently promote employment. Use the existing ridge/grid and bounded-price conventions where compatible; fit scaling, interactions, penalties, and calibration using training/validation only.

The model ladder on identical forecast rows is:

| Model | Inputs |
| --- | --- |
| No change | Zero future change |
| Own | Recipient price at `tau`, pre-release price geometry/activity, own changes over `[release-300s, release)`, `[release, tau-60s]`, and `[tau-60s, tau]`, time to decision, declared calendar terms |
| News | Own inputs plus verified release surprise vector, newly released revisions, and prespecified surprise-by-time-to-decision/pre-event-state terms representing heterogeneous direct updating |
| Network | Exactly the news design plus the single admissible donor return above; any residualized donor feature is fitted out-of-fold on training data and declared as a separate sensitivity |

Audit the current hardcoded `shock`, `delayed_shock`, and simulator-specific neighbour controls before reuse. Define empirical delayed-direct-response terms in economic/time units and include them in **both** news and network. Do not invent a delayed shock, add a zero-filled control, or rename a trade row into the certified quote schema to satisfy an API.

For each release, average absolute error equally across the common eligible receiver set. Average these event-level errors equally across releases. Primary gain is `Delta = MAE_news - MAE_network`, in absolute probability units on `[0,1]`. Preserve the existing scientifically meaningful gain `0.005` (0.5 percentage points); power assessment may find it unattainable. No post-result threshold reduction. Report the common-sample comparison and the broader own/news cohort separately, with full missingness denominators. No row-level pseudo-replication or post-event volume weighting.

Model promotion requires all data/calibration gates, positive paired uncertainty evidence, and point gain at least 0.005. Use a one-sided 95% block-bootstrap lower bound above zero for the primary contrast; calibrate this complete decision rule. A claim that the true gain exceeds 0.005 additionally requires its lower bound above 0.005. Report the estimate and two-sided interval regardless of promotion. Secondary families, horizons, graphs, and lags get a frozen multiplicity policy or exploratory labels, not alternative chances to pass the primary test.

## 5. Obtain Independent Common-News Evidence

Create a source feasibility ledger before bulk acquisition. BLS archives establish release values, not consensus expectations. Preserve first-release actuals, original units/seasonal adjustment, reference period, publication time, revision state, raw bytes, URL, retrieval time, and historical publication evidence.

For each forecast observation retain event/statistic identity, value/units, source kind, survey/provider, forecast cutoff, original publication timestamp and its proof, archive capture timestamp, raw hash/locator, revision policy, and verification outcome. Capture time today is not proof that the value existed before release. Use a demonstrably pre-release archived forecast or a contemporaneously published survey summary. A single forecaster is not a consensus and must be labeled accordingly.

Search public sources in this order: existing local evidence; timestamped pre-release survey/preview articles with explicit numbers; independently archived pre-release calendar snapshots; documented public survey archives with compatible monthly horizons and vintages. Reuters or comparable survey previews and calendar providers are search candidates, not sources already verified by this plan. Quarterly forecasts, a current calendar's historical forecast column without vintage evidence, revised databases, and the local community prediction snapshots are not automatic substitutes.

For development, inspect at least three events per family across the available period and test whether each required dimension can be verified. Record exact failed URLs, access/terms problems, missing vintages, and dimension coverage. Use one prespecified source hierarchy throughout; do not choose whichever forecast best explains prices. If a public historical route fails, test a second independent source category before declaring the blocker. Do not evade access restrictions or purchase access.

CPI controls require headline and core seasonally adjusted month-on-month surprises. Employment controls require payroll change, unemployment rate, and earnings month-on-month surprises plus first-published payroll revisions as newly released information. A revision does not require an invented consensus. Record material simultaneous announcements and exclude contaminated primary windows when their news cannot be controlled under the frozen policy.

Compute `surprise = first_release_actual - verified_pre_release_expectation` in matched units. Standardize only from training events. Missing required dimensions block the corresponding primary news/network comparison, while eligible absorption rows remain usable. A reduced-vector model is separately labeled exploratory because omitted shared news can masquerade as a neighbour effect. Verified surprise is a necessary control for this declared study, not sufficient causal identification.

## 6. Sampling, Inspection, and Registration

Treat all ten January-May 2025 releases and every event already inspected in prior activity probes as development. Extend the release calendar within the verified archive span using original release packages, then evaluate metadata/rule/news availability before opening response values for uninspected events. Record any inspection immediately in an append-only ledger. Do not relabel inspected events as held out because the registry is empty.

Before fitting uninspected outcomes, create separate `configs/study_v2.yaml`, `configs/cohort_v2.yaml`, `configs/event_windows_v2.yaml`, `configs/neighbor_graph_v2.yaml`, and `reports/preregistration_v2.md`. Keep the new external extraction configuration separate from frozen v1. Record exact event IDs, chronological cutoffs, development/validation/test membership, exclusion rules, graph rules, source hierarchy, estimator, timing, loss, effect threshold, multiplicity, simulation seeds/scenarios, compute limits, and hashes. The local freeze is not external registration.

Start from chronological 60/20/20 partitions, but determine whether the resulting independent test-event count can detect 0.005 using development-only power analysis. Never substitute a universal minimum number of rows/events for power. All contracts, horizons, news components, and venues for one release share its `cluster_id` and split. Purge overlapping feature/target windows; handle dependence across releases sharing a policy-decision exposure with chronological calendar blocks. Set block length using development dependence and test sensitivity at longer blocks. A source-label cutoff is not an observed label-availability time.

Use one explicit empirical registry, proposed `data/registry/empirical_study.sqlite3`, across all run directories. Reserve test events before reading their outcomes for scoring, finalize reservations even on failure, and refuse reuse in another spec/output directory. Store actual fitted input classes in manifests so the current unrecorded-provenance outcome cannot recur. Lock outputs and data/config/source-tree identities; the repository currently contains untracked code, so a Git commit hash alone is insufficient.

If no adequate uninspected historical test remains, finish the historical study as exploratory and produce a prospective protocol with exact data acquisition, pre-release forecast capture, event schedule, and power-derived stopping rule. Never claim future releases have occurred. A prospective protocol is a remaining study prerequisite, not completed confirmation.

## 7. Calibrate the Actual Historical Analysis

Extend the simulator with the transaction observation process and pass simulated tapes through the same graph, panel, feature, fitting, selection, and reporting functions as real data. Preserve hidden mechanism truth outside the feature schema. Existing `reproduce` outputs are software evidence only and do not certify this new route.

Required null classes cover: common news with heterogeneous direct response delays and no communication; sparse/asynchronous transaction arrivals; stale anchors/endpoints and tied timestamps; different nonlinear threshold responses; omitted shared shocks and forecast measurement error; correlated trade-arrival intensity; and mechanically related payoffs. Positive recovery adds a declared communication edge while holding comparable nuisance draws fixed. State which nulls test the maintained model and which stress its misspecification; failure on plausible unmeasured-news stress prevents a strong propagation interpretation.

Fit nuisance ranges only from development observation masks and residuals. Include low-liquidity and regime-specific settings; report where identification has no power. Do not tune away a failed null or increase transmission strength until recovery turns green. Predeclare a plausible effect-size grid, derive its induced oracle predictive gain, and report power as a function of signal, event count, and observation quality. Do not demand 80% promotion probability exactly at a threshold boundary: define the recovery alternative above the decision threshold and report detectable gains explicitly.

Separate simulation development seeds from final calibration seeds. Freeze the scenario grid and complete promotion rule first. Begin final calibration with 200 repetitions per primary scenario and report exact binomial intervals; more repetitions follow a predeclared fixed extension, not optional stopping at a favorable bound. For the `m` prespecified primary null scenarios, use simultaneous one-sided bounds (for example Bonferroni confidence `1 - 0.05/m`) and require every FPR upper bound <=0.05. For the prespecified recovery alternative require a one-sided 95% lower power bound >=0.80. Inadequate Monte Carlo precision is inconclusive. The existing 40-repetition artifacts are neither adequate certification nor a replacement for this exercise.

Calibrate rejection/promotional decisions, including tuning, common-sample filtering, uncertainty and any multiple testing; counting only `gain >= threshold` would not certify the final decision procedure. Record scenario version, full estimator identity, feature/graph/masking hashes, seeds, repetitions, failures, false positives, and recovery counts. A changed analysis invalidates the corresponding calibration certificate. Use a small runtime pilot to set batch sizes and resource caps; preserve results/checkpoints if a declared compute cap is reached.

## 8. Execution Stages and Acceptance

All new filenames/commands in this section are proposed deliverables. Use existing modules where their contracts fit; avoid creating duplicate engines.

| Stage | Work and code ownership | Observable acceptance |
| --- | --- | --- |
| S0: reconcile | Review current source, `.audit` artifacts, dirty state, and existing tests; create `reports/study_execution_status.md` with evidence links and checkpoint | Current versus saved evidence is explicit; baseline checks captured; no existing work discarded |
| S1: evidence feasibility | Extend release import and rule-evidence validation; add a small expectation importer/validator reusing `Expectation`, `Clock`, `Provenance`, and sealed storage; write `reports/source_feasibility.md` | Raw payload -> parsed first actual/forecast/rule -> verified event/contract joins demonstrated on development events; missing dimensions/versions rejected |
| S2: complete cohort extraction | Extend `ingest/external_history.py`, `ingest/audit.py`, `trade_panel.py`, storage, and CLI to select declared candidate IDs and event windows before bounded scans; join pair-level coverage | Every declared event-contract-horizon cell appears, including missing/closed/no-trade cases; shard/window counts reconcile; no row cap or first-N-contract truncation masquerades as complete data |
| S3: graph and forecast panel | Implement graph construction and explicit source-time forecast rows, proposed `neighbors.py` and `historical_forecast.py` | Hand-reconciled real donor/recipient case plus missing/tied/closed cases; no self/complement leak, no future source input, stable graph under post-event mutations |
| S4: analysis integration | Wire real timing/news/network fitting through a study entrypoint; reuse `timing_model.py`, compatible parts of `models.py`/`evaluation.py`, and shared registry | A CLI-driven development run records actual fitted row IDs, features, predictions, model/transform hashes, paired event losses, exclusions and provenance; different model samples fail |
| S5: design lock and calibration | Freeze v2 files and inspection ledger; run transaction nulls, recovery, development power and block-size sensitivity through S3-S4 | Independent calibration artifact tied to the exact analysis; gates are pass/fail/inconclusive with numerical uncertainty, not copied status labels |
| S6: one empirical evaluation | Only after S1-S5 permit it, reserve and score locked events once; otherwise complete the explicitly exploratory or blocked branch | Immutable predictions/losses, reservation receipt, counts and intervals; no retuning after test inspection; every negative/inconclusive outcome retained |
| S7: robustness and replication | Run preregistered age/lag/tie, missingness, placebo, reverse-edge, leave-one-release/block-out and regime analyses; optional separately gated replication | All planned analyses appear, including failures and unavailable ones; no robustness choice replaces the primary result |
| S8: final package | Update README/data card/reproduction guide and produce `reports/empirical_study/paper.md`, figures, claim ledger and execution status | Numbers/figures trace to frozen artifacts; independent reproduction in a fresh directory; actual limitations and terminal study outcome stated |

S1 source/rule work and S2 extraction can proceed independently after S0. S3 needs verified semantics but can develop against clearly marked fixtures while S1 runs. S4 follows S3; final S5 calibration uses the final S4 estimator. S6 cannot precede calibration or the empirical lock. Keep a single owner for shared schema/CLI/registry integration. Independent bounded source audits or tests may be delegated through the receiving client's supported tools; the OMP owner integrates and verifies them.

### Verification Requirements

Run the existing baseline commands at S0, then affected tests after each change and the full suite at integration:

```bash
uv run --no-sync market-propagation --help
uv run --no-sync pytest -m 'not network'
uv run --no-sync ruff check src tests scripts
uv run --no-sync ruff format --check src tests scripts
```

Use a fresh output directory for the current acceptance harness; its default can replace an existing directory. The following is a command to run during execution, not a claim it ran during planning:

```bash
uv run --no-sync python scripts/acceptance_external.py \
  --out .audit/study-v2-baseline-20260915
```

This harness still has a default 20,000-row cap, so it is a regression check, not the final study acceptance. Add a cohort-targeted acceptance path with complete declared extraction, verified shard hashes, one shared registry, and a fresh output directory. Document its actual CLI only after implementation. Reuse verified content-addressed inventories; do not repeatedly hash the whole archive without need, and do not call `--skip-hashes` a full integrity check.

New behavioral checks must cover:

- Post-release or revised expectations, mismatched units/reference periods, incomplete release vectors, and altered evidence bytes are rejected.
- A future rule snapshot cannot assert a historical in-force interval; orientation does not depend on the winner; exact strike/decision semantics govern graph matches.
- Every selected release remains in the coverage denominator; caps fail explicitly; normalization is resumable without lost/duplicated occurrences.
- No donor observation after its cutoff, no recipient target overlap with inputs, no tie broken into a false lead, no missing signal changed into zero.
- Future prices, held-out outcomes, metadata results, post-event volume, and row ordering cannot alter earlier features/topology/training transforms.
- News/network use identical row IDs and weights; all release-cluster rows stay in one fold; overlapping labels are purged and test reservations survive failure/restart.
- Simulation truth never enters features; the historical and simulated routes invoke the same feature and decision code; stale certificates fail estimator-identity validation.
- A changed shard/config/graph invalidates lineage; a fresh-directory reproduction preserves deterministic content identities and predictions, allowing recorded runtime metadata to differ.
- On real data, manually trace at least one valid release/donor/recipient target to raw occurrences and separately verify a missing/closed case. If no valid case exists, explicitly leave this acceptance item blocked.

## 9. Robustness and Reporting

Prespecify age-cap and lag-guard sensitivity, endpoint/tie envelopes, excluding mechanically linked payoff families, matched time-of-day/session/regime placebo releases, reversed candidate direction, pre-release movements, leave-one-release and calendar-block-out estimates, and missingness by pre-event state. Pre-release movement need not equal zero, and a reverse edge need not be absent. Trade-observation conditioning can cause selection bias; missing-cell reporting alone does not remove it. Assumed latency scenarios are sensitivity analyses, not measured receipt corrections.

The report must include the cohort attrition table, surprise-source coverage, graph definition/examples, temporal feature/target diagram, absorption curves with event/block uncertainty, paired event-level loss differences, calibration error/power with intervals, robustness failures, and a claim-to-artifact ledger. Plot response prices as transaction prices on a stated payout axis. Render and inspect every produced figure. A PDF is optional unless a TeX manuscript is added; then compile and inspect the rendered pages.

Refresh the literature matrix before writing the empirical discussion, using original studies on price discovery, common-news lead/lag identification, asynchronous observation, and prediction-market payoff constraints. Record citations actually consulted. The scope is a local research package; external publication is not part of the authorization.

## 10. Stop Rules and Honest Completion

Track engineering readiness, data eligibility, calibration, empirical evaluation, and scientific conclusion separately. A report-writing or packaging gate cannot certify the study.

| Condition | Required action and final wording |
| --- | --- |
| Verified news, graph, observations, sufficient power and valid test; positive primary comparison | Complete empirical conditional-prediction study; report measured gain with conditional/source-time limits |
| Eligible evaluation with no meaningful improvement | Complete negative empirical study; retain simpler baseline and report uncertainty/effect bounds |
| Wide intervals, weak power, calibration uncertainty or plausible null failure | Complete available analyses, label propagation inconclusive/unsupported, and report exact failures; do not call this evidence of no diffusion |
| Independent surprise unavailable but rules/observations adequate | Finish actual absorption study and all independently executable engineering/calibration; withhold news-conditioned/propagation claims and deliver source-search evidence |
| Rule vintages or valid trade endpoints unavailable | Deliver a verified feasibility/blocked-data package and implemented tested path; explicitly state that an actual empirical study remains blocked |
| Historical data exhausted by inspection or too few independent events | Deliver exploratory results and a ready prospective protocol; confirmation remains pending future data |

Do not stop at another plan, a green fixture suite, a configured capability, a 64-document tally, or a blocked report when useful independent work remains. Conversely, do not manufacture an actual study by relaxing rules, fabricating surprises, substituting synthetic estimates, reusing test events, or claiming collection of future data. For every unresolved prerequisite record attempted sources/actions, exact evidence, impact, and smallest remaining external requirement.

Final handback from OMP must name the terminal study outcome, changed files, exact commands and exits, fitted real-event counts by family and split, graph/expectation coverage, null/power results, consumed test IDs, report paths, and remaining blockers. A successful handoff by the planning agent means only that this plan was delivered to the intended client, not that the study has already run.
