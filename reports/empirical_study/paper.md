# Information diffusion in policy-rate prediction markets: execution report

Date: 15 September 2026. Status: **terminal study outcome is blocked**. This is a
completed execution, not a completed empirical result.

## 1. Terminal outcome

| Dimension | State |
| --- | --- |
| Engineering readiness | complete: the pipeline runs end to end, the modules are tested, and the acceptance run below exercises the full declared path |
| Data eligibility | **blocked**: no candidate contract carries an attested rule-vintage interval |
| Candidate universe | declared: 785 release-contract pairs, 697 of which never traded, all kept in the denominator |
| Exposure graph | built over 155 real contracts; 0 edges; 623 edges structurally admissible and withheld by the rule-vintage requirement alone |
| Calibration | run at 200 repetitions: 8 of 10 declared nulls at 0 promoted of 200 each, recovery 196 of 200, verdict `inconclusive` because two nulls are not estimable |
| Empirical evaluation | not performed: the primary panel has no valid row |
| Scientific conclusion | none claimed |

The primary condition for the plan's blocked branch is met, and the blocking reason
is named exactly. It is not the venue's listing policy and it is not the trading
session: under the declared calendar, predicates, liveness and window rules, **623
edges are structurally admissible** — measured with a diagnostic sentinel that is
explicitly not evidence — and all 623 are withheld because no record attests any
contract's rule version as in force. That is a documentation gap, and it is the
study's binding constraint.

## 2. Corrections to the previous revision of this report

Three claims in the earlier revision are withdrawn. They were measurements of the
wrong object rather than measurements of the archive.

| Withdrawn claim | Why it is withdrawn | Replacement finding |
| --- | --- | --- |
| "Kalshi lists one decision date at a time, so no matched pair is ever simultaneously listed; 0 of 10 events" | The archive's `open_time` is the instant the *previous* meeting resolved, not the listing instant, and the graph had derived its own calendar from the supplied contracts. A calendar derived from the contracts cannot tell a missing predecessor from one that was never handed in, so the builder silently reached back a further meeting. | The declaration now comes from `configs/neighbor_graph_v2.yaml`; 155 contracts carry a readable predicate, and 785 receiver slots reach the rule gate with a live, predicate-identical predecessor |
| "Level contracts fail empirically: 1 of 110 observations complete" | The candidate set omitted the declared `FEDDECISION` series (a `LIKE 'FED-%'` test drops it) and the matched-threshold list was ad hoc rather than declared. | Candidates are the declared four-series grid: 689 contracts, 785 declared pairs |
| "`graph=None`, which is what the local archive produces" | An unsupplied graph is a missing input; a graph that was built and found no admissible donor is a measured absence. Reporting the first as the second reported an unbuilt graph as a result. | An absent graph is `neighbor_graph_not_supplied`; the real graph is built and its per-contract reasons are sealed in `graph_decisions.json` |

## 3. What was measured, and on what

Everything below is a count or an interval over real archive rows. No synthetic
fixture contributes a reported number.

### 3.1 The complete declared panel

Built by `scripts/build_study_panel.py` with the declared per-release grid applied
before the scan, and no row cap.

| Quantity | Value |
| --- | --- |
| Declared policy series | 4 (`FED` 347, `KXFED` 142, `KXFEDDECISION` 130, `FEDDECISION` 70) |
| Declared-series markets | 689 |
| Candidate contracts selected (union, by listing interval only) | 107 |
| Trades extracted | 163,795 |
| Shards read | 16 |
| Extraction bounded | false (`max_rows_applied: null`) |
| Panel rows | 3,925 |
| Declared release-contract pairs | **785** |
| Declared pairs with no trade in the window | 697 |
| Distinct release clusters | 10 of 10 |
| Candidate universe flag | `candidate_universe_from_declared_listing_grid` |
| `rule_verified_pairs` | **0 of 785** |
| `valid_rows` | **0 of 3,925** |
| Panel content hash | `0df1db6a97af472a3a9679551cf4b086317cbb5941a79aec98ceb9d45d2abcae` |

Candidacy uses pre-event information only: a contract is a candidate for a release
when it belongs to a declared policy series and its own recorded listing interval
covers the release instant. The grid is the denominator, so a declared contract that
never traded keeps its masked rows rather than falling out of the universe. The
panel's `candidate_pairs_from_post_release_activity_only: 45` is a coverage fact
about which declared pairs happened to print before the release, not a selection
rule.

### 3.2 Why every row is masked

| Exclusion reason (full lists, from `flags_json`) | Rows (of 3,925) |
| --- | ---: |
| `rule_version_unknown` | 3,925 |
| `missing_baseline` | 3,710 |
| `no_post_release_trade` | 3,672 |
| `baseline_beyond_cap` | 155 |
| `endpoint_beyond_cap` | 155 |

At the primary horizon (300s) the panel carries 785 rows — one per declared pair —
of which **33** have an observed post-release endpoint and **43** an observed
baseline. Rule reasons precede observation reasons in the reason precedence, so the
headline column reads `rule_version_unknown` on all 3,925 rows; the observation
reasons are read from the full list, which is what keeps an unobserved endpoint from
being treated as merely unverified.

Masking applies to the estimand rather than to the observations: a masked row keeps
its two leg prices and has a null `response`. That is why an exploratory measurement
is possible at all, and why it is not the primary result.

### 3.3 Absorption and coverage by release

Primary horizon, 300 seconds.

| Release | Rows | Baseline observed | Endpoint observed |
| --- | ---: | ---: | ---: |
| `cpi_2025_01` | 92 | 0 | 0 |
| `cpi_2025_02` | 82 | 1 | 4 |
| `cpi_2025_03` | 82 | 3 | 1 |
| `cpi_2025_04` | 71 | 7 | 5 |
| `cpi_2025_05` | 60 | 7 | 3 |
| `empsit_2025_01` | 92 | 1 | 5 |
| `empsit_2025_02` | 82 | 2 | 4 |
| `empsit_2025_03` | 82 | 1 | 3 |
| `empsit_2025_04` | 71 | 14 | 4 |
| `empsit_2025_05` | 71 | 7 | 4 |

The January CPI release has no observation at all in this cohort, and its 92
declared pairs stay in the denominator rather than dropping out.

## 4. The primary propagation estimand is unestimable, for one named reason

### 4.1 The graph is built over real contracts

`scripts/build_forecast_panel.py` reads the declared calendar from
`configs/neighbor_graph_v2.yaml`, reads each contract's payout from the venue's own
archived text with `ingest.policy_predicates`, and builds the graph at each release's
forecast origin with the whole measured window.

| Quantity | Value |
| --- | --- |
| Contracts with a readable predicate | 155 |
| Contracts refused | 534 (533 month not dated by the declared calendar, 1 unreadable payout text) |
| Receiver decisions over 10 releases | 1,550 |
| `rule_vintage_unverified` | **785**, all from the receiver's own rule check |
| `resolved_before_forecast_origin` | 480 |
| `receiver_not_open_at_origin` | 285 |
| Edges | **0** |
| Empty-graph digest | `4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945` |

A refusal reads, for example:

```text
no verified rule version is stated to be in force for receiver FED-25DEC-T2.75
across 2025-01-15T13:35:00+00:00 to 2025-01-15T13:40:00+00:00: the rule hash None
and the interval [an unpublished instant, open) are not enough, because a hash binds
the text and only the interval can certify the window
```

That check runs on the receiver before any donor is sought, so the 785 does not by
itself establish that a donor exists. That question was measured separately: the
same graph was rebuilt with the rule requirement satisfied by a **diagnostic
sentinel** that certifies every window. The sentinel is explicitly not evidence
about any contract's rule vintage, and its output is a structural measurement, not
an admissible graph.

| Structural probe (diagnostic, not evidence) | Value |
| --- | --- |
| Edges admitted | **623** |
| `resolved_before_forecast_origin` | 628 |
| `no_matched_threshold` | 14 |
| `receiver_not_open_at_origin` | 285 |

So 623 edges — roughly 62 per release — are admissible under the declared calendar,
predicates, liveness and window rules, and all 623 are withheld by the rule-vintage
requirement alone. Artifact: `.audit/study-v3/structural_probe.json`.

The predicate is read from published text, not from a ticker: `Above 0.25%` on a
title naming the target federal funds rate is `upper_bound_federal_funds_target_rate`
with threshold `0.25`; `Cut 25bps` on a title naming the Fed's meeting is
`target_rate_change_bps` with threshold `-25`. Two contracts are donors only when
every one of `rate_definition`, `threshold`, `inequality`, `yes_axis` and
`orientation` agrees, so a near-match threshold is never substituted and the
mutually exclusive strikes of one meeting are never each other's donor.

Evidence: `.audit/study-v3/graph_decisions.json`.

### 4.2 Rule vintages are not attested

`reports/contract_rule_registry.json` is a specification document
(`status: frozen_local_unregistered`) and carries no per-contract rule record, so no
contract on this archive has an attested rule version. The earlier statement that the
venue does not serve the 2025 contracts was an artefact of using the
current-markets endpoint; the historical endpoint returns HTTP 200 with all five
`KXFEDDECISION-25JAN` contracts and their `rules_primary` text. That is rule text
*read now*, not an in-force interval, and this study is not entitled to infer a
vintage from it — nor from a settlement outcome, nor from a contract's own listing
window. The gate therefore stays blocked, for the correct reason.

### 4.3 The forecast panel

`scripts/build_forecast_panel.py` builds one row per release and declared receiver.

| Quantity | Value |
| --- | --- |
| Forecast rows | 785 |
| Distinct receivers | 107 |
| Releases | 10 |
| Valid rows (recipient increment observed) | 12 |
| Rows with a target | 29 |
| Rows with `own_lag` | 43 |
| Rows with `neighbor_lag` | 0 |
| Content hash | `e57417471b809878ec53a489fd40ffe1f9bd0b99dca551d7af14974d97d6eea0` |

A declared receiver that never traded keeps its masked row; a receiver whose only
prints fall outside the window keeps its masked row too.

### 4.4 The news rung is unestimable

`ingest/expectations.py` implements the point-in-time expectation contract and
refuses, with its own code, a forecast published at or after the release, a forecast
scored against a revision, a unit that disagrees with the release's own declared
statistic, an unnamed consensus, a market-implied value, an incomplete news vector
and altered evidence bytes. No expectation source exists on this checkout, so the
loader raises `expectation_source_is_absent` and the news and network rungs cannot be
fitted. Without the news baseline, a common shock that moved both contracts is
indistinguishable from transmission between them. Source ledger:
`reports/source_feasibility.md`.

### 4.5 What the runner reports

`market-propagation study-external` on the declared panel and the forecast panel:

```text
claim                     absorption_response_in_source_time_transaction_data
claim_not_made            conditional_predictive_propagation
families                  cpi -> blocked, employment -> blocked
propagation.supported     false
forecast_ladder           no_change blocked, own blocked, news blocked, network blocked
common missing columns    delayed_shock, neighbor_lag, neighbor_lag_control, shock
exit code                 2 (ran; blocked result)
```

Every rung is scored on one identical sample, so a column one rung needs removes
those rows from the others too; the common gap is reported apart from each rung's
own. `neighbor_lag` exists as a column and is null on every row, and is reported as
unsupplied rather than fitted as a constant. No rung was fitted on a substitute
column, and no rung was reported complete.

## 5. Calibration of the implemented decision rule

**Run, and `inconclusive`.** The declared calibration is 200 repetitions per primary
scenario, with simulated transaction tapes passed through the same graph,
observation, feature, fitting, tuning, paired-uncertainty and promotion code as real
data, and simultaneous null bounds over the declared primary null scenarios. It has
run at exactly that design, and the intervals below are the ones the rule's own
promotion path produced.

| Quantity | Value |
| --- | --- |
| Declared null scenarios | 10 |
| Estimable nulls | 8, promoted in 0 of 200 repetitions each |
| Null one-sided upper bound, simultaneous level 0.99375 | 0.0251 against a 0.05 ceiling |
| Recovery `communication` | 196 of 200 promoted, rate 0.98, one-sided lower bound 0.9548 against a 0.80 target |
| Verdict | `inconclusive` |

The verdict is inconclusive rather than pass because `resolution_pause` and
`spread_only` are not estimable at any repetition count, and a family bound cannot be
certified over a declaration two of whose nulls contribute no rate. `spread_only`
declares that the latent value does not move, so no shock is recoverable and the
nested comparison has no complete row; `resolution_pause` halts the venue across its
own measured window, so its rows are invalid rather than filled. Each blocked 200 of
200 repetitions on a not-run comparison. The run reports both rather than dropping
them, since dropping them would have widened the bound the surviving nulls are held
to and read as evidence they never supplied.

This certifies the behaviour of the decision rule on a declared synthetic process,
and nothing more: it is not a finding about any real venue, release or contract, and
it does not make the real graph estimable, which still admits no edge.

The earlier 48-repeat `falsification.network_falsification` call is recorded in
`configs/study_v2.yaml` and `configs/neighbor_graph_v2.yaml` as superseded and
insufficient, with both reasons stated: it counts a gain-threshold event on the
quote/simulator path rather than the declared transaction observation process, and at
48 repetitions its one-sided bounds cannot separate a 0.05 false-positive ceiling
from a 0.80 power target. Its own reported failure (false positives 8/48, one-sided
upper 0.2807 against a 0.05 ceiling; recovery 21/48, one-sided lower 0.3150 against a
0.80 target) is retained in `.audit/study-v2/calibration_study_scale.json` as a
record of that call, not as this study's calibration.

## 6. Exploratory absorption measurement

The plan sanctions an exploratory candidate panel when rule evidence is missing.
This one is labelled and bounded.

| Quantity | Value |
| --- | --- |
| Rows with both legs observed | 136 of 3,925 |
| Pairs with both legs observed | 136 of 785 |
| Rows whose only recorded reasons are rule reasons | 47 |
| Rows measurable | **47** |
| Rows with both legs but an observation exclusion | 89 |
| Rows without both legs | 3,789 |

Fitted by `study.run_study`'s exploratory branch with the declared ladder, the
declared ridge grid, whole-release chronological splits and release-clustered
uncertainty:

| Family | Status | Design rows | Releases | Held out |
| --- | --- | ---: | ---: | --- |
| `cpi` | blocked | 0 | 5 | split rejected: needs at least 3 releases, got 2 with a design row |
| `employment` | complete but degenerate | 21 | 3 | 1 release; no release-level interval is identified |

Every employment gain is inconclusive with a null interval, because one held-out
release cannot produce a release-level bootstrap. Row-level resampling is refused
rather than substituted.

**Claim ceiling.** This measures the transaction price change of contracts that are
very likely the intended predicate but whose rule vintage is unverified. It makes no
news claim and no propagation claim, and it is not a substitute for the primary
panel.

## 7. Robustness

Reported, including the analyses that could not be run.

| Analysis | State |
| --- | --- |
| Exclusion accounting by release | run; see section 3.3 |
| Missingness denominators | run; every declared release and every declared pair stays in the denominator |
| Declared-grid enforcement | run; a grid omitting a declared release is refused, exit 1 |
| Altered sealed bytes | run; a one-byte change is rejected by content hash, exit 1 |
| Age-cap sensitivity | **not run**: no valid rows, so no age-cap-sensitive estimate exists |
| Endpoint/tie envelopes | carried on every masked row, unexploited |
| Reversed-edge diagnostic | **not run**: no edge exists in either direction |
| Leave-one-release-out | **not run**: no fitted primary estimate |
| Placebo releases matched on time of day | **not run**: no fitted primary estimate |
| Multi-null Bonferroni certificate | run inside the calibration; simultaneous level 0.99375 over 8 estimable nulls |
| Transaction observation process | run; the calibration's tapes pass through the declared transaction observation path |

The reversed-edge analysis is a diagnostic rather than a null, because feedback and
shared news can produce prediction in either direction. It cannot run here, because
no edge is admissible in either direction.

## 8. What would unblock the study

Requirements, in the order that matters. None is a coding task.

1. **A per-contract rule-vintage record** for each candidate contract: the contract
   id, the sha256 of the rule text, the source it was read from, the method that
   verified it, and the instants bounding which version was in force. The graph
   already consumes exactly that shape, and 623 structurally admissible edges are
   waiting on it, with 785 receivers blocked by their own rule check before a donor
   is even sought. Current rule text, settlement outcomes and a contract's own
   listing window are not admissible substitutes.
2. **A point-in-time expectation source** covering the declared news vector
   (`cpi_headline_sa_mom_pct`, `payrolls_change_thousands`) for each release.
3. **The transaction-tape calibration** at 200 repetitions per primary scenario. Run;
   see section 5. Its verdict stays `inconclusive` until a majority of its declared
   nulls are estimable, which is a property of the simulated scenarios rather than of
   the archive.
4. **More releases.** Ten releases cannot carry a confirmatory claim: the earlier
   calibration call's power figure of 0.44 against a 0.80 target says so, and the
   declared grid's 0.042 observed fraction leaves most cells unobserved. The
   expansion machinery exists in `ingest/macro_releases.py` and the exchange archive
   runs to 2026-01-29, but every added release brings contracts whose rule vintages
   are equally unattested, so requirements 1 and 4 are coupled.

Requirement 1 bounds the study. Until it is met the primary panel stays blocked
however many releases are added.

## 9. Reproduction

```bash
# 1. Declared-grid panel, no row cap (writes the panel, the grid and the trades)
uv run --no-sync python scripts/build_study_panel.py --out .audit/study-v3

# 2. Graph decisions and the source-time forecast panel
uv run --no-sync python scripts/build_forecast_panel.py --out .audit/study-v3

# 3. The same panel through the CLI, from the declared grid
uv run --no-sync python - <<'PY'
import json, importlib.util
spec = importlib.util.spec_from_file_location("bsp", "scripts/build_study_panel.py")
mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
from market_propagation.trade_panel import load_event_specs
cfg = mod._config()
series = {str(n) for n in cfg["policy_series"]}
markets = mod._candidate_markets(series)
events = load_event_specs(cfg["inputs"]["release_dataset"],
                         rule_evidence_path=cfg["inputs"].get("rule_evidence_source"))
grid = {e.event_id: sorted(
    ["kalshi", m["ticker"]] for m in markets
    if (o := mod._instant(m["open_time"])) and (c := mod._instant(m["close_time"]))
    and o <= e.event_time < c) for e in events}
json.dump(grid, open(".audit/acceptance/v2/candidate_grid.json", "w"), indent=1, sort_keys=True)
PY
uv run --no-sync market-propagation build-trade-panel \
  --config configs/external_history_v1.yaml \
  --trades .audit/study-v3/historical_trades.parquet \
  --output .audit/acceptance/v2/panel \
  --candidate-grid .audit/acceptance/v2/candidate_grid.json

# 4. Fit and audit, with the ladder on the forecast panel and a named registry
uv run --no-sync market-propagation study-external \
  --panel .audit/acceptance/v2/panel/trade_panel.parquet \
  --forecast-panel .audit/study-v3/forecast_panel.parquet \
  --registry .audit/acceptance/v2/registry.sqlite3 \
  --output .audit/acceptance/v2/study
```

Steps 1 and 3 produce byte-identical panels
(`0df1db6a97af472a3a9679551cf4b086317cbb5941a79aec98ceb9d45d2abcae`), which is the
cross-path identity check. The registry defaults to the shared
`data/registry/empirical_study.sqlite3`; step 4 names its own so the acceptance run
does not write into the checkout's ledger. No test release was reserved: reservation
is the act that opens a locked empirical evaluation, and no evaluation was opened
because the primary panel has no valid row. The exploratory fit's internal
chronological holdout is not a registry reservation and is not counted as one.

## 10. Claim ledger

| Claim | Status | Artifact |
| --- | --- | --- |
| The pipeline runs end to end on real archives | supported | `.audit/acceptance/v2/panel.json`, `.audit/acceptance/v2/study.json` |
| The complete declared grid is the denominator | supported | 785 pairs, 697 with no window trade, `declared_listing_grid` |
| The declared cohort is extracted without a row cap | supported | 163,795 trades, `bounded: false` |
| The candidate universe was not selected by post-event activity | supported | `candidate_universe_from_declared_listing_grid` |
| The exposure graph is built over real contracts with read predicates | supported | 155 predicates, `.audit/study-v3/graph_decisions.json` |
| The primary graph is blocked for want of rule-vintage evidence | supported | 785 `rule_vintage_unverified` refusals, 0 edges, `.audit/study-v3/graph_decisions.json` |
| The rule gap has a measured size | supported | 623 structurally admissible edges under a labelled diagnostic sentinel, `.audit/study-v3/structural_probe.json` |
| The primary panel is blocked for want of rule evidence | supported | 0 of 785 `rule_verified_pairs`, 0 valid rows |
| The news rung is unestimable | supported | `expectation_source_is_absent`, `reports/source_feasibility.md` |
| The ladder is blocked rather than fitted on a substitute | supported | four blocked rungs with named columns |
| The decision rule is calibrated on a synthetic process, with an inconclusive verdict | supported | 8 of 10 nulls at 0 of 200; recovery 196 of 200; two nulls not estimable; `calibration.status: measured` in `configs/study_v2.yaml` |
| Absorption responds to the release | **not claimed** | exploratory only, degenerate |
| Information diffuses between policy-rate contracts | **not claimed** | no admissible edge |

The last two rows are the point of the exercise. Neither is asserted, and neither is
replaced by a weaker claim dressed as the same one.
