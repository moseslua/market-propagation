# Empirical dependencies

Date: 17 September 2026. Acquisition follow-through added after the full end-to-end run.

This file lists the **external inputs the studies require and cannot produce from what is
already on this checkout**. It is a dependency ledger, not a plan, not a result, and not
evidence about any contract, release or venue.

## Acquisition follow-through

The persistent local collector completed a real cycle: 163 rule captures and
attested contracts, 9,784 Polymarket metadata records, and 163 market rows with
61,013 trades. Receipts and raw-byte manifests are under
`data/prospective/acquisition/`; rules are under `data/prospective/rules/`.
The refreshed forward progress ledger covers 706 of 706 candidate windows across
six future releases. All six releases remain pending publication. This is evidence
of acquisition and rule-coverage prerequisites, not a confirmatory sample.

| Dependency | Measured follow-through | Remaining limit |
| --- | --- | --- |
| D1/D2 historical | Queue retains 785 pairs, 107 contracts and 2,355 explicit field/interval gaps; current primary market, event, series and linked-document responses archived | 0 historical windows attested; exact discovered market/event URL archive probes returned HTTP 503 |
| D3a | All 10 original BLS releases replay from verified raw bytes; five report revisions | ALFRED/FRED direct retrieval timed out; the alternate reader returned the form explanation, not a vintage export |
| D3b | Public pages from Trading Economics, Investing.com, Econoday and Philadelphia Fed archived | No historical monthly consensus with demonstrable pre-release publication validated in this bounded probe |
| D5(b) | Official Bybit fee tiers parsed from saved HTML; BTCUSDT depth and contract specifications acquired for Bybit and Binance | Binance fee pages were empty directly and verification pages through the alternate reader; account tier, region, effective intervals and future execution remain unverified |

Source probes and their hashes are in `.audit/evidence-recovery/`,
`.audit/evidence-acquisition/sources-20260917/`, `.audit/expectation-recovery/`, and
`.audit/perp-cost-recovery/`. See [future research directions](future_research_directions.md)
for replay commands and the precise conditional fee example. The existing protocol
seal still verifies against all 31 covered files. No statistical gate was relaxed.

The remaining tables below retain the earlier end-to-end snapshot. Their use of
"fresh" refers to that run; the acquisition changes above are a separate observation.

## What this refresh re-measured, and what it carries forward

The states marked **fresh** below were measured in a full pipeline run on this date:
`normalize-external` → `coverage-external` → `build_study_panel.py` → `absorption-panel`
→ `study-external` → `report-external`, plus `match-cross-venue` and the perp
differentials report. Their artifacts are under `.audit/e2e/`.

Everything else — the acquisition routes, the channel reachability table, the D1
provenance work — is **carried forward** from the earlier ledger. It was not re-tested by
this run, and it is not asserted here as freshly measured.

Two states moved, and one is material:

| Row | Before | Now |
| --- | --- | --- |
| **D5 (a)** second distinct perp build | **absent** — 1 build, 24 assets | **satisfied** — 17 builds, 43 assets |
| **D1** rule vintage for the studied cohort | absent — 0 of 785 | absent — 0 of 785 **(unchanged, reproduced)** |

D5(b) cannot be completed from the PerpDexList source alone. The additional venue
fee and depth observations above provide partial inputs, with named limits.

## The pipeline's own list of what is unmet

This is the same dependency set as the system reports it, which is the least
interpretable form. Source: `.audit/e2e/report/external_report.json` and
`.audit/e2e/coverage/coverage_external.json`.

**Coverage gates** (G0 `blocked`):

| Gate | Satisfied | Source | Reason |
| --- | --- | --- | --- |
| `rule_gate` | **no** | external_coverage_grid | no candidate carries a rule version verified against its own rule document |
| `rule_vintage_gate` | **no** | not_established | not established by any artifact |
| `source_semantics_gate` | **no** | not_established | not established by any artifact |
| `supported_frequency_gate` | yes | pipeline_configuration | declared horizons 60–3600 s |

`unsatisfied_scientific_gates`: `rule_vintage_gate`, `source_semantics_gate`.

**Report blockers** (all eight named, not summarised):

| Code | Scope | Gate-blocking | What it withholds |
| --- | --- | --- | --- |
| `no_governed_valid_row` | measurement | **yes** | no valid row exists, so no response is reported and none is substituted from a coarser window |
| `panel_reports_no_response` | measurement | **yes** | every governed row is masked, so the blocked measurement is recorded rather than an estimate |
| `rows_masked_by_reason` | measurement | no | `rule_version_unknown` = 3,925 |
| `rule_evidence_missing` | evidence | no | no row carries a verified rule vintage; primary cohort eligibility and G0 rest on no rule evidence |
| `pair_window_counts_not_joined` | evidence | no | the joined contract coverage grid is absent, so pair-level window counts are not computed |
| `confirmatory_estimation_not_permitted` | analysis | no | `registered_estimation.confirmatory_estimation_permitted` is false |
| `receipt_clock_unavailable` | capability | no | availability is `['source_time_only']`, which is not receipt evidence |
| `quantity_weighting_unavailable` | capability | no | 253 rows carry a verified source quantity, a contract count on its own axis; the cleaned Polymarket layers omit quantity entirely |

**Capabilities** (observed): `historical_trades` **true** (panel verified, 3,925 rows),
`initial_release_verified` **true** (declared), and **false**: `rule_vintage_verified`,
`expectation_verified`, `economic_size_verified`, `historical_quotes`, `receipt_clock`.

> Clock caveat, verbatim from the report: *source-clock alignment is retrospective event
> alignment over recorded venue or block times; it is not evidence of what a live
> participant knew.*

## Ordering

The order below is the order that matters, not a priority list. **D1 bounds the
programme:** every other dependency can be satisfied and the primary estimands stay
unestimable until D1 is met. D2 gates D1 for the studied cohort, so it is a prerequisite
of D1 rather than a substitute for it. D3, D5 and D6 are independent of D1 in both
directions. D4 and D7 are coupled to D1 and are worthless before it. D8 is not a
dependency that can be satisfied at all; it records what stays unclaimed even when every
other row is met.

| Id | Dependency | Blocks | State |
| --- | --- | --- | --- |
| D1 | Per-contract rule-vintage record | A, C (and B once matched) | **absent** — 0 of 785 |
| D2 | Observation overlap: universe observable before its own window | D1 for the studied cohort | **not satisfied for the retrospective cohort** — the union is wired, so the remaining reason is the ordering: a capture cannot precede a 2025 window |
| D3 | Point-in-time expectation source | news and network rungs (A, C) | **absent** — provider not present |
| D4 | Cross-venue matched instrument live at a declared release instant | B | **partially satisfied** — the parser is written and reads 63 of 229 records with every component; 0 of 10 matched, because every readable pair is blocked on the first venue's unobserved components |
| D5 | (a) Second distinct perp build | D | **(a) satisfied** — 17 builds, 43 assets |
| D5 | (b) Observable execution-cost layer | D | **partial** — Bybit base fees and both venue books/specifications acquired; Binance fees and execution assumptions remain unverified |
| D6 | Estimable declared null scenarios | calibration verdict | **satisfied** — 10 of 10 estimable, verdict `pass` |
| D7 | More releases, and observed endpoints on declared pairs | power for any confirmatory claim | **insufficient** — 33 of 785 endpoints at h=300 s; the declared schedule is now verified against the BLS calendar, the endpoint coverage is what remains |
| D8 | A confirmatory sample | the two claims of interest | **not claimable** — exploratory only, degenerate |

---

## D1. A per-contract rule-vintage record, for every candidate contract

**Required, per contract.** Eight fields, exactly the set the graph consumes:
`contract_id`, `rule_hash`, `source_url`, `verified_by`, `in_force_from`, `in_force_to`,
`observed_at`, `settlement_semantics`.

**Why it is required.** A rule hash binds a verdict to exact text; only an interval can
certify a window. The exposure graph and the absorption panel both ask "which version of
this contract's rule text was in force over this window", and neither can answer it from
a digest alone.

**Measured state (fresh — reproduced exactly).**

| Quantity | Value |
| --- | --- |
| `rule_verified_pairs` | **0 of 785** |
| Panel rows / valid rows | 3,925 / **0** |
| Rows excluded, by reason | `rule_version_unknown` **3,925** (every row) |
| `blocked_rule_candidates` on the coverage grid | **400** |
| Observation fraction | 0.0420 |
| Report flags | `no_valid_panel_rows_so_absorption_is_measured_on_masked_rows_only`, `propagation_rung_blocked_inputs_absent` |

Every row is present and every row is unusable. The rows exist; the rule evidence that
would admit them does not.

**Current evidence source is a specification, not records.**
`reports/contract_rule_registry.json` carries `status: frozen_local_unregistered`,
`frozen_on: 2026-09-13`, `external_registration: none_claimed` and no per-contract rule
record. It is named by configuration three ways:

| Configuration key | File | Records |
| --- | --- | --- |
| `rule_vintage.evidence_source` | `configs/neighbor_graph_v2.yaml` | `evidence_source_records_rule_versions: false` |
| `evidence_source` | `configs/cohort_v2.yaml` | `evidence_source_records_rule_versions: false`, `status: absent` |
| `inputs.rule_evidence_source` | `configs/external_history_v1.yaml` | no version flag; absent evidence leaves a rule-covered candidate out of the primary cohort |

**Where to get it (carried forward).** No admissible retrospective source has been
established by the bounded searches performed here. That is not a proof that no such
source exists. The live index is
`GET https://api.elections.kalshi.com/trade-api/v2/series/{TICKER}`, which returns a
`contract_url` and a `contract_terms_url` per series. That yields two stable asset URLs
per declared series:

| URL | Carries |
| --- | --- |
| `assets.kalshi.com/contract_terms/{SERIES}.pdf` | the live Terms and Conditions for the product |
| `assets.kalshi.com/regulatory/product-certifications/{SERIES}.pdf` | the venue's own copy of its CFTC 40.2(a) filing |

A product-template document is versionless: **neither document states its own effective
date**, so it cannot bound a contract-level window without a dated archive of the URL. The
Wayback prefix queries `assets.kalshi.com/contract_terms/FED*` and
`assets.kalshi.com/regulatory/product-certifications/FED*` were exhaustive and returned
only a mention-type product and nothing respectively. Those results apply to the
queried prefixes only. The later exact-URL market/event queries returned HTTP 503,
which is an access failure. **Forward capture is the working route** on this checkout;
historical recovery remains contingent on an admissible dated source.

**Inadmissible substitutes** — refused by name: `open_time`, `close_time`, `created_time`,
`updated_time`, `settlement_ts`, a capture instant, current rule text read after the
window, a settlement outcome, and a contract's own listing window. A trade-history join
cannot establish it either; the report says so in `rule_evidence_missing`.

---

## D2. Observation overlap: the studied contracts must be observable before their own window

**Required.** A rule record whose interval opens at instant $t$ certifies a window
$[t, \cdot)$ and refuses every instant before $t$. A contract whose window closed *before*
the observation cannot be certified by that observation, however good the observation is.

**Measured state (fresh).** The mechanism works. `capture-rules` fetched the venue's own
live listing and `attest-rules` emitted **163 records for 163 live contracts** across the
four declared series, each with `in_force_from` equal to the instant the serving system
stated and `in_force_to: null`, **0 refusals, 326 captures held**. Every one of those
records refuses the 2025 windows — correctly, by construction. The gap is not the
mechanism but the overlap, and on this checkout the union does not move it: the captured
contracts are already archived ones, so the union and the archive-only read coincide over
the retrospective cohort, and what the capture can certify is still a window after the
archive's own last row.

**Two consequences before D1 can deliver for the studied cohort.**

1. **The universe now includes contracts observed live. Settled, and wired rather than
   deferred.** The contract universe is the union of the two declared observation paths —
   `kalshi_own_markets`, this repository's own capture of the venue's live listing, and
   `kalshi_markets`, the vendor archive — defined once in
   `src/market_propagation/ingest/kalshi_universe.py` and read from there by the graph
   builder, the study and forecast panels, the cross-venue matcher and the CLI.

   It was not done quietly, because it is not a change of population: the membership rule
   the preregistration already declares — a contract is a candidate when its series is one
   of the four declared policy series and its own recorded listing interval covers the
   release instant — is observation-source agnostic, so the archive-only implementation had
   been **narrower than the frozen declaration**. The union is an
   implementation-conformance fix, and that is why the retrospective arm's declared
   population does not move.

   Measured on this checkout, the union holds **689 contracts**, split **526
   `archived_only`, 0 `live_only`, 163 `archived_and_live`**. The zero `live_only` count is
   why the change is currently inert for the retrospective 2025 cohort. It is not inert
   forward: the vendor archive's rows end 2026-01-29, so the forward arm's windows can only
   be covered by this repository's own capture, and under an archive-only universe those
   windows would have no candidate contracts at all.

   A contract observed through either path is a candidate. Each row carries which path or
   paths observed it, membership is never conditioned on presence in the vendor archive,
   and the module decides no eligibility of its own, so `study_eligible` and the
   rule-vintage gate are unchanged. The cohort decision itself is recorded in
   `reports/population_change_d2.md`.
2. The graph's loader now admits a record whose `in_force_to` is null, so the
   `"in_force_to": null` shape this pipeline writes is read as an open end. A field named
   in that module's `OPEN_ENDED_RECORD_FIELDS` must still be *stated*; an absent end is
   refused, because an unstated end and a stated open end are different facts.

The observation run is grouped on the digest of the captured **rule text** rather than on
the digest of the archived page, so a re-fetch whose live fields moved while the rule text
did not no longer closes the interval. The measurement that motivated the change stands:
one `KXFED` page was 122,578 bytes and 122,577 bytes roughly 41 seconds apart, same
contracts, same rule text.

**Where to get it.** No external channel exists. It is satisfied by which universe the
graph reads and by where the cadence is run.

---

## D3. A point-in-time expectation source covering the declared news vector

**Required.** For each release, a forecast published **at or before** the release instant,
scored against that release's own declared statistic, in the declared units, from a named
consensus, not market-implied. The declared vector is `cpi_headline_sa_mom_pct` and
`payrolls_change_thousands`.

**Why it is required.** Without a news baseline, a common shock that moved two contracts
is indistinguishable from transmission between them. Structural, not a matter of degree.

**Measured state (fresh).** No expectation source exists on this checkout. The report
records `expectation_verified: false` with the reason that no expectation column exists in
the panel — and that a missing expectation is never a zero-valued surprise.
`load_expectations` raises `expectation_source_is_absent`, and the `news` and `network`
rungs are blocked with the declared columns named (`delayed_shock`, `neighbor_lag`,
`neighbor_lag_control`, `shock`). The propagation rung is flagged
`propagation_rung_blocked_inputs_absent`.

`ingest/expectations.py` already refuses, each with its own code: a forecast published at
or after the release, one scored against a revision, a unit disagreeing with the release's
own declared statistic, an unnamed consensus, a market-implied value, an incomplete news
vector, and altered evidence bytes. The contract is implemented and waiting on a provider.

**Where to get it — re-probed this session; the revision half has a working surface.** Two
halves, and only one is reachable for free from here. The revision half needs no credential,
and its surfaces were re-probed rather than carried forward:

| Endpoint | Result |
| --- | --- |
| `fred.stlouisfed.org/graph/fredgraph.csv?id=…` | works — observations directly |
| `alfred.stlouisfed.org/series/downloaddata?seid=CPIAUCSL` | **works** — the vintage-selection form, naming the series and its first vintage |
| `alfred.stlouisfed.org/series?seid=…` | works — HTML carrying the revision table |
| `alfred.stlouisfed.org/graph/fredgraph.csv?id=…&vintage_date=…` | HTTP **404**, on three spellings: bare `vintage_date`, with `cosd`/`coed`, and the plain form |

So the vintage half is reachable through ALFRED's own **form** and not through a
`vintage_date=` URL parameter. That is a concrete prerequisite rather than an open question:
a vintage read has to submit the form's own request shape, and three parameter spellings of
the CSV route are refused. None of this is missing a credential. The consensus half still
has no tested free source; candidates are Econoday, Trading Economics, the Investing.com
calendar and the Philadelphia Fed SPF at quarterly frequency. A market-implied value is
refused by the declared standard and cannot serve.

---

## D4. A cross-venue matched instrument live at a declared release instant

**Required.** A matched event × venue pair whose two sides carry the same payout semantics
under `all_required_fields_must_match`. Ticker resemblance is a lead only.

**Measured state (fresh).**

| Quantity | Value |
| --- | --- |
| Candidate pairs formed | 157,781 |
| **EXACT / ECONOMICALLY_EQUIVALENT / APPROXIMATE** | **0 / 0 / 0** |
| REJECT | **157,781** |
| Primary analysis pairs | **0** |
| Venue records supplied | Kalshi 689, Polymarket 229 |
| Venue records **readable** | Kalshi **377**, Polymarket **63** |
| Pairs refused for want of a declared parser | **0** (was 157,781) |

The second venue's parser is **written and declared**, and the refusal that named its
absence is gone. `configs/matching_v1.yaml` declares
`parser: declared_second_venue_market_text` over
`payout_text_fields: [question, description]`, and the grammar reads the venue's own market
text and never its slug — which `tests/test_polymarket_predicates.py` pins by handing the
grammar a record whose slug states the strike and the meeting perfectly, with both text
fields unreadable, and requiring a refusal.

The 63 readable records carry **every component**, including the `reference_period` and
`settlement_criterion` that the first venue's path has to receive from its caller. The
remaining 166 candidates refuse by name, and each code names one fact:

| Refusal (per record) | Records |
| --- | --- |
| `description_states_no_readable_settlement_subject` | 119 |
| `contract_month_is_not_dated_by_the_declared_calendar` | 19 |
| `market_text_states_no_payoff_direction_for_the_yes_side` | 13 |
| `published_contract_text_states_no_readable_payout` | 6 |
| `stated_thresholds_disagree_across_the_market_record` | 5 |
| `venue_text_states_no_readable_meeting_reference_period` | 4 |

**No pair grades a match, and the binding reason is the *first* venue.** Of the 157,781
pairs, **23,751** are refused for a required component being unobserved — and
`377 × 63 = 23,751` exactly, so that is *every* pair of two readable contracts and no
other. The unobserved components are `reference_period` (377 records) and
`settlement_criterion` (377 records), and they are unobserved **only on Kalshi's side**:
`cross_venue.first_venue_reads` passes both as `None` because the venue's own market record
publishes neither, while the second venue's 63 reads carry both. The next-largest refusals
are `reference_horizon_differs` (22,766) and `underlying_economic_event_differs` (15,882),
which are genuine component differences between the two venues' claims.

The dependency's shape has therefore changed rather than closed. It is **no longer a missing
parser**; it is now exactly the coupling recorded below, measured. A matched pair inherits
the per-contract vintage requirement, and the rule records that would carry
`settlement_criterion` and `reference_period` are the same records D1 needs. **A perfect
second-venue grammar cannot produce a match while the first venue's reads are unobserved on
two required components.**

**Candidate selection is a rule, not evidence, and its identity moved to the venue's key.**
From the registry: `records_available: 163,289`,
`records_matching_the_declared_pattern: 229`, `candidates_supplied: 229`,
`cap_applied_to: contract_identity`, `cap_hid_records: false`,
`pattern_is_a_selection_rule_and_not_evidence: true`. A candidate is now **identified** by
`condition_id` and **selected** by `market_slug`, declared as two separate fields because a
name and a key are different things and a lookup by name would be a lookup by the very field
this layer refuses to read a predicate from. The 229 is a keyword-selected set, so the
157,781-pair denominator is a declared selection and not a market universe — and it is
**unchanged** by that identity move, which is what makes the change a conformance fix
rather than a population change.

**Coupling.** Closing D4 alone does not make study B runnable, and the measurement above now
shows why in numbers rather than in principle: a matched pair inherits the same per-contract
vintage requirement, so D4 without D1 leaves B blocked on D1 — which is precisely the state
this checkout is in.

**Where to get it — acquired, and how.** The requirement text is
`gamma-api.polymarket.com/public-search?q=…`, the only measured route that answers for a
settled market: `GET /markets?condition_ids=<id>` and `GET /markets?slug=<slug>` both
returned `[]` for a market the search route returns in full, so no reader may be handed one
of them as a fallback. The sweep is declared in `configs/matching_v1.yaml` under the venue's
own `metadata_acquisition` block — six queries in the venue's vocabulary, a page bound and a
record bound, each query carrying why it is in the universe — and it held **9,784 records
across 25 pages, every one carrying the instant the serving system stated and the hash of
the page bytes it was read from**, covering **229 of 229** declared candidates. The captured
bytes are acquired data rather than repository sources and are excluded by `.gitignore`.

---

## D5(a). A second distinct perp build — **satisfied by collection**

**Required.** At least two distinct builds, because persistence, absorption speed and
competitive decay are build-to-build quantities and a single cross-section has no second
observation.

**Measured state (fresh).** Collection has closed this.

| Quantity | Value |
| --- | --- |
| Distinct source build stamps | **17** |
| Assets held | **43** |
| Asset × build pairs | 320 |
| Assets observed at more than one build | **43 of 43** |
| Assets with at least one admissible spread | **43 of 43** |
| Funding differentials formed | **207,543** |
| Cross-venue basis pairs formed | **202,434** |
| Sign-stable across observations (sampled) | **true** |

Persistence is now a measurable quantity rather than an unavailable one. For example:

| Asset | Builds held | Admissible spreads | Median spread (APR) | Sign stable |
| --- | --- | --- | --- | --- |
| `crypto/ADA` | 8 | 4,488 | 0.1489 | true |
| `crypto/AIN` | 8 | 224 | 1.9042 | true |
| `crypto/AKE` | 8 | 624 | 0.27015 | true |

**What this does and does not license.** Gross quoted spreads persist with a stable sign
across builds for the assets held. That is a statement about **quoted** spreads and venue
price levels. It is not a net return, not a cost estimate, not slippage, and not a
capacity figure — see D5(b).

## D5(b). An observable execution-cost layer — **partially observed**

**Measured state (fresh).** Every differential carries the cost refusal:

| Refusal | Differentials |
| --- | --- |
| `execution_cost_not_observable_from_this_source` | **207,543** (all) |
| `funding_interval_not_derivable` | 18,132 |
| `long_interval:funding_interval_not_derivable` | 9,510 |
| `short_interval:funding_interval_not_derivable` | 4,008 |
| `long_interval:funding_interval_derivations_disagree` | 2,944 |
| `short_interval:funding_interval_derivations_disagree` | 2,308 |

The report's own claim limits, which are the load-bearing part:

- `differentials_within_one_build_are_simultaneous: true`
- `differentials_across_builds_are_not_simultaneous: true`
- `net_return_or_capacity_claimed: **false**`
- the cost layer is not observable from this source, so **every spread here is a quoted
  spread and not an executable opportunity**

PerpDexList collection alone cannot remove this refusal. The cost inputs must come
from the relevant perpetual-futures venues; the previously cited Kalshi fee paths
were unrelated to this dependency.

The follow-through acquired Bybit's official base fee table and both venues'
BTCUSDT depth/specifications. `replay_perp_cost_evidence.py` parses the saved table,
checks contract and lot compatibility, and replays a matched 0.131 BTC quantity
against each saved book. The known component is 11.0243659150 USDT, including
10.9981659150 USDT of assumed Bybit VIP 0 taker fees, plus an unknown Binance fee.
Books were fetched sequentially; exits, realized fills and account-specific rates
were not observed. This is conditional frozen-book arithmetic, not a future-cost
bound, executable opportunity, net return, or capacity estimate.

**Independence.** Study D's blocker is unrelated to D1–D4. More rule work does not help
here, and more collection does not help there.

---

## D6. Estimable declared null scenarios, for the calibration verdict

**Required.** A majority of the declared primary null scenarios must contribute a
promotion rate, so a family-wise simultaneous bound can be certified over them.

**Measured state (carried forward).** The calibration ran at exactly the declared design
and returned **`pass`**, with an empty `verdict_reasons` list. The design dependency is
**closed** by changing the declared scenario definitions themselves: all ten nulls are now
estimable, and each contributes a complete nested comparison row.

| Quantity | Value |
| --- | --- |
| Repetitions per primary scenario | 200 |
| Releases per repetition | 120 |
| Bootstrap samples / base seed | 200 / `20260913` |
| Declared nulls / estimable nulls | 10 / **10**, each promoted **0 of 200** |
| Null one-sided upper bound, simultaneous level 0.995 | **0.02614** against a 0.05 ceiling |
| Recovery `communication` | **196 of 200**, rate 0.98, lower bound 0.9548 against an 0.80 target |
| Verdict | **`pass`** |

Certificate: `data/calibration/calibration_certificate.json`; registry record
`calibration-2856211b642b-bd7e5e807f5c`. The ten estimable nulls, sorted:
`coarse_sampling`, `dropped_messages`, `heterogeneous_sensitivity`, `later_reversal`,
`omitted_shock`, `opposing_sign`, `resolution_pause`, `rule_mismatch`,
`shared_news_delay`, `spread_only`.

Two declared nulls previously produced **no comparison row at all**, blocking 200 of 200
repetitions and leaving the family bound uncertifiable. Both causes were defects in the
scenarios' own declarations, and both are fixed:

1. **`spread_only`** declares `news_active=False`, so every contract's sensitivity is 0.
   `simulated_release_shocks` recovered the generator's per-release common shock as
   `latent / (orientation * strength)` and skipped any event whose strength was falsy, so
   it returned an **empty mapping**; the ladder's `shock` and `delayed_shock` columns were
   then filled with nulls and `nested_comparison` found no complete row. An event whose
   roles all carry a declared zero sensitivity now receives an explicit `0.0` shock,
   because a declared zero is an exact value and not a missing measurement.
2. **`resolution_pause`** declared `pause=(300.0, 900.0)`. The calibration's declared
   forecast settings are `forecast_origin_seconds=300` and `future_horizon_seconds=300`,
   so every primary row's window is `[event+300s, event+600s]` — entirely inside that
   halt, which marked every row halted with a null target. The declared halt is now
   `(700.0, 1000.0)`, which opens after the primary window closes at +600 s, so the halt
   still invalidates every window that spans it without consuming all of them.

**Where to get it.** This dependency **no longer blocks**, and it was never an
acquisition: it was a **design** dependency, closed by the declared scenario-definition
changes above rather than by fetching anything. It remains a statement about the decision
rule on a **synthetic process** — the calibration is not evidence about any real contract,
release or venue, and it does not make the real graph estimable.

---

## D7. More releases, and observed endpoints on the declared pairs

**Required.** (a) More than ten releases, if a confirmatory claim is ever intended.
(b) Observed post-release endpoints and baselines on enough declared pairs for an estimand
to exist even after D1 is met.

**Measured state (fresh — reproduced).** Of the 785 rows at the primary horizon (300 s),
**33** have an observed post-release endpoint and **43** an observed baseline. 697 pairs
have no trade in any declared window. Observed fraction **0.0420**. October's release has
no observation at all in this cohort, and its 92 declared pairs stay in the denominator
rather than dropping out.

The exploratory measurement, on masked rows only, counted 47 measurable rows of 3,925 and
136 rows with both legs — which is why the fit reports both families blocked and flags
`no_valid_panel_rows_so_absorption_is_measured_on_masked_rows_only`.

**Where to get it — the schedule is verified, and the declared arm already conforms to it.**
The first tranche needs no new source: the exchange trade archive already runs to
2026-01-29, so releases from 2025-06 through 2026-01 are inside the warehouse the extractor
already reads, and `ingest/macro_releases.py` holds the expansion machinery. What they need
is a release schedule to declare against, and that authority was probed in this session
rather than named as intended:

| Endpoint | Result |
| --- | --- |
| `www.bls.gov/schedule/news_release/cpi.htm` | works — the CPI schedule table: reference month, release date, 08:30 AM |
| `www.bls.gov/schedule/news_release/empsit.htm` | works — the Employment Situation schedule |
| `www.bls.gov/schedule/news_release/bls.ics` | offered by both pages as the machine-readable calendar |

Read on 2026-09-17, the two tables carry **September through November 2026**: CPI on Oct.
14, Nov. 10 and Dec. 10, and Employment Situation on Oct. 2, Nov. 6 and Dec. 4, all stated
as 08:30 AM America/New_York. The declared forward arm's six events **match all six of
those instants exactly**, including the EDT/EST changeover: October's 08:30 EDT is declared
as `12:30Z` and November's and December's 08:30 EST as `13:30Z`, which is what a naive
fixed-offset reading would have got wrong. So this arm's schedule is verified against the
authority that publishes it, and the extension rule has a reachable source rather than an
intended one. Reading them confirms the declaration; it adds no release. The
endpoint-coverage half is still not a source at all.

**Coupling with D1.** Every added release brings contracts whose rule vintages are equally
unattested, so D7 is worthless before D1.

---

## D8. A confirmatory sample, which no dependency can create

This row is not an acquisition target. Its purpose is to stop the rows above being read as
a route to the two claims of interest. Even with D1, D3 and D7 satisfied, both stay
**not claimed**:

| Claim | Status |
| --- | --- |
| Absorption responds to the release | **not claimed** — exploratory only, degenerate |
| Information diffuses between policy-rate contracts | **not claimed** — no admissible edge |

Neither is asserted, and neither may be replaced by a weaker claim dressed as the same
one. The report's `confirmatory_estimation_not_permitted` blocker says the same thing in
the artifact: `confirmatory_estimation_permitted` is false, so the report describes
observed rows and reports no confirmatory estimate.

---

## Channel reachability (carried forward — tested earlier, not re-tested in this run)

Every state in this file that names a source was produced by fetching it at the time it
was recorded, not by assuming it. The results are kept here so the next reader does not
re-test a settled question.

| Endpoint | Result |
| --- | --- |
| `api.elections.kalshi.com/trade-api/v2/series/{TICKER}` | works — JSON metadata plus both asset URLs |
| `assets.kalshi.com/contract_terms/{SERIES}.pdf` | works — full Terms and Conditions |
| `assets.kalshi.com/regulatory/product-certifications/{SERIES}.pdf` | works — dated CFTC filing |
| `fred.stlouisfed.org/graph/fredgraph.csv?id=…` | works — key-less CSV |
| `www.bls.gov/schedule/news_release/cpi.htm` | works — the official CPI release schedule, with date and 08:30 AM local time |
| `www.bls.gov/schedule/news_release/empsit.htm` | works — the official Employment Situation release schedule |
| `alfred.stlouisfed.org/series?seid=…` | works — HTML carrying the revision table |
| `alfred.stlouisfed.org/series/downloaddata?seid=…` | works — the vintage-selection form (the `graph/fredgraph.csv?vintage_date=` route is 404 on three spellings) |
| `docs.kalshi.com/getting_started/fee_schedule` | HTTP **404** — no fee schedule at that path |
| `kalshi.com/docs/kalshi-fee-schedule.pdf` | HTTP **429** on two attempts — that host refuses this egress, so the venue fee schedule is unreadable from here rather than absent |
| `web.archive.org/cdx/search/cdx` | works — JSON enumeration (`matchType`, `collapse`) |
| `web.archive.org/web/{ts}id_/{url}` | works — raw archived documents retrieved |
| `www.cftc.gov/…/TradingOrganizationProducts` | loads, but rows are client-rendered: not enumerable |
| `gamma-api.polymarket.com` | **works on retry** — timed out earlier in the same session |
| `fapi.binance.com`, `api.bybit.com` | **work on retry** — both timed out earlier in the same session |
| `archive.org/wayback/available` | HTTP **429** |
| `alfred.stlouisfed.org/graph/fredgraph.csv?vintage_date=…` | HTTP **404** |
| `kalshi.com/regulatory/notices`, `kalshi.com/regulatory/filings` | HTTP **429** |

A timeout is not evidence of absence. All three hosts that timed out answered on retry, so
this egress drops connections rather than blocking hosts. Retry before recording anything
as unreachable. The absences recorded for the Fed-series terms and certification documents
are different in kind: they come from exhaustive prefix queries against a host that was
answering.

## Coupling summary

- **D1 bounds everything.** Until a per-contract rule-vintage record exists for the studied
  cohort, the primary panel stays blocked however many releases are added.
- **D2 gates D1** for the 2025 cohort specifically. The mechanism exists; the overlap does
  not.
- **D3 is independent of D1 in both directions.** A complete rule record leaves a common
  shock indistinguishable from transmission; a complete news vector leaves every receiver
  refused on its own rule check.
- **D4 is worthless before D1**, because a matched pair inherits the same vintage
  requirement. Now measured rather than inferred: all **23,751** refusals on a readable pair
  are the first venue's unobserved `reference_period` and `settlement_criterion`
  (`377 × 63`), so the second venue's written and declared grammar cannot produce a match
  until D1's rule records exist.
- **D7 is worthless before D1**, for the same reason, and D1 is worthless for a
  confirmatory claim without D7's power.
- **D5(a) is now satisfied and D5(b) is not**, and they are independent of D1–D4, D6 and
  D7 in both directions.
- **D6 is a design dependency**, not a data one — and it is now **satisfied**: all ten
  declared nulls are estimable and the calibration verdict is a `pass`. It remains a
  statement about the rule on a synthetic process, not evidence about any contract.
- **D8 is not satisfiable.** It states that the two claims stay unclaimed even with D1, D3
  and D7 met.

## Artifacts read for this refresh

`.audit/e2e/trades/historical_trades.parquet.manifest.json`,
`.audit/e2e/studypanel/study_panel_summary.json`,
`.audit/e2e/coverage/coverage_external.json`,
`.audit/e2e/absorption-fresh`, `.audit/e2e/study-fresh/study_result.json`,
`.audit/e2e/report/external_report.json`, `.audit/e2e/match`,
`.audit/e2e/perp/perp_differentials.json`, `.audit/e2e/attest/`,
`.audit/e2e/decisions.tsv`.
