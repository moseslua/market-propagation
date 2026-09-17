# Empirical dependencies

Date: 17 September 2026. Refreshed from a full end-to-end run.

This file lists the **external inputs the studies require and cannot produce from what is
already on this checkout**. It is a dependency ledger, not a plan, not a result, and not
evidence about any contract, release or venue.

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

D5(b), the observable execution-cost layer, is **unchanged and cannot be moved by
collection**.

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
| D2 | Observation overlap: universe observable before its own window | D1 for the studied cohort | **not satisfied** — capture route covers live contracts only |
| D3 | Point-in-time expectation source | news and network rungs (A, C) | **absent** — provider not present |
| D4 | Cross-venue matched instrument live at a declared release instant | B | **absent** — 0 of 10 |
| D5 | (a) Second distinct perp build | D | **(a) satisfied** — 17 builds, 43 assets |
| D5 | (b) Observable execution-cost layer | D | **absent** — not observable from this source |
| D6 | Estimable declared null scenarios | calibration verdict | **partial** — 8 of 10 |
| D7 | More releases, and observed endpoints on declared pairs | power for any confirmatory claim | **insufficient** — 33 of 785 endpoints at h=300 s |
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

**Where to get it (carried forward).** There is no retrospective source, and that is a
measured result rather than an untested one. The live index is
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
only a mention-type product and nothing respectively — a negative result, not a truncated
page. Acquisition is therefore **forward-only**: a dated cadence over those URLs is the
only route that can ever produce a record for a window the study has not yet passed.

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
mechanism but the overlap: the graph's contract universe is built from the archived 2025
parquet shards, which do not contain the contracts that were captured.

**Two consequences to settle before D1 can deliver for the studied cohort.**

1. The universe must be able to include contracts observed live, or the observation
   cadence must run far enough ahead of a future cohort that the archived universe and the
   observed universe intersect. **This one is left open deliberately.** Admitting
   live-observed contracts changes which contracts the study is about, so it is a cohort
   decision for the study owner rather than a wiring change to make quietly.
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

**Where to get it (carried forward).** Two halves, and only one is reachable for free from
here. The revision half needs no credential: `fred.stlouisfed.org/graph/fredgraph.csv?id=…`
returns observations directly, and `alfred.stlouisfed.org/series?seid=…` exposes the
release-date and revision structure. The `vintage_date=` CSV form returned HTTP 404 from
this egress, so automated vintage extraction is unconfirmed. The consensus half has no
tested free source; candidates are Econoday, Trading Economics, the Investing.com calendar
and the Philadelphia Fed SPF at quarterly frequency. A market-implied value is refused by
the declared standard and cannot serve.

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
| Venue records read | Kalshi 689, Polymarket 229 |

The refusals are semantic, and the largest ones are about published text rather than about
matching:

| Refusal | Pairs |
| --- | --- |
| `venue_payout_text_has_no_parser_declared_in_this_repository` | 157,781 |
| `one_side_states_no_readable_payoff_predicate` | 157,781 |
| `contract_month_is_not_dated_by_the_declared_calendar` | 71,219 |
| `published_contract_text_states_no_readable_payout` | 229 |

Counts overlap, because a pair can fail several checks; the first two are on every pair.
**This is the sharpest form of the dependency yet recorded: it is a missing parser, not a
missing venue.** The second venue's records are present and readable as records; what does
not exist is a declared parser that turns its payout text into a predicate this repository
can compare.

**Candidate selection is a rule, not evidence.** From the registry:
`limit_applied: 250`, `cap_hid_records: false`, `records_available: 163,289`,
`records_matching_the_declared_pattern: 229`,
`pattern_is_a_selection_rule_and_not_evidence: true`. The 229 is a keyword-selected set,
so the 157,781-pair denominator is a declared selection and not a market universe.

**Coupling.** Closing D4 alone does not make study B runnable: a matched pair inherits the
same per-contract vintage requirement, so D4 without D1 leaves B blocked on D1.

**Where to get it (carried forward).**
`gamma-api.polymarket.com/public-search?q=…` works and returns the event with its strike
markets in one call; a timeout on that host is transient and must be retried before being
recorded as unreachable. Read earlier, the search for `Fed Decision` returns
`fed-decision-in-september-762` whose strike labels are basis-point changes
(`50+ bps decrease`, `25 bps decrease`, `No change`) — the `target_rate_change_bps`
definition the declared predicate set names. That establishes the acquisition channel; it
does **not** satisfy D4.

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

## D5(b). An observable execution-cost layer — **not satisfiable by collection**

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

This is the dependency **no amount of collection removes**. A second build arrives by
waiting; a cost layer does not. What is missing is the fee side, which has to come from
venue fee schedules; the depth side is partly present in the collected cross-section.

**Independence.** Study D's blocker is unrelated to D1–D4. More rule work does not help
here, and more collection does not help there.

---

## D6. Estimable declared null scenarios, for the calibration verdict

**Required.** A majority of the declared primary null scenarios must contribute a
promotion rate, so a family-wise simultaneous bound can be certified over them.

**Measured state (carried forward).** The calibration ran at exactly the declared design
and returned **`inconclusive`**, not `pass`.

| Quantity | Value |
| --- | --- |
| Repetitions per primary scenario | 200 |
| Releases per repetition | 120 |
| Declared nulls / estimable nulls | 10 / **8**, each promoted **0 of 200** |
| Null one-sided upper bound, simultaneous level 0.99375 | **0.0251** against a 0.05 ceiling |
| Recovery `communication` | **196 of 200**, rate 0.98, lower bound 0.9548 against an 0.80 target |
| Verdict | **`inconclusive`** |

Two declared nulls are **not estimable at any repetition count**: `spread_only` declares
the latent value does not move, so no shock is recoverable; `resolution_pause` halts the
venue across its own measured window, so its rows are invalid rather than filled. Each
blocked **200 of 200** repetitions. The run reports both rather than dropping them,
because dropping them would widen the bound the surviving nulls are held to.

**Where to get it.** Nowhere — this is a **design** dependency, not an acquisition. What
must change is the declared scenario definitions, so each contributes a complete nested
comparison row.

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

**Where to get it (carried forward).** The first tranche needs no new source: the exchange
trade archive already runs to 2026-01-29, so releases from 2025-06 through 2026-01 are
inside the warehouse the extractor already reads, and `ingest/macro_releases.py` holds the
expansion machinery. What they need is a release schedule to declare against; the BLS
release calendar is the obvious authority and was **not probed**, so it is named as
intended rather than verified. The endpoint-coverage half is not a source at all.

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
| `alfred.stlouisfed.org/series?seid=…` | works — HTML carrying the revision table |
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
  requirement.
- **D7 is worthless before D1**, for the same reason, and D1 is worthless for a
  confirmatory claim without D7's power.
- **D5(a) is now satisfied and D5(b) is not**, and they are independent of D1–D4, D6 and
  D7 in both directions.
- **D6 is a design dependency**, not a data one.
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
