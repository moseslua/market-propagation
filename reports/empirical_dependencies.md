# Empirical dependencies

Date: 17 September 2026.

This file lists the **external inputs the studies require and cannot produce from
what is already on this checkout**. It is a dependency ledger, not a plan, not a
result, and not evidence about any contract, release or venue. Every state below is
a number that was measured on this archive; every artifact named below was read to
produce it. Where a dependency is a *code or document* gap rather than a missing
empirical input, that is stated in its own row instead of being folded into the
data gap.

Source of the states: `reports/empirical_study/paper.md`,
`reports/study_execution_status.md`, `configs/studies/study_{a,b,c,d}*.yaml`,
`configs/neighbor_graph_v2.yaml`, `configs/cohort_v2.yaml`,
`configs/rule_attestation_v1.yaml`, `reports/contract_rule_registry.json`.

Each dependency also states **where the input can be acquired**. Every channel named as
working was fetched to produce this file rather than assumed, and the tested results —
including the hosts that did not answer — are recorded under *Channel reachability*
below, so an unreachable host is not mistaken for an absent source.

## Ordering

The order below is the order that matters, not a priority list. **D1 bounds the
programme:** every other dependency can be satisfied and the primary estimands stay
unestimable until D1 is met. D2 gates D1 for the studied cohort, so it is a
prerequisite of D1 rather than a substitute for it. D3, D5 and D6 are independent of
D1 in both directions — satisfying one does not move the other. D4 and D7 are coupled
to D1 and are worthless before it. D8 is not a dependency that can be satisfied at
all; it records what stays unclaimed even when every other row is met.

| Id | Dependency | Blocks | State |
| --- | --- | --- | --- |
| D1 | Per-contract rule-vintage record | A, C (and B once matched) | **absent** — 0 of 785 |
| D2 | Observation overlap: universe observable before its own window | D1 for the studied cohort | **not satisfied** — capture route covers live contracts only |
| D3 | Point-in-time expectation source | news and network rungs (A, C) | **absent** — provider not present |
| D4 | Cross-venue matched instrument live at a declared release instant | B | **absent** — 0 of 10 |
| D5 | Second distinct perp build, and an observable execution-cost layer | D | **absent** — 1 build held |
| D6 | Estimable declared null scenarios | calibration verdict | **partial** — 8 of 10 |
| D7 | More releases, and observed endpoints on declared pairs | power for any confirmatory claim | **insufficient** — 33 of 785 endpoints at h=300s |
| D8 | A confirmatory sample | the two claims of interest | **not claimable** — exploratory only, degenerate |

---

## D1. A per-contract rule-vintage record, for every candidate contract

**Required, per contract.** Eight fields, exactly the set the graph consumes:
`contract_id`, `rule_hash`, `source_url`, `verified_by`, `in_force_from`,
`in_force_to`, `observed_at`, `settlement_semantics`.

**Why it is required.** A rule hash binds a verdict to exact text; only an interval
can certify a window. The exposure graph and the absorption panel both ask "which
version of this contract's rule text was in force over this window", and neither can
answer it from a digest alone.

**Measured state.**

| Quantity | Value |
| --- | --- |
| `rule_verified_pairs` | **0 of 785** |
| `valid_rows` on the panel | **0 of 3,925** |
| Receiver decisions refused on their own rule check | **785** (`rule_vintage_unverified`) |
| Edges admitted | **0** |
| Structurally admissible edges withheld by this requirement alone | **623** (~62 per release) |

The 785 is the receiver's own rule check, which runs *before* any donor is sought, so
it does not by itself measure the gap. The 623 is the gap, measured with a diagnostic
sentinel that certifies every window and is **explicitly not evidence**.

**Current evidence source is a specification, not records.**
`reports/contract_rule_registry.json` carries `status: frozen_local_unregistered`,
`frozen_on: 2026-09-13`, `external_registration: none_claimed` and no per-contract
rule record. It is named by configuration three ways:

| Configuration key | File | Records |
| --- | --- | --- |
| `rule_vintage.evidence_source` | `configs/neighbor_graph_v2.yaml` | `evidence_source_records_rule_versions: false` |
| `evidence_source` | `configs/cohort_v2.yaml` | `evidence_source_records_rule_versions: false`, `status: absent` |
| `inputs.rule_evidence_source` | `configs/external_history_v1.yaml` | no version flag; absent evidence leaves a rule-covered candidate out of the primary cohort |

The registry also carries `verified_match_count: 0` on every family, and its
`cross_venue_identical_claims` family (`relation_detail:
genuinely_identical_cross_venue_claim`) records `blocked_by:
polymarket_access_timed_out` — the inherited blocker that D4 corrects.

**Where to get it.** There is no retrospective source, and that is now a measured
result rather than an untested one. The live index is
`GET https://api.elections.kalshi.com/trade-api/v2/series/{TICKER}`, which returns a
`contract_url` and a `contract_terms_url` per series — verified for `KXFED` and
`KXFEDDECISION`. That yields two stable asset URLs per declared series:

| URL | Carries |
| --- | --- |
| `assets.kalshi.com/contract_terms/{SERIES}.pdf` | the live Terms and Conditions for the product |
| `assets.kalshi.com/regulatory/product-certifications/{SERIES}.pdf` | the venue's own copy of its CFTC 40.2(a) filing |

The `FED` certification was fetched: signed 2021-06-30, effective 2021-07-02,
Appendix A carrying Rule 100.5 in full. The live terms document for the same series
was fetched too, and **it differs materially from the 2021 filing** — issuance
cadence, last trading date, position terms and expiration time all changed — while
**neither document states its own effective date**, and the live one still refers to
"each scheduled meeting for 2022". A product-template document is therefore
versionless: it cannot bound a contract-level window without a dated archive of the
URL. The archive does not supply one.

| Wayback CDX prefix query | Result |
| --- | --- |
| `assets.kalshi.com/contract_terms/FED*` | `FEDMENTION.pdf` only — a mention-type product, not the policy-rate contract |
| `assets.kalshi.com/regulatory/product-certifications/FED*` | **empty** |

Prefix queries are exhaustive for those prefixes, so this is a negative result rather
than a truncated page. What the archive does hold under `contract_terms/` is about
eleven other series (`BTC`, `MLBGAME`, `MLBSPREAD`, `MLBTOTAL`, `EARNINGSMENTION`,
`GOLFFINISH`, `GOLFROUNDSCORE`, `ITFMATCH`, `MENTION`, `FEDMENTION`,
`MODELRELEASEDATE`), and every one of those captures carries a crawl date between
2026-07-23 and 2026-09-15 — after every window this study measures. Under
`product-certifications/` only `MODELRELEASEDATE.pdf` and `RUN.pdf` are held, and
`kalshi.com/regulatory/filings` has exactly one capture (2021-07-01) that renders as
an empty shell.

The closest thing to an enumerable dated notice index is
`assets.kalshi.com/regulatory/notices/`, archived with about twelve dated PDFs, and
`kalshi.com/regulatory/notices`, archived twice (2024-01-05, 2026-04-20). Those
notices are incentive-program and fee documents rather than per-contract rule changes,
and the live pages return HTTP 429 from this egress.

**Consequence.** Acquisition is forward-only and specific: eight URLs — four series
× {series JSON, terms PDF, certification PDF} — plus the listing the capture command
already reads. A dated cadence over that set is the only route that can ever produce a
record for a window this study has not yet passed.

**Inadmissible substitutes** — each is refused by name, not by convention:
`open_time`, `close_time`, `created_time`, `updated_time`, `settlement_ts`, a
capture instant, current rule text read after the window, a settlement outcome, and a
contract's own listing window.

**Artifacts.** `.audit/study-v3/graph_decisions.json`,
`.audit/study-v3/structural_probe.json`, `reports/contract_rule_registry.json`.

---

## D2. Observation overlap: the studied contracts must be observable before their own window

**Required.** A rule record whose interval opens at instant $t$ certifies a window
$[t, \cdot)$ and refuses every instant before $t$. A contract whose window closed
*before* the observation therefore cannot be certified by that observation, however
good the observation is. The studied cohort is the 2025 release windows; an
observation taken in 2026 cannot reach back over them.

**Measured state.** A capture route now exists and works:
`market-propagation capture-rules` fetched the venue's own live listing on
2026-09-16 and `market-propagation attest-rules` emitted **163 records for 163 live
contracts** across the four declared series, each with `in_force_from` equal to the
instant the serving system stated and `in_force_to: null`. Every one of those records
refuses the 2025 windows — correctly, by construction. The gap is not the mechanism
but the overlap: the graph's contract universe is built from the archived 2025
parquet shards, which do not contain the contracts that were captured.

**Two consequences to settle before D1 can deliver for the studied cohort.**

1. The universe must be able to include contracts observed live, or the observation
   cadence must run far enough ahead of a future cohort that the archived universe and
   the observed universe intersect. **This one is left open deliberately.** Admitting
   live-observed contracts changes which contracts the study is about, so it is a
   cohort decision for the study owner rather than a wiring change to make quietly.
2. ~~The graph's loader discards an open interval.~~ **Closed.** `rule_records` in
   `scripts/build_forecast_panel.py` now admits a record whose `in_force_to` is null, so
   the `"in_force_to": null` shape this pipeline writes is read as an open end. A field
   named in that module's `OPEN_ENDED_RECORD_FIELDS` must still be *stated*; an absent
   end is refused, because an unstated end and a stated open end are different facts.

**Where to get it.** No external channel exists. The requirement is a property of the
observation instant relative to the window rather than of any document, so it is
satisfied by which universe the graph reads and by where the cadence is run — either
the universe admits live-observed contracts, or the cadence runs far enough ahead of a
future cohort that the archived and observed universes intersect.

**Resolved since this was written.** The observation run is now grouped on the digest
of the captured *rule text* rather than on the digest of the archived page, so a
re-fetch whose live fields moved while the rule text did not no longer closes the
interval; the published `rule_hash` remains the digest of the archived bytes, which is
what the consumer binds. The measurement that motivated the change stands: one `KXFED`
page was 122,578 bytes and 122,577 bytes roughly 41 seconds apart, same contracts, same
rule text. Without this, a daily cadence would have certified a rule version that never
changed one day at a time and never left it open.

**Note on kind.** This row is the one place in this file where a required change is
partly a *consumption path* rather than a missing external input, and it is stated
that way deliberately.

---

## D3. A point-in-time expectation source covering the declared news vector

**Required.** For each release, a forecast that was published **at or before** the
release instant and is scored against that release's own declared statistic, in the
declared units, from a named consensus, not market-implied. The declared vector is
`cpi_headline_sa_mom_pct` and `payrolls_change_thousands`.

**Why it is required.** Without a news baseline, a common shock that moved two
contracts is indistinguishable from transmission between them. Requirement is
structural, not a matter of degree.

**Measured state.** No expectation source exists on this checkout. `load_expectations`
raises `expectation_source_is_absent`, and the `news` and `network` rungs are blocked
with the declared columns named (`delayed_shock`, `neighbor_lag`,
`neighbor_lag_control`, `shock`).

`ingest/expectations.py` already refuses, each with its own code: a forecast published
at or after the release, a forecast scored against a revision, a unit disagreeing with
the release's own declared statistic, an unnamed consensus, a market-implied value, an
incomplete news vector, and altered evidence bytes. The contract is implemented and
waiting on a provider; the absence is a reported input gap, not a zero-valued
surprise.

**Where to get it.** Two halves, and only one is reachable for free from here.

The revision half needs no credential.
`https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES}&cosd=…&coed=…` returned
observations directly (`CPIAUCSL` 2025-01 `318.961`, 2025-02 `319.679`, 2025-03
`319.785`), and `https://alfred.stlouisfed.org/series?seid=CPIAUCSL` exposes the
release-date and unit/seasonal-adjustment revision structure that the "scored against
a revision" refusal needs. The ALFRED graph CSV form carrying `vintage_date=` returned
HTTP 404 from here, so automated vintage extraction is unconfirmed — the series page
is readable, but it was read as a page.

The consensus half has no tested free source. Candidates are Econoday, Trading
Economics, the Investing.com calendar, and the Philadelphia Fed's Survey of
Professional Forecasters at quarterly frequency. A market-implied value is refused by
the declared standard and cannot serve, so a venue's own price is not a workaround.
Both halves live outside this repository, and whether a given provider's field names
and units satisfy the declared vector is a decision for the study owner rather than a
sourcing question.

**Artifact.** `reports/source_feasibility.md`.

---

## D4. A cross-venue matched instrument live at a declared release instant

**Required.** A matched event $\times$ venue pair whose two sides carry the same
payout semantics under `all_required_fields_must_match` — matching on
`rate_definition`, `threshold`, `inequality`, `yes_axis`, `orientation`, and the
payoff fields `reports/contract_rule_registry.json` declares. Ticker resemblance is a
lead only and `similar_titles_sufficient` is `false`.

**Measured state.** **0 of 10** declared release instants has a per-meeting
policy-decision market on the second venue. This is a semantic finding, not a
transport one, and it corrects the inherited record: access is fine.

| Quantity | Value |
| --- | --- |
| Release windows with intraday second-venue trades | **10 of 10** |
| Intraday rows unresolved at the release instant | 2,162 to 7,305 per window |
| Daily-aligned shards, span | 1,248; 2022-11-21 to 2026-04-28 |
| First-venue trade rows, span | 154,505,005; 2021-06-30 to 2026-01-29 |
| Verified cross-venue matches | **0** |

The inherited blocker `polymarket_access_timed_out` is **stale** and must not be
carried forward. The union of the two facts is worse for the design than the access
story was: access works, and the matched instrument does not exist in the window.

**Coupling.** Closing D4 alone does not make study B runnable. A matched pair would
carry the same per-contract vintage requirement as study A, so D4 without D1 leaves
B blocked on D1.

**Where to get it.** `https://gamma-api.polymarket.com/public-search?q=…` is the
working channel, and it returns the event together with its strike markets in one
call. The endpoint timed out on repeated attempts earlier in the same session and
answered on retry, so a timeout on this host is a transient result and should be
retried before it is recorded as unreachable — which is exactly the mistake this file
was about to make.

Read on 2026-09-17, the search for `Fed Decision` returns the event
`fed-decision-in-september-762` — "Fed Decision in September?", `negRisk: true`,
`volume` 203,839,635, `liquidity` 10,354,242 — whose strike markets carry
`groupItemTitle` values `50+ bps decrease`, `25 bps decrease` and `No change`, and
whose description defines the underlying as *the upper bound of the target federal
funds range*, resolved from the FOMC statement per the Federal Reserve's own calendar
and `openmarket.htm`.

Those strike labels are basis-point changes, which is the `target_rate_change_bps`
rate definition the declared predicate set names. Two things follow, and neither of
them is that D4 is now satisfied:

- the acquisition channel is established, so the equivalence check can be run against
  a live listing instead of against a stale registry entry; and
- the **0 of 10 stands as measured** for the ten declared 2025 instants. That is a
  result about those instants, not about whether this family exists at all, and
  whether it was listed at any of them has not been re-measured here.

**Artifacts.** `reports/contract_rule_registry.json`, `reports/data_card.md`,
`.audit/external-measurements.md`.

---

## D5. A second distinct perp build, and an observable execution-cost layer

**Required.** (a) At least two distinct builds, because persistence, absorption speed
and competitive decay are all build-to-build quantities and a single cross-section has
no second observation. (b) A cost layer, so that a *quoted* differential can be
distinguished from an exploitable one.

**Measured state.**

| Quantity | Value |
| --- | --- |
| Sweep log entries | 2 |
| Sweeps performed | 1 |
| Sweeps skipped because the build was unchanged | 1 |
| Distinct builds held | **1** (2026-09-16T10:30:34Z) |
| Assets held | 24 |
| Venues with quotes | 47 |
| Quotes in the cross-section | 889 |
| Funding differentials | 18,547 |
| Cross-venue basis pairs | 18,071 |
| Differentials with a per-hour spread | 16,311 |
| Differentials with only an annualised spread | 2,236 |
| Quotes with open interest / 24h volume / paid 30d | 881 / 886 / 650 |
| `execution_cost_layer_observable` | **false** |
| `capacity_estimable` | **false** |

**Two independent blockers.** Collection time removes the first; **no amount of
collection removes the second**. Gross quoted spreads only: no arbitrage claim, no
realised return net of costs, no capacity claim is made anywhere.

**Independence.** Study D's blocker is unrelated to D1–D4. More rule work does not
help here, and more collection does not help there.

**Where to get it.** Part (a) has two routes and the second is now verified rather
than pending. The first needs no new host, because the collector already detects the
condition itself: `sweeps_skipped_because_the_build_was_unchanged: 1` means a second
sweep was attempted and declined because the build had not moved, so a second distinct
build arrives by running the same collection again once the venue's build changes and
only elapsed time is missing.

The second route supplies a dated funding-rate series directly instead of waiting for
one. Both hosts tried answer: `fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT`
returns regular funding rates carrying `fundingTime` and `fundingRate`, and
`api.bybit.com/v5/market/funding/history?category=linear&symbol=BTCUSDT` returns the
same two fields for its linear category. That is a longitudinal series from named
venues, which is the axis the build-to-build quantities need; whether those venues and
assets fall inside the declared `asset_x_venue_pair_x_time` unit is a question about
the declared unit rather than about the source.

Part (b) is the one no collection time removes. The cost layer has to come from venue
fee schedules plus observed quote depth; the cross-section already holds 889 quotes and
16,311 differentials carrying a per-hour spread, so the depth side is partly present
and what is missing is the fee side.

**Artifacts.** `data/perp/sweeps.jsonl`, `src/market_propagation/perp/`,
`configs/perp_arbitrage_v1.yaml`.

---

## D6. Estimable declared null scenarios, for the calibration verdict

**Required.** A majority of the declared primary null scenarios must contribute a
promotion rate, so a family-wise simultaneous bound can be certified over them.

**Measured state.** The calibration ran at exactly the declared design and returned
**`inconclusive`**, not `pass`.

| Quantity | Value |
| --- | --- |
| Repetitions per primary scenario | 200 |
| Releases per repetition | 120 |
| Workers, seed, bootstrap draws | 9, 20260913, 200 |
| Declared nulls / estimable nulls | 10 / **8**, each promoted **0 of 200** |
| Null one-sided upper bound, simultaneous level 0.99375 | **0.0251** against a 0.05 ceiling |
| Recovery `communication` | **196 of 200**, rate 0.98, lower bound 0.9548 against an 0.80 target |
| Verdict | **`inconclusive`** |

Two declared nulls are **not estimable at any repetition count**:
`spread_only` declares the latent value does not move, so no shock is recoverable and
the nested comparison has no complete row; `resolution_pause` halts the venue across
its own measured window, so its rows are invalid rather than filled. Each blocked
**200 of 200** repetitions on a not-run comparison. The run reports both rather than
dropping them, because dropping them would widen the bound the surviving nulls are
held to and read as evidence they never supplied.

This is a property of the **declared simulated scenarios**, not of the archive, and it
is therefore a design dependency: the fix is a scenario definition that yields a
complete comparison row, not more data.

**Where to get it.** Nowhere — this is not an acquisition. What has to change is the
declared scenario definitions, so that each contributes a complete nested comparison
row: a `spread_only` scenario whose latent value yields a recoverable shock, and a
`resolution_pause` scenario that does not halt the venue across its own measured
window. Both are properties of the simulation the study declares, and neither is
supplied by more archive data.

The earlier 48-repeat `network_falsification` call is recorded as superseded and
insufficient in `configs/study_v2.yaml` and `configs/neighbor_graph_v2.yaml`; its
numbers stay in `.audit/study-v2/calibration_study_scale.json` as a record of that
call, not as this study's calibration.

---

## D7. More releases, and observed endpoints on the declared pairs

**Required.** (a) More than ten releases, if a confirmatory claim is ever intended.
(b) Observed post-release endpoints and baselines on enough declared pairs for an
estimand to exist even after D1 is met.

**Why it is required.** Ten releases cannot carry a confirmatory claim: the earlier
calibration call's power figure of 0.44 against an 0.80 target says so, and the
declared grid's observed fraction is 0.042. D1 does not fix this; masking is what D1
removes, and row *count* is a separate constraint.

**Measured state.** Of the 785 rows at the primary horizon (300 s), **33** have an
observed post-release endpoint and **43** an observed baseline. Exclusion reasons over
3,925 rows: `rule_version_unknown` 3,925, `missing_baseline` 3,710,
`no_post_release_trade` 3,672, `baseline_beyond_cap` 155, `endpoint_beyond_cap` 155.

The January 2025 CPI release has **no observation at all** in this cohort, and its 92
declared pairs stay in the denominator rather than dropping out.

**Where to get it.** The first tranche needs no new source. The exchange trade archive
already runs to 2026-01-29, so releases from 2025-06 through 2026-01 are inside the
warehouse the extractor already reads, and `ingest/macro_releases.py` holds the
expansion machinery. What the extra releases need is a release schedule to declare
against; the BLS release calendar is the obvious authority and was **not probed this
session**, so it is named as the intended source rather than a verified one.

The endpoint-coverage half is not a source at all: 33 observed endpoints of 785
declared pairs is what the archive's own trading produced, and no provider sells it.

**Coupling with D1.** Every added release brings contracts whose rule vintages are
equally unattested, so D7 and D1 are coupled and D7 is worthless before D1.

**Artifact.** `reports/empirical_study/paper.md` section 3.3.

---

## D8. A confirmatory sample, which no dependency can create

The exploratory absorption measurement is bounded and labelled: 47 measurable rows,
`cpi` blocked with 0 design rows (the split needs at least 3 releases and got 2 with
a design row), `employment` complete but degenerate (21 rows across 3 releases, 1
held out, so no release-level interval is identified). Row-level resampling is
refused rather than substituted.

**Where to get it.** No channel exists, and this row is not an acquisition target. Its
purpose is to stop the rows above being read as a route to these two claims.

Even with D1, D3 and D7 satisfied, the two claims of interest stay **not claimed**:

| Claim | Status |
| --- | --- |
| Absorption responds to the release | **not claimed** — exploratory only, degenerate |
| Information diffuses between policy-rate contracts | **not claimed** — no admissible edge |

Neither is asserted, and neither may be replaced by a weaker claim dressed as the
same one.

---

## Channel reachability, tested 2026-09-17

Every state in this file that names a source was produced by fetching it, not by
assuming it. The results are recorded here so the next reader does not re-test a
settled question.

| Endpoint | Result |
| --- | --- |
| `api.elections.kalshi.com/trade-api/v2/series/{TICKER}` | works — JSON metadata plus both asset URLs |
| `assets.kalshi.com/contract_terms/{SERIES}.pdf` | works — full Terms and Conditions |
| `assets.kalshi.com/regulatory/product-certifications/{SERIES}.pdf` | works — dated CFTC filing |
| `fred.stlouisfed.org/graph/fredgraph.csv?id=…` | works — key-less CSV |
| `alfred.stlouisfed.org/series?seid=…` | works — HTML carrying the revision table |
| `web.archive.org/cdx/search/cdx` | works — JSON enumeration (`matchType`, `collapse`) |
| `web.archive.org/web/{ts}id_/{url}` | works — raw archived documents retrieved |
| `www.cftc.gov/…/TradingOrganizationProducts` | loads, but its rows are client-rendered: not enumerable |
| `gamma-api.polymarket.com` | **works on retry** — timed out earlier in the same session; flaky, not blocked |
| `fapi.binance.com`, `api.bybit.com` | **work on retry** — both timed out earlier in the same session |
| `archive.org/wayback/available` | HTTP **429** |
| `alfred.stlouisfed.org/graph/fredgraph.csv?vintage_date=…` | HTTP **404** |
| `kalshi.com/regulatory/notices`, `kalshi.com/regulatory/filings` | HTTP **429** |

A timeout is not evidence of absence and is not recorded as one. All three hosts that
timed out during this session — `gamma-api.polymarket.com`, `fapi.binance.com`,
`api.bybit.com` — answered on retry, so this egress drops connections rather than
blocking hosts, and a single timeout here says nothing about a source. Retry before
recording anything as unreachable.

The absences recorded above for the Fed-series terms and certification documents are
different in kind: they come from exhaustive prefix queries against a host that was
answering, not from an unreachable host.

## Coupling summary

- **D1 bounds everything.** Until a per-contract rule-vintage record exists for the
  studied cohort, the primary panel stays blocked however many releases are added.
- **D2 gates D1** for the 2025 cohort specifically. The mechanism now exists; the
  overlap does not.
- **D3 is independent of D1 in both directions.** Even a complete rule record leaves
  a common shock indistinguishable from transmission, and a complete news vector
  leaves every receiver refused on its own rule check.
- **D4 is worthless before D1**, because a matched pair inherits the same vintage
  requirement.
- **D7 is worthless before D1**, for the same reason, and D1 is worthless for a
  confirmatory claim without D7's power.
- **D5 is independent of D1–D4, D6 and D7** in both directions.
- **D6 is a design dependency**, not a data one, and is independent of all of the
  above.
- **D8 is not satisfiable.** It is the statement that the two claims stay unclaimed
  even with D1, D3 and D7 met.
