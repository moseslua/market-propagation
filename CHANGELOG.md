# Changelog

All notable changes to this repository, newest first. Format follows
Keep a Changelog.

This is a research pipeline, so an entry records what a change lets the pipeline
*establish*, not only what it adds. Where a change produced a measurement, the
measurement is recorded here beside it. Where a claim remains unreachable, that is
recorded too: a quantity this repository has not observed is a null with a named
reason code, and a changelog that dropped those would read as progress the study
cannot support.

## [Unreleased] — 2026-09-17

### Added

- **Prospective rule capture** — the command `capture-rules`, the source kind
  `dated_observation_of_the_live_rule_text`, and `--emit-graph-records` on
  `attest-rules`. 4 tests. The command reads the venue's own live listing for each
  series `configs/cohort_v2.yaml` declares, archives every page, and writes one
  immutable capture per live contract. It issues GET requests only, carries no
  credential, and refuses to derive a bound from the run's own clock: the instant each
  record opens at is the one the *serving* system states in its own `Date` header.
  - `ResponseEnvelope` gained `server_date`, read from the response and never from
    `received_time`. A response that states no instant leaves it null, and a capture
    whose response states none stays admissible and unattested rather than being
    bounded by this run's fetch.
  - The accepted kind is narrower than it looks. A record's interval opens at the
    capture instant, so the consumer's own `applies_to()` refuses every window that
    ended before it. That is what keeps the 2025 cohort refused by construction rather
    than by intent, and it is why this is a new route rather than a relaxation of the
    retrospective refusal on `current_venue_rule_text`, which is unchanged.
- **`reports/empirical_dependencies.md`** — the external inputs the studies require and
  cannot produce from this checkout, each with what it blocks, its measured state, the
  substitutes that are refused, and its acquisition channel. A channel is marked
  working only where it was fetched; every host that did not answer is recorded as
  unanswered rather than as absent.
- **Declared horizon to 2027** — `configs/neighbor_graph_v2.yaml` gained the published
  FOMC dates from 2026-01-01 to 2027-12-31, read from the Federal Reserve's calendar on
  2026-09-17. `basis` and `verified_from_a_fetched_source` are unchanged: these are a
  declared schedule, and nothing here attests any contract's rule text.
- **Capture cadence** — one entry in `configs/scheduled-reports.cron`, daily at 06:41
  host time. A capture bounds only windows beginning at or after the instant the
  serving system states, so the observation has to be taken ahead of the window it is
  used on. Nothing in this repository installs the file.

- **Forward cohort arm** — `configs/cohort_forward.yaml` (`forward_2026h2`) and the
  `arms:` block naming both arms in `configs/cohort_v2.yaml`. The arm is the next three
  scheduled CPI and next three Employment Situation releases, October through December
  2026, at the instants the BLS calendars state. The retrospective arm
  (`core_2025h1`, `configs/cohort.yaml`) is untouched and stays the arm reported as
  blocked; `pooling_permitted: false`, four named forbidden cross-arm uses, and a
  per-arm denominator keep a result from being read against the wrong arm.
  - The arm is extended by a declared rule, not declared complete, because the BLS
    schedule runs about three months ahead. `extension_rule` names the authority (the
    BLS schedules only), the append step (the family's earliest not-yet-declared
    release), the cadence (before each release instant) and the hazard: appending after
    an outcome is known, or letting anything about a contract's liveness or trading
    decide membership, would select the arm on its own results. Membership is fixed by
    a calendar this repository does not write.
  - `no_post_publication_addition_or_removal` records that a release which contributes
    no row stays in the arm and is reported with its reason, so a denominator cannot be
    trimmed after the fact.
- **Release-page capture** — `scripts/capture_bls_releases.py`, 8 tests. It writes the
  receipt `import_bls_archives.py` reads, but only for a page that is a complete
  published release: it requires a complete document and a plausible size, then runs
  the importer's own parser and requires both first-release values and agreement
  between the page's own embargo line and the declared calendar. `status_basis` and
  `payload_evidence` record what was actually observed, so this receipt is
  distinguishable from one written beside a real status line. It refuses the sealed
  2025 capture directory, refuses to overwrite a capture, and refuses an event with no
  declared archive URL.
- **Monthly market/trade capture** — the command `capture-markets`,
  `src/market_propagation/ingest/market_capture.py`, 33 tests, and the two declared
  layers `kalshi_own_markets` / `kalshi_own_trades` under `data/external/kalshi-own/`.
  The venue's live partition retains roughly three months, so a window not captured
  while it is still live can no longer be acquired: this is the cadence that acquires
  it, and it exists because the forward arm's windows would otherwise arrive with no
  transactions to measure.
  - The layer is its own directory under the configured archive root rather than a
    shard inside `kalshi-trades/`, and its `input_class` is
    `locally_captured_public_data`. Our rows are never attributed to the vendor
    dataset's producer or licence, and a reader can tell the two provenances apart in
    a sealed dataset because `kalshi_trade_from_row(layer=...)` puts the layer name in
    `provenance.source`.
  - Both Kalshi layers are read through one path. `KALSHI_LAYERS` replaced three
    separate `layer == KALSHI_LAYER` equality tests, so a new Kalshi layer cannot be
    half-registered: an unregistered one would be read as `epoch_seconds` and labelled
    `polymarket`, which is a silent misreading rather than a missing one.
  - The two-source rule is declared in `configs/external_history_v1.yaml` under
    `two_source_authority:`. A window exactly one layer covers is governed by that
    layer and the resolution records which one, its shard count and its contract count;
    a window both cover is **refused** unless a layer is named explicitly, because one
    contract held in both layers would otherwise be counted twice and inflate the
    denominator every rate is divided by. `fallback_to_the_other_layer: prohibited`.
  - Coverage is read from shard file-name stamps, not from row times. Measured: our
    captured shard's `created_time` is the contract's own creation time, so reading
    coverage from row times would claim our layer reaches windows it cannot and
    recreate the retroactive error the capture exists to avoid.
- **One definition of the contract universe** —
  `src/market_propagation/ingest/kalshi_universe.py`, 7 tests. The universe is read as
  the **union** of the two declared observation paths (`kalshi_own_markets`, this
  repository's own capture, and `kalshi_markets`, the vendor archive) rather than from
  the archive alone, and never as their intersection. Every row carries
  `seen_archive`, `seen_live` and an `observation_origin` of `archived_only`,
  `live_only` or `archived_and_live`; a contract both paths hold is one contract with
  two observations, so the overlap cannot inflate the denominator. Where the two paths
  disagree, the declared layer order resolves the field, and a field only one path
  states is still carried. The union identity is **asserted rather than reported**:
  counts that do not sum to the universe raise `UniverseError`. A caller that names its
  own glob reads exactly that path and claims no declared provenance.
- **Protocol freeze** — `src/market_propagation/protocol_freeze.py`, 13 tests, and the
  command `protocol-freeze`. It hashes the 18 declarations (the cohort, ladder, timing,
  threshold, window and evidence configurations, plus the preregistration and its
  addenda) and the 13 modules that define the estimands, records the instant the freeze
  was taken (T0) and the declared stopping rule, and writes one sealed freeze.
  `--verify` re-hashes the same files against a held freeze and names every one that
  moved rather than stopping at the first; an intact checkout exits `0` and a drifted one
  exits `2` naming the file. T0 is required to write a freeze and must state an offset,
  because a freeze with no stated instant cannot be told from one taken after a result
  was seen. A covered file that is absent is recorded as `null` rather than omitted, so a
  file that appears later is a change rather than an addition the freeze never saw.
  - Exercised against this session's own work rather than only in tests: a freeze taken
    before the D2 and D6 declaration changes was re-verified after them and flagged
    exactly the seven files that had moved — `configs/study_v2.yaml`,
    `configs/cohort_v2.yaml`, `configs/neighbor_graph_v2.yaml`, both study
    configurations under `configs/studies/`, and both preregistration documents — which
    is the behaviour the check exists for. The sealed freeze is then taken over the
    final state at `2026-09-17T06:19:40Z`, 31 covered files, freeze hash
    `c0802022518d`, and verifies clean.
- **Confirmatory progress ledger** — `src/market_propagation/confirmatory.py`, 9 tests,
  and the command `confirmatory-progress`. For every release both declared arms state,
  it reports whether a held rule capture states an instant at or before it, so progress
  toward a confirmatory sample is computed from the artifacts rather than asserted in
  prose. The test is the attestation module's own `bounding_instant`, so a capture whose
  response states no instant opens nothing rather than being dated by the run's clock. A
  prerequisite no artifact can answer is reported `unobservable` with its reason —
  also never as met, and never as unmet, because "we cannot read it" and "it is not
  there" are different facts.

### Changed

- **The candidate universe is read from both observation paths.** `build_study_panel.py`,
  `build_forecast_panel.py`, `cross_venue.py` and the CLI now read the one universe
  module rather than deriving a universe each, so the five readers cannot drift apart.
  `match-cross-venue` still resolves and records which layer is authoritative for a
  window, but that resolution no longer decides membership: omitting `--markets-glob`
  reads the union, and the result states
  `first_venue_universe_is_the_union_of_observation_paths`. Naming a glob still reads
  exactly that path.
  - This is a **conformance fix, not an estimand change.** The preregistration declares
    membership by series and listing interval and names no observation path; "recorded"
    had been implemented as "recorded in the vendor archive", which is narrower than the
    declaration. The four declared policy series, the listing-interval test, the
    grid-as-denominator and the 785 declared pairs are all unchanged.
  - Measured on this checkout: **689 contracts in the union — 526 `archived_only`, 0
    `live_only`, 163 `archived_and_live`**, over 1 live shard and 4 archive shards. The
    retrospective arm is therefore unchanged by it; the forward arm's declared universe
    would have been empty without it, because the archive's rows end 2026-01-29 and every
    forward release falls after that.
  - The audit for other cohort-defining archive intersections is closed. Five readers were
    moved onto the one module (the graph builder, the study panel, the forecast panel, the
    cross-venue matcher and the CLI) and the last two names for the old archive-only
    universe are **deleted rather than left unused**: `DEFAULT_MARKETS_GLOB` (declared and
    referenced nowhere) and `cross_venue.KALSHI_MARKETS_GLOB` (exported and read nowhere).
    A live but unused constant naming one layer is how the archive-only universe gets
    quietly re-adopted, so a caller now has to import the union to read a universe at all.
    The `--markets-glob` help text said omitting it resolved the declared layer for the
    window; omitting it reads the union, and it now says so.
  - `live_only` is **0 on this checkout, and verified to be genuine rather than a union
    defect**: the live layer holds 163 distinct contracts and every one is also in the
    archive, measured by **0** live rows carrying an `open_time` after the archive's last
    row (2026-01-29). No captured contract could have been absent. Criterion 1 stays dormant
    until a capture covers a contract listed after the archive ends — the forward arm's
    exact position — so dormant means the case has not arisen, not that the mechanism is
    untested; fixtures in `tests/test_kalshi_universe.py` pin both directions.
- `reports/population_change_d2.md` is new: the estimand record for the change above,
  written as a page rather than an edit because admitting live-observed contracts was
  recorded as a cohort decision rather than a wiring change. `reports/preregistration_v2.md`
  carries it as a dated addendum beside the frozen clause instead of rewriting it.

### Fixed

- **Two declared nulls produced no comparison row at all.** The calibration's verdict
  was `inconclusive` because `spread_only` and `resolution_pause` blocked **200 of 200
  repetitions**, so the family-wise simultaneous bound was not certifiable for the
  declaration as written. Both causes were in the declarations rather than in the
  estimator, and both are now fixed; 15 tests pin them, and a revert check confirms each
  test fails when its fix is undone.
  - **A declared zero shock reached the news control as a missing value.**
    `spread_only` declares `news_active=False`, so every contract's sensitivity is 0.
    `simulated_release_shocks` recovers the generator's per-release common shock as
    `latent / (orientation * strength)` and skipped any event whose strength was falsy,
    so it returned an **empty** mapping; the ladder's `shock` and `delayed_shock` columns
    were then filled with `None`, and `nested_comparison` found **0 complete rows of 20**.
    An event for which no role carries a declared sensitivity now gets an explicit `0.0`,
    because the process declares an exact zero common news term rather than an unmeasured
    one — the same absence-is-not-zero distinction this repository enforces elsewhere.
    Measured before the fix: `shock` 0/20 and `delayed_shock` 0/20 non-null.
  - **The declared halt covered the entire primary measurement window.** The calibration's
    declared forecast settings are `forecast_origin_seconds=300`, `future_horizon_seconds=300`,
    so every primary row's window is `[event+300s, event+600s]`. `resolution_pause` declared
    `pause=(300.0, 900.0)`, which contains that window end to end, so **every** primary row
    was marked `halted_during_window` and its target was null: **0 of 20 rows** carried a
    target. The declared halt is now `(700.0, 1000.0)`, which opens after the primary
    window closes at +600s, so the halt still invalidates every window that spans it while
    no longer consuming all of them.
  - Verified after the fix at `n_releases=60` over three seeds: `spread_only` and
    `resolution_pause` both `complete` with `promoted=False`, while `communication` stays
    `complete` with `promoted=True` at every seed.
- **The observation run closed itself.** The run was grouped on the digest of the
  archived page, and the venue's live listing carries fields that move between fetches
  while the rule text inside it does not. Measured: one `KXFED` page was 122,578 bytes
  at 16:31:38 and 122,577 bytes at 16:32:19, same contracts, same rule text. Grouping
  on the payload therefore closed the interval on the next fetch, so a daily cadence
  would have certified a rule version that never changed one day at a time and never
  left it open. The run is now grouped on the digest of the recorded rule text; the
  published `rule_hash` remains the digest of the archived bytes, which is the binding
  the consumer requires and the payload a reader can re-verify.
- **The graph's loader discarded every open interval.** `rule_records` admitted a record
  only when every required field was non-empty, and `in_force_to` is a required field,
  so `"in_force_to": null` — the shape the capture pipeline writes — was dropped rather
  than read as an open end. A field named in `OPEN_ENDED_RECORD_FIELDS` must now be
  *stated* and may be null; an absent end is still refused, because an unstated end and
  a stated open end are different facts.

### Measurements recorded

- **Capture against the live venue.** 163 captures written across all four declared
  series, `pages_blocked` 0, `contracts_skipped_no_rule_text` 0, 0 captures without a
  stated instant, exit `0`. `rules/raw/` holds one blob per page, each named by the
  sha256 of its own bytes.
- **Attestation of what was captured.** 163 contracts examined, **163 attested**, 0
  refusals, exit `0`. Every emitted record carries `in_force_from` equal to the instant
  the serving system stated, `in_force_to` null, `in_force_to_is_open` true, and a
  `rule_hash` equal to the sha256 of the archived listing bytes it cites.
- **The Fed series' own rule documents do not survive in the archive.** Exhaustive
  prefix queries return `FEDMENTION.pdf` only for `contract_terms/FED*` and nothing at
  all for `product-certifications/FED*`. What is held under `contract_terms/` covers
  about eleven other series, and every capture carries a crawl date between 2026-07-23
  and 2026-09-15 — after every window this study measures. The venue's own copy of its
  CFTC filing is dated 2021-06-30 and effective 2021-07-02; the live Terms and
  Conditions for the same series now differs from it in issuance cadence, last trading
  date, position terms and expiration time while stating no effective date of its own.
  A product-template document is versionless, which is why `must_name_the_contract`
  carries the whole weight here and is not relaxed.
- **The capture chain reproduces a sealed row.** A page for `cpi_2025_01` captured
  fresh through a browser on 2026-09-17, put through `capture_bls_releases.py` and then
  `import_bls_archives.py`, parsed to the **same six values** and the **same
  `raw_hash` (`6e90f322…`)** as the row already sealed for that event. The chain that
  the forward arm depends on was therefore verified against an existing artifact rather
  than only exercised.
- **The archive pages are not reachable by direct HTTP from this checkout.** Every
  header set tried returns `403` — this repository's research user agent, a current
  browser user agent, with and without `Accept`/`Accept-Language`, and with no user
  agent at all — while a browser session returns `200` with the full page. That is why
  the sealed 2025 receipts carry `acquisition_method: standard_browser_http_response`
  and the capture directory is named `bls-browser`, and why the capture script has no
  direct-fetch mode: a mode that returned 403 would invite a reader to think the source
  refused rather than the client being unable to ask.
- **The two market layers are disjoint in time, with a permanent gap.** The vendor
  archive's rows end 2026-01-29 and this repository's own capture begins 2026-09-16, so
  no window is covered by both on this checkout and no window between those dates is
  covered by either. The gap cannot be closed by collecting later: it is the interval
  in which nobody held the bytes. It is why the capture cadence is a prerequisite for
  the forward arm rather than an optimisation, and why the forward arm's first release
  (2026-10-02) is the earliest window any layer will cover.
- **Capture against the live venue, run for verification.** `capture-markets` over
  2026-09-14 → 2026-09-17 wrote 10 market rows to a sealed shard in the archive layout:
  21 columns including `rules_primary` and `rules_secondary`, UTC microsecond
  timestamps, and prices as exact integer cents (`yes_bid` 71 where the venue states
  `0.7100`). The resolution reported `basis: sole_covering_layer` with the vendor layer
  covering 147 declared-series contracts and our layer covering none, and the partition
  decision recorded the live cutoff as 2026-07-18 from `trades_created_ts`. The live
  ledger carries each contract's own published rule text where the vendor archive's
  volume column stores a zero.
- **The calibration is re-run after the two null declarations were fixed.** 200
  repetitions per scenario at 120 releases, seed 20260913, 9 workers — exactly the
  declared design, over the same production path. **Verdict `pass`**, where the previous
  run's was `inconclusive` because two declared nulls emitted no comparison row at all.
  All **10 of 10** declared null scenarios are now estimable and every one of them
  promoted in **0 of 200** repetitions, a one-sided upper bound of **0.02614** at
  simultaneous level **0.995** against the 0.05 ceiling. Recovery `communication`
  promoted **196 of 200**, rate 0.98, one-sided lower bound **0.9548** against the 0.80
  target. `spread_only` and `resolution_pause` each now complete **200 of 200** at 0
  promoted, where each previously blocked **200 of 200**. Certificate
  `data/calibration/calibration_certificate.json`; registry record
  `calibration-2856211b642b-bd7e5e807f5c`.
  - The bound moved **outward**, from 0.0251 to 0.02614, and that is the correct
    direction for it to move. The simultaneous level is divided across the estimable
    nulls, so it rose from 0.99375 to 0.995 only because two more nulls entered the
    family: the weaker per-null bound buys coverage of a declaration that previously had
    two members contributing nothing. A tighter bound over fewer nulls would have been
    the worse result.
  - This certifies the behaviour of the promotion rule on a declared synthetic process.
    It is not an empirical finding about any venue, release or contract, and it unblocks
    no claim: `confirmatory_estimation_permitted` stays false.
- **Test suite.** 1076 → **1165** tests. `ruff check` and `ruff format --check` clean.

### Not established

- **No contract in the studied cohort carries an attested in-force rule interval.** The
  capture route observes live contracts; the graph's universe is built from the archived
  2025 shards, which do not contain them. The constraint has moved from "no mechanism
  exists" to "the observed universe and the studied universe do not overlap", and
  admitting live-observed contracts is a cohort decision rather than a wiring change.
  `rule_verified_pairs` remains 0 of 785 and the graph admits 0 edges.
- **No forward window is covered by the vendor archive.** Its rows end 2026-01-29, so
  the forward arm's windows can only be covered by this repository's own capture, which
  has to run on its monthly cadence from now on. A month missed is a window whose bytes
  are gone, and the live partition retains about three months.
- **The second venue's Fed-decision family is observable but not matched.** A search of
  its public listing on 2026-09-17 returns a per-meeting family with basis-point-change
  strikes. The recorded 0 of 10 stands for the ten declared 2025 instants; whether the
  family was listed at any of them has not been re-measured.
- **Three hosts recorded as unreachable early in the session answered on retry**
  (`gamma-api.polymarket.com`, `fapi.binance.com`, `api.bybit.com`), so a single
  timeout from this checkout says nothing about a source.

## [Unreleased] — 2026-09-16

### Added

- **Perpetual-futures collector** — `src/market_propagation/perp/` (`parse.py`,
  `universe.py`, `differentials.py`, `store.py`, `collector.py`), the thin entry
  point `scripts/perp_collector.py`, and the declared source configuration
  `configs/perp_arbitrage_v1.yaml`. 52 tests.
  - The source publishes rolling windows and no per-episode history, so collection
    began concurrently with the historical work: a cross-section not taken is not
    recoverable. The collector has since swept every hour unattended, 40 assets each
    time, 0 blocked and 0 unshaped.
  - The observation time is the source's own `Data generated at` stamp, stored beside
    the local receive time, which measures only latency. A page carrying no stamp
    raises rather than being dated by the local clock.
  - The funding interval is derived two independent ways (from the annualised rate and
    from the settled count over the rolling 30-day window) and `resolve_interval`
    refuses when they disagree, because the interval sets the per-hour normalisation
    every cross-venue differential depends on.
  - Differentials and basis are **not stored**. They are pure functions of one
    snapshot and are computed on demand, so a stored copy cannot drift from the quotes
    it came from. Raw response bytes are archived before parsing, so a parser change
    re-parses held pages instead of re-fetching them.
  - Every differential carries `execution_cost_not_observable_from_this_source`: the
    source's execution-cost surface is a client-rendered shell with no data, so a
    stored spread is a **quoted** spread and never an executable opportunity.
- **Absorption estimator** — `src/market_propagation/absorption.py`. 21 tests.
  Estimates the fraction of a release's terminal reaction absorbed at each declared
  horizon, with a release-clustered interval taken from the existing
  `evaluation.clustered_bootstrap` rather than a second bootstrap written here. The
  terminal horizon is keyword-only with **no default**, because it defines the
  reaction the absorption time is a fraction of: the same path returns a different
  h50 under a different horizon.
- **Cross-venue matching** — `src/market_propagation/matching.py` and
  `configs/matching_v1.yaml`. 28 tests. Grades a candidate pair `EXACT`,
  `ECONOMICALLY_EQUIVALENT`, `APPROXIMATE` or `REJECT` against six declared predicate
  components. No similarity score is computed anywhere, and no grade or refusal is
  expressed in terms of one: a title moves no grade in either direction.
- **Rule attestation** — `src/market_propagation/ingest/rule_attestation.py` and
  `configs/rule_attestation_v1.yaml`. 51 tests. Reads held captures and reports, per
  contract, whether an admissibly dated source bounds its rule interval. It refuses
  `open_time`, `close_time`, `created_time`, `updated_time` and `settlement_ts` as
  rule bounds — the conflation that sank the withdrawn one-decision-date claim.
- **Cross-venue candidate driver** — `src/market_propagation/cross_venue.py`. 17
  tests. Forms the candidate universe from two venues' own records and grades every
  pair. The candidate filter is recorded as a *selection* rule and never as evidence:
  the result states the pattern, the totals available and the cap actually applied, so
  a count from a bounded universe cannot be read as a count from the whole layer.
- **Preregistration for four studies** — `configs/studies/study_a_absorption.yaml`,
  `study_b_crossvenue.yaml`, `study_c_crossmeeting.yaml`, `study_d_perp.yaml` and
  `reports/preregistration_studies.md`. Each declares its unit, clock, horizons,
  outcome, sample, primary specification, multiplicity, clustering and claim limits
  before any result, and each states what it may not claim.
- **Four commands** — `perp-collect`, `attest-rules`, `absorption-panel`,
  `match-cross-venue`. Each exits `2` when its own result reports blocked or absent
  evidence, so a scheduler reads `2` as a computed blocked finding rather than a crash.

### Fixed

- **Unobserved figures reached storage as bare nulls.** Only five of a market row's
  eight figures were refusal-checked, and the funding page's `total_paid`,
  `average_apr`, `largest` and `smallest` were not checked at all. Measured on the
  first three-asset collection, both layers were affected: 30 funding rows carried a
  null average APR and no reason code (10 per asset), and 15 of ETH's 63 market rows
  carried a null `paid_30d` and no code. A bare null was indistinguishable from a gap
  the source had reported.
  - All eight market figures and all six funding figures are now checked. Verifying
    that claim column by column exposed one remaining hole in the same class: a dash in
    the funding page's `last settled` cell produced a null with no code. No such row
    existed in the held data, so it had never fired, but it is fixed and tested rather
    than left as a latent version of the bug just repaired.
  - Verified on the live cross-section, per column: **0** rows carry a null in any of
    the 14 figure columns without a reason code naming that column, against 1374 market
    and 1008 funding rows.
- **A single canary could not date the source.** The source does not rebuild every
  section at one instant. Measured across five consecutive builds, the `rwa` pages ran
  **8 to 9 seconds behind** the `crypto` pages every time:

  | build | crypto | rwa | delta |
  | --- | --- | --- | --- |
  | 10:30 | 10:30:34Z | 10:30:26Z | 8s |
  | 11:36 | 11:36:19Z | 11:36:11Z | 8s |
  | 12:42 | 12:42:20Z | 12:42:11Z | 9s |
  | 13:51 | 13:51:56Z | 13:51:48Z | 8s |
  | 14:43 | 14:43:59Z | 14:43:51Z | 8s |

  One canary per asset class is now read and a sweep is skipped only when every
  class's stamp is unchanged. The previous rule would have skipped a sweep in which
  only the slower class had advanced, lapsing that class by a build for no reason.
- Every source file now conforms to the declared lint and format rules, so
  `ruff check` and `ruff format --check` are both clean.

### Changed

- `configs/matching_v1.yaml` gained a `candidate_selection` block declaring which
  records enter the candidate universe. It is the one place a text pattern appears at
  all, and it is a selection rule: a record it excludes is outside the universe rather
  than refused for resembling something.
- `reports/reproduction_guide.md` gained Step 10 (the live cross-section collector) and
  Step 11 (the cross-venue candidate universe). Its overview went from two rebuilding
  paths to three, and now distinguishes the one path that reads a source which keeps no
  history from the two that can be rerun against archives already held.

### Measurements recorded

- **Cross-venue funnel.** 689 first-venue candidates, of which 155 yield a readable
  predicate and 534 are refused (533 because their month is not on the declared
  decision calendar, 1 because the archived text states no readable payout); 229
  second-venue candidates, of which **0 are readable** because that venue's cleaned
  layer carries no settlement-rule text and the configuration declares no parser for
  it. Every one of the 157,781 cross-venue pairs is `REJECT`, and the run exits `2`.
- **Absorption on the real panel.** 785 pairs, **785 refused** — 742 for a missing
  pre-release baseline print, 43 for a missing response at the terminal horizon.
  Nothing was imputed, and no median was reported over a set that cannot support one.
- **Test suite.** 907 → **1076** tests, of which 169 are new across the five files
  above. `ruff check` clean; 110 files formatted.

### Not established

These are unchanged by the work above and are recorded so the entries do not read as
broader than they are.

- **No contract carries an attested in-force rule interval.** 623 edges are
  structurally admissible and are withheld by the rule-vintage requirement alone. This
  is a documentation gap rather than a search failure.
- **No validated point-in-time expectation source.** Pre-release captures exist for 9
  of the 10 releases, but none is sealed as an expectation record with its bytes, its
  digest and its verifier.
- **The fourth study is descriptive only.** Persistence is a build-to-build quantity,
  and the cost layer is a second, independent blocker that no amount of collection time
  removes.
- **`neighbor_lag` is null on every real forecast row**, so the network rung of the
  ladder cannot be fitted on observed data.
- An empty match set from `match-cross-venue` states that no supplied pair graded
  `EXACT`, **not** that no matching contract exists.
