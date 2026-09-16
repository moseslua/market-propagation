# Source feasibility ledger

Evidence gathered during study execution for the two external inputs the primary
estimand needs: a verified contract payoff rule vintage and an independent
pre-release expectation. Every row records what was attempted and what came back. A
source that failed is recorded with its exact failure rather than omitted, because
the plan requires a second independent source category to be tested before a blocker
is declared.

This revision corrects three entries in the previous one. The corrections are listed
first, because a ledger that silently drops a wrong row is not a ledger.

## 0. Corrections to the previous revision

| Withdrawn entry | Was recorded as | Corrected finding |
| --- | --- | --- |
| "`KXFEDDECISION` lists one decision date at a time, so no matched pair is simultaneously listed" | a retrieval conclusion | this was inferred from the archive's `open_time`, which is the instant the previous meeting resolved, not the listing instant. It is not a venue property and it is withdrawn. |
| "The live API does not serve the 2025 contracts at all" | HTTP 404 / empty body on the current-markets endpoint | the **historical** endpoint serves them: `GET /trade-api/v2/historical/markets?event_ticker=KXFEDDECISION-25JAN&limit=1000` returns HTTP 200 with all five January 2025 contracts and their `rules_primary`/`rules_secondary` text. Raw body and headers are captured in `.audit/astra-repair/kalshi_historical_jan.json` and `.headers`. |
| "`rule_verified_pairs` is 0 of 88" | a panel of 440 rows | the declared grid gives 785 pairs and 3,925 rows; `rule_verified_pairs` is 0 of 785. |
| "No pre-release snapshot retrieved" | the archive held no capture near any release | **withdrawn**: 9 of the 10 declared releases have a pre-release capture, and `cpi_2025_01` was read in full — snapshot `20250115085107`, captured 4h39m before its release, carrying the headline and core month-on-month consensus. The captures are not yet bound as validated expectation records. See `.audit/astra-repair/evidence_three_gates.md`, Gate 3. |

The second correction does **not** unblock the rule gate. Rule *text read now* is not
an in-force interval: nothing in a 2026 fetch bounds which version a 2025 contract
carried during the release window. The gate stays blocked, for the correct reason.

## 1. Contract payoff rules

The primary cohort admits a contract by verified settlement semantics, not by ticker
resemblance. The rule must bind the contract id, the rule text hash, the source, the
observation time and the in-force interval.

### What the series actually are

Four Kalshi series are declared policy series. Each states its payout in the venue's
own archived text, which is what `ingest/policy_predicates.py` reads.

| Series | Predicate, as the venue itself states it | Archived contracts |
| --- | --- | --- |
| `KXFEDDECISION` | "Will the Federal Reserve Cut rates by 25bps at their {month} {year} meeting?" Strikes `C25`, `C26`, `H0`, `H25`, `H26` (and `H26`-style strict extensions) | 130 |
| `FEDDECISION` | the same stated payout forms | 70 |
| `FED` | "Will the target federal funds rate be above {threshold}%?" / "… above 0.25% following the Fed's {month} meeting" | 347 |
| `KXFED` | the same stated payout forms on the Kalshi series prefix | 142 |

Measured: 689 contracts across the four series; 155 carry a predicate
`ingest/policy_predicates.py` can read and a month the declared calendar dates. The
other 534 are refused, 533 of them because the declared calendar dates no meeting in
their month (the calendar covers 2024-11 through 2025-12, so 2021–2024 and late-2025
contracts are unplaceable) and one because its published payout text is not one of
the declared forms.

The strike vocabulary is identical across every `KXFEDDECISION` expiry, which is what
makes a matched-semantics pair constructible in principle and, on this archive, a
live one in practice. With the rule requirement satisfied by a diagnostic sentinel
that is explicitly not evidence, the declared calendar, predicates, liveness and
window rules admit **623 edges** across the ten releases
(`.audit/study-v3/structural_probe.json`). The graph's own 785
`rule_vintage_unverified` refusals come from the receiver's rule check, which runs
before any donor is sought, so that count alone does not measure the gap; the 623
does.

### Retrieval attempts for rule text

| Route | Result |
| --- | --- |
| `GET /trade-api/v2/historical/markets?event_ticker=KXFEDDECISION-25JAN&limit=1000` | **HTTP 200**, all five January 2025 contracts with `rules_primary` and `rules_secondary` |
| `GET /trade-api/v2/markets/{ticker}` | HTTP 404 for `KXFEDDECISION-25JAN-H0`, `FED-25MAR-T4.25`, `KXFED-25MAR-T4.25` |
| `GET /trade-api/v2/markets?tickers=KXFEDDECISION-25JAN-H0` | HTTP 200, body `{"cursor": "", "markets": []}` — the current-markets endpoint does not serve settled contracts |
| `GET /trade-api/v2/series/KXFEDDECISION` | HTTP 200, `contract_terms`, `additional_prohibitions`, `category: Economics`. Current terms only. |
| `GET /trade-api/v2/markets?series_ticker=KXFEDDECISION` | HTTP 200, current expiries with structured `custom_strike` and `mutually_exclusive: true` |
| `web.archive.org/cdx/search/cdx?url=…` | reachable (the "HTTP 503 on every attempt" record is from an earlier environment and is withdrawn). Coverage for the four declared series measured at **zero**: 16 of 16 queries returned, and 0 of 155 candidate contracts has a capture. See `scripts/probe_rule_archive_coverage.py` |
| `cftc.gov/filings/…` product certifications | HTTP 200, free, dated, and authoritative. Each filing carries the Official Product Name, the rulebook name, and Appendix A with the Terms and Conditions, Payout Criterion, Expiration and Settlement Value. Filed per **product template**, not per market, strike or event |

**Conclusion.** The shape of the needed evidence is obtainable and the historical
endpoint returns the contracts' rule text, but no route returns an **in-force
interval** for a 2025 contract. A rule vintage therefore cannot be attested from
these routes, and `reports/contract_rule_registry.json`
(`status: frozen_local_unregistered`) carries no per-contract rule record to fall
back on. The rule-vintage gate remains blocked. Settlement outcomes, current rule
text and a contract's own listing window are all recorded as inadmissible
substitutes, in `configs/neighbor_graph_v2.yaml`.

### The dated-snapshot route, measured at zero coverage

The archive route was previously recorded as unreachable. It is reachable, and it was
measured per candidate rather than sampled.

| Quantity | Value |
| --- | --- |
| Declared candidate contracts | 155 |
| CDX queries issued | 16 (four series across two API hosts and two page hosts); all 16 returned |
| Candidates with a market-API capture | **0** |
| Candidates with a capture dated before their decision date | **0** |
| Candidates with no capture at all | **155** |

A zero from a failed query is not a measurement, so the count of returned queries is
load bearing here: the archive answered every request and had nothing for these
contracts. What it does hold, so the zero is not read as an unsearched archive:

* Six policy-series contracts have captures, and none is a declared candidate:
  `FED-015`, `FED-CUT-SEPTEMBER-25BPS`, `KXFED-CUT-SEPTEMBER-25BPS`,
  `KXFEDDECISION-26JAN-C25`, `KXFEDDECISION-26JAN-H0`, `KXFEDDECISION-26MAR`.
* The only market-API capture naming any declared series anywhere in the archive is
  `KXFEDDECISION-25DEC-T5.25` at 2026-03-26, archived with status **401**: a 585 byte
  error page carrying no rule text. It is not one of the 155 either.
* Market-page captures are client-rendered shells. One was fetched in full: 13 KB
  with no contract text in it.

Artifact: `.audit/study-v3/rule_archive_coverage.json`. Probe:
`scripts/probe_rule_archive_coverage.py`.

### The regulatory-filing route, located but not shown to cover these contracts

Kalshi is a registered DCM, so it self-certifies each product with the CFTC under
Part 40, and the CFTC publishes those filings as free, dated PDFs. One was fetched
and read in full: `ptc09022529868.pdf`, a Regulation 40.2(a) notification dated
2 September 2025 for the `"Will <outcomes> occur in <events>?"` template, carrying
Appendix A with the Terms and Conditions, Payout Criterion, Expiration time and
Settlement Value.

That is better-shaped evidence than anything else found: authoritative, dated, free,
and it contains actual contract terms rather than marketing text. Three limits stop
it from closing the gate, and none of them is a search failure:

1. **It is per product template, not per market.** The fetched filing names no
   strike, no event and no individual market, so it cannot bound one contract's text.
2. **The Fed-decision product predates the study window.** Kalshi has listed those
   contracts since at least 2021, so the certification covering the product family is
   older than the January to May 2025 windows and its date cannot bound a version in
   force inside them.
3. **No enumerable index was reachable.** `cftc.gov/filings/ptc/YY/MM/` and
   `cftc.gov/filings/orgrules/` return HTTP 404, and `kalshi.com/regulatory/filings`
   returns HTTP 429 (rate limited, not missing). Coverage for the specific contracts
   is therefore not established in either direction.

Whether a rulebook-level filing can attest a contract-level vintage at all is a
question about the study's declared evidence standard, not a search problem.
`configs/neighbor_graph_v2.yaml` requires a per-contract in-force interval, and
widening that requirement is a decision for the study owner rather than for this
execution.

### What is recorded instead

The panel keeps the consequence visible rather than smoothing it: all 3,925 rows of
the declared complete panel carry `rule_version_unknown`,
`rule_verified_pairs` is `0` of `785`, and every row is masked. The leg prices are
retained on those rows, because the masking applies to the estimand rather than to
what was observed. The graph records the same refusal per contract: 785
`rule_vintage_unverified` decisions in `.audit/study-v3/graph_decisions.json`.

## 2. Pre-release expectations

The plan requires a demonstrably pre-release archived forecast or a
contemporaneously published survey summary. A value captured today is not proof that
it existed before the release, so the retrieval time cannot substitute for the
publication time.

### The validator now exists; the source does not

`market_propagation.ingest.expectations` implements the point-in-time contract and
refuses, with its own code: a forecast published at or after the release
(`expectation_published_at_or_after_the_release`); a forecast scored against a
revision (`expectation_targets_a_revised_actual_not_the_first_print`); a unit that
disagrees with the release's own declared statistic
(`unit_does_not_match_the_release_statistic`); an unnamed consensus or verification
method; a market-implied value (`market_implied_expectation_cannot_validate_the_market`);
an incomplete news vector (`news_vector_is_incomplete`); and evidence bytes whose
digest no longer matches the record
(`evidence_bytes_do_not_match_the_recorded_digest`).

No source exists to validate, so the loader raises `expectation_source_is_absent`.
The absence is a refusal code, not a zero-valued surprise.

### Local evidence first

`data/public/bls-normalized/releases.parquet` is the sealed release dataset the
coverage stage reads. It carries, for all ten development events, the first-release
actuals, the reference period, the revision block, the source URL, the raw payload
hash and a `received_time` of 2026-09-13. It carries **no** expectation field, and
`usable_time` is null on every row, which is why the study runs on the source clock.
The releases it carries declare six CPI statistics and six employment statistics; the
declared news vector reads `cpi_headline_sa_mom_pct` and
`payrolls_change_thousands`, and both exist as first prints in that dataset.

### External attempts

| Source category | Route | Result |
| --- | --- | --- |
| Archived calendar snapshot | `web.archive.org` CDX for tradingeconomics, investing.com economic calendar, forexfactory | the archive is reachable, and the earlier "HTTP 503 (three hosts), timeout (two)" record is withdrawn. Captures exist but none is near the releases: `tradingeconomics.com/united-states/inflation-cpi` 2011-04-24, `investing.com/economic-calendar/cpi-733` 2015-02-28, `forexfactory.com/calendar` 2005-05-20 (status 404). No pre-release snapshot retrieved. |
| Contemporaneous survey summary | `tradingeconomics.com/united-states/inflation-cpi` | HTTP 200, 368 KB. The earlier "HTTP 503" record is withdrawn; the page is reachable. It may carry past-release consensus values, but a page read in 2026 cannot establish that a consensus was public *before* a January 2025 release, which is the requirement the validator enforces. Not admitted without a dated capture. |
| Contemporaneous survey summary | `investing.com/economic-calendar/cpi-733` and `…/nonfarm-payrolls-227` | HTTP 403 (was recorded as 503 and timeout; the site is blocking rather than missing) |
| Contemporaneous survey summary | `forexfactory.com/calendar` | HTTP 403 (was recorded as a timeout) |
| Survey / preview article | web search for a January 2025 CPI consensus with explicit figures | aggregator and social posts, no primary pre-release preview with verifiable figures and publication dates. Not usable as evidence. |

**Conclusion.** The earlier "no pre-release snapshot retrieved" line is withdrawn.
Nine of the ten declared releases have a pre-release archived capture, and
`cpi_2025_01` was read: snapshot `20250115085107`, captured 4h39m before its release,
carrying the headline and core month-on-month consensus. The retrieval route is
therefore proven, and `expectation_source_is_absent` now describes the *bound* source
rather than the archive: no capture has yet been sealed as an expectation record with
its own bytes, digest and named verifier, and the employment vector has not been
checked for first-published payroll revisions. Until both are done the
news-conditioned and conditional-predictive-propagation rungs remain unestimable and
`expectation_verified` stays `false`. This is stated as a blocker rather than worked
around: the timing-only ladder is the only rung that runs without an expectation by
design, and the exploratory absorption measurement makes no news claim.

## 3. What each rung therefore needs

| Rung | Needs | State |
| --- | --- | --- |
| Absorption (timing-only) | a transaction panel with both legs observed | runnable; exploratory because rule evidence is absent |
| News-conditioned | a validated pre-release expectation vector | blocked: pre-release captures exist for 9 of 10 releases, but none is bound as a validated expectation record |
| Conditional predictive propagation | the news rung, plus a lagged return from a matched neighbour under a verified rule vintage | blocked twice: no news vector, and no verified rule interval for the 623 structurally admissible edges |

## 4. Smallest remaining external requirement

Two independent acquisitions would unblock the study, and neither is a coding task:

1. **A per-contract rule-vintage record**, in the shape
   `configs/neighbor_graph_v2.yaml` already declares: contract id, rule text hash,
   source, verification method, and the instants bounding the version's in-force
   interval. 623 structurally admissible edges and 785 receiver-side refusals are
   blocked on exactly this. Historical rule text alone is not enough, and a
   settlement outcome is never enough.
2. **A pre-release expectation series.** Reuters or comparable survey previews with
   explicit figures, or a licensed consensus database with monthly vintages. The
   plan's source hierarchy names these as candidates. The archive route is now open,
   so the smallest remaining step is narrower than a licensed feed: bind the nine
   pre-release captures as validated expectation records, and verify the employment
   vector including first-published payroll revisions.

Until both exist, the primary confirmatory panel stays blocked. Expanding the release
calendar does not substitute for either: more releases bring more contracts whose
rule vintages are equally unattested.
