# Evidence acquisition and future research

Measured on 17 September 2026. The retrospective estimand remains unidentified
from the available provenance. All work below preserves the frozen population,
matching requirements and statistical gates.

## Research execution results

The subsequent research run is under `.audit/research-evidence-20260917/`.
Its durable task ledger is `.omx/ultragoal/ledger.jsonl`. Source acquisition and
analysis completed where the evidence permitted them. On the first pass, the objectives requiring
admissible forecasts for all ten releases and complete two-venue fee evidence
remained unmet because of the source limitations below. The later reader recovery
supplies both published base fee tables for conditional calculations. The archive
continuation validates all five payroll forecasts and three CPI forecasts for
descriptive first-print surprises. Exact-statistic coverage is 8 of 10 releases.

```bash
uv run --no-sync python scripts/research_evidence_report.py \
  --root .audit/research-evidence-20260917 \
  --out .audit/research-evidence-20260917/results.json
```

The replay verifies the source bytes it reads. `rules/sources.json` and
`expectations/sources.json` identify the archived payloads. The acquisition
recipe is `.audit/research-evidence-20260917/run_sources.py`; it skips previously
recorded requests. The report reads sealed historical inputs without changing them.

### Provenance and definition changes

Current primary payloads were acquired for all 107 historical contracts. Each
payload binds its ticker, payout text and explicit meeting-date text. That closes
the current-text discovery gap for the 785 historical release-contract pairs.
It supplies no independently dated historical interval. The availability API
returned HTTP 429 for the four probed official document/event URLs. The earlier
CDX queries returned 503. Neither outcome proves that historical material is absent.

`KXFEDDECISION-26SEP-H26` has two different captured primary texts. The earlier
one says `Hike of >25bps`; the later one has `Hike of   25bps`. The secondary text
is unchanged. These bytes establish a changed comparison expression, not when
the change took effect or how the venue interpreted the later ambiguous wording.
The existing attestation command closes the earlier observed version at
`2026-09-16T19:31:00Z`. Its first observation is `2026-09-16T16:35:00Z`.
Both instants come from captures. They do not establish a 2025 rule vintage.

The held certification PDF is dated April 5, 2023 and names the FEDDECISION
product template. Its date is a filing date. It does not bind the instantiated
2025 contract parameters or establish an unchanged version through each window.

### Initial forecast candidates and metric mismatch

Eight first-party FactSet candidates were parsed from saved pages. Four payroll
medians, in thousands of jobs, are 153 for December 2024, 170 for January 2025,
130 for March 2025 and 135 for April 2025. Their current pages state publication
dates before the respective releases. The archive requests returned 429, so the
run has not established a pre-event archived forecast or a licensed point-in-time
source. None was admitted to the frozen news model.

The four CPI candidates forecast unadjusted year-over-year inflation. Their
values cannot replace the declared seasonally adjusted month-over-month series.
All ten release IDs remain in the result, including the two without a parsed
FactSet candidate. The ALFRED form was inspected with its real field names and
vintage options. Both read-only download requests returned HTTP 500 rather than
a vintage archive. Original BLS release bytes remain the usable revision evidence.

### Measured attrition and revisions

At the 300-second horizon, the fixed 785-pair denominator has 43 observed
baselines, 33 observed endpoints and 21 pairs with both. All 785 rows remain
scientifically masked. Historical provenance recovery alone would not supply the
missing observations. These are observation counts, not absorption estimates.

Five original Employment Situation releases report ten prior-month revision
steps. Their mean absolute size is 26.2 thousand jobs and their maximum absolute
size is 51 thousand jobs. These steps are correlated and cover only the revisions
reported in those five releases. They are not independent forecast errors or a
complete vintage history. That initial pass calculated no consensus surprise.

### Archived forecasts and descriptive surprises

The continuation recovered pre-release monthly forecast quotations for all ten
releases. It verifies both the archive-index response and the captured page from
their held hashes, checks the archive wrapper's original URL and capture time,
and extracts the exact quotation and its attribution. Five payroll and three CPI records pass
the unchanged `validate_expectation` and `load_expectations` entry points.

```bash
uv run --no-sync python scripts/replay_archived_expectations.py \
  --registry .audit/research-evidence-20260917/continuation-6/archive_registry.json \
  --out .audit/research-evidence-20260917/continuation-6/forecast-replay
```

Exit 2 reports incomplete ten-release coverage. The output retains every release
and its refusal reason. The earlier `research_evidence_report.py` command still
reports the initial current-page probe; it does not include these later archives.

| Release | Consensus | Forecast, thousand jobs | First print | Surprise |
| --- | --- | ---: | ---: | ---: |
| January 10 | CNBC-reported economist consensus; underlying poll unnamed | 155 | 256 | +101 |
| February 7 | Dow Jones | 169 | 143 | -26 |
| March 7 | FactSet median | 160 | 151 | -9 |
| April 4 | Dow Jones | 140 | 228 | +88 |
| May 2 | FactSet median | 135 | 177 | +42 |

For January 15 CPI, the Dow Jones forecast is 0.3%, the first print is 0.4%,
and the surprise is +0.1 percentage point. For February 12 CPI, the forecast is 0.3%, the first print is 0.5%,
and the surprise is +0.2 percentage point. For March 12 CPI, the corresponding
values are 0.3%, 0.2%, and -0.1 percentage point.

These are source-specific forecasts from heterogeneous polls and observation
times, not a fixed-provider latest-consensus series. The March and May FactSet
pages provide no timezone-qualified publication instant. Their records preserve
that unknown and use the independently verified archive time as a conservative
publication upper bound for the pre-release gate. Every exported record states
that its clock is ineligible for publication or latency measurement. The replay
uses original BLS first prints, not the revised actuals discussed in FactSet's
historical comparisons.

All five archived CPI quotations state monthly headline forecasts but do not
explicitly identify seasonal adjustment. BLS publishes both adjusted and
unadjusted monthly changes, so its release convention alone cannot resolve the
forecast statistic. Separate CNBC result reports explicitly associate the same
January, February and March Dow Jones polls with seasonally adjusted monthly headline CPI.
Their exact event, poll,
URL and hash bindings are pinned in the replay. Those result reports define the
statistic only; the forecast values and pre-release existence come from the
independent earlier archives. The resumed January acquisition supplies a new
Dow Jones forecast captured at `2025-01-15T10:39:07Z`, before the 13:30 release.
The earlier Reuters record remains unchanged and unresolved in `continuation-4/`.
April's FactSet seasonal basis remains unverified, so that forecast remains a
semantic refusal. The May continuation recovered WION's explicit Bloomberg
monthly headline forecast of 0.3% from a `2025-05-13T06:42:03Z` snapshot, before
the 12:30 release. Its seasonal basis is also unstated. Both April and May remain
unscored semantic refusals; neither is a missing quotation now. Earlier
CME/Econoday, forecast-PDF and other failed archive probes remain preserved.

The latest registry, per-release outcomes, family-specific expectation exports,
and exact hashes are under `continuation-6/`. The prior eight-record replay is in
`continuation-5/`, and the seven-record replay is in `continuation-4/`.
Earlier acquisition attempts remain
under `continuation-3/`, including the successful April payroll query at an earlier
archive target time. No record is wired into the frozen study configuration.
Historical rule vintages and masked market responses still prevent a propagation
fit; descriptive forecast surprises do not clear those gates.

### Fixed-direction perp evidence

The prior `perp_differentials_report.py` field `sign_stable_across_observations`
does not measure directional persistence. `funding_differentials` reorders the
long and short sides at every build, making its spread nonnegative by construction.
The new replay keeps the BTCUSDT orientation fixed as Bybit APR minus Binance APR.
Across 22 held builds from `2026-09-16T10:30:34Z` through
`2026-09-17T10:34:48Z`, it finds 11 positive and 11 negative observations with
five sign reversals between adjacent observed builds. Missing time between builds
remains unobserved. These are quoted annualized rates, not earned funding.

The refreshed broad spread report contains 569,099 quoted differentials over
46 source build stamps and 46 assets. Every differential still carries the
execution-cost limitation. Its sign-stability field must not support a claim.

The initial conditional cost replay was run at target notionals of 1,000, 10,000 and
100,000 USDT against the same saved books. The known components were respectively
1.0940210450, 11.0243659150 and 110.2436591500 USDT, each plus unknown Binance fees.
These use matched quantities of 0.013, 0.131 and 1.310 BTC. They assume Bybit VIP 0
base taker fees. The books were sequential observations and do not show future
exit costs, actual fills, net returns or capacity.

The next empirical steps need exact-statistic CPI definitions for April and May,
pre-release expectations for unemployment rate and average hourly earnings for
all five employment releases, an independently dated contract rule history and
future release observations. The current source failures remain in the evidence
ledger. No scientific gate was relaxed to replace those inputs.

### Alternate-route follow-through

The public reader returned a rendered copy of
`https://www.binance.com/en/fee/futureFee` at `2026-09-17T12:23:07Z`.
The saved reader-response hash is
`ab6e596bb63b984f3671e00deaf360da1198786386b36902d6b588084f92b531`.
Its base USDT regular-user row gives 0.0200% maker and 0.0500% taker.
The parser selects that column independently of the discounted USDT and USDC
columns. It uses the fee table, not the FAQ's explicitly hypothetical examples.

```bash
uv run --no-sync python scripts/replay_perp_cost_evidence.py \
  --sources .audit/evidence-acquisition/sources-20260917 \
  --binance-sources .audit/research-evidence-20260917/continuation-2 \
  --notional 10000 \
  --out .audit/research-evidence-20260917/continuation-2/cost-10000.json
```

The fully specified conditional base-fee plus frozen-book totals are
2.0862596950, 21.0230784650 and 210.2307846500 USDT for the respective
1,000, 10,000 and 100,000 USDT targets. These assume regular-user Binance and
VIP 0 Bybit taker rates, without discounts. The saved Binance evidence is a
third-party rendered copy, not origin response bytes. Its retrieval time does
not establish an effective interval. Account applicability, synchronized books,
future exits and realized execution remain unobserved. The execution-cost layer
and any net-return or capacity claim remain incomplete.

Arquivo.pt's documented URL-version API returned HTTP 200 and empty result lists
for the four queried FactSet payroll pages. That is a negative result for those
URLs in that archive, not proof of universal absence. The installed browser
adapter rejected this session's API-key authentication mode before creating a
tab. No authentication, permission or account settings were changed.
All new source responses and the replay recipe are in
`.audit/research-evidence-20260917/continuation-2/`.

## Acquisition outcome

The local launchd service `local.information-diffusion.prospective` runs
`scripts/collect_prospective_evidence.py --loop`. Its first real cycle completed
rule capture, attestation, second-venue metadata and market/trade capture:

| Artifact | Observed count |
| --- | ---: |
| Live rule captures / attested contracts | 163 / 163 |
| Polymarket metadata records | 9,784 |
| Captured market rows / trade rows | 163 / 61,013 |
| Historical release-contract pairs in recovery queue | 785 |
| Semantic and interval gap rows | 2,355 |
| Original BLS release vintages verified / releases reporting revisions | 10 / 5 |
| Validated archived expectation records | 8, including 5 payroll and 3 CPI |
| Archived monthly forecast quotations recovered | 10 of 10 releases |

Each job retains a distinct attempt directory, command, logs, source records and
raw-byte references. The source raw stores deduplicate bytes while retaining
separate observations. Successful slots are skipped on restart; partial results
and failures remain named. Daily jobs follow 06:41 host time. Monthly jobs follow
day 1 at 06:47 host time and request the preceding 35 days to UTC midnight.
The initial September catch-up was executed before the host-time correction and
recorded its actual window ending 1 September at 06:47 UTC; its receipt is retained.

The service depends on this Mac being awake, online and logged in. It does not
automate browser-only BLS page acquisition, release-window quote sampling or a
confirmatory fit. The monthly trade archive is source-time history, not proof of
live participant receipt times. Analysis inputs must be selected explicitly from
the attempt receipts; the service does not overwrite sealed analysis datasets.

Operational commands:

```bash
uv run --no-sync python scripts/collect_prospective_evidence.py --report
launchctl print gui/$(id -u)/local.information-diffusion.prospective
uv run --no-sync market-propagation protocol-freeze \
  --verify .audit/protocol/protocol_freeze_resealed.json
```

The existing seal `afaa2a302582c8c731ec2153f41067b819931b64b86d7310cb12f3a048e9bb12`
still matches all 31 covered files. Acquisition changes do not retroactively
register observations as confirmatory data.

## Research directions

### 1. Forward coverage and attrition

**Question:** After each scheduled release, how much of the fixed candidate
denominator has usable baselines and endpoints?

**Evidence now:** `.audit/evidence-acquisition/probes/forward-progress.json`
records 706 of 706 candidate windows with a covering rule capture across six
future releases. Each release is still `pending_release_has_not_published`.

```bash
uv run --no-sync market-propagation confirmatory-progress \
  --root data/prospective/rules --output .audit/forward-progress-new.json
```

**Needed:** published BLS pages and post-release market observations. The existing
`capture_bls_releases.py` verifies browser-captured pages; it does not fetch them.
Run the existing panel builders only after selecting and verifying the new input
layers. Keep unobserved candidates in the denominator. Stop before estimation
when the release, rule version, baseline or endpoint is absent.

### 2. Definition changes and evidence lifetime

**Question:** Which contract definitions change between observations, and which
changes affect payoff comparability?

**Evidence now:** `.audit/evidence-acquisition/probes/replay-integrity.json`
separates changing page bytes from changing rule text. All 163 contracts occur on
changed pages; one has differing stored rule text, `KXFEDDECISION-26SEP-H26`.
Differences are leads for inspection, not automatic substantive-rule changes.
The same probe compares old and new second-venue record subjects and identifies
which differences intersect the previously declared study candidates.

**Entry point:** `market-propagation attest-rules --root data/prospective/rules`
and the observation manifests in the successful attempt receipts. Compare exact
primary/secondary rule text, lifecycle fields and the full cited payload. Stop
historical certification when no dated contract-bound interval covers the window;
never infer an effective date from the retrieval time.

### 3. Historical provenance recovery

**Question:** Can a discovered primary source supply the missing semantics and
dated rule interval for a historical candidate?

```bash
uv run --no-sync python scripts/recover_contract_evidence.py \
  --out .audit/evidence-recovery/20260917-expanded
```

Replay verifies held hashes and recreates all 2,355 queue rows. Use a fresh output
directory and `--fetch --probe-contracts 2 --archive-limit 4` for another explicitly
bounded acquisition. URLs come from documented endpoints and links in primary
metadata. The current exact market/event archive queries returned HTTP 503.
That result does not show that the archive has no historical material. Stop a
route on access restriction or the request bound; only admissible dated evidence
may change a historical rule gate.

### 4. Revision diagnostics without surprise claims

```bash
uv run --no-sync python scripts/replay_expectation_evidence.py \
  --sources .audit/evidence-acquisition/sources-20260917 \
  --out .audit/expectation-recovery/report.json
```

The script re-parses original BLS bytes and checks their values, reference months
and embargo/calendar agreement against the sealed release dataset. Five releases
report revisions. These support diagnostics of what those original releases say,
not a complete ALFRED revision history or a consensus surprise series.

Public provider pages were archived from Trading Economics, Investing.com,
Econoday and Philadelphia Fed. The replay now has ten pre-release headline
quotations, but April and May CPI seasonal definitions remain unverified, and the
employment replay covers payroll change only. No unemployment-rate or earnings
month-on-month expectation set has passed the validator. Quarterly SPF cannot
substitute for the declared monthly news vector. Stop any news/network estimation
until all required dimensions pass the existing expectation validator.

### 5. Conditional perpetual-futures costs

```bash
uv run --no-sync python scripts/replay_perp_cost_evidence.py \
  --sources .audit/evidence-acquisition/sources-20260917 \
  --binance-sources .audit/research-evidence-20260917/continuation-2 \
  --notional 10000 --out .audit/research-evidence-20260917/continuation-2/cost-10000.json
```

The official [Bybit fee table](https://www.bybit.com/en/help-center/article/Trading-Fee-Structure)
was saved as HTML before extraction. The script reads its tiered maker/taker
columns and verifies BTCUSDT perpetual contract and quantity-step compatibility
using both venues' saved specifications. For a matched 0.131 BTC quantity, the
assumed Bybit VIP 0 round-trip taker fee is 10.9981659150 USDT. The known fee plus
displayed round-trip spread component is 11.0243659150 USDT, before Binance fees.
The recovered Binance base rate adds 9.998712550 USDT for a total of
21.0230784650 USDT in this conditional example.

The [Binance fee page](https://www.binance.com/en/fee/futureFee) returned empty
content directly and a verification page through the first alternate reader.
A later public reader returned its table, saved with that acquisition method.
No rate was substituted from search snippets. Account tier, regional terms, effective
intervals, synchronized books, future exit depth and realized fills remain missing.
Stop at the conditional expression; it is not executable profit or a future-cost
bound. Never combine current fee/depth captures with historical spreads as though
they were contemporaneous.

## Reproducible source acquisition

```bash
uv run --no-sync python scripts/acquire_evidence_sources.py \
  --out .audit/evidence-acquisition/new-source-attempt
```

This issues one bounded public GET per named source, archives returned bytes with
the existing raw store, retains failed attempts and resumes an interrupted batch.
Reusing an output directory skips already recorded outcomes, including failures.
Use a new directory for a deliberate re-probe. A fetched empty page is still empty
evidence; downstream parsers must establish whether it contains the required fact.
