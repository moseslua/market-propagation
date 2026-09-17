# Evidence acquisition and future research

Measured on 17 September 2026. The retrospective estimand remains unidentified
from the available provenance. All work below preserves the frozen population,
matching requirements and statistical gates.

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
| Validated historical consensus records | 0 |

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
Econoday and Philadelphia Fed. No monthly historical forecast with demonstrated
pre-release publication was validated. Quarterly SPF cannot substitute for the
declared monthly news vector. Stop any news/network estimation until its required
consensus evidence passes the existing expectation validator.

### 5. Conditional perpetual-futures costs

```bash
uv run --no-sync python scripts/replay_perp_cost_evidence.py \
  --sources .audit/evidence-acquisition/sources-20260917 \
  --notional 10000 --out .audit/perp-cost-recovery/cost-example.json
```

The official [Bybit fee table](https://www.bybit.com/en/help-center/article/Trading-Fee-Structure)
was saved as HTML before extraction. The script reads its tiered maker/taker
columns and verifies BTCUSDT perpetual contract and quantity-step compatibility
using both venues' saved specifications. For a matched 0.131 BTC quantity, the
assumed Bybit VIP 0 round-trip taker fee is 10.9981659150 USDT. The known fee plus
displayed round-trip spread component is 11.0243659150 USDT, before Binance fees.

The [Binance fee page](https://www.binance.com/en/fee/futureFee) returned empty
content directly and a verification page through the alternate reader. No rate
was substituted from search snippets. Account tier, regional terms, effective
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
