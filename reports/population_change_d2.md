# Population change: the contract universe

Date: 17 September 2026.

This page records one change to how the study's candidate population is **read**, and why
it moves no declared estimand. It exists because admitting live-observed contracts was
recorded elsewhere as a cohort decision rather than a wiring change, and a cohort decision
has to be written down where a reader can find it rather than made quietly in a loader.

## What changed

The contract universe was read from the vendor archive alone:

| Layer | Glob | Class |
| --- | --- | --- |
| `kalshi_markets` | `data/external/kalshi-trades/markets-*.parquet` | vendor archive, CC-BY-4.0 |

It is now the **union** of two declared observation paths:

| Layer | Glob | Class |
| --- | --- | --- |
| `kalshi_own_markets` | `data/external/kalshi-own/markets/markets-*.parquet` | locally captured public data |
| `kalshi_markets` | `data/external/kalshi-trades/markets-*.parquet` | vendor archive, CC-BY-4.0 |

The **union, never the intersection**. A contract either path observed is a candidate. A
contract both paths observed is one contract with two observations, not two rows, so the
denominator cannot be inflated by the overlap.

Every row carries which path(s) observed it, as `observation_origin` in
`archived_only` / `live_only` / `archived_and_live`. Where the two paths state different
values for the same field, the declared layer order resolves it — the live layer is read
first — and a field only one path states is still carried, so the union is not the live
layer with the archive's gaps left in it.

The universe is defined once, in `src/market_propagation/ingest/kalshi_universe.py`, and
read from there by the graph builder, the study panel, the forecast panel, the cross-venue
matcher and the CLI. No caller derives a universe of its own, so the five readers cannot
drift apart.

## Why this is a conformance fix and not an estimand change

The preregistration declares membership in section 2 as follows:

> a contract is a candidate for a release when its series is one of the four declared
> policy series (`FED`, `FEDDECISION`, `KXFED`, `KXFEDDECISION`) and its own recorded
> listing interval covers the release instant.

That rule names a **series** and a **listing interval**. It names no observation path.
"Recorded" had been implemented as "recorded in the vendor archive", which is narrower
than the declaration; the union brings the implementation up to the declaration rather
than past it.

Nothing declared moves. The four declared policy series, the listing-interval test, the
grid-as-denominator, the 785 declared pairs and the treatment of a never-traded pair (its
masked rows stay in the grid) are unchanged. This is why the retrospective arm's frozen
population is untouched by it.

## What it changes, per arm

**Retrospective arm `core_2025h1` — nothing, measured.** No contract in the union is
`live_only` on this checkout, because the only live capture held so far begins 2026-09-16
and the arm's releases are from 2025. The arm's population is therefore identical under
both readings, and every statement already made about its ten releases stands.

**Forward arm `forward_2026h2` — material.** The vendor archive's rows end 2026-01-29.
The forward arm's releases are October through December 2026, so they can only ever be
covered by this repository's own capture. Under an archive-only universe those releases
would have had **no candidate contracts at all**: the arm's grid would have been empty by
construction rather than blocked on missing evidence, and an empty grid is not a fact
about the releases — it is a fact about which file was read. The union is what makes the
forward arm's declared candidate universe exist to be populated by the capture cadence.

## Measured effect on this checkout

| Quantity | Value |
| --- | --- |
| Contracts in the union | **689** |
| `archived_only` | **526** |
| `live_only` | **0** |
| `archived_and_live` | **163** |
| Shards per layer | `kalshi_own_markets` 1, `kalshi_markets` 4 |
| Layers present / empty | both present, none empty |

The union identity is **asserted rather than reported**: diagnostics whose counts do not sum
to the universe raise `UniverseError`, so a provenance split that disagrees with the
population fails the read instead of being printed beside it.

**Why `live_only` is zero, and why that is not a defect.** The live layer holds 163 distinct
contracts and **every one of them is also in the archive**. That was measured directly
rather than inferred: **0** live rows carry an `open_time` after the archive's own last row
(2026-01-29), so no captured contract *could* be absent from the archive in the first place,
and the 163 overlaps are real. The union reads them correctly.

Criterion 1 — a live-observed contract admitted that the archive lacks — therefore stays
**dormant until a capture covers a contract listed after the archive's end**. That is
precisely the forward arm's position, because the archive cannot reach a contract first
listed after 2026-01-29 and the capture cadence is what will see one. Dormant here means
*the case has not arisen*, not *the mechanism is untested*: the union mechanics are pinned
by fixtures in `tests/test_kalshi_universe.py`, including a `live_only` contract that
survives the union and a contract both paths hold that collapses to one identity.

## What it does not change

- **No eligibility change.** Study eligibility, `study_eligible` and the rule-vintage gate
  are untouched, and the count of study-eligible contracts stays **0 of 785**. The universe
  module decides no eligibility at all; it answers which contracts exist, not which are
  admissible.
- **No provenance claim is borrowed.** A caller that names its own glob reads exactly that
  path and is recorded as claiming no declared observation provenance, rather than being
  labelled with a layer it did not come from. Only the two declared paths carry a layer
  label.
- **Archive presence is no longer the definition of existence.** It remains one observation
  mechanism among two.

## What this does to D2

D2 required that the studied contracts be observable before their own window.
`reports/empirical_dependencies.md` recorded the universe question as *left open
deliberately* because admitting live-observed contracts changes which contracts the study
is about. That decision is now taken, and this page is its record.

D2 is still **not satisfied for the retrospective cohort**, and this change does not
satisfy it: the 2025 windows are refused by the observation route's own interval check, and
the union admits no live-observed contract into them because there is none to admit. What
has changed is that the universe is no longer the reason. The remaining gap is the overlap
itself, and the capture cadence is the only thing that closes it for a future arm.

## How a reader can check this

The provenance travels with the result rather than only being described here.
`match-cross-venue` records `first_venue_observation_provenance` for the contracts it read
and `first_venue_universe_is_the_union_of_observation_paths`, which is true exactly when no
`--markets-glob` was named. Omitting `--markets-glob` reads the union; naming one reads
that path alone. The study panel's own summary carries the same split under `population`.
