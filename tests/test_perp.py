"""Tests for the PerpDexList parser, interval derivation and differentials.

Fixtures are synthetic markdown copied in shape from the live pages, so these
tests never touch the network. The cases chosen are the ones where a wrong
answer would be silent rather than loud: an em dash read as zero, a settled
payment conflated with a quoted rate, an interval guessed from one derivation,
and a quoted spread presented as though it were executable.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from decimal import Decimal
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from market_propagation.ingest.transport import TransportError
from market_propagation.perp import collector
from market_propagation.perp.differentials import (
    REFUSAL_EXECUTION_COST_UNAVAILABLE,
    cross_venue_basis,
    funding_differentials,
)
from market_propagation.perp.parse import (
    REFUSAL_INTERVAL_DISAGREES,
    REFUSAL_INTERVAL_UNDERIVABLE,
    PageShapeError,
    interval_from_apr,
    interval_from_settlements,
    parse_build_stamp,
    parse_funding_page,
    parse_market_page,
    parse_percent,
    parse_size,
    resolve_interval,
)
from market_propagation.perp.store import SnapshotStore

BUILD = "2026-09-16T09:29:18Z"
RECEIVED = dt.datetime(2026, 9, 16, 9, 30, tzinfo=dt.UTC)

HEADER = (
    "| Exchange | Symbol | Price | 24h volume | Open interest "
    "| Funding / interval | Funding APR | Paid 24h | Paid 7d | Paid 30d |"
)
RULE = "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"


def market_page(*rows: str, build: str | None = BUILD) -> str:
    body = "\n".join([HEADER, RULE, *rows])
    stamp = f"Data generated at {build}." if build else "No stamp on this page."
    return f"# BTC perpetual futures\n\n## Funding rates by venue\n\n{body}\n\n{stamp}\n"


#: An observed zero, an unobserved em dash, and a bare venue name (a CEX with no
#: page of its own) in one table.
UNOBSERVED_ROW = (
    "| variational | ETH | 2399.54 | $186.86M "
    "| $259.97M | -0.0052% | -5.71% | \u2014 | \u2014 | \u2014 |"
)
ZERO_ROW = "| gateio | BTC_USDT | 75816.35 | $0.00 | $0.00 | 0.0017% | +1.86% | +0.0121% | +0.1020% | +0.3871% |"
EIGHT_HOUR_ROW = "| binance | BTCUSDT | 75820.05 | $16.11B | $8.12B | 0.0009% | +0.98% | +0.0166% | +0.1141% | +0.6039% |"
ONE_HOUR_ROW = "| hyperliquid | BTC | 75820.50 | $5.01B | $2.74B | 0.0011% | +9.64% | +0.0281% | +0.1313% | +0.7223% |"


def test_em_dash_is_unobserved_not_zero():
    snapshot = parse_market_page(
        market_page(UNOBSERVED_ROW), asset_class="crypto", asset="BTC", received_at=RECEIVED
    )
    quote = snapshot.quotes[0]
    assert quote.paid_30d is None
    assert quote.paid_24h is None
    # A bare null would read as a gap a consumer has to interpret, so the reason
    # travels with the row.
    assert any(r.startswith("paid_30d:") for r in quote.refusals)


def test_observed_zero_is_distinct_from_unobserved():
    snapshot = parse_market_page(
        market_page(ZERO_ROW), asset_class="crypto", asset="BTC", received_at=RECEIVED
    )
    quote = snapshot.quotes[0]
    assert quote.volume_24h_usd == Decimal(0)
    assert quote.open_interest_usd == Decimal(0)
    # A venue that traded nothing carries no refusal: it was observed.
    assert quote.refusals == ()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("$16.11B", Decimal("16110000000")),
        ("$893.06M", Decimal("893060000")),
        ("$2.9K", Decimal("2900")),
        ("$0.00", Decimal(0)),
        ("\u2014", None),
        ("", None),
    ],
)
def test_size_parsing(raw, expected):
    assert parse_size(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("+0.98%", Decimal("0.0098")),
        ("-0.11%", Decimal("-0.0011")),
        ("0.0009%", Decimal("0.000009")),
        ("\u2014", None),
    ],
)
def test_percent_parsing(raw, expected):
    assert parse_percent(raw) == expected


def test_build_stamp_missing_raises_rather_than_defaulting():
    # Defaulting to the local clock would place an unstamped page in a series the
    # source never made, so it must fail.
    with pytest.raises(PageShapeError):
        parse_build_stamp("A page with no stamp at all.")


def test_header_mismatch_raises_rather_than_shifting_columns():
    shifted = market_page(EIGHT_HOUR_ROW).replace("Funding APR", "Annualised")
    with pytest.raises(PageShapeError):
        parse_market_page(shifted, asset_class="crypto", asset="BTC", received_at=RECEIVED)


def test_venue_link_and_bare_name_both_resolve():
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW, ONE_HOUR_ROW),
        asset_class="crypto",
        asset="BTC",
        received_at=RECEIVED,
    )
    assert [q.venue for q in snapshot.quotes] == ["binance", "hyperliquid"]


def test_build_time_is_the_source_stamp_not_the_receive_clock():
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW), asset_class="crypto", asset="BTC", received_at=RECEIVED
    )
    assert snapshot.build_time == dt.datetime(2026, 9, 16, 9, 29, 18, tzinfo=dt.UTC)
    assert snapshot.received_at == RECEIVED
    assert snapshot.build_time != snapshot.received_at


def test_interval_derives_from_apr_alone():
    # 0.0009% per settlement annualising to 0.98% implies eight-hour periods.
    assert interval_from_apr(Decimal("0.000009"), Decimal("0.0098")) == 8
    assert interval_from_apr(Decimal("0.000011"), Decimal("0.0964")) == 1


def test_interval_derives_from_settlement_count_alone():
    # 720 settlements over a 30-day window is one an hour.
    assert interval_from_settlements(720) == 1
    assert interval_from_settlements(90) == 8
    assert interval_from_settlements(180) == 4


def test_both_derivations_agreeing_yields_the_interval():
    resolved = resolve_interval(Decimal("0.000009"), Decimal("0.0098"), 90)
    assert not isinstance(resolved, tuple)
    assert resolved.hours == 8
    assert resolved.from_apr_hours == 8
    assert resolved.from_settlements_hours == 8


def test_derivations_disagreeing_refuses_rather_than_choosing():
    # 8h from the annualised rate, 1h from the settled count. The interval sets
    # the per-hour normalisation every differential depends on, so neither side
    # may be preferred silently.
    resolved = resolve_interval(Decimal("0.000009"), Decimal("0.0098"), 720)
    assert resolved == (None, REFUSAL_INTERVAL_DISAGREES)


def test_interval_underivable_when_both_inputs_absent():
    assert resolve_interval(None, None, None) == (None, REFUSAL_INTERVAL_UNDERIVABLE)


def test_single_derivation_is_used_when_the_other_is_absent():
    resolved = resolve_interval(Decimal("0.000009"), Decimal("0.0098"), None)
    assert not isinstance(resolved, tuple)
    assert resolved.hours == 8
    assert resolved.from_settlements_hours is None


def test_differentials_always_carry_the_cost_layer_refusal():
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW, ONE_HOUR_ROW),
        asset_class="crypto",
        asset="BTC",
        received_at=RECEIVED,
    )
    diffs = funding_differentials(snapshot)
    assert diffs
    for diff in diffs:
        assert REFUSAL_EXECUTION_COST_UNAVAILABLE in diff.refusals


def test_differential_is_ordered_cheapest_long_first_and_spreads_apart():
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW, ONE_HOUR_ROW),
        asset_class="crypto",
        asset="BTC",
        received_at=RECEIVED,
    )
    diff = funding_differentials(snapshot)[0]
    assert diff.long_venue == "binance"
    assert diff.short_venue == "hyperliquid"
    assert diff.apr_spread == Decimal("0.0964") - Decimal("0.0098")
    assert diff.apr_spread > 0


def test_per_hour_spread_withheld_when_interval_undiscoverable():
    # No annualised rate on one side, so no interval and no per-hour figure. The
    # annualised comparison is unaffected because the source already annualised it.
    unpaired = "| mexc | BTC_USDT | 75824.05 | $4.50B | $3.53B | 0.0008% | \u2014 | +0.0167% | +0.1135% | +0.6014% |"
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW, unpaired),
        asset_class="crypto",
        asset="BTC",
        received_at=RECEIVED,
    )
    # A missing APR removes the row from pair formation entirely rather than
    # letting it pair against a zero.
    assert len(snapshot.quotes) == 2
    assert funding_differentials(snapshot) == ()


def test_basis_is_the_log_price_difference():
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW, ONE_HOUR_ROW),
        asset_class="crypto",
        asset="BTC",
        received_at=RECEIVED,
    )
    basis = cross_venue_basis(snapshot)
    assert len(basis) == 1
    assert basis[0].lower_venue == "binance"
    assert basis[0].higher_venue == "hyperliquid"
    assert basis[0].log_basis > 0


def test_funding_page_parses_settled_payments_and_refuses_average():
    text = (
        "# BTC funding rate history\n\n"
        "| Exchange | Settlements | Total paid | Average APR | Largest | Smallest | Last settled |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
        "| hibachi | 717 | +1.3037% | +15.93% | +0.0369% | -0.0040% | 2026-09-16 09:00 |\n"
        "| binance | 90 | +0.5790% | \u2014 | +0.0100% | -0.0049% | 2026-09-16 08:00 |\n\n"
        f"Data generated at {BUILD}.\n"
    )
    snapshot = parse_funding_page(
        text, asset_class="crypto", asset="BTC", received_at=RECEIVED, window_days=30
    )
    assert [s.venue for s in snapshot.summaries] == ["hibachi", "binance"]
    first = snapshot.summaries[0]
    assert first.settlements == 717
    assert first.total_paid == Decimal("0.013037")
    assert first.last_settled == dt.datetime(2026, 9, 16, 9, 0, tzinfo=dt.UTC)
    second = snapshot.summaries[1]
    assert second.average_apr is None
    # The unobserved average is named, and the count it sits beside still parses.
    assert "average_apr:source_reported_no_figure" in second.refusals
    assert second.settlements == 90


def test_an_unobserved_last_settled_is_named_rather_than_left_bare():
    """Every unobserved cell carries a code, and the settlement instant is no exception."""
    text = (
        "# BTC funding rate history\n\n"
        "| Exchange | Settlements | Total paid | Average APR | Largest | Smallest | Last settled |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
        "| apex | 90 | \u2014 | \u2014 | \u2014 | \u2014 | \u2014 |\n\n"
        f"Data generated at {BUILD}.\n"
    )
    snapshot = parse_funding_page(
        text, asset_class="crypto", asset="BTC", received_at=RECEIVED, window_days=30
    )
    summary = snapshot.summaries[0]
    assert summary.last_settled is None
    assert "last_settled:source_reported_no_figure" in summary.refusals
    # The count it sits beside was observed and is not refused.
    assert summary.settlements == 90


def test_store_is_idempotent_for_one_build(tmp_path):
    snapshot = parse_market_page(
        market_page(EIGHT_HOUR_ROW), asset_class="crypto", asset="BTC", received_at=RECEIVED
    )
    store = SnapshotStore.open(tmp_path)
    first = store.write_market(snapshot)
    second = store.write_market(snapshot)
    assert first == second
    assert store.market_builds("crypto", "BTC") == ["20260916T092918Z"]
    assert store.has_build("market", "crypto", "BTC", snapshot.build_time)


def test_store_keeps_distinct_builds_apart(tmp_path):
    store = SnapshotStore.open(tmp_path)
    for minutes in (0, 61):
        stamp = dt.datetime(2026, 9, 16, 9, 29, 18, tzinfo=dt.UTC) + dt.timedelta(minutes=minutes)
        snapshot = parse_market_page(
            market_page(EIGHT_HOUR_ROW, build=stamp.strftime("%Y-%m-%dT%H:%M:%SZ")),
            asset_class="crypto",
            asset="BTC",
            received_at=RECEIVED,
        )
        store.write_market(snapshot)
    assert store.market_builds("crypto", "BTC") == ["20260916T092918Z", "20260916T103018Z"]


# --------------------------------------------------------------------------- #
# The collector: which assets it sweeps, what it skips, and how it fails.
# --------------------------------------------------------------------------- #

BASE = "https://source.test"

CONFIG: dict = {
    "source": {"base_url": BASE, "accept": "text/markdown"},
    "cadence": {"build_stamp_canary": "/markets/crypto/BTC", "priority_hourly_minutes": 60},
    "universe": {"include_asset_classes": ["crypto"], "priority_limit": 40},
}


def index_page(ranked: Sequence[str], listed: Sequence[str] | None = None) -> str:
    """An index whose "most traded" order is ``ranked``, listing ``listed`` assets."""

    def bullet(label: str, path: str) -> str:
        return "- " + chr(91) + label + chr(93) + chr(40) + path + chr(41)

    listed = list(ranked if listed is None else listed)
    ranking = chr(10).join(bullet(a, "/markets/crypto/" + a) for a in ranked)
    routes: list[str] = []
    for asset in listed:
        routes.append(bullet(asset, "/markets/crypto/" + asset))
        routes.append(bullet("funding", "/markets/crypto/" + asset + "/funding"))
    body = chr(10).join(routes)
    return (
        "# Markets"
        + chr(10) * 2
        + "## Most traded assets"
        + chr(10) * 2
        + ranking
        + chr(10) * 2
        + "## All assets"
        + chr(10) * 2
        + body
        + chr(10)
    )


def funding_page(build: str = BUILD) -> str:
    return (
        "# Funding history\n\n"
        "| Exchange | Settlements | Total paid | Average APR | Largest | Smallest | Last settled |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
        "| binance | 90 | +0.5790% | +0.98% | +0.0100% | -0.0049% | 2026-09-16 08:00 |\n\n"
        f"Data generated at {build}.\n"
    )


class FakeTransport:
    """A transport that serves canned pages and records every URL it was asked for."""

    def __init__(self, pages: dict[str, str], *, fail: dict[str, Exception] | None = None) -> None:
        self.pages = pages
        self.fail = fail or {}
        self.requests: list[str] = []

    def get(self, url: str, *, source: str | None = None, accept: str | None = None):
        self.requests.append(url)
        if url in self.fail:
            raise self.fail[url]
        if url not in self.pages:
            raise TransportError(f"404 {url}", url=url, reason="http_404", status_code=404)
        return SimpleNamespace(text=self.pages[url], url=url)


def site(assets: Sequence[str], *, build: str = BUILD, pages: dict | None = None) -> dict[str, str]:
    served = {
        f"{BASE}/markets": index_page(assets),
        f"{BASE}/markets/crypto/BTC": market_page(EIGHT_HOUR_ROW, build=build),
        **{f"{BASE}/markets/crypto/{a}": market_page(EIGHT_HOUR_ROW, build=build) for a in assets},
        **{f"{BASE}/markets/crypto/{a}/funding": funding_page(build) for a in assets},
    }
    served.update(pages or {})
    return served


def test_select_assets_prefers_the_published_ranking_over_alphabetical_order():
    # Published ranking puts ZEC first; alphabetical order would put BTC first.
    text = index_page(ranked=["ZEC", "BTC"], listed=["BTC", "ETH", "ZEC"])
    chosen, _ = collector.select_assets(text, CONFIG, limit=2, use_all=False)
    assert [ref.asset for ref in chosen] == ["ZEC", "BTC"]


def test_select_assets_appends_assets_the_ranking_omits():
    text = index_page(ranked=["ZEC"], listed=["BTC", "ETH", "ZEC"])
    chosen, _ = collector.select_assets(text, CONFIG, limit=3, use_all=False)
    assert [ref.asset for ref in chosen] == ["ZEC", "BTC", "ETH"]


def test_select_assets_all_ignores_the_priority_cap():
    text = index_page(ranked=["ZEC"], listed=["BTC", "ETH", "ZEC"])
    chosen, _ = collector.select_assets(text, CONFIG, limit=1, use_all=True)
    assert [ref.asset for ref in chosen] == ["BTC", "ETH", "ZEC"]


def test_select_assets_honours_the_declared_asset_classes():
    # MSTR is an equity perp: the index lists it, the declared classes do not.
    text = index_page(ranked=["BTC"], listed=["BTC"]) + "- MSTR\n"
    chosen, _ = collector.select_assets(text, CONFIG, limit=None, use_all=True)
    assert [ref.asset for ref in chosen] == ["BTC"]


def test_select_assets_reports_which_assets_have_a_funding_route():
    _, with_funding = collector.select_assets(
        index_page(ranked=["BTC", "ETH"]), CONFIG, limit=None, use_all=False
    )
    assert with_funding == frozenset({("crypto", "BTC"), ("crypto", "ETH")})


def test_sweep_reads_the_canary_before_any_asset_page(tmp_path):
    transport = FakeTransport(site(["BTC"]))
    collector.sweep(
        transport,
        SnapshotStore.open(tmp_path),
        CONFIG,
        limit=1,
        use_all=False,
        include_funding=False,
        force=False,
    )
    assert transport.requests[0].endswith("/markets/crypto/BTC")


def test_sweep_stores_a_new_build_and_attributes_both_page_kinds(tmp_path):
    transport = FakeTransport(site(["BTC"]))
    record = collector.sweep(
        transport,
        SnapshotStore.open(tmp_path),
        CONFIG,
        limit=1,
        use_all=False,
        include_funding=True,
        force=False,
    )
    assert record["skipped"] is False
    assert record["assets_stored"] == 1
    assert record["blocked"] == 0
    assert record["unshaped"] == 0
    assert record["cost_layer"] == REFUSAL_EXECUTION_COST_UNAVAILABLE
    kinds = {(o["asset"][1], o["kind"]): o["outcome"] for o in record["outcomes"]}
    assert kinds[("BTC", "market")] == collector.OUTCOME_STORED
    assert kinds[("BTC", "funding")] == collector.OUTCOME_STORED


def test_sweep_inside_an_unchanged_build_costs_one_request(tmp_path):
    store = SnapshotStore.open(tmp_path)
    first = FakeTransport(site(["BTC", "ETH"]))
    collector.sweep(
        first, store, CONFIG, limit=None, use_all=False, include_funding=True, force=False
    )
    second = FakeTransport(site(["BTC", "ETH"]))
    record = collector.sweep(
        second, store, CONFIG, limit=None, use_all=False, include_funding=True, force=False
    )
    assert record["skipped"] is True
    assert record["skip_reason"] == "source_build_unchanged"
    assert record["assets_attempted"] == 0
    assert len(second.requests) == 1


def test_force_sweeps_again_without_double_counting(tmp_path):
    store = SnapshotStore.open(tmp_path)
    collector.sweep(
        FakeTransport(site(["BTC"])),
        store,
        CONFIG,
        limit=1,
        use_all=False,
        include_funding=True,
        force=False,
    )
    record = collector.sweep(
        FakeTransport(site(["BTC"])),
        store,
        CONFIG,
        limit=1,
        use_all=False,
        include_funding=True,
        force=True,
    )
    # The build is already held, so a forced re-fetch must not report new rows.
    assert record["skipped"] is False
    assert record["assets_stored"] == 0
    assert {o["outcome"] for o in record["outcomes"]} == {collector.OUTCOME_ALREADY_HELD}


def test_one_blocked_page_does_not_cost_the_other_assets(tmp_path):
    assets = ["BTC", "ETH"]
    blocked_url = f"{BASE}/markets/crypto/ETH"
    transport = FakeTransport(
        site(assets),
        fail={
            blocked_url: TransportError("429", url=blocked_url, reason="http_429", status_code=429)
        },
    )
    record = collector.sweep(
        transport,
        SnapshotStore.open(tmp_path),
        CONFIG,
        limit=None,
        use_all=False,
        include_funding=True,
        force=False,
    )
    assert record["blocked"] == 1
    assert record["assets_stored"] == 1
    by_asset = {o["asset"][1]: o for o in record["outcomes"] if o["kind"] == "market"}
    assert by_asset["ETH"]["outcome"] == collector.OUTCOME_BLOCKED
    assert by_asset["ETH"]["detail"]
    assert by_asset["BTC"]["outcome"] == collector.OUTCOME_STORED


def test_a_shape_change_is_recorded_apart_from_a_transport_block(tmp_path):
    assets = ["BTC", "ETH"]
    pages = site(assets)
    pages[f"{BASE}/markets/crypto/ETH"] = market_page(EIGHT_HOUR_ROW).replace(
        "Funding APR", "Annualised"
    )
    record = collector.sweep(
        FakeTransport(pages),
        SnapshotStore.open(tmp_path),
        CONFIG,
        limit=None,
        use_all=False,
        include_funding=False,
        force=False,
    )
    assert record["unshaped"] == 1
    assert record["blocked"] == 0


def test_unobserved_figures_survive_a_sweep_as_nulls_with_codes(tmp_path):
    """The whole path: page, parser, store, and back out of the Parquet."""
    pages = site(["BTC"])
    pages[f"{BASE}/markets/crypto/BTC"] = market_page(UNOBSERVED_ROW)
    store = SnapshotStore.open(tmp_path)
    collector.sweep(
        FakeTransport(pages),
        store,
        CONFIG,
        limit=1,
        use_all=False,
        include_funding=False,
        force=False,
    )
    slug = store.market_builds("crypto", "BTC")[0]
    table = pq.read_table(store.root / "market" / "crypto" / "BTC" / f"{slug}.parquet")
    row = table.to_pylist()[0]
    assert row["paid_30d"] is None
    assert "paid_30d:source_reported_no_figure" in row["refusals"]


def test_loop_records_a_failing_sweep_and_carries_on(tmp_path):
    """An unattended collector must not stop at the first unexpected response."""
    store = SnapshotStore.open(tmp_path)
    transport = FakeTransport({}, fail={f"{BASE}/markets/crypto/BTC": RuntimeError("boom")})
    slept: list[float] = []

    class Stop(Exception):
        pass

    def sleeper(seconds: float) -> None:
        slept.append(seconds)
        # Stop the otherwise-infinite loop on the second wait, so the test observes
        # that the first failure did not end the run.
        if len(slept) >= 2:
            raise Stop

    with pytest.raises(Stop):
        collector.collect(
            transport,
            store,
            CONFIG,
            limit=1,
            use_all=False,
            include_funding=False,
            force=False,
            once=False,
            interval_minutes=60,
            sleeper=sleeper,
        )

    assert slept == [3600.0, 3600.0]
    records = store.sweeps()
    assert len(records) == 2
    assert all(r["sweep_failed"] for r in records)
    assert "RuntimeError" in records[0]["error"]
    # The failure is a sweep-level fact, so it claims nothing about any page.
    assert records[0]["assets_attempted"] == 0


def test_loop_can_be_asked_for_exactly_one_sweep(tmp_path):
    store = SnapshotStore.open(tmp_path)
    slept: list[float] = []
    code = collector.collect(
        FakeTransport(site(["BTC"])),
        store,
        CONFIG,
        limit=1,
        use_all=False,
        include_funding=True,
        force=False,
        once=True,
        interval_minutes=60,
        sleeper=slept.append,
    )
    assert code == 0
    assert slept == []
    assert len(store.sweeps()) == 1


def test_report_counts_failures_apart_from_performed_sweeps(tmp_path):
    store = SnapshotStore.open(tmp_path)
    store.log_sweep(
        {"started_at": "2026-09-16T10:00:00+00:00", "build_time": "b1", "skipped": False}
    )
    store.log_sweep(
        {"started_at": "2026-09-16T11:00:00+00:00", "build_time": "b1", "skipped": True}
    )
    store.log_sweep(
        {"started_at": "2026-09-16T12:00:00+00:00", "sweep_failed": True, "skipped": False}
    )
    held = collector.report(store)
    assert held["sweeps_performed"] == 2
    assert held["sweeps_skipped"] == 1
    assert held["sweeps_failed"] == 1
    assert held["distinct_builds"] == 1
    assert held["cost_layer_observable"] is False


def test_cli_defaults_agree_with_the_package_defaults():
    """The CLI repeats two defaults; this is the thing that notices if they drift."""
    from market_propagation import cli
    from market_propagation.perp import store as perp_store

    assert cli.DEFAULT_PERP_CONFIG == collector.CONFIG_PATH
    assert cli.DEFAULT_PERP_ROOT == perp_store.DEFAULT_ROOT


# --------------------------------------------------------------------------- #
# The canaries. The source does not rebuild every section at one instant, so the
# skip rule has to compare every class, not one page.
# --------------------------------------------------------------------------- #

TWO_CLASS_CONFIG: dict = {
    "source": {"base_url": BASE, "accept": "text/markdown"},
    "cadence": {
        "build_stamp_canaries": {
            "crypto": "/markets/crypto/BTC",
            "rwa": "/markets/rwa/XAU",
        },
        "priority_hourly_minutes": 60,
    },
    "universe": {"include_asset_classes": ["crypto", "rwa"], "priority_limit": 40},
}


def _link(label: str, path: str) -> str:
    return "- " + chr(91) + label + chr(93) + chr(40) + path + chr(41)


def two_class_site(crypto_build: str, rwa_build: str) -> dict[str, str]:
    """One site holding a crypto asset and an rwa asset at their own build stamps."""
    index = (
        "# Markets\n\n## Most traded assets\n\n"
        f"{_link('BTC', '/markets/crypto/BTC')}\n{_link('XAU', '/markets/rwa/XAU')}\n\n"
        "## All assets\n\n"
        f"{_link('BTC', '/markets/crypto/BTC')}\n"
        f"{_link('funding', '/markets/crypto/BTC/funding')}\n"
        f"{_link('XAU', '/markets/rwa/XAU')}\n"
        f"{_link('funding', '/markets/rwa/XAU/funding')}\n"
    )
    return {
        f"{BASE}/markets": index,
        f"{BASE}/markets/crypto/BTC": market_page(EIGHT_HOUR_ROW, build=crypto_build),
        f"{BASE}/markets/rwa/XAU": market_page(EIGHT_HOUR_ROW, build=rwa_build),
        f"{BASE}/markets/crypto/BTC/funding": funding_page(crypto_build),
        f"{BASE}/markets/rwa/XAU/funding": funding_page(rwa_build),
    }


CRYPTO_1030 = "2026-09-16T10:30:34Z"
RWA_1030 = "2026-09-16T10:30:26Z"
RWA_1136 = "2026-09-16T11:36:11Z"


def test_a_single_canary_configuration_dates_the_whole_site():
    assert collector.canary_paths(CONFIG) == {collector.SITE_CANARY_KEY: "/markets/crypto/BTC"}


def test_a_per_class_configuration_lists_one_canary_for_each_class():
    assert collector.canary_paths(TWO_CLASS_CONFIG) == {
        "crypto": "/markets/crypto/BTC",
        "rwa": "/markets/rwa/XAU",
    }


def test_the_sweep_records_each_classs_own_build_stamp(tmp_path):
    transport = FakeTransport(two_class_site(CRYPTO_1030, RWA_1030))
    record = collector.sweep(
        transport,
        SnapshotStore.open(tmp_path),
        TWO_CLASS_CONFIG,
        limit=None,
        use_all=True,
        include_funding=False,
        force=False,
    )
    assert record["skipped"] is False
    assert record["build_times"] == {
        "crypto": "2026-09-16T10:30:34+00:00",
        "rwa": "2026-09-16T10:30:26+00:00",
    }
    # The primary stamp is the first declared canary's, and the skew is preserved
    # rather than collapsed into one instant.
    assert record["build_time"] == "2026-09-16T10:30:34+00:00"


def test_a_sweep_is_not_skipped_when_only_the_slower_class_advances(tmp_path):
    """The measured case: rwa lags crypto, so one canary would lapse rwa by a build."""
    store = SnapshotStore.open(tmp_path)
    collector.sweep(
        FakeTransport(two_class_site(CRYPTO_1030, RWA_1030)),
        store,
        TWO_CLASS_CONFIG,
        limit=None,
        use_all=True,
        include_funding=False,
        force=False,
    )

    # Crypto is unchanged; rwa has advanced. A single crypto canary would skip this.
    second = FakeTransport(two_class_site(CRYPTO_1030, RWA_1136))
    record = collector.sweep(
        second,
        store,
        TWO_CLASS_CONFIG,
        limit=None,
        use_all=True,
        include_funding=False,
        force=False,
    )
    assert record["skipped"] is False
    assert record["build_times"]["rwa"] == "2026-09-16T11:36:11+00:00"
    assert record["build_times"]["crypto"] == "2026-09-16T10:30:34+00:00"


def test_a_sweep_is_skipped_when_every_class_is_unchanged(tmp_path):
    store = SnapshotStore.open(tmp_path)
    collector.sweep(
        FakeTransport(two_class_site(CRYPTO_1030, RWA_1030)),
        store,
        TWO_CLASS_CONFIG,
        limit=None,
        use_all=True,
        include_funding=False,
        force=False,
    )
    second = FakeTransport(two_class_site(CRYPTO_1030, RWA_1030))
    record = collector.sweep(
        second,
        store,
        TWO_CLASS_CONFIG,
        limit=None,
        use_all=True,
        include_funding=False,
        force=False,
    )
    assert record["skipped"] is True
    assert record["skip_reason"] == "source_build_unchanged"
    # One request per declared class, and no asset page.
    assert len(second.requests) == 2
    assert sorted(second.requests) == [
        f"{BASE}/markets/crypto/BTC",
        f"{BASE}/markets/rwa/XAU",
    ]


def test_the_report_separates_builds_by_class(tmp_path):
    store = SnapshotStore.open(tmp_path)
    for rwa in (RWA_1030, RWA_1136):
        collector.sweep(
            FakeTransport(two_class_site(CRYPTO_1030, rwa)),
            store,
            TWO_CLASS_CONFIG,
            limit=None,
            use_all=True,
            include_funding=False,
            force=False,
        )
    held = collector.report(store)
    assert held["sweeps_performed"] == 2
    # Crypto never advanced while rwa advanced once: the per-class counts differ,
    # which a single build count would have hidden.
    assert held["distinct_builds_by_class"] == {"crypto": 1, "rwa": 2}
