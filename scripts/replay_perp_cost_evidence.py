"""Replay saved venue fee tables and depth as conditional, frozen-book costs."""

from __future__ import annotations

import argparse
import json
from decimal import ROUND_DOWN, Decimal
from html.parser import HTMLParser
from pathlib import Path

from market_propagation.storage import RawStore


class Tables(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self.table: list[list[str]] | None = None
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "table":
            self.table = []
        elif tag == "tr" and self.table is not None:
            self.row = []
        elif tag in {"td", "th"} and self.row is not None:
            self.cell = []

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.cell is not None and self.row is not None:
            self.row.append(" ".join(" ".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None and self.table is not None:
            self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def bybit_fee_rows(body: bytes) -> list[dict]:
    parser = Tables()
    parser.feed(body.decode("utf-8"))
    tables = [
        table
        for table in parser.tables
        if "Perpetual & Futures Contracts Trading"
        in " ".join(cell for row in table[:3] for cell in row)
        and "Options Trading" in " ".join(cell for row in table[:3] for cell in row)
    ]
    if len(tables) != 1:
        raise ValueError("expected exactly one base perpetual/futures fee table")
    table = tables[0]
    header = next((row for row in table if row.count("Taker Fee Rate") == 3), None)
    if header != ["Taker Fee Rate", "Maker Fee Rate"] * 3:
        raise ValueError("base fee table columns changed; refusing positional fee extraction")
    rows = []
    for row in table:
        if len(row) != 7 or not row[0].startswith(("VIP ", "Supreme VIP")):
            continue
        rates = [Decimal(value.removesuffix("%")) / 100 for value in row[1:]]
        if not all(value.endswith("%") for value in row[1:]) or not all(
            0 <= rate < 1 for rate in rates
        ):
            raise ValueError("invalid fee rate")
        rows.append({"tier": row[0], "taker": str(rates[2]), "maker": str(rates[3])})
    if not any(row["tier"] == "VIP 0" for row in rows):
        raise ValueError("base fee table carries no VIP 0 row")
    return rows


def fill(levels: list, quantity: Decimal) -> Decimal:
    remaining = quantity
    value = Decimal(0)
    for price, available in levels:
        used = min(remaining, Decimal(available))
        value += used * Decimal(price)
        remaining -= used
        if remaining == 0:
            return value
    raise ValueError(f"insufficient displayed depth for {quantity}: {remaining} unfilled")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--notional", type=Decimal, default=Decimal("10000"))
    args = parser.parse_args()
    if not args.notional.is_finite() or args.notional <= 0:
        parser.error("notional must be a finite positive amount in USDT")
    manifest = json.loads((args.sources / "manifest.json").read_text())
    store = RawStore(args.sources / "raw")

    def body(name: str) -> bytes:
        record = manifest[name]
        if record["status"] != "fetched":
            raise ValueError(f"{name}: {record.get('reason')}")
        return store.get(record["raw_hash"])

    fees = bybit_fee_rows(body("bybit_fees"))
    bybit = json.loads(body("bybit_depth"))
    if bybit["retCode"] != 0 or bybit["result"]["s"] != "BTCUSDT":
        raise ValueError("Bybit depth response did not identify BTCUSDT")
    bybit = bybit["result"]
    binance = json.loads(body("binance_depth"))
    bybit_spec = json.loads(body("bybit_specs"))["result"]["list"][0]
    binance_spec = next(
        item for item in json.loads(body("binance_specs"))["symbols"] if item["symbol"] == "BTCUSDT"
    )
    if (
        bybit_spec["contractType"] != "LinearPerpetual"
        or binance_spec["contractType"] != "PERPETUAL"
    ):
        raise ValueError("the instruments must both be linear perpetuals")
    if bybit_spec["settleCoin"] != "USDT" or binance_spec["marginAsset"] != "USDT":
        raise ValueError("the instruments do not share USDT settlement")
    binance_lot = next(f for f in binance_spec["filters"] if f["filterType"] == "LOT_SIZE")
    steps = [Decimal(bybit_spec["lotSizeFilter"]["qtyStep"]), Decimal(binance_lot["stepSize"])]
    step = max(steps)
    if any(step % other != 0 for other in steps):
        raise ValueError("lot steps need a shared integer multiple")
    mid = (Decimal(bybit["a"][0][0]) + Decimal(bybit["b"][0][0])) / 2
    quantity = (args.notional / mid / step).to_integral_value(rounding=ROUND_DOWN) * step
    minimum = max(
        Decimal(binance_lot["minQty"]), Decimal(bybit_spec["lotSizeFilter"]["minOrderQty"])
    )
    maximum = min(
        Decimal(binance_lot["maxQty"]), Decimal(bybit_spec["lotSizeFilter"]["maxMktOrderQty"])
    )
    if not minimum <= quantity <= maximum:
        raise ValueError("worked-example quantity is outside the held lot limits")
    legs = {}
    for name, bids, asks, source_ts in (
        ("bybit", bybit["b"], bybit["a"], bybit["ts"]),
        ("binance", binance["bids"], binance["asks"], binance["E"]),
    ):
        bought, sold = fill(asks, quantity), fill(bids, quantity)
        if bought < sold:
            raise ValueError("crossed book cannot support this cost example")
        legs[name] = {
            "buy_notional": str(bought),
            "sell_notional": str(sold),
            "displayed_round_trip_spread_cost": str(bought - sold),
            "source_timestamp_ms": source_ts,
        }
    rate = Decimal(next(row["taker"] for row in fees if row["tier"] == "VIP 0"))
    bybit_turnover = sum(Decimal(legs["bybit"][key]) for key in ("buy_notional", "sell_notional"))
    known_component = bybit_turnover * rate + sum(
        Decimal(leg["displayed_round_trip_spread_cost"]) for leg in legs.values()
    )
    report = {
        "instrument": "BTCUSDT",
        "venues": ["binance", "bybit"],
        "notional_target_usdt": str(args.notional),
        "matched_quantity_btc": str(quantity),
        "fee_source": manifest["bybit_fees"],
        "bybit_base_fee_rows": fees,
        "fee_assumption": "Bybit VIP 0 base rates; no regional or account discount; taker on both sides",
        "effective_at": None,
        "effective_at_reason": "page_update_is_not_an_effective_interval",
        "bybit_funding_interval_minutes": bybit_spec["fundingInterval"],
        "legs": legs,
        "bybit_round_trip_fee_usdt": str(bybit_turnover * rate),
        "conditional_known_component_usdt": str(known_component),
        "conditional_cost_expression": "known_component + Binance_turnover * unknown_Binance_taker_rate",
        "binance_turnover_usdt": str(
            sum(Decimal(legs["binance"][key]) for key in ("buy_notional", "sell_notional"))
        ),
        "sources": {
            name: manifest[name]
            for name in (
                "bybit_depth",
                "binance_depth",
                "bybit_specs",
                "binance_specs",
                "binance_fees",
                "binance_fee_explanation",
            )
        },
        "blockers": [
            "binance_fee_schedule_not_readable",
            "account_region_and_tier_unverified",
            "fee_effective_interval_unobserved",
            "future_exit_book_unobserved",
            "cross_venue_books_not_synchronized",
            "execution_and_capacity_unmeasured",
        ],
        "measurement_scope": "conditional_frozen_book_replay_not_a_live_fill_or_cost_bound_on_future_trades",
        "execution_cost_layer_complete": False,
        "net_return_or_capacity_claimed": False,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: report[key]
                for key in (
                    "matched_quantity_btc",
                    "bybit_round_trip_fee_usdt",
                    "conditional_known_component_usdt",
                    "blockers",
                )
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
