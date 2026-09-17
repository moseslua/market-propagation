import argparse
import datetime as dt
import json
from pathlib import Path

from market_propagation.ingest.transport import HttpTransport, RetryPolicy, TransportError
from market_propagation.storage import RawStore

SOURCES = {
    "bybit_fees": "https://www.bybit.com/en/help-center/article/Trading-Fee-Structure",
    "binance_fees": "https://www.binance.com/en/fee/futureFee",
    "binance_fee_explanation": "https://www.binance.com/en/support/faq/detail/360033544231",
    "bybit_depth": "https://api.bybit.com/v5/market/orderbook?category=linear&symbol=BTCUSDT&limit=50",
    "binance_depth": "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=50",
    "bybit_specs": "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol=BTCUSDT",
    "binance_specs": "https://fapi.binance.com/fapi/v1/exchangeInfo",
    "alfred_form": "https://alfred.stlouisfed.org/series/downloaddata?seid=CPIAUCSL",
    "fred_cpi": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL",
    "fred_payroll": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PAYEMS",
    "tradingeconomics_cpi": "https://tradingeconomics.com/united-states/inflation-rate-mom",
    "investing_payroll": "https://www.investing.com/economic-calendar/nonfarm-payrolls-227",
    "econoday": "https://us.econoday.com/byweek.asp",
    "spf": "https://www.philadelphiafed.org/surveys-and-data/real-time-data-research/survey-of-professional-forecasters",
}

parser = argparse.ArgumentParser(
    description="Archive bounded public evidence sources; retain failures and resume incomplete batches."
)
parser.add_argument("--out", type=Path, required=True)
parser.add_argument("--source", action="append", choices=sorted(SOURCES))
args = parser.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
manifest_path = args.out / "manifest.json"
manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
with HttpTransport(
    RawStore(args.out / "raw"),
    timeout_seconds=20,
    policy=RetryPolicy(attempts=1, min_interval_seconds=0.3),
) as transport:
    for name, url in SOURCES.items():
        if name in manifest or (args.source and name not in args.source):
            continue
        try:
            response = transport.get(url, source="evidence_acquisition")
            record = {
                "url": url,
                "status": "fetched",
                "raw_hash": response.provenance.raw_hash,
                "retrieved_at": response.received_time.isoformat(),
                "source_observed_at": response.server_date.isoformat()
                if response.server_date
                else None,
                "content_type": response.content_type,
                "bytes": len(response.body),
            }
        except TransportError as error:
            record = {
                "url": url,
                "status": "blocked",
                "reason": error.reason,
                "detail": str(error),
                "attempted_at": dt.datetime.now(dt.UTC).isoformat(),
                "http_status": error.status_code,
                "raw_hash": error.payload_hash,
            }
        manifest[name] = record
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
        print(name, record["status"], record.get("bytes"), record.get("reason"), flush=True)
