import importlib.util
from pathlib import Path

import pytest


def module():
    spec = importlib.util.spec_from_file_location(
        "cost_replay", Path(__file__).parents[1] / "scripts/replay_perp_cost_evidence.py"
    )
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


TABLE = b"""Title: USD-M Futures Trading Fee Rate
URL Source: https://www.binance.com/en/fee/futureFee
| Level | USDT Maker / Taker | USDT Maker / Taker BNB 10% off | USDC Maker / Taker |
| --- | --- | --- | --- |
| Regular User | 0.0300%/0.0700% | 0.0270%/0.0630% | 0.0100%/0.0200% |
| VIP 1 | 0.0200%/0.0600% | 0.0180%/0.0540% | 0.0000%/0.0100% |
"""


def test_binance_parser_reads_base_usdt_fees_not_discounted_or_usdc():
    rows = module().binance_fee_rows(TABLE)
    assert rows == [
        {"tier": "Regular User", "maker": "0.0003", "taker": "0.0007"},
        {"tier": "VIP 1", "maker": "0.0002", "taker": "0.0006"},
    ]


def test_binance_parser_refuses_missing_base_usdt_column():
    changed = TABLE.replace(b"| USDT Maker / Taker |", b"| USDC Maker / Taker |")
    with pytest.raises(ValueError, match="base USDT"):
        module().binance_fee_rows(changed)


def test_binance_parser_refuses_ambiguous_or_wrong_source():
    with pytest.raises(ValueError, match="source"):
        module().binance_fee_rows(TABLE.replace(b"www.binance.com", b"example.com"))
    with pytest.raises(ValueError, match="exactly one"):
        module().binance_fee_rows(TABLE + TABLE)
