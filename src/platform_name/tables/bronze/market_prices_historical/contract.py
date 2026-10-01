"""Dataset contract for bronze.market_prices_historical.

Lives next to this table's processor.py and metadata.yaml, rather than in
a shared, ever-growing contracts file -- see contracts/base.py for the
generic DatasetContract/ColumnContract model this builds on.
"""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="market_prices",
    columns={
        "trade_date": ColumnContract(dtype="datetime64[ns]", nullable=False),
        "ticker": ColumnContract(dtype="object", nullable=False),
        "open_price": ColumnContract(dtype="float64", nullable=True),
        "high_price": ColumnContract(dtype="float64", nullable=True),
        "low_price": ColumnContract(dtype="float64", nullable=True),
        "close_price": ColumnContract(dtype="float64", nullable=False),
        "adj_close": ColumnContract(dtype="float64", nullable=False),
        "volume": ColumnContract(dtype="int64", nullable=True),
    },
    business_keys=("trade_date", "ticker"),
    min_row_count=1,
)
