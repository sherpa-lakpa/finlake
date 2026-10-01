"""Dataset contract for silver.daily_prices."""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="daily_prices",
    columns={
        "trade_date": ColumnContract(dtype="datetime64[ns]", nullable=False),
        "security_id": ColumnContract(dtype="object", nullable=False),
        "ticker": ColumnContract(dtype="object", nullable=False),
        "close_price": ColumnContract(dtype="float64", nullable=False),
        "adj_close": ColumnContract(dtype="float64", nullable=False),
        "asset_class": ColumnContract(dtype="object", nullable=False),
        "exchange": ColumnContract(dtype="object", nullable=False),
    },
    business_keys=("trade_date", "security_id"),
    min_row_count=1,
)
