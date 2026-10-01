"""Dataset contract for gold.returns."""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="returns",
    columns={
        "trade_date": ColumnContract(dtype="datetime64[ns]", nullable=False),
        "security_id": ColumnContract(dtype="object", nullable=False),
        "daily_return": ColumnContract(dtype="float64", nullable=True),
    },
    business_keys=("trade_date", "security_id"),
    min_row_count=0,
)
