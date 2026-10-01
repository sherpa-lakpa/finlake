"""Dataset contract for gold.top_movers."""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="top_movers",
    columns={
        "trade_date": ColumnContract(dtype="datetime64[ns]", nullable=False),
        "security_id": ColumnContract(dtype="object", nullable=False),
        "daily_return": ColumnContract(dtype="float64", nullable=True),
        "movement_rank": ColumnContract(dtype="int64", nullable=False),
    },
    business_keys=("trade_date", "security_id"),
    min_row_count=0,
)
