"""Dataset contract for gold.performance_summary."""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="performance_summary",
    columns={
        "security_id": ColumnContract(dtype="object", nullable=False, unique=True),
        "observation_count": ColumnContract(dtype="int64", nullable=False),
        "mean_daily_return": ColumnContract(dtype="float64", nullable=True),
        "volatility": ColumnContract(dtype="float64", nullable=True),
    },
    business_keys=("security_id",),
    min_row_count=0,
)
