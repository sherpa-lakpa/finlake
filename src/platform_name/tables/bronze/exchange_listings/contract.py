"""Dataset contract for bronze.exchange_listings."""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="exchange_listings",
    columns={
        "ticker": ColumnContract(dtype="object", nullable=False, unique=True),
        "exchange": ColumnContract(dtype="object", nullable=False),
    },
    business_keys=("ticker",),
    min_row_count=1,
)
