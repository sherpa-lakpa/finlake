"""Dataset contract for silver.security_master."""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

CONTRACT = DatasetContract(
    name="security_master",
    columns={
        "security_id": ColumnContract(dtype="object", nullable=False, unique=True),
        "ticker": ColumnContract(dtype="object", nullable=False, unique=True),
        "asset_class": ColumnContract(dtype="object", nullable=False),
        "exchange": ColumnContract(dtype="object", nullable=False),
    },
    business_keys=("security_id",),
    min_row_count=1,
)
