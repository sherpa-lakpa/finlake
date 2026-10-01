"""Concrete dataset contracts for the example domain.

These live outside the generic framework (``contracts/base.py``,
``quality/framework.py``) precisely because they encode domain knowledge --
new business domains add their own contracts here (or in their own module)
without touching the generic quality framework.
"""
from __future__ import annotations

from platform_name.contracts.base import ColumnContract, DatasetContract

MARKET_PRICES_CONTRACT = DatasetContract(
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

EXCHANGE_LISTINGS_CONTRACT = DatasetContract(
    name="exchange_listings",
    columns={
        "ticker": ColumnContract(dtype="object", nullable=False, unique=True),
        "exchange": ColumnContract(dtype="object", nullable=False),
    },
    business_keys=("ticker",),
    min_row_count=1,
)

SECURITY_MASTER_CONTRACT = DatasetContract(
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

DAILY_PRICES_CONTRACT = DatasetContract(
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

RETURNS_CONTRACT = DatasetContract(
    name="returns",
    columns={
        "trade_date": ColumnContract(dtype="datetime64[ns]", nullable=False),
        "security_id": ColumnContract(dtype="object", nullable=False),
        "daily_return": ColumnContract(dtype="float64", nullable=True),
    },
    business_keys=("trade_date", "security_id"),
    min_row_count=0,
)

PERFORMANCE_SUMMARY_CONTRACT = DatasetContract(
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
