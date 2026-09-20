"""Gold processor: performance summary statistics per security."""
from __future__ import annotations

import pandas as pd

from platform_name.contracts.definitions import PERFORMANCE_SUMMARY_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor


class PerformanceSummaryProcessor(BaseProcessor):
    """Aggregates daily returns into per-security summary statistics."""

    def __init__(self, returns_path: str, output_path: str) -> None:
        self.returns_path = returns_path
        self.output_path = output_path
        self.storage = LocalFileSystemStorage()

    def read(self) -> pd.DataFrame:
        return self.storage.read_parquet(self.returns_path)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        grouped = data.dropna(subset=["daily_return"]).groupby("security_id")["daily_return"]
        summary = grouped.agg(
            observation_count="count",
            mean_daily_return="mean",
            volatility="std",
        ).reset_index()
        summary["observation_count"] = summary["observation_count"].astype("int64")
        return summary

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, PERFORMANCE_SUMMARY_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
