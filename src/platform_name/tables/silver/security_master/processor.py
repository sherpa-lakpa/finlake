"""Silver processor: derives a canonical security master from bronze
market price observations.
"""
from __future__ import annotations

import pandas as pd

from platform_name.contracts.definitions import SECURITY_MASTER_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor


class SecurityMasterProcessor(BaseProcessor):
    """Reads bronze market prices, derives one row per unique ticker."""

    def __init__(self, input_path: str, output_path: str) -> None:
        self.input_path = input_path
        self.output_path = output_path
        self.storage = LocalFileSystemStorage()

    def read(self) -> pd.DataFrame:
        return self.storage.read_parquet(self.input_path)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        tickers = sorted(data["ticker"].dropna().unique())
        security_master = pd.DataFrame(
            {
                "security_id": tickers,
                "ticker": tickers,
                # The example domain does not carry a real asset-class source;
                # a real implementation would enrich this from a reference feed.
                "asset_class": ["EQUITY"] * len(tickers),
                "exchange": ["NASDAQ"] * len(tickers),
            }
        )
        return security_master

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, SECURITY_MASTER_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
