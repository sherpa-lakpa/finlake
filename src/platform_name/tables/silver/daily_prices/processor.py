"""Silver processor: canonical daily prices, enriched from security master."""
from __future__ import annotations

import pandas as pd

from platform_name.contracts.definitions import DAILY_PRICES_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor


class DailyPricesProcessor(BaseProcessor):
    """Joins bronze market prices with the silver security master."""

    def __init__(
        self,
        bronze_path: str,
        security_master_path: str,
        output_path: str,
        source: str = "historical",
    ) -> None:
        self.bronze_path = bronze_path
        self.security_master_path = security_master_path
        self.output_path = output_path
        self.source = source
        self.storage = LocalFileSystemStorage()

    def read(self) -> dict[str, pd.DataFrame]:
        return {
            "market_prices": self.storage.read_parquet(self.bronze_path),
            "security_master": self.storage.read_parquet(self.security_master_path),
        }

    def transform(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        market_prices = data["market_prices"]
        security_master = data["security_master"]

        merged = market_prices.merge(
            security_master[["security_id", "ticker", "asset_class", "exchange"]],
            on="ticker",
            how="inner",
        )
        merged["source_system"] = self.source

        # columns = ["trade_date", "security_id", "ticker", "close_price", "asset_class", "source_system"]
        columns = ["trade_date", "security_id", "ticker", "close_price", "adj_close", "asset_class", "exchange"]
        return merged[columns].sort_values(["security_id", "trade_date"]).reset_index(drop=True)

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, DAILY_PRICES_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
