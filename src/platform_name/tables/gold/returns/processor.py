"""Gold processor: daily simple returns per security."""
from __future__ import annotations

import pandas as pd

from platform_name.contracts.definitions import RETURNS_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor


class GoldReturnsProcessor(BaseProcessor):
    """Computes ``adj_close.pct_change()`` per security from daily prices.

    Uses the split/dividend-adjusted close (``adj_close``), not the raw
    ``close_price`` -- a stock split or dividend would otherwise show up
    as a large, spurious one-day "return" that has nothing to do with the
    security's actual performance. This is standard practice for computing
    returns, and is exactly why bronze/silver keep both columns rather
    than only the raw close.
    """

    def __init__(self, prices_path: str, output_path: str) -> None:
        self.prices_path = prices_path
        self.output_path = output_path
        self.storage = LocalFileSystemStorage()

    def read(self) -> pd.DataFrame:
        return self.storage.read_parquet(self.prices_path)

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        ordered = data.sort_values(["security_id", "trade_date"]).copy()
        ordered["daily_return"] = ordered.groupby("security_id")["adj_close"].pct_change()
        return ordered[["trade_date", "security_id", "daily_return"]].reset_index(drop=True)

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, RETURNS_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
