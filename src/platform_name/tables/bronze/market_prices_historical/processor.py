"""Bronze processor: historical per-symbol CSV dump -> normalized bronze
Parquet.

Matches the real "AMEX, NYSE, and NASDAQ stocks histories" dataset shape:
one CSV file per ticker under ``full_history/<TICKER>.csv``, with columns
``date, volume, open, close, high, low, adjclose`` and no ticker column of
its own -- the ticker is the filename. This is the one-time (or
periodically refreshed) bulk backfill; see
``bronze/market_prices_daily/processor.py`` for the ongoing incremental
feed that continues past wherever a given dump's history ends.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from platform_name.contracts.definitions import MARKET_PRICES_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor

_RAW_COLUMN_RENAMES = {
    "date": "trade_date",
    "open": "open_price",
    "close": "close_price",
    "high": "high_price",
    "low": "low_price",
    "adjclose": "adj_close",
}

_OUTPUT_COLUMNS = [
    "trade_date",
    "ticker",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "adj_close",
    "volume",
    "source_system",
]


class MarketPricesHistoricalProcessor(BaseProcessor):
    """Reads every ``full_history/<TICKER>.csv`` file under ``source_path``,
    normalizes them into the canonical bronze schema, validates, and writes
    a single Parquet file.
    """

    def __init__(self, source_path: str, output_path: str, source: str = "historical_dump") -> None:
        self.source_path = source_path
        self.output_path = output_path
        self.source = source
        self.storage = LocalFileSystemStorage()

    def read(self) -> dict[str, pd.DataFrame]:
        full_history_dir = Path(self.source_path) / "full_history"
        csv_files = self.storage.list_files(full_history_dir, pattern="*.csv")
        if not csv_files:
            raise FileNotFoundError(
                f"No per-symbol CSV files found under: {full_history_dir}"
            )
        # Ticker comes from the filename, not a column in the file -- keep
        # each symbol's frame paired with its ticker until transform().
        return {path.stem.upper(): self.storage.read_csv(path) for path in csv_files}

    def transform(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        frames = []
        for ticker, frame in data.items():
            frame = frame.rename(columns=_RAW_COLUMN_RENAMES)
            frame["ticker"] = ticker
            frames.append(frame)

        combined = pd.concat(frames, ignore_index=True)
        combined["trade_date"] = pd.to_datetime(combined["trade_date"])
        for price_column in ("open_price", "high_price", "low_price", "close_price", "adj_close"):
            combined[price_column] = combined[price_column].astype(float)
        combined["volume"] = combined["volume"].fillna(0).astype("int64")
        combined["source_system"] = self.source

        return (
            combined[_OUTPUT_COLUMNS]
            .sort_values(["trade_date", "ticker"])
            .reset_index(drop=True)
        )

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, MARKET_PRICES_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
