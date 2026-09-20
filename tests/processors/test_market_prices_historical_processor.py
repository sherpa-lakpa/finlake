"""Tests for MarketPricesHistoricalProcessor: per-symbol full_history/*.csv
files, ticker derived from filename (no ticker column in the file)."""
from pathlib import Path

import pandas as pd
import pytest

from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.bronze.market_prices_historical.processor import (
    MarketPricesHistoricalProcessor,
)


@pytest.fixture
def landing_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "landing" / "historical" / "full_history"
    directory.mkdir(parents=True)
    pd.DataFrame(
        {
            "date": ["2020-06-29", "2020-06-30"],
            "volume": [1000, 1100],
            "open": [99.0, 100.0],
            "close": [100.0, 101.5],
            "high": [100.5, 102.0],
            "low": [98.5, 99.8],
            "adjclose": [99.6, 101.1],
        }
    ).to_csv(directory / "aapl.csv", index=False)
    pd.DataFrame(
        {
            "date": ["2020-06-29", "2020-06-30"],
            "volume": [2000, 2100],
            "open": [49.0, 50.0],
            "close": [50.0, 50.5],
            "high": [50.5, 51.0],
            "low": [48.8, 49.8],
            "adjclose": [49.7, 50.2],
        }
    ).to_csv(directory / "MSFT.csv", index=False)
    return directory.parent


def test_derives_ticker_from_filename_not_a_column(landing_dir, tmp_path):
    output_path = tmp_path / "bronze" / "market_prices_historical.parquet"
    processor = MarketPricesHistoricalProcessor(str(landing_dir), str(output_path))

    result = processor.run()

    assert set(result["ticker"]) == {"AAPL", "MSFT"}  # lowercase filename uppercased


def test_normalizes_columns_and_keeps_both_close_and_adj_close(landing_dir, tmp_path):
    processor = MarketPricesHistoricalProcessor(str(landing_dir), str(tmp_path / "out.parquet"))

    result = processor.run()

    assert list(result.columns) == [
        "trade_date", "ticker", "open_price", "high_price", "low_price",
        "close_price", "adj_close", "volume", "source_system",
    ]
    assert len(result) == 4
    aapl = result[result["ticker"] == "AAPL"].sort_values("trade_date").reset_index(drop=True)
    assert aapl.loc[0, "close_price"] == pytest.approx(100.0)
    assert aapl.loc[0, "adj_close"] == pytest.approx(99.6)


def test_writes_parquet_output(landing_dir, tmp_path):
    output_path = tmp_path / "bronze" / "market_prices_historical.parquet"
    processor = MarketPricesHistoricalProcessor(str(landing_dir), str(output_path))
    processor.run()

    assert output_path.exists()
    persisted = LocalFileSystemStorage().read_parquet(output_path)
    assert len(persisted) == 4


def test_raises_when_no_full_history_directory_or_files(tmp_path):
    empty_landing = tmp_path / "landing" / "historical"
    empty_landing.mkdir(parents=True)
    processor = MarketPricesHistoricalProcessor(str(empty_landing), str(tmp_path / "out.parquet"))

    with pytest.raises(FileNotFoundError):
        processor.run()
