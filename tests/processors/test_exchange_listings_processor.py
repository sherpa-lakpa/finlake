"""Tests for ExchangeListingsProcessor: NASDAQ/NYSE/AMEX.txt -> (ticker,
exchange) reference data."""
from pathlib import Path

import pytest

from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.bronze.exchange_listings.processor import ExchangeListingsProcessor


@pytest.fixture
def landing_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "landing" / "historical"
    directory.mkdir(parents=True)
    (directory / "NASDAQ.txt").write_text("AAPL\nMSFT\n")
    (directory / "NYSE.txt").write_text("IBM\n")
    (directory / "AMEX.txt").write_text("")
    return directory


def test_produces_one_row_per_ticker_with_its_exchange(landing_dir, tmp_path):
    output_path = tmp_path / "bronze" / "exchange_listings.parquet"
    processor = ExchangeListingsProcessor(str(landing_dir), str(output_path))

    result = processor.run()

    rows = dict(zip(result["ticker"], result["exchange"]))
    assert rows == {"AAPL": "NASDAQ", "MSFT": "NASDAQ", "IBM": "NYSE"}


def test_deduplicates_a_ticker_listed_on_multiple_exchanges(tmp_path):
    directory = tmp_path / "landing"
    directory.mkdir()
    (directory / "NASDAQ.txt").write_text("DUPE\n")
    (directory / "NYSE.txt").write_text("DUPE\n")
    (directory / "AMEX.txt").write_text("")

    processor = ExchangeListingsProcessor(str(directory), str(tmp_path / "out.parquet"))
    result = processor.run()

    assert len(result[result["ticker"] == "DUPE"]) == 1


def test_raises_when_a_listing_file_is_missing(tmp_path):
    directory = tmp_path / "landing"
    directory.mkdir()
    (directory / "NASDAQ.txt").write_text("AAPL\n")
    # NYSE.txt and AMEX.txt deliberately absent

    processor = ExchangeListingsProcessor(str(directory), str(tmp_path / "out.parquet"))
    with pytest.raises(FileNotFoundError):
        processor.run()


def test_writes_parquet_output(landing_dir, tmp_path):
    output_path = tmp_path / "bronze" / "exchange_listings.parquet"
    processor = ExchangeListingsProcessor(str(landing_dir), str(output_path))
    processor.run()

    assert output_path.exists()
    persisted = LocalFileSystemStorage().read_parquet(output_path)
    assert len(persisted) == 3
