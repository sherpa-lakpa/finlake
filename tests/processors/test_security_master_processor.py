from pathlib import Path

import pandas as pd
import pytest

from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.silver.security_master.processor import SecurityMasterProcessor


@pytest.fixture
def bronze_sources(tmp_path: Path) -> dict[str, Path]:
    storage = LocalFileSystemStorage()

    historical_path = tmp_path / "bronze" / "market_prices_historical.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2020-06-29", "2020-06-29"]),
                "ticker": ["AAPL", "MSFT"],
                "close_price": [100.0, 50.0],
                "adj_close": [99.6, 49.7],
                "volume": [1000, 2000],
                "source_system": ["historical_dump"] * 2,
            }
        ),
        historical_path,
    )

    daily_path = tmp_path / "bronze" / "market_prices_daily.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2024-01-02"]),
                "ticker": ["AMD"],  # a ticker NOT in the historical source
                "close_price": [25.0],
                "adj_close": [24.9],
                "volume": [3000],
                "source_system": ["yfinance_daily"],
            }
        ),
        daily_path,
    )

    listings_path = tmp_path / "bronze" / "exchange_listings.parquet"
    storage.write_parquet(
        pd.DataFrame({"ticker": ["AAPL", "MSFT"], "exchange": ["NASDAQ", "NASDAQ"]}),
        listings_path,
    )

    return {"historical": historical_path, "daily": daily_path, "listings": listings_path}


def test_unions_tickers_from_both_bronze_price_sources(bronze_sources, tmp_path):
    output_path = tmp_path / "silver" / "security_master.parquet"
    processor = SecurityMasterProcessor(
        str(bronze_sources["historical"]), str(bronze_sources["daily"]),
        str(bronze_sources["listings"]), str(output_path),
    )

    result = processor.run()

    assert sorted(result["ticker"]) == ["AAPL", "AMD", "MSFT"]
    assert (result["security_id"] == result["ticker"]).all()
    assert (result["asset_class"] == "EQUITY").all()


def test_ticker_missing_from_listings_gets_unknown_exchange(bronze_sources, tmp_path):
    """AMD only appears in the daily bronze source, and isn't in the
    (deliberately AAPL/MSFT-only) exchange_listings fixture -- it should
    still get a security record, just with an honest UNKNOWN exchange."""
    processor = SecurityMasterProcessor(
        str(bronze_sources["historical"]), str(bronze_sources["daily"]),
        str(bronze_sources["listings"]), str(tmp_path / "out.parquet"),
    )

    result = processor.run().set_index("ticker")

    assert result.loc["AAPL", "exchange"] == "NASDAQ"
    assert result.loc["MSFT", "exchange"] == "NASDAQ"
    assert result.loc["AMD", "exchange"] == "UNKNOWN"


def test_security_ids_are_unique(bronze_sources, tmp_path):
    processor = SecurityMasterProcessor(
        str(bronze_sources["historical"]), str(bronze_sources["daily"]),
        str(bronze_sources["listings"]), str(tmp_path / "out.parquet"),
    )
    result = processor.run()
    assert not result["security_id"].duplicated().any()
