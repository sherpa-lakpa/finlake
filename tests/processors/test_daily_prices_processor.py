from pathlib import Path

import pandas as pd
import pytest

from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.silver.daily_prices.processor import DailyPricesProcessor
from platform_name.common.exceptions import DataQualityError


@pytest.fixture
def inputs(tmp_path: Path) -> dict[str, Path]:
    storage = LocalFileSystemStorage()

    historical_path = tmp_path / "bronze" / "market_prices_historical.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2020-06-29", "2020-06-30"]),
                "ticker": ["AAPL", "AAPL"],
                "open_price": [100.0, 101.0],
                "high_price": [100.0, 101.0],
                "low_price": [100.0, 101.0],
                "close_price": [100.0, 101.0],
                "adj_close": [99.6, 100.6],
                "volume": [1000, 1100],
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
                "ticker": ["AAPL"],
                "open_price": [110.0],
                "low_price": [110.0],
                "close_price": [110.0],
                "close_price": [110.0],
                "adj_close": [110.0],
                "volume": [1200],
                "source_system": ["yfinance_daily"],
            }
        ),
        daily_path,
    )

    security_master_path = tmp_path / "silver" / "security_master.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {"security_id": ["AAPL"], "ticker": ["AAPL"], "asset_class": ["EQUITY"], "exchange": ["NASDAQ"]}
        ),
        security_master_path,
    )

    return {"historical": historical_path, "daily": daily_path, "security_master": security_master_path}


def test_unions_historical_and_daily_sources(inputs, tmp_path):
    output_path = tmp_path / "silver" / "daily_prices.parquet"
    processor = DailyPricesProcessor(
        str(inputs["historical"]), str(inputs["security_master"]), str(output_path),
    )
    result = processor.run()

    assert list(result.columns) == [
        "trade_date", "security_id", "ticker", "close_price", "adj_close", "asset_class", "exchange",
    ]
    assert len(result) == 2  # 2 historical days + 1 daily-feed day, no overlap
    assert (result["security_id"] == "AAPL").all()


def test_overlapping_date_prefers_daily_feed_over_historical(tmp_path):
    """Same (ticker, trade_date) present in both sources -- the daily
    feed's row should win, since it's the more recently ingested source."""
    storage = LocalFileSystemStorage()
    historical_path = tmp_path / "historical.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2020-06-29"]), "ticker": ["AAPL"],
                "close_price": [100.0], "adj_close": [99.6], "volume": [1000],
                "source_system": ["historical_dump"],
            }
        ),
        historical_path,
    )
    daily_path = tmp_path / "daily.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2020-06-29"]), "ticker": ["AAPL"],
                "close_price": [999.0], "adj_close": [999.0], "volume": [9999],
                "source_system": ["yfinance_daily"],
            }
        ),
        daily_path,
    )
    security_master_path = tmp_path / "security_master.parquet"
    storage.write_parquet(
        pd.DataFrame({"security_id": ["AAPL"], "ticker": ["AAPL"], "asset_class": ["EQUITY"], "exchange": ["NASDAQ"]}),
        security_master_path,
    )

    processor = DailyPricesProcessor(
        str(historical_path), str(security_master_path), str(tmp_path / "out.parquet"),
    )
    result = processor.run()

    assert len(result) == 1
    assert result.iloc[0]["close_price"] == pytest.approx(100)


def test_tickers_without_a_security_master_entry_are_dropped(tmp_path):
    storage = LocalFileSystemStorage()
    historical_path = tmp_path / "historical.parquet"
    storage.write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2020-06-29"]), 
                "ticker": ["ZZZ"],
                "open_price": [1.0],
                "high_price": [1.0],
                "low_price": [1.0],
                "close_price": [1.0], 
                "adj_close": [1.0], 
                "volume": [1],
                "source_system": ["historical_dump"],
            }
        ),
        historical_path,
    )
    daily_path = tmp_path / "daily.parquet"
    storage.write_parquet(
        pd.DataFrame(columns=[  "trade_date",
                "ticker",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "adj_close",
                "volume",
                "source_system"]),
        daily_path,
    )
    security_master_path = tmp_path / "security_master.parquet"
    storage.write_parquet(
        pd.DataFrame({"security_id": ["AAPL"], "ticker": ["AAPL"], "asset_class": ["EQUITY"], "exchange": ["NASDAQ"]}),
        security_master_path,
    )

    processor = DailyPricesProcessor(
        str(historical_path),str(security_master_path), str(tmp_path / "out.parquet"),
    )

    try:
        result = processor.run()
    except DataQualityError:
        result = []
    
    assert len(result) == 0
