"""Tests for MarketPricesDailyProcessor: real-world resume-from-last-date
incremental logic, using an injected fake fetcher -- no real yfinance
call or network access needed to test any of this."""
from pathlib import Path

import pandas as pd
import pytest

from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.bronze.market_prices_daily.processor import MarketPricesDailyProcessor


def _fake_fetch(ticker: str, start) -> pd.DataFrame:
    dates = pd.date_range(start=start, periods=2, freq="D")
    base = {"AAPL": 100.0, "MSFT": 50.0}.get(ticker, 10.0)
    return pd.DataFrame(
        {
            "Open": [base, base * 1.01],
            "High": [base * 1.02, base * 1.03],
            "Low": [base * 0.98, base * 0.99],
            "Close": [base * 1.01, base * 1.015],
            "Adj Close": [base * 1.005, base * 1.01],
            "Volume": [1_000_000, 1_100_000],
        },
        index=pd.DatetimeIndex(dates, name="Date"),
    )


def test_first_run_fetches_from_since_default(tmp_path):
    output_path = tmp_path / "bronze" / "market_prices_daily.parquet"
    processor = MarketPricesDailyProcessor(
        str(output_path), symbols=["AAPL", "MSFT"], since_default="2020-07-01",
        as_of="2020-07-02", fetch_history=_fake_fetch,
    )

    result = processor.run()

    assert len(result) == 4  # 2 symbols x 2 days
    assert set(result["trade_date"].dt.strftime("%Y-%m-%d")) == {"2020-07-01", "2020-07-02"}
    assert set(result["source_system"]) == {"yfinance_daily"}


def test_keeps_both_close_price_and_adj_close_separately(tmp_path):
    processor = MarketPricesDailyProcessor(
        str(tmp_path / "out.parquet"), symbols=["AAPL"], since_default="2020-07-01",
        as_of="2020-07-01", fetch_history=_fake_fetch,
    )
    result = processor.run()
    row = result.iloc[0]
    assert row["close_price"] != row["adj_close"]  # our fake fetcher intentionally differs


def test_second_run_resumes_from_day_after_last_ingested_date(tmp_path):
    output_path = tmp_path / "bronze" / "market_prices_daily.parquet"
    calls: list[tuple[str, str]] = []

    def tracking_fetch(ticker, start):
        calls.append((ticker, str(start)))
        return _fake_fetch(ticker, start)

    first = MarketPricesDailyProcessor(
        str(output_path), symbols=["AAPL"], since_default="2020-07-01",
        as_of="2020-07-02", fetch_history=tracking_fetch,
    )
    first.run()

    second = MarketPricesDailyProcessor(
        str(output_path), symbols=["AAPL"], since_default="2020-07-01",
        as_of="2020-07-05", fetch_history=tracking_fetch,
    )
    result = second.run()

    # First run fetched from since_default; second run must resume from the
    # day AFTER the last date the first run actually ingested (2020-07-02),
    # not re-fetch from since_default again.
    assert calls[0] == ("AAPL", "2020-07-01")
    assert calls[1] == ("AAPL", "2020-07-03")
    assert not result.duplicated(subset=["ticker", "trade_date"]).any()


def test_symbol_already_caught_up_is_not_refetched(tmp_path):
    output_path = tmp_path / "bronze" / "market_prices_daily.parquet"
    calls: list[str] = []

    def tracking_fetch(ticker, start):
        calls.append(ticker)
        return _fake_fetch(ticker, start)

    first = MarketPricesDailyProcessor(
        str(output_path), symbols=["AAPL"], since_default="2020-07-01",
        as_of="2020-07-02", fetch_history=tracking_fetch,
    )
    first.run()
    calls.clear()

    # as_of is BEFORE the resume date (2020-07-03) -- nothing new to fetch.
    second = MarketPricesDailyProcessor(
        str(output_path), symbols=["AAPL"], since_default="2020-07-01",
        as_of="2020-07-02", fetch_history=tracking_fetch,
    )
    result = second.run()

    assert calls == []
    assert len(result) == 2  # unchanged from the first run's output


def test_writes_parquet_output(tmp_path):
    output_path = tmp_path / "bronze" / "market_prices_daily.parquet"
    processor = MarketPricesDailyProcessor(
        str(output_path), symbols=["AAPL"], since_default="2020-07-01",
        as_of="2020-07-01", fetch_history=_fake_fetch,
    )
    processor.run()

    assert output_path.exists()
    persisted = LocalFileSystemStorage().read_parquet(output_path)
    assert len(persisted) == 2
