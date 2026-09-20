from pathlib import Path

import pandas as pd
import pytest

from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.gold.returns.processor import GoldReturnsProcessor
from platform_name.tables.gold.performance_summary.processor import PerformanceSummaryProcessor


@pytest.fixture
def daily_prices(tmp_path: Path) -> Path:
    path = tmp_path / "silver" / "daily_prices.parquet"
    LocalFileSystemStorage().write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-02", "2024-01-03"]
                ),
                "security_id": ["AAA", "AAA", "AAA", "BBB", "BBB"],
                "ticker": ["AAA", "AAA", "AAA", "BBB", "BBB"],
                # close_price and adj_close deliberately differ here, so a
                # test asserting on adj_close-derived returns would fail if
                # the processor were still (incorrectly) using close_price.
                "close_price": [999.0, 999.0, 999.0, 999.0, 999.0],
                "adj_close": [100.0, 110.0, 99.0, 50.0, 55.0],
                "asset_class": ["EQUITY"] * 5,
                "exchange": ["NASDAQ"] * 5,
                "source_system": ["historical_dump"] * 5,
            }
        ),
        path,
    )
    return path


def test_computes_daily_return_per_security(daily_prices, tmp_path):
    output_path = tmp_path / "gold" / "returns.parquet"
    processor = GoldReturnsProcessor(str(daily_prices), str(output_path))

    result = processor.run()

    assert list(result.columns) == ["trade_date", "security_id", "daily_return"]
    aaa = result[result["security_id"] == "AAA"].reset_index(drop=True)
    assert pd.isna(aaa.loc[0, "daily_return"])
    assert aaa.loc[1, "daily_return"] == pytest.approx(0.10)
    assert aaa.loc[2, "daily_return"] == pytest.approx((99.0 - 110.0) / 110.0)


def test_first_observation_per_security_has_null_return(daily_prices, tmp_path):
    processor = GoldReturnsProcessor(str(daily_prices), str(tmp_path / "out.parquet"))
    result = processor.run()
    bbb = result[result["security_id"] == "BBB"].reset_index(drop=True)
    assert pd.isna(bbb.loc[0, "daily_return"])
    assert bbb.loc[1, "daily_return"] == pytest.approx(0.10)


@pytest.fixture
def returns(tmp_path: Path) -> Path:
    path = tmp_path / "gold" / "returns.parquet"
    LocalFileSystemStorage().write_parquet(
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-02", "2024-01-03"]
                ),
                "security_id": ["AAA", "AAA", "AAA", "BBB", "BBB"],
                "daily_return": [None, 0.10, -0.10, None, 0.05],
            }
        ),
        path,
    )
    return path


def test_performance_summary_aggregates_per_security(returns, tmp_path):
    output_path = tmp_path / "gold" / "performance_summary.parquet"
    processor = PerformanceSummaryProcessor(str(returns), str(output_path))

    result = processor.run().set_index("security_id")

    assert result.loc["AAA", "observation_count"] == 2
    assert result.loc["AAA", "mean_daily_return"] == pytest.approx(0.0)
    assert result.loc["BBB", "observation_count"] == 1


def test_performance_summary_output_is_unique_per_security(returns, tmp_path):
    processor = PerformanceSummaryProcessor(str(returns), str(tmp_path / "out.parquet"))
    result = processor.run()
    assert not result["security_id"].duplicated().any()
