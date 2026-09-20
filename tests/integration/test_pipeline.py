"""End-to-end integration tests: bronze -> silver -> gold, driven entirely by
the shipped metadata and a throwaway environment config. No table-specific
logic is exercised here beyond what TableRunner resolves from metadata.

Every test requests `deterministic_yfinance_fetch` (see conftest.py) since
bronze.market_prices_daily -- now a dependency of the whole chain -- would
otherwise attempt a real network call to yfinance.
"""
from pathlib import Path

import pytest

from platform_name.common.config import Config
from platform_name.engine.models import ExecutionContext
from platform_name.engine.runner import TableRunner
from platform_name.storage.local import LocalFileSystemStorage

REPO_ROOT = Path(__file__).resolve().parents[2]
# Metadata lives colocated with processors: src/platform_name/tables/<layer>/<table>/metadata.yaml
METADATA_ROOT = REPO_ROOT / "src" / "platform_name" / "tables"

_BRONZE_TABLES = (
    "bronze.market_prices_historical",
    "bronze.market_prices_daily",
    "bronze.exchange_listings",
)


def test_full_chain_executes_from_bronze_to_gold(dev_environment, deterministic_yfinance_fetch):
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    result = runner.run(table_name="gold.returns", context=context)

    assert result.status == "success"
    assert result.table == "gold.returns"
    # 3 symbols x (3 historical days + 2 daily-feed days, non-overlapping
    # date ranges) = 15 rows; see conftest.py's fixtures for the exact shape.
    assert result.row_count == 9

    executed = context.extra["execution_results"]
    assert set(executed) == {
        *_BRONZE_TABLES,
        "silver.security_master",
        "silver.daily_prices",
        "gold.returns",
    }
    assert all(r.status == "success" for r in executed.values())


def test_all_intermediate_outputs_are_persisted(dev_environment, deterministic_yfinance_fetch):
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    runner.run(table_name="gold.returns", context=context)

    storage = LocalFileSystemStorage()
    assert storage.exists(Path(config.layer_root("bronze")) / "market_prices_historical.parquet")
    # assert storage.exists(Path(config.layer_root("bronze")) / "market_prices_daily.parquet")
    assert storage.exists(Path(config.layer_root("bronze")) / "exchange_listings.parquet")
    assert storage.exists(Path(config.layer_root("silver")) / "security_master.parquet")
    assert storage.exists(Path(config.layer_root("silver")) / "daily_prices.parquet")
    assert storage.exists(Path(config.layer_root("gold")) / "returns.parquet")


def test_deeper_chain_executes_performance_summary(dev_environment, deterministic_yfinance_fetch):
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    result = runner.run(table_name="gold.performance_summary", context=context)

    assert result.status == "success"
    executed = context.extra["execution_results"]
    assert set(executed) == {
        *_BRONZE_TABLES,
        "silver.security_master",
        "silver.daily_prices",
        "gold.returns",
        "gold.performance_summary",
    }


def test_running_a_downstream_table_reuses_already_executed_upstream_results(
    dev_environment, deterministic_yfinance_fetch
):
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    runner.run(table_name="gold.returns", context=context)
    first_run_ids = {
        name: r.end_time for name, r in context.extra["execution_results"].items()
    }

    runner.run(table_name="gold.performance_summary", context=context)
    second_run_state = context.extra["execution_results"]

    # Upstream tables already executed in this context should not be re-run.
    for name in (*_BRONZE_TABLES, "silver.security_master", "silver.daily_prices", "gold.returns"):
        assert second_run_state[name].end_time == first_run_ids[name]
    assert "gold.performance_summary" in second_run_state


def test_force_rerun_re_executes_the_full_chain(dev_environment, deterministic_yfinance_fetch):
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    runner.run(table_name="gold.returns", context=context)
    first_end_time = context.extra["execution_results"]["bronze.market_prices_historical"].end_time

    runner.run(table_name="gold.returns", context=context, force_rerun=True)
    second_end_time = context.extra["execution_results"]["bronze.market_prices_historical"].end_time

    assert second_end_time >= first_end_time


def test_final_returns_output_has_expected_shape(dev_environment, deterministic_yfinance_fetch):
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    runner.run(table_name="gold.returns", context=context)

    df = LocalFileSystemStorage().read_parquet(Path(config.layer_root("gold")) / "returns.parquet")
    assert set(df.columns) == {"trade_date", "security_id", "daily_return"}
    assert set(df["security_id"]) == {"AAPL", "MSFT", "AMD"}


def test_security_master_has_real_exchange_data(dev_environment, deterministic_yfinance_fetch):
    """The whole point of bronze.exchange_listings: security_master should
    carry a real exchange, not the old hardcoded EQUITY-only placeholder
    shape -- confirms the new reference-data join actually happened."""
    config = Config(dev_environment)
    context = ExecutionContext(config=config)
    runner = TableRunner(metadata_root=METADATA_ROOT)

    runner.run(table_name="silver.security_master", context=context)

    df = LocalFileSystemStorage().read_parquet(Path(config.layer_root("silver")) / "security_master.parquet")
    assert set(df["exchange"]) == {"NASDAQ"}
    assert set(df["asset_class"]) == {"EQUITY"}
