"""Shared pytest fixtures.

Tests build their own isolated, deterministic environment under a pytest
``tmp_path`` rather than depending on the repository's ``data/`` directory or
any external service, so the suite is fully self-contained and repeatable.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
# Metadata now lives colocated with processors under
# src/platform_name/tables/<layer>/<table>/metadata.yaml -- see
# engine/table_registry.py and docs/TUTORIAL.md.
METADATA_ROOT = REPO_ROOT / "src" / "platform_name" / "tables"


@pytest.fixture
def sample_market_prices_csv(tmp_path: Path) -> Path:
    """Writes a small, deterministic landing zone matching the real
    dataset shape and returns its directory: per-symbol CSVs under
    full_history/<TICKER>.csv (no ticker column -- derived from the
    filename), plus NASDAQ/NYSE/AMEX listing files.

    Uses the same tickers (AAPL, MSFT, AMD) as
    bronze.market_prices_daily's shipped metadata.yaml watchlist, so tests
    exercising the full registry-driven pipeline see one consistent
    security universe rather than the historical and daily bronze sources
    unioning into a confusing mismatched set.
    """
    landing_dir = tmp_path / "landing" / "historical"
    full_history_dir = landing_dir / "full_history"
    full_history_dir.mkdir(parents=True)

    per_symbol = {
        "AAPL": {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "volume": [1000, 1100, 1050],
            "open": [99.5, 100.2, 101.0],
            "close": [100.0, 101.5, 99.8],
            "high": [100.8, 102.0, 101.2],
            "low": [99.0, 100.0, 99.5],
            "adjclose": [99.6, 101.1, 99.4],
        },
        "MSFT": {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "volume": [2000, 2100, 2200],
            "open": [49.8, 50.1, 50.6],
            "close": [50.0, 50.5, 51.2],
            "high": [50.4, 50.9, 51.5],
            "low": [49.5, 49.9, 50.3],
            "adjclose": [49.7, 50.2, 50.9],
        },
        "AMD": {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "volume": [3000, 3100, 3200],
            "open": [24.8, 25.1, 25.4],
            "close": [25.0, 25.3, 25.6],
            "high": [25.4, 25.6, 25.9],
            "low": [24.5, 24.9, 25.1],
            "adjclose": [24.9, 25.2, 25.5],
        },
    }
    for ticker, columns in per_symbol.items():
        pd.DataFrame(columns).to_csv(full_history_dir / f"{ticker}.csv", index=False)

    (landing_dir / "NASDAQ.txt").write_text("AAPL\nMSFT\nAMD\n")
    (landing_dir / "NYSE.txt").write_text("")
    (landing_dir / "AMEX.txt").write_text("")
    (landing_dir / "all_symbols.txt").write_text("AAPL\nMSFT\nAMD\n")
    (landing_dir / "excluded_symbols.txt").write_text("")

    return landing_dir.parent  # the "landing" root


@pytest.fixture
def dev_environment(tmp_path: Path, sample_market_prices_csv: Path) -> Path:
    """Builds a throwaway dev.yaml pointing landing at the sample CSV and
    bronze/silver/gold at empty tmp directories. Returns the config file path.
    """
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    for directory in (bronze, silver, gold):
        directory.mkdir(parents=True, exist_ok=True)

    config_path = tmp_path / "dev.yaml"
    config_path.write_text(
        "\n".join(
            [
                "environment: dev",
                "paths:",
                f"  landing: {sample_market_prices_csv}",
                f"  bronze: {bronze}",
                f"  silver: {silver}",
                f"  gold: {gold}",
                "market_data:",
                "  source: historical",
            ]
        )
    )
    return config_path


@pytest.fixture
def registry():
    """Loads the real, shipped metadata directory (validated as part of the
    architecture test suite too)."""
    from platform_name.engine.table_registry import TableRegistry

    return TableRegistry(METADATA_ROOT).load()


@pytest.fixture
def deterministic_yfinance_fetch(monkeypatch):
    """Replaces bronze.market_prices_daily's real yfinance call with a
    deterministic fake, so registry-driven runs (via TableRunner /
    ProcessorFactory, which always use the shipped metadata's default
    fetcher) work offline and without yfinance installed.

    Patched at module level, before construction, so
    MarketPricesDailyProcessor's `fetch_history or _default_fetch_history`
    default resolves to this fake at call time -- see
    tables/bronze/market_prices_daily/processor.py.
    """
    import platform_name.tables.bronze.market_prices_daily.processor as daily_module

    def fake_fetch_history(ticker: str, start):
        dates = pd.date_range(start=start, periods=2, freq="D")
        base = {"AAPL": 100.0, "MSFT": 50.0, "AMD": 25.0}.get(ticker, 10.0)
        return pd.DataFrame(
            {
                "Open": [base, base * 1.01],
                "High": [base * 1.02, base * 1.03],
                "Low": [base * 0.98, base * 0.99],
                "Close": [base * 1.01, base * 1.015],
                "Adj Close": [base * 1.01, base * 1.015],
                "Volume": [1_000_000, 1_100_000],
            },
            index=pd.DatetimeIndex(dates, name="Date"),
        )

    monkeypatch.setattr(daily_module, "_default_fetch_history", fake_fetch_history)
    return fake_fetch_history
