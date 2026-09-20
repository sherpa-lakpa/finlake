"""Bronze processor: exchange listing files -> normalized (ticker, exchange)
reference data.

Reads the plain-text, one-symbol-per-line listing files that ship with the
historical dataset (``NASDAQ.txt``, ``NYSE.txt``, ``AMEX.txt``) and
normalizes them into a single ticker-to-exchange mapping. This is real
reference data -- unlike the placeholder ``asset_class="EQUITY"`` the
example domain used to hardcode, ``silver.security_master`` now enriches
every security with the exchange it actually trades on.

``excluded_symbols.txt`` (symbols the dataset's own historical dump
couldn't retrieve) and ``all_symbols.txt`` (a convenience union of the
three exchange files) are not read here -- the per-exchange files are the
authoritative source, and ``all_symbols.txt`` would be redundant with
unioning them.
"""
from __future__ import annotations

import pandas as pd

from platform_name.contracts.definitions import EXCHANGE_LISTINGS_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor

# Maps a listing filename (under source_path) to the exchange name it
# represents. Deliberately data, not a table-specific hack elsewhere in
# the framework -- this mapping is this processor's own business logic.
_LISTING_FILES = {
    "NASDAQ.txt": "NASDAQ",
    "NYSE.txt": "NYSE",
    "AMEX.txt": "AMEX",
}


class ExchangeListingsProcessor(BaseProcessor):
    """Reads NASDAQ.txt/NYSE.txt/AMEX.txt (one ticker symbol per line) and
    produces one row per (ticker, exchange)."""

    def __init__(self, source_path: str, output_path: str) -> None:
        self.source_path = source_path
        self.output_path = output_path
        self.storage = LocalFileSystemStorage()

    def read(self) -> dict[str, list[str]]:
        from pathlib import Path

        listings: dict[str, list[str]] = {}
        for filename, exchange in _LISTING_FILES.items():
            path = Path(self.source_path) / filename
            if not path.exists():
                raise FileNotFoundError(
                    f"Expected exchange listing file not found: {path}"
                )
            lines = path.read_text(encoding="utf-8").splitlines()
            listings[exchange] = [line.strip() for line in lines if line.strip()]
        return listings

    def transform(self, data: dict[str, list[str]]) -> pd.DataFrame:
        rows = [
            {"ticker": ticker.upper(), "exchange": exchange}
            for exchange, tickers in data.items()
            for ticker in tickers
        ]
        df = pd.DataFrame(rows, columns=["ticker", "exchange"])
        # A ticker should only ever be listed on one exchange in this
        # dataset; if the same symbol appears in more than one listing
        # file, keep the first one seen (stable, deterministic) rather
        # than silently duplicating it downstream.
        df = df.drop_duplicates(subset=["ticker"], keep="first")
        return df.sort_values("ticker").reset_index(drop=True)

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, EXCHANGE_LISTINGS_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        self.storage.write_parquet(data, self.output_path)
