"""Bronze processor: incremental daily bars from the yfinance API.

Continues where the historical dump leaves off. On each run, for every
configured symbol, it looks at what's already in this table's own output
(if any) to find the last ingested trade_date per ticker, then fetches
only what's new since then -- falling back to ``since_default`` (which
should match wherever your historical dump's coverage ends) for a symbol
seen for the first time.

``fetch_history`` is injectable specifically so this processor's
resume-point and merge logic is fully unit-testable without a real network
call or ``yfinance`` installed -- see tests/processors/test_market_prices_daily_processor.py.
The default implementation is the real API call.

yfinance note: as of yfinance 0.2.28+, ``auto_adjust=True`` is the
default, which folds split/dividend adjustments directly into ``Close``
and drops the separate ``Adj Close`` column entirely. This processor
passes ``auto_adjust=False`` explicitly to keep raw close and adjusted
close as two separate columns, matching the historical dump's
``close``/``adjclose`` shape -- without that, this table's schema would
silently diverge from bronze.market_prices_historical's.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Callable

import pandas as pd

from platform_name.contracts.definitions import MARKET_PRICES_CONTRACT
from platform_name.quality.framework import validate_dataset_contract
from platform_name.storage.local import LocalFileSystemStorage
from platform_name.tables.base import BaseProcessor

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

_YFINANCE_COLUMN_RENAMES = {
    "Date": "trade_date",
    "Open": "open_price",
    "High": "high_price",
    "Low": "low_price",
    "Close": "close_price",
    "Adj Close": "adj_close",
    "Volume": "volume",
}


def _default_fetch_history(ticker: str, start: date) -> pd.DataFrame:
    """Real yfinance call -- imported lazily so this module (and every
    other symbol's resume-point logic) never requires `yfinance` to be
    installed just to import it; only calling this specific function does.
    """
    import yfinance as yf

    return yf.Ticker(ticker).history(start=start, auto_adjust=False)


class MarketPricesDailyProcessor(BaseProcessor):
    def __init__(
        self,
        output_path: str,
        symbols: list[str] | None = None,
        since_default: str = "2020-07-01",
        source: str = "yfinance_daily",
        as_of: str | None = None,
        fetch_history: Callable[[str, date], pd.DataFrame] | None = None,
    ) -> None:
        self.output_path = output_path
        self.symbols = [s.upper() for s in (symbols or [])]
        self.since_default = pd.Timestamp(since_default)
        self.source = source
        self.as_of = pd.Timestamp(as_of) if as_of else pd.Timestamp.today().normalize()
        self.fetch_history = fetch_history or _default_fetch_history
        self.storage = LocalFileSystemStorage()

    def _existing_output(self) -> pd.DataFrame | None:
        if self.storage.exists(self.output_path):
            return self.storage.read_parquet(self.output_path)
        return None

    def _resume_date(self, ticker: str, existing: pd.DataFrame | None) -> pd.Timestamp:
        if existing is not None and (existing["ticker"] == ticker).any():
            last_ingested = existing.loc[existing["ticker"] == ticker, "trade_date"].max()
            return last_ingested + pd.Timedelta(days=1)
        return self.since_default

    def read(self) -> tuple[pd.DataFrame | None, dict[str, pd.DataFrame]]:
        existing = self._existing_output()
        fetched: dict[str, pd.DataFrame] = {}

        for ticker in self.symbols:
            resume_from = self._resume_date(ticker, existing)
            if resume_from.date() > self.as_of.date():
                continue  # already caught up for this symbol

            raw = self.fetch_history(ticker, resume_from.date())
            if raw is not None and not raw.empty:
                fetched[ticker] = raw

        return existing, fetched

    def transform(self, data: tuple[pd.DataFrame | None, dict[str, pd.DataFrame]]) -> pd.DataFrame:
        existing, fetched = data

        new_frames: list[pd.DataFrame] = []
        for ticker, raw in fetched.items():
            frame = raw.reset_index().rename(columns=_YFINANCE_COLUMN_RENAMES)
            trade_date = pd.to_datetime(frame["trade_date"])
            if trade_date.dt.tz is not None:
                trade_date = trade_date.dt.tz_localize(None)
            frame["trade_date"] = trade_date
            frame["ticker"] = ticker
            frame["source_system"] = self.source
            new_frames.append(frame[_OUTPUT_COLUMNS])

        if not new_frames:
            # Nothing new for any symbol this run (every symbol already
            # caught up). Returning `existing` as-is, rather than
            # concatenating it with an empty, untyped placeholder frame,
            # matters: an empty pd.DataFrame(columns=...) defaults every
            # column to `object` dtype, and concatenating that with a
            # properly-typed `existing` can silently upcast the whole
            # result to `object` in some pandas versions -- which then
            # fails contract validation for reasons that have nothing to
            # do with the data actually being wrong.
            if existing is not None:
                return existing.sort_values(["trade_date", "ticker"]).reset_index(drop=True)
            return pd.DataFrame(columns=_OUTPUT_COLUMNS)

        new_rows = pd.concat(new_frames, ignore_index=True)
        combined = pd.concat([existing, new_rows], ignore_index=True) if existing is not None else new_rows

        # Idempotent re-run: if this table is retried after fetching the
        # same date range twice, prefer the freshly-fetched row -- new_rows
        # is concatenated after existing, so keep="last" resolves ties in
        # its favor.
        combined = combined.drop_duplicates(subset=["ticker", "trade_date"], keep="last")
        return combined.sort_values(["trade_date", "ticker"]).reset_index(drop=True)

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        validate_dataset_contract(data, MARKET_PRICES_CONTRACT)
        return data

    def write(self, data: pd.DataFrame) -> None:
        # Physically a full overwrite of the accumulated table -- see
        # metadata.yaml's write_mode for why that's still the correct
        # `load.strategy: incremental` logically: only new rows were
        # *fetched*, existing rows were untouched, this is simply the
        # local/Pandas equivalent of an incremental append (no native
        # append-to-Parquet-on-disk primitive exists the way Delta's
        # mode("append")/MERGE does).
        self.storage.write_parquet(data, self.output_path)
