"""Dataset contract for bronze.market_prices_daily.

Re-exports bronze.market_prices_historical's contract rather than
duplicating it: both bronze tables produce the exact same schema by
design (that's what lets silver.daily_prices union them directly), so one
authoritative definition -- not a copy that could quietly drift -- is
kept here. If this table's shape ever needs to diverge from the
historical one, define its own DatasetContract here instead of importing.
"""
from __future__ import annotations

from platform_name.tables.bronze.market_prices_historical.contract import CONTRACT

__all__ = ["CONTRACT"]
