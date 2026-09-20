"""Storage adapter interface.

Processors read/write data through a storage adapter rather than calling
``pandas.read_parquet`` / filesystem APIs directly wherever possible, so
that swapping local filesystem storage for ADLS Gen2, S3, or a Databricks
volume later does not require rewriting business logic. The example
processors in this repository use :class:`LocalFileSystemStorage` directly
for simplicity, but new storage backends only need to implement this
interface.
"""
from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pandas as pd


class StorageAdapter(Protocol):
    """Minimal storage contract used by processors."""

    def read_parquet(self, path: str | Path) -> pd.DataFrame: ...

    def write_parquet(self, df: pd.DataFrame, path: str | Path) -> None: ...

    def read_csv(self, path: str | Path) -> pd.DataFrame: ...

    def list_files(self, directory: str | Path, pattern: str = "*") -> list[Path]: ...

    def exists(self, path: str | Path) -> bool: ...
