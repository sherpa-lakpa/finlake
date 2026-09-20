"""Local filesystem implementation of :class:`StorageAdapter`.

This is the only storage backend implemented today. Azure Data Lake Storage
Gen2, Databricks (Unity Catalog / DBFS), and S3 adapters can be added later
by implementing the same interface -- processors would not need to change.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd


class LocalFileSystemStorage:
    """Reads/writes Parquet and CSV files on the local filesystem."""

    def read_parquet(self, path: str | Path) -> pd.DataFrame:
        return pd.read_parquet(Path(path))

    def write_parquet(self, df: pd.DataFrame, path: str | Path) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)

    def read_csv(self, path: str | Path) -> pd.DataFrame:
        return pd.read_csv(Path(path))

    def list_files(self, directory: str | Path, pattern: str = "*") -> list[Path]:
        directory_path = Path(directory)
        if not directory_path.exists():
            return []
        return sorted(p for p in directory_path.glob(pattern) if p.is_file())

    def exists(self, path: str | Path) -> bool:
        return Path(path).exists()
