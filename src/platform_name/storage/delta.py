"""Delta Lake storage adapter (PySpark-backed).

Gives PySpark processors the same kind of storage abstraction
:class:`~platform_name.storage.local.LocalFileSystemStorage` gives Pandas
processors: a small, swappable object that owns *how* data is physically
read and written, so processor business logic never calls Spark I/O APIs
directly. This does **not** implement the generic
:class:`~platform_name.storage.base.StorageAdapter` Protocol verbatim
(Delta's natural operations -- path vs. table, merge, etc. -- don't map
1:1 onto Parquet-file operations), but it follows the same design
intent: one small class owning physical storage concerns, imported by
processors, never by the generic engine.

Requires a live ``SparkSession`` with Delta Lake support configured. Not
imported by anything in ``engine/`` -- only by PySpark processors that
choose to use it (see ``tables/pyspark_base.py``).
"""
from __future__ import annotations

from typing import Any


class DeltaResultSinkUnavailable(RuntimeError):
    """Raised when a Delta operation is attempted without a usable Spark
    session backing it."""


class DeltaStorageAdapter:
    """Reads and writes Delta tables, addressed either by filesystem/ADLS
    path or by Unity Catalog table name.
    """

    def __init__(self, spark: Any) -> None:
        if spark is None:
            raise DeltaResultSinkUnavailable(
                "DeltaStorageAdapter requires a live SparkSession."
            )
        self.spark = spark

    # -- path-addressed (e.g. abfss://...) -----------------------------

    def read_delta(self, path: str):
        """Reads a Delta table addressed by storage path."""
        return self.spark.read.format("delta").load(path)

    def write_delta(self, df: Any, path: str, mode: str = "overwrite") -> None:
        """Writes a Delta table addressed by storage path.

        ``mode`` follows Spark's DataFrameWriter semantics
        (``overwrite`` / ``append`` / ``error`` / ``ignore``). Callers
        implementing `LoadStrategy.UPSERT`/`SCD1`/`SCD2` semantics should
        use :meth:`merge_into` instead of ``write_delta(..., mode="overwrite")``
        once that logic is implemented for a given table -- see the
        idempotent-retry caveat in docs/DEPLOYMENT.md.
        """
        df.write.format("delta").mode(mode).save(path)

    # -- Unity Catalog table-addressed -----------------------------------

    def read_delta_table(self, table_name: str):
        """Reads a Delta table addressed by its Unity Catalog name
        (``catalog.schema.table``)."""
        return self.spark.read.table(table_name)

    def write_delta_table(self, df: Any, table_name: str, mode: str = "overwrite") -> None:
        df.write.format("delta").mode(mode).saveAsTable(table_name)

    # -- shared -----------------------------------------------------------

    def exists(self, path: str) -> bool:
        try:
            from delta.tables import DeltaTable

            return DeltaTable.isDeltaTable(self.spark, path)
        except Exception:
            return False

    def merge_into(
        self,
        target_path: str,
        updates_df: Any,
        merge_condition: str,
        when_matched_update: bool = True,
        when_not_matched_insert: bool = True,
    ) -> None:
        """Performs a Delta ``MERGE`` -- the primitive an ``upsert``/``scd1``
        load strategy's processor would use once implemented. Not called by
        anything in this repository yet; provided as the documented
        extension point for the idempotent-retry work flagged in
        docs/DEPLOYMENT.md's "What's intentionally not built yet" section.
        """
        from delta.tables import DeltaTable

        target = DeltaTable.forPath(self.spark, target_path)
        merge_builder = target.alias("target").merge(
            updates_df.alias("updates"), merge_condition
        )
        if when_matched_update:
            merge_builder = merge_builder.whenMatchedUpdateAll()
        if when_not_matched_insert:
            merge_builder = merge_builder.whenNotMatchedInsertAll()
        merge_builder.execute()
