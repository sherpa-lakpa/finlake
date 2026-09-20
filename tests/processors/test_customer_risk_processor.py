"""Tests for the real, shipped CustomerRiskProcessor (execution.mode:
pyspark).

Requires the optional `pyspark` dependency (the `spark` extra) to even
import -- this whole module is skipped, not failed, when it isn't
installed, since a genuine PySpark DataFrame-API processor legitimately
cannot be imported without PySpark itself (this is expected, not a
framework gap -- see docs/PYSPARK_AND_SQL_GUIDE.md).

Run with: pip install -e ".[dev,spark]" && pytest tests/processors/test_customer_risk_processor.py
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark", reason="requires the optional `spark` extra: pip install -e '.[spark]'")

from pyspark.sql import SparkSession  # noqa: E402

from platform_name.tables.gold.customer_risk.processor import CustomerRiskProcessor  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test-customer-risk").getOrCreate()
    yield session
    session.stop()


def test_computes_mean_absolute_daily_return_per_security(spark, tmp_path):
    returns_path = str(tmp_path / "returns.parquet")
    output_path = str(tmp_path / "customer_risk.parquet")

    input_df = spark.createDataFrame(
        [
            ("2024-01-02", "AAA", None),
            ("2024-01-03", "AAA", 0.10),
            ("2024-01-04", "AAA", -0.20),
            ("2024-01-02", "BBB", None),
            ("2024-01-03", "BBB", 0.05),
        ],
        ["trade_date", "security_id", "daily_return"],
    )
    input_df.write.mode("overwrite").parquet(returns_path)

    processor = CustomerRiskProcessor(spark=spark, returns_path=returns_path, output_path=output_path)
    result_df = processor.run()

    rows = {row["security_id"]: row["risk_score"] for row in result_df.collect()}
    assert rows["AAA"] == pytest.approx(0.15)  # mean(|0.10|, |-0.20|)
    assert rows["BBB"] == pytest.approx(0.05)

    persisted = spark.read.parquet(output_path)
    assert persisted.count() == 2


def test_requires_a_spark_session():
    with pytest.raises(ValueError):
        CustomerRiskProcessor(spark=None, returns_path="x", output_path="y")
