"""Reusable data-quality validation framework.

``validate_dataset_contract`` checks a Pandas DataFrame against a
:class:`~platform_name.contracts.base.DatasetContract`: required columns,
dtypes, nullability, uniqueness, business-key uniqueness, and minimum row
count. It is reusable by bronze, silver, and gold processors alike.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from platform_name.common.exceptions import DataQualityError
from platform_name.contracts.base import DatasetContract


@dataclass
class ValidationResult:
    dataset: str
    violations: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.violations

    def add(self, message: str) -> None:
        self.violations.append(message)


def _check_required_columns(df: pd.DataFrame, contract: DatasetContract, result: ValidationResult) -> None:
    missing = [col for col in contract.required_columns if col not in df.columns]
    if missing:
        result.add(f"missing required columns: {missing}")


def _dtype_matches(series: pd.Series, expected_dtype: str) -> bool:
    """Category-based dtype check rather than an exact string match.

    Pandas versions differ on the concrete dtype used to represent strings
    (``object`` vs. a dedicated ``str``/``string`` dtype), datetimes with/without
    timezone, etc. Checking by *category* (string / datetime / integer / float
    / bool) is robust across pandas versions while still catching genuine
    type mismatches (e.g. a numeric column expected to be a string).
    """
    base = expected_dtype.split("[")[0]
    if base == "object":
        return bool(pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series))
    if base.startswith("datetime"):
        return bool(pd.api.types.is_datetime64_any_dtype(series))
    if base in ("int64", "int32", "int"):
        return bool(pd.api.types.is_integer_dtype(series))
    if base in ("float64", "float32", "float"):
        return bool(pd.api.types.is_float_dtype(series) or pd.api.types.is_integer_dtype(series))
    if base == "bool":
        return bool(pd.api.types.is_bool_dtype(series))
    return str(series.dtype).startswith(base)


def _check_dtypes_and_nulls(df: pd.DataFrame, contract: DatasetContract, result: ValidationResult) -> None:
    for column_name, column_contract in contract.columns.items():
        if column_name not in df.columns:
            continue  # already reported by required-columns check if applicable

        if column_contract.dtype is not None and not _dtype_matches(df[column_name], column_contract.dtype):
            result.add(
                f"column '{column_name}' has dtype '{df[column_name].dtype}', "
                f"expected '{column_contract.dtype}'"
            )

        if not column_contract.nullable and df[column_name].isnull().any():
            null_count = int(df[column_name].isnull().sum())
            result.add(f"column '{column_name}' has {null_count} null value(s) but is non-nullable")

        if column_contract.unique and df[column_name].duplicated().any():
            dup_count = int(df[column_name].duplicated().sum())
            result.add(f"column '{column_name}' has {dup_count} duplicate value(s) but must be unique")


def _check_business_keys(df: pd.DataFrame, contract: DatasetContract, result: ValidationResult) -> None:
    if not contract.business_keys:
        return
    missing_keys = [key for key in contract.business_keys if key not in df.columns]
    if missing_keys:
        result.add(f"business key column(s) missing: {missing_keys}")
        return
    duplicated = df.duplicated(subset=list(contract.business_keys))
    if duplicated.any():
        result.add(
            f"business key {contract.business_keys} is not unique: "
            f"{int(duplicated.sum())} duplicate row(s)"
        )


def _check_row_count(df: pd.DataFrame, contract: DatasetContract, result: ValidationResult) -> None:
    if len(df) < contract.min_row_count:
        result.add(f"row count {len(df)} is below minimum required {contract.min_row_count}")


def validate_dataset(df: pd.DataFrame, contract: DatasetContract) -> ValidationResult:
    """Run all checks and return a :class:`ValidationResult` (does not raise)."""
    result = ValidationResult(dataset=contract.name)
    _check_required_columns(df, contract, result)
    _check_dtypes_and_nulls(df, contract, result)
    _check_business_keys(df, contract, result)
    _check_row_count(df, contract, result)
    return result


def validate_dataset_contract(df: pd.DataFrame, contract: DatasetContract) -> None:
    """Validate ``df`` against ``contract``, raising :class:`DataQualityError`
    with all violations if any are found."""
    result = validate_dataset(df, contract)
    if not result.is_valid:
        raise DataQualityError(
            f"Dataset '{contract.name}' failed contract validation.",
            context={"dataset": contract.name, "violations": result.violations},
        )
