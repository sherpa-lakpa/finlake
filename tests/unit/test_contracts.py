import pandas as pd
import pytest

from platform_name.contracts.base import ColumnContract, DatasetContract
from platform_name.contracts.definitions import SECURITY_MASTER_CONTRACT
from platform_name.common.exceptions import DataQualityError
from platform_name.quality.framework import validate_dataset, validate_dataset_contract


def test_required_columns_default_to_all_declared_columns():
    contract = DatasetContract(
        name="example",
        columns={"a": ColumnContract(), "b": ColumnContract()},
    )
    assert contract.required_columns == ("a", "b")


def test_valid_dataset_passes():
    df = pd.DataFrame(
        {
            "security_id": ["AAA", "BBB"],
            "ticker": ["AAA", "BBB"],
            "asset_class": ["EQUITY", "EQUITY"],
            "exchange": ["NASDAQ", "NASDAQ"],
        }
    )
    validate_dataset_contract(df, SECURITY_MASTER_CONTRACT)  # should not raise


def test_missing_required_column_fails():
    df = pd.DataFrame({"security_id": ["AAA"], "ticker": ["AAA"]})
    result = validate_dataset(df, SECURITY_MASTER_CONTRACT)
    assert not result.is_valid
    assert any("asset_class" in v for v in result.violations)


def test_duplicate_business_key_fails():
    df = pd.DataFrame(
        {
            "security_id": ["AAA", "AAA"],
            "ticker": ["AAA", "AAA"],
            "asset_class": ["EQUITY", "EQUITY"],
        }
    )
    with pytest.raises(DataQualityError):
        validate_dataset_contract(df, SECURITY_MASTER_CONTRACT)


def test_null_in_non_nullable_column_fails():
    df = pd.DataFrame(
        {
            "security_id": ["AAA", None],
            "ticker": ["AAA", "BBB"],
            "asset_class": ["EQUITY", "EQUITY"],
        }
    )
    result = validate_dataset(df, SECURITY_MASTER_CONTRACT)
    assert not result.is_valid


def test_row_count_below_minimum_fails():
    contract = DatasetContract(name="example", columns={"a": ColumnContract()}, min_row_count=5)
    df = pd.DataFrame({"a": [1, 2]})
    result = validate_dataset(df, contract)
    assert not result.is_valid
    assert any("row count" in v for v in result.violations)
