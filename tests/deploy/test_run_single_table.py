"""Tests for the single-table production entry point.

Simulates exactly how Databricks Workflows will invoke this script: one
table per process, ordering guaranteed externally rather than by this
script re-resolving the dependency graph.
"""
from __future__ import annotations

import json

import pandas as pd
import pytest

from platform_name.common.exceptions import RegistryError
from platform_name.engine.models import ExecutionResult
from platform_name.entrypoints.run_single_table import parse_args, run
from platform_name.observability.result_sink import ResultSink

REPO_ROOT_METADATA = "src/platform_name/tables"


class _RecordingSink:
    def __init__(self) -> None:
        self.recorded: list[ExecutionResult] = []

    def record(self, result: ExecutionResult) -> None:
        self.recorded.append(result)


def test_parses_required_arguments():
    args = parse_args(["--table", "gold.returns", "--env", "prod"])
    assert args.table == "gold.returns"
    assert args.env == "prod"


def test_running_a_table_whose_upstream_output_is_missing_fails_cleanly(dev_environment):
    """Simulates the exact triage scenario: this task fires, but an upstream
    table's output was never written (missing depends_on, or upstream truly
    failed). The failure must be table-scoped and clearly attributable.
    """
    sink = _RecordingSink()

    with pytest.raises(FileNotFoundError):
        run(
            table_name="silver.daily_prices",
            environment="dev",
            metadata_root=REPO_ROOT_METADATA,
            config_path=dev_environment,
            sink=sink,
        )

    assert len(sink.recorded) == 1
    result = sink.recorded[0]
    assert result.table == "silver.daily_prices"
    assert result.status == "failed"
    assert result.error is not None


def test_running_an_unknown_table_raises_registry_error(dev_environment):
    with pytest.raises(RegistryError):
        run(
            table_name="gold.does_not_exist",
            environment="dev",
            metadata_root=REPO_ROOT_METADATA,
            config_path=dev_environment,
            sink=_RecordingSink(),
        )


def test_running_bronze_table_succeeds_and_records_result(dev_environment):
    """bronze.market_prices_historical has no dependencies, so it's one of
    the tables this entry point can run standalone without any prior task
    having executed -- exactly like a root task in the generated job."""
    sink = _RecordingSink()

    result = run(
        table_name="bronze.market_prices_historical",
        environment="dev",
        metadata_root=REPO_ROOT_METADATA,
        config_path=dev_environment,
        sink=sink,
    )

    assert result.status == "success"
    assert result.row_count == 9  # 3 symbols x 3 days each, see conftest.py
    assert len(sink.recorded) == 1
    assert sink.recorded[0] is result


def test_result_sink_receives_json_serializable_dict(dev_environment):
    sink = _RecordingSink()
    run(
        table_name="bronze.market_prices_historical",
        environment="dev",
        metadata_root=REPO_ROOT_METADATA,
        config_path=dev_environment,
        sink=sink,
    )
    # Must not raise -- this is what StdoutResultSink does in production.
    json.dumps(sink.recorded[0].to_dict())
