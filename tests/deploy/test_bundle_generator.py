"""Tests for the metadata -> Databricks Asset Bundle job generator.

These tests assert the generated task graph matches the dependency graph
exactly -- if they ever diverge, local (`TableRunner`) and production
(generated Workflow) execution order would diverge too, which is the one
thing this generator exists to prevent.
"""
from __future__ import annotations

import yaml

from platform_name.deploy.bundle_generator import (
    JOB_RESOURCE_NAME,
    _task_key,
    build_job_resource,
    build_task_definitions,
    render_yaml,
)


def test_task_key_translates_dots_to_double_underscore():
    assert _task_key("gold.returns") == "gold__returns"
    assert _task_key("silver.daily_prices") == "silver__daily_prices"


def test_generates_one_task_per_table(registry):
    tasks = build_task_definitions(registry)
    task_keys = {task["task_key"] for task in tasks}
    assert task_keys == {
        "bronze__market_prices_historical",
        "bronze__market_prices_daily",
        "bronze__exchange_listings",
        "silver__security_master",
        "silver__daily_prices",
        "gold__returns",
        "gold__performance_summary",
        "gold__customer_risk",
        "gold__top_movers",
    }


def test_depends_on_matches_metadata_dependencies_exactly(registry):
    tasks = {task["task_key"]: task for task in build_task_definitions(registry)}

    assert "depends_on" not in tasks["bronze__market_prices_historical"]
    assert "depends_on" not in tasks["bronze__market_prices_daily"]
    assert "depends_on" not in tasks["bronze__exchange_listings"]

    assert {d["task_key"] for d in tasks["silver__security_master"]["depends_on"]} == {
        "bronze__market_prices_historical",
        # "bronze__market_prices_daily",
        # "bronze__exchange_listings",
    }

    assert {d["task_key"] for d in tasks["silver__daily_prices"]["depends_on"]} == {
        "bronze__market_prices_historical",
        # "bronze__market_prices_daily",
        "silver__security_master",
    }

    assert {d["task_key"] for d in tasks["gold__returns"]["depends_on"]} == {
        "silver__daily_prices"
    }

    assert {d["task_key"] for d in tasks["gold__performance_summary"]["depends_on"]} == {
        "gold__returns"
    }

    assert {d["task_key"] for d in tasks["gold__customer_risk"]["depends_on"]} == {
        "gold__returns"
    }

    assert {d["task_key"] for d in tasks["gold__top_movers"]["depends_on"]} == {
        "gold__returns"
    }


def test_every_task_invokes_the_same_generic_entry_point(registry):
    """No task should ever reference a table-specific script -- every task
    calls the same entry point with a different --table parameter."""
    tasks = build_task_definitions(registry)
    entry_points = {task["python_wheel_task"]["entry_point"] for task in tasks}
    assert entry_points == {"run_single_table"}


def test_each_task_parameterizes_its_own_table_name(registry):
    tasks = {task["task_key"]: task for task in build_task_definitions(registry)}
    params = tasks["gold__returns"]["python_wheel_task"]["parameters"]
    assert params == ["--table", "gold.returns", "--env", "{{job.parameters.environment}}"]


def test_rendered_yaml_is_valid_and_round_trips(registry):
    rendered = render_yaml(registry)
    # Strip the leading comment header before parsing.
    document = yaml.safe_load(rendered)
    job = document["resources"]["jobs"][JOB_RESOURCE_NAME]
    assert len(job["tasks"]) == 9
    assert job["parameters"] == [{"name": "environment", "default": "${var.environment}"}]


def test_job_resource_has_no_business_specific_top_level_structure(registry):
    """The job *shape* (clusters, parameters, notification target) must not
    vary per table -- only the generated `tasks` list should."""
    resource = build_job_resource(registry)
    job = resource["resources"]["jobs"][JOB_RESOURCE_NAME]
    assert set(job) == {"name", "parameters", "job_clusters", "tasks", "email_notifications"}
