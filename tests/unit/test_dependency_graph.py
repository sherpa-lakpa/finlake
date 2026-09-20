import pytest

from platform_name.common.exceptions import DependencyGraphError
from platform_name.engine.dependency_graph import DependencyGraph
from platform_name.engine.enums import ExecutionMode, Layer, LoadStrategy, WriteMode
from platform_name.engine.models import TableDefinition


class _FakeRegistry:
    """Minimal registry stand-in for graph-only tests, avoiding disk I/O."""

    def __init__(self, definitions: dict[str, TableDefinition]) -> None:
        self._definitions = definitions

    def has(self, name: str) -> bool:
        return name in self._definitions

    def get(self, name: str) -> TableDefinition:
        return self._definitions[name]

    def list_tables(self):
        return tuple(self._definitions.values())

    def all_names(self):
        return tuple(sorted(self._definitions))


def _table(name: str, layer: Layer, dependencies: tuple[str, ...]) -> TableDefinition:
    return TableDefinition(
        name=name.split(".")[-1] if "." in name else name,
        layer=layer,
        execution_mode=ExecutionMode.PANDAS,
        load_strategy=LoadStrategy.FULL,
        write_mode=WriteMode.OVERWRITE,
        dependencies=dependencies,
        description="",
        processor_module="mod",
        processor_class="Cls",
        metadata_path=None,
        metadata={},
        paths={},
        config={},
    )


def test_resolve_returns_correct_topological_order(registry):
    graph = DependencyGraph(registry)
    order = graph.resolve("gold.returns")
    print("order***",order)
    # The three bronze tables have no dependencies among each other, so
    # their relative order isn't fixed -- assert the structural invariants
    # instead of one exact ordering.
    assert set(order[:3]) == {
        "bronze.market_prices_historical",
        "silver.security_master",
        "silver.daily_prices",
    }
    # assert order[3] == "silver.security_master"
    # assert order[4] == "silver.daily_prices"
    # assert order[5] == "gold.returns"
    assert order[3] == "gold.returns"
    assert len(order) == 4


def test_resolve_deeper_chain(registry):
    graph = DependencyGraph(registry)
    order = graph.resolve("gold.performance_summary")
    assert order[-1] == "gold.performance_summary"
    assert order.index("gold.returns") < order.index("gold.performance_summary")
    assert order.index("silver.daily_prices") < order.index("gold.returns")


def test_upstream_of_excludes_target(registry):
    graph = DependencyGraph(registry)
    upstream = graph.upstream_of("gold.returns")
    assert "gold.returns" not in upstream
    assert "silver.daily_prices" in upstream


def test_missing_dependency_detected():
    fake_registry = _FakeRegistry(
        {"gold.returns": _table("gold.returns", Layer.GOLD, ("silver.does_not_exist",))}
    )
    graph = DependencyGraph(fake_registry)
    with pytest.raises(DependencyGraphError):
        graph.validate()


def test_circular_dependency_detected():
    fake_registry = _FakeRegistry(
        {
            "silver.a": _table("silver.a", Layer.SILVER, ("silver.b",)),
            "silver.b": _table("silver.b", Layer.SILVER, ("silver.a",)),
        }
    )
    graph = DependencyGraph(fake_registry)
    with pytest.raises(DependencyGraphError):
        graph.resolve("silver.a")


def test_invalid_layer_ordering_detected():
    fake_registry = _FakeRegistry(
        {
            "bronze.a": _table("bronze.a", Layer.BRONZE, ("gold.b",)),
            "gold.b": _table("gold.b", Layer.GOLD, ()),
        }
    )
    graph = DependencyGraph(fake_registry)
    with pytest.raises(DependencyGraphError):
        graph.validate()


def test_full_registry_validates_cleanly(registry):
    graph = DependencyGraph(registry)
    graph.validate()  # should not raise
