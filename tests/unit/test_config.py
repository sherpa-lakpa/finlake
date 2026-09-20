import pytest

from platform_name.common.exceptions import ConfigurationError
from platform_name.common.config import Config


def test_loads_valid_environment_config(dev_environment):
    config = Config(dev_environment)
    assert config.environment == "dev"
    assert set(config.paths) == {"landing", "bronze", "silver", "gold"}


def test_layer_root_returns_configured_path(dev_environment):
    config = Config(dev_environment)
    assert config.layer_root("bronze") == config.paths["bronze"]


def test_layer_root_raises_for_unknown_layer(dev_environment):
    config = Config(dev_environment)
    with pytest.raises(ConfigurationError):
        config.layer_root("not_a_layer")


def test_get_supports_dotted_paths(dev_environment):
    config = Config(dev_environment)
    assert config.get("market_data.source") == "historical"
    assert config.get("market_data.missing", "default") == "default"


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigurationError):
        Config(tmp_path / "does_not_exist.yaml")


def test_missing_environment_key_raises(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("paths:\n  landing: a\n  bronze: b\n  silver: c\n  gold: d\n")
    with pytest.raises(ConfigurationError):
        Config(bad_config)


def test_missing_paths_raises(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("environment: dev\n")
    with pytest.raises(ConfigurationError):
        Config(bad_config)


def test_missing_path_root_raises(tmp_path):
    bad_config = tmp_path / "bad.yaml"
    bad_config.write_text("environment: dev\npaths:\n  landing: a\n  bronze: b\n  silver: c\n")
    with pytest.raises(ConfigurationError):
        Config(bad_config)
