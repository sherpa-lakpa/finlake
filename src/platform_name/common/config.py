"""Environment configuration abstraction.

``Config`` loads a single environment YAML file (dev/test/prod) and exposes:

* the environment name
* the layer root paths (landing/bronze/silver/gold)
* arbitrary, domain-agnostic configuration values

It performs no business logic and knows nothing about specific tables.
The same table metadata is expected to work unchanged across environments,
because only the *paths* section differs between dev/test/prod configs.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from platform_name.common.exceptions import ConfigurationError

_REQUIRED_PATH_ROOTS = ("landing", "bronze", "silver", "gold")


class Config:
    """Loads and validates a single environment configuration file."""

    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self._raw: dict[str, Any] = self._load(self.config_path)
        self._validate(self._raw, self.config_path)

        self.environment: str = self._raw["environment"]
        self._paths: dict[str, str] = dict(self._raw["paths"])

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        if not path.exists():
            raise ConfigurationError(
                "Environment configuration file not found.",
                context={"path": str(path)},
            )
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        except yaml.YAMLError as err:
            raise ConfigurationError(
                "Environment configuration file is not valid YAML.",
                context={"path": str(path), "error": str(err)},
            ) from err

        if not isinstance(data, dict):
            raise ConfigurationError(
                "Environment configuration must be a YAML mapping.",
                context={"path": str(path)},
            )
        return data

    @staticmethod
    def _validate(data: dict[str, Any], path: Path) -> None:
        if "environment" not in data or not data["environment"]:
            raise ConfigurationError(
                "Environment configuration is missing required key 'environment'.",
                context={"path": str(path)},
            )

        paths = data.get("paths")
        if not isinstance(paths, dict):
            raise ConfigurationError(
                "Environment configuration is missing required 'paths' mapping.",
                context={"path": str(path)},
            )

        missing = [root for root in _REQUIRED_PATH_ROOTS if root not in paths]
        if missing:
            raise ConfigurationError(
                "Environment configuration is missing required path roots.",
                context={"path": str(path), "missing_roots": missing},
            )

    def layer_root(self, layer_name: str) -> str:
        """Return the configured root path for a layer (landing/bronze/silver/gold)."""
        try:
            return self._paths[layer_name]
        except KeyError as err:
            raise ConfigurationError(
                "No path root configured for requested layer.",
                context={
                    "environment": self.environment,
                    "layer": layer_name,
                    "available_roots": list(self._paths),
                },
            ) from err

    @property
    def paths(self) -> dict[str, str]:
        return dict(self._paths)

    def get(self, key: str, default: Any = None) -> Any:
        """Access arbitrary, non-path configuration values (dot-path supported)."""
        node: Any = self._raw
        for part in key.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return default
        return node

    def as_dict(self) -> dict[str, Any]:
        return dict(self._raw)

    def __repr__(self) -> str:  # pragma: no cover - convenience only
        return f"Config(environment={self.environment!r}, path={str(self.config_path)!r})"
