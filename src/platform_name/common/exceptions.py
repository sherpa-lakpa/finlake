"""Exception hierarchy for the platform.

All exceptions preserve the original underlying exception (via ``raise ... from err``
at the call site) while adding structured, human-readable context about *where*
in the system the failure occurred (environment, table, processor, dependency,
configuration key, etc.).
"""
from __future__ import annotations

from typing import Any


class PlatformError(Exception):
    """Base class for all platform errors.

    Subclasses build a clear, multi-line message that identifies the table,
    processor, and/or configuration involved, so failures are actionable
    without having to read a stack trace.
    """

    def __init__(self, message: str, *, context: dict[str, Any] | None = None) -> None:
        self.context = context or {}
        full_message = message
        if self.context:
            details = "\n".join(f"  {key}: {value}" for key, value in self.context.items())
            full_message = f"{message}\n{details}"
        super().__init__(full_message)


class ConfigurationError(PlatformError):
    """Raised when environment/platform configuration is missing or invalid."""


class MetadataError(PlatformError):
    """Raised when table metadata (YAML) is missing, malformed, or invalid."""


class RegistryError(PlatformError):
    """Raised when the table registry cannot find or resolve a table."""


class ProcessorLoadError(PlatformError):
    """Raised when a processor module/class cannot be dynamically imported."""


class ProcessorConstructionError(PlatformError):
    """Raised when a processor cannot be instantiated from resolved arguments."""


class DependencyGraphError(PlatformError):
    """Raised for dependency graph problems: cycles, missing nodes, bad edges."""


class DataQualityError(PlatformError):
    """Raised when a dataset fails validation against its contract."""


class ExecutionError(PlatformError):
    """Raised when a processor fails during execution (read/transform/write)."""
