"""Base processor contract.

Processors implement the *business logic* for a single table: reading
inputs, transforming them, validating the result, and writing output. The
engine only ever calls :meth:`BaseProcessor.run`.

Subclasses are free to override ``run`` entirely for cases that don't fit
the read/transform/validate/write template, but implementing the four
lifecycle hooks is the recommended, testable default.
"""
from __future__ import annotations

from typing import Any


class BaseProcessor:
    """Standard processor lifecycle: read -> transform -> validate -> write."""

    def read(self) -> Any:
        raise NotImplementedError

    def transform(self, data: Any) -> Any:
        raise NotImplementedError

    def validate(self, data: Any) -> Any:
        """Default: no-op. Override to enforce a :class:`DatasetContract`."""
        return data

    def write(self, data: Any) -> None:
        raise NotImplementedError

    def run(self) -> Any:
        data = self.read()
        data = self.transform(data)
        data = self.validate(data)
        self.write(data)
        return data
