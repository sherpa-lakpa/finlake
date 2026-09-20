"""Dynamic processor loading.

Given metadata declaring a module + class name, ``ProcessorLoader`` imports
the module and resolves the class. It never hardcodes any specific processor
class -- every processor is resolved purely from the string names declared
in table metadata.
"""
from __future__ import annotations

import importlib
from typing import Any

from platform_name.common.exceptions import ProcessorLoadError
from platform_name.engine.models import TableDefinition


class ProcessorLoader:
    """Resolves a processor class from a :class:`TableDefinition`."""

    def load(self, table_definition: TableDefinition) -> type:
        module_name = table_definition.processor_module
        class_name = table_definition.processor_class

        if not module_name or not class_name:
            raise ProcessorLoadError(
                "Table metadata does not declare a processor module/class.",
                context={"table": table_definition.fully_qualified_name},
            )

        try:
            module = importlib.import_module(module_name)
        except ImportError as err:
            raise ProcessorLoadError(
                "Unable to import processor module declared in table metadata.",
                context={
                    "table": table_definition.fully_qualified_name,
                    "module": module_name,
                    "error": str(err),
                },
            ) from err

        processor_class: Any = getattr(module, class_name, None)
        if processor_class is None:
            raise ProcessorLoadError(
                "Processor class not found in the declared module.",
                context={
                    "table": table_definition.fully_qualified_name,
                    "module": module_name,
                    "class": class_name,
                    "available_attributes": sorted(
                        name for name in dir(module) if not name.startswith("_")
                    ),
                },
            )

        if not isinstance(processor_class, type):
            raise ProcessorLoadError(
                "Resolved processor attribute is not a class.",
                context={
                    "table": table_definition.fully_qualified_name,
                    "module": module_name,
                    "class": class_name,
                },
            )

        return processor_class
