"""Build Fabric item definitions and bind environment-specific item IDs.

Placeholder-templated (unlike fabric-medallion-banking-poc's hardcoded-GUID
notebooks/pipeline, which only works for a single never-rebuilt workspace):
checked-in pipeline JSON uses {{WORKSPACE_ID}}, {{NOTEBOOK_ID:name}},
{{ITEM_ID:name}}, {{ITEM_NAME:name}} placeholders, resolved at deploy time by
bind_pipeline() against a deep copy — the checked-in template is never
mutated. Needed here because this is a from-scratch build: every notebook
and item ID is newly minted, so hardcoding them into source (as the
single-workspace banking POC does) would mean hand-editing JSON after every
provisioning run.
"""
from __future__ import annotations

import base64
import copy
import json
import re
import uuid
from typing import Any

PLACEHOLDER = re.compile(r"^\{\{([A-Z_]+)(?::([^{}]+))?\}\}$")


def _b64(value: str | dict[str, Any]) -> str:
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def logical_id(item_type: str, display_name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"fabric-medallion-multisourcev2-poc/{item_type}/{display_name}"))


def item_definition(item_type: str, display_name: str, part_path: str, content: str | dict[str, Any]) -> dict[str, Any]:
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": item_type, "displayName": display_name},
        "config": {"version": "2.0", "logicalId": logical_id(item_type, display_name)},
    }
    return {
        "parts": [
            {"path": part_path, "payload": _b64(content), "payloadType": "InlineBase64"},
            {"path": ".platform", "payload": _b64(platform), "payloadType": "InlineBase64"},
        ]
    }


def notebook_definition(display_name: str, content: str) -> dict[str, Any]:
    return item_definition("Notebook", display_name, "notebook-content.py", content)


def pipeline_definition(display_name: str, content: dict[str, Any]) -> dict[str, Any]:
    return item_definition("DataPipeline", display_name, "pipeline-content.json", content)


def bind_pipeline(
    template: dict[str, Any],
    workspace_id: str,
    notebook_ids: dict[str, str],
    item_ids: dict[str, str] | None = None,
    item_names: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Resolve symbolic IDs without modifying the checked-in template."""
    bound = copy.deepcopy(template)
    item_ids = item_ids or {}
    item_names = item_names or {}

    def visit(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: visit(child) for key, child in value.items()}
        if isinstance(value, list):
            return [visit(child) for child in value]
        if isinstance(value, str):
            match = PLACEHOLDER.match(value)
            if not match:
                return value
            kind, name = match.groups()
            if kind == "WORKSPACE_ID" and name is None:
                return workspace_id
            if kind == "NOTEBOOK_ID" and name in notebook_ids:
                return notebook_ids[name]
            if kind == "ITEM_ID" and name in item_ids:
                return item_ids[name]
            if kind == "ITEM_NAME" and name in item_names:
                return item_names[name]
            raise KeyError(f"Unresolved pipeline binding {value}")
        return value

    return visit(bound)
