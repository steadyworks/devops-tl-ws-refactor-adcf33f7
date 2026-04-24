# type: ignore

import copy
import inspect
from enum import Enum
from typing import Any, Iterable, cast

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel, TypeAdapter

import backend.lib.websocket.types as ws_types


# --- JSON-schema utils ---------------------------------------------------------
def _ensure_components_schemas(doc: dict[str, Any]) -> dict[str, Any]:
    doc.setdefault("components", {}).setdefault("schemas", {})
    return doc["components"]["schemas"]


def _schema_of(obj: type[Any]) -> dict[str, Any]:
    """Return a JSON Schema for a Pydantic BaseModel or Enum with stable refs."""
    try:
        # BaseModel subclass
        return obj.model_json_schema(ref_template="#/components/schemas/{model}")
    except AttributeError:
        # Enum or other pydantic-compatible type
        return TypeAdapter(obj).json_schema(ref_template="#/components/schemas/{model}")


def _walk_replace_refs(node: Any) -> None:
    """Rewrite any $ref that points to '#/$defs/...' → '#/components/schemas/...'.
    Operates in-place."""
    if isinstance(node, dict):
        if (
            "$ref" in node
            and isinstance(node["$ref"], str)
            and node["$ref"].startswith("#/$defs/")
        ):
            tail = node["$ref"].split("#/$defs/", 1)[1]
            node["$ref"] = f"#/components/schemas/{tail}"
        for v in node.values():
            _walk_replace_refs(v)
    elif isinstance(node, list):
        for v in node:
            _walk_replace_refs(v)


def _hoist_defs(
    schema: dict[str, Any], components_schemas: dict[str, Any]
) -> dict[str, Any]:
    """Move local $defs into components.schemas and fix $ref paths. Returns cleaned schema."""
    schema = copy.deepcopy(schema)
    local_defs = schema.pop("$defs", None)
    if local_defs:
        # Add any missing defs to global components
        for name, def_schema in local_defs.items():
            if name not in components_schemas:
                # Also rewrite nested refs inside the def itself
                _walk_replace_refs(def_schema)
                components_schemas[name] = def_schema
        # Fix refs inside the parent schema
        _walk_replace_refs(schema)
    return schema


# --- Auto-collect your WS types ------------------------------------------------


def _collect_ws_types() -> Iterable[type[Any]]:
    for _, obj in inspect.getmembers(ws_types):
        if inspect.isclass(obj):
            # Include Pydantic models
            if issubclass(obj, BaseModel) and obj is not BaseModel:
                yield obj
            # Include Enums
            elif issubclass(obj, Enum) and obj is not Enum:
                yield obj


def _rewrite_nullable(schema: Any) -> None:
    """Recursively rewrite anyOf [X, null] → type: [X, "null"],
    and rewrite $ref cases to allOf + nullable=True.
    """
    if isinstance(schema, dict):
        if "anyOf" in schema:
            any_of = cast("Any", schema["anyOf"])

            if isinstance(any_of, list) and any(
                isinstance(fragment, dict) and fragment.get("type") == "null"
                for fragment in any_of
            ):
                non_null = [
                    fragment
                    for fragment in any_of
                    if not (
                        isinstance(fragment, dict) and fragment.get("type") == "null"
                    )
                ]
                if len(non_null) == 1 and isinstance(non_null[0], dict):
                    base = dict(non_null[0])  # shallow copy

                    if "$ref" in base:
                        # ✅ spec-compliant form: allOf + nullable
                        replacement: dict[str, Any] = {
                            "allOf": [{"$ref": base["$ref"]}],
                            "nullable": True,
                        }
                    else:
                        replacement = base
                        old_type = replacement.get("type")
                        if old_type is not None:
                            if isinstance(old_type, list):
                                if "null" not in old_type:
                                    replacement["type"] = old_type + ["null"]
                            else:
                                replacement["type"] = [old_type, "null"]

                    # merge metadata (title, description, etc.)
                    for key, value in schema.items():
                        if key != "anyOf":
                            replacement.setdefault(key, value)

                    schema.clear()
                    schema.update(replacement)

        # recurse
        for value in list(schema.values()):
            _rewrite_nullable(value)

    elif isinstance(schema, list):
        for fragment in schema:
            _rewrite_nullable(fragment)


def custom_openapi(app: FastAPI) -> dict[str, Any]:
    """Generate OpenAPI schema but rewrite nullables for legacy generators."""
    if getattr(app, "openapi_schema", None):
        return app.openapi_schema  # type: ignore[return-value]

    openapi_schema: dict[str, Any] = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    components_schemas = _ensure_components_schemas(openapi_schema)
    for typ in _collect_ws_types():
        name = typ.__name__
        if name in components_schemas:
            continue
        raw_schema = _schema_of(typ)
        cleaned = _hoist_defs(raw_schema, components_schemas)
        components_schemas[name] = cleaned

    _rewrite_nullable(openapi_schema)
    app.openapi_schema = openapi_schema  # type: ignore[attr-defined]
    return openapi_schema
