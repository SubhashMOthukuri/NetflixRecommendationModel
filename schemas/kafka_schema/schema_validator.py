"""
Schema Validator - ensures producer payloads conform to data contracts before Kafka.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from libs.logger import get_logger
from libs.exceptions import SchemaValidationError
from schemas.data_contracts import get_user_events_schema

class SchemaValidator:
    """
    Lightweight validator that checks producer payloads against our Avro contracts.
    Uses simple recursive checks; swap in fastavro or jsonschema for full validation.
    """

    def __init__(self, logger=None) -> None:
        self.logger = logger or get_logger(__name__)
        self.schema = get_user_events_schema()

    def validate_user_event(self, payload: Dict[str, Any]) -> None:
        """
        Validate a user event payload. Raises SchemaValidationError on failure.
        """
        missing = self._check_required_fields(self.schema, payload)
        if missing:
            raise SchemaValidationError(
                "User event missing required fields",
                context={"missing_fields": missing},
            )

    # ------------------------ Helpers ------------------------

    def _check_required_fields(self, schema: Dict[str, Any], payload: Dict[str, Any]) -> list:
        """
        Check required fields for an Avro record schema.
        Returns list of missing field names.
        """
        missing = []
        for field in schema.get("fields", []):
            name = field["name"]
            field_type = field["type"]
            optional = self._is_optional(field_type)

            if name not in payload:
                if not optional:
                    missing.append(name)
                continue

            value = payload[name]

            # Recurse nested records
            if self._is_record(field_type) and isinstance(value, dict):
                nested_schema = self._get_record_schema(field_type)
                missing.extend(
                    [f"{name}.{sub}" for sub in self._check_required_fields(nested_schema, value)]
                )
        return missing

    def _is_optional(self, field_type: Any) -> bool:
        if isinstance(field_type, list):
            return "null" in field_type
        if isinstance(field_type, dict) and field_type.get("type") == "null":
            return True
        return False

    def _is_record(self, field_type: Any) -> bool:
        if isinstance(field_type, dict):
            return field_type.get("type") == "record"
        if isinstance(field_type, list):
            return any(isinstance(t, dict) and t.get("type") == "record" for t in field_type)
        return False

    def _get_record_schema(self, field_type: Any) -> Dict[str, Any]:
        if isinstance(field_type, dict) and field_type.get("type") == "record":
            return field_type
        if isinstance(field_type, list):
            for t in field_type:
                if isinstance(t, dict) and t.get("type") == "record":
                    return t
        raise SchemaValidationError("Invalid record schema", context={"field_type": str(field_type)})

def get_schema_validator() -> SchemaValidator:
    return SchemaValidator()

