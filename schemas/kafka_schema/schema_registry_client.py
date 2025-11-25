"""
Schema Registry Client - Production-ready wrapper around Confluent Schema Registry
Used by producers to validate events before sending them to Kafka.
"""
from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

import requests
from requests import Response

from libs.config_loader import get_kafka_config
from libs.logger import get_logger
from libs.exceptions import (
    PipelineException,
    SchemaValidationError,
)


class SchemaRegistryClient:
    """
    Minimal Schema Registry client that mirrors Netflix-style production usage:
    - Retrieves schemas by subject/version
    - Registers new schemas
    - Provides lightweight payload validation (required field checks)
    - Caches schemas locally to reduce round-trips
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        environment: Optional[str] = None,
        logger=None,
    ) -> None:
        self.logger = logger or get_logger(__name__)
        self.config = config or get_kafka_config(environment)

        schema_cfg = self.config.get("schema_registry", {})
        self.base_url: str = schema_cfg.get("url", "")
        if not self.base_url:
            raise PipelineException(
                "Schema registry URL missing from kafka.yaml -> schema_registry.url"
            )

        self.default_subject: str = schema_cfg.get("subject_name", "")
        self.compatibility: str = schema_cfg.get("compatibility", "BACKWARD")

        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/vnd.schemaregistry.v1+json"})

        # Simple in-memory cache {subject: {"schema": dict, "id": int, "version": int}}
        self._schema_cache: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
        """
        Register a new schema version. Returns schema ID assigned by registry.
        """
        subject = subject or self.default_subject
        payload = {"schema": json.dumps(schema)}

        response = self._request(
            "POST",
            f"/subjects/{subject}/versions",
            json=payload,
        )
        schema_id = response.get("id")

        # Cache latest schema
        self._schema_cache[subject] = {
            "schema": schema,
            "id": schema_id,
            "version": response.get("version"),
        }

        self.logger.info(
            "Schema registered",
            extra={"subject": subject, "schema_id": schema_id},
        )
        return schema_id

    def get_latest_schema(self, subject: Optional[str] = None) -> Tuple[int, Dict[str, Any], int]:
        """
        Fetch latest schema for a subject. Returns (schema_id, schema_dict, version).
        """
        subject = subject or self.default_subject

        if subject in self._schema_cache:
            cached = self._schema_cache[subject]
            return cached["id"], cached["schema"], cached["version"]

        response = self._request("GET", f"/subjects/{subject}/versions/latest")
        schema_dict = json.loads(response.get("schema", "{}"))
        schema_id = response.get("id")
        version = response.get("version")

        self._schema_cache[subject] = {
            "schema": schema_dict,
            "id": schema_id,
            "version": version,
        }
        return schema_id, schema_dict, version

    def get_schema_by_version(self, subject: str, version: int) -> Dict[str, Any]:
        """
        Fetch schema for a subject and version.
        """
        response = self._request("GET", f"/subjects/{subject}/versions/{version}")
        return json.loads(response.get("schema", "{}"))

    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """
        Fetch schema by ID (useful when Kafka message contains only schema id).
        """
        response = self._request("GET", f"/schemas/ids/{schema_id}")
        return json.loads(response.get("schema", "{}"))

    def validate_payload(
        self,
        payload: Dict[str, Any],
        subject: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Lightweight validation to ensure required fields exist before producing.
        NOTE: This is a simplified check. Full validation should use libraries
        such as fastavro or jsonschema depending on schema type.
        """
        if schema is None:
            _, schema, _ = self.get_latest_schema(subject)

        if schema.get("type") != "record":
            # Only simple record enforcement supported here
            return

        missing_fields = []
        for field in schema.get("fields", []):
            field_name = field.get("name")
            field_type = field.get("type")

            # Determine if field is optional (nullable)
            optional = False
            if isinstance(field_type, list):
                optional = "null" in field_type
            elif isinstance(field_type, dict) and field_type.get("type") == "null":
                optional = True

            if not optional and field_name not in payload:
                missing_fields.append(field_name)

        if missing_fields:
            raise SchemaValidationError(
                "Payload missing required fields",
                context={"missing_fields": missing_fields},
            )

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """
        Internal helper to call Schema Registry REST API and handle errors uniformly.
        """
        url = f"{self.base_url.rstrip('/')}{path}"
        try:
            response: Response = self.session.request(method, url, timeout=10, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as err:
            message = err.response.text if err.response else str(err)
            self.logger.error(
                "Schema registry HTTP error",
                extra={"url": url, "status_code": err.response.status_code if err.response else None},
            )
            raise PipelineException(f"Schema registry error: {message}") from err
        except requests.RequestException as err:
            self.logger.error("Schema registry request failed", extra={"url": url})
            raise PipelineException(f"Failed to reach schema registry: {err}") from err


# Convenience factory for callers
def get_schema_registry_client(environment: Optional[str] = None) -> SchemaRegistryClient:
    """
    Helper to instantiate SchemaRegistryClient with given environment.
    """
    return SchemaRegistryClient(environment=environment)

