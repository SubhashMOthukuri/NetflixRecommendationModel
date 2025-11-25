"""
Data Contracts - Authoritative Avro schema definitions for the producer layer.
Defines the user_events schema used by the recommendation producer.
"""
from __future__ import annotations

from typing import Dict, Any

# Avro schema for video recommendation events (Netflix-style)
USER_EVENTS_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.netflix.recommendations",
    "doc": "User interaction events used by the recommendation system.",
    "fields": [
        {"name": "event_id", "type": "string", "doc": "UUID for this event"},
        {"name": "event_type", "type": "string", "doc": "Type of event (video_play_start, etc.)"},
        {"name": "timestamp", "type": "long", "doc": "Epoch millis"},
        {"name": "timestamp_iso", "type": "string", "doc": "ISO8601 timestamp"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": ["null", "string"], "default": None},
        {
            "name": "device",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "DeviceInfo",
                    "fields": [
                        {"name": "type", "type": ["null", "string"], "default": None},
                        {"name": "os", "type": ["null", "string"], "default": None},
                        {"name": "browser", "type": ["null", "string"], "default": None},
                        {"name": "device_id", "type": ["null", "string"], "default": None},
                        {"name": "screen_resolution", "type": ["null", "string"], "default": None},
                    ],
                },
            ],
            "default": None,
        },
        {
            "name": "content",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "ContentInfo",
                    "fields": [
                        {"name": "content_id", "type": "string"},
                        {"name": "title", "type": ["null", "string"], "default": None},
                        {"name": "content_type", "type": "string"},
                        {"name": "season", "type": ["null", "int"], "default": None},
                        {"name": "episode", "type": ["null", "int"], "default": None},
                        {"name": "duration_seconds", "type": "int"},
                        {"name": "genre", "type": ["null", "string"], "default": None},
                        {"name": "release_year", "type": ["null", "int"], "default": None},
                    ],
                },
            ],
            "default": None,
        },
        {
            "name": "context",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "ContextInfo",
                    "fields": [
                        {"name": "page", "type": ["null", "string"], "default": None},
                        {"name": "referrer", "type": ["null", "string"], "default": None},
                        {"name": "position", "type": ["null", "int"], "default": None},
                        {"name": "autoplay", "type": ["null", "boolean"], "default": None},
                        {"name": "previous_content", "type": ["null", "string"], "default": None},
                    ],
                },
            ],
            "default": None,
        },
        {
            "name": "metadata",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Metadata",
                    "fields": [
                        {"name": "ip_address", "type": ["null", "string"], "default": None},
                        {"name": "user_agent", "type": ["null", "string"], "default": None},
                        {"name": "country", "type": ["null", "string"], "default": None},
                        {"name": "timezone", "type": ["null", "string"], "default": None},
                        {"name": "language", "type": ["null", "string"], "default": None},
                        {"name": "app_version", "type": ["null", "string"], "default": None},
                    ],
                },
            ],
            "default": None,
        },
    ],
}


def get_user_events_schema() -> Dict[str, Any]:
    """
    Return the Avro schema for user events. Keeps a single source of truth.
    """
    return USER_EVENTS_SCHEMA

