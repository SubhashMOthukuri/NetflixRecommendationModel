"""
Bronze Data Contract - Production ML Pipeline
Defines the authoritative schema for Bronze layer data
Bronze layer = Raw data with minimal processing (validated, normalized, deduplicated)
"""
from typing import Dict, Any

# Bronze Layer Schema Definition
# This defines what data looks like AFTER ingestion into bronze layer
# Data is: validated, PII-scrubbed, normalized, deduplicated

BRONZE_SCHEMA = {
    "type": "record",
    "name": "BronzeUserEvent",
    "namespace": "com.netflix.bronze",
    "doc": "Bronze layer schema for user events - validated and normalized",
    "fields": [
        # Event identification
        {
            "name": "event_id",
            "type": "string",
            "doc": "Unique event identifier (UUID)"
        },
        {
            "name": "event_type",
            "type": "string",
            "doc": "Type of event (e.g., video_play_start, post_like, add_to_cart)"
        },
        {
            "name": "timestamp_ms",
            "type": "long",
            "doc": "Event timestamp in milliseconds (Unix epoch)"
        },
        {
            "name": "timestamp_iso",
            "type": ["null", "string"],
            "default": None,
            "doc": "ISO 8601 timestamp for human readability"
        },
        
        # User information (PII may be scrubbed)
        {
            "name": "user_id",
            "type": ["null", "string"],
            "default": None,
            "doc": "User identifier (may be hashed/scrubbed for PII)"
        },
        {
            "name": "session_id",
            "type": ["null", "string"],
            "default": None,
            "doc": "Session identifier"
        },
        
        # Event data (normalized structure)
        {
            "name": "event_data",
            "type": {
                "type": "record",
                "name": "EventData",
                "fields": [
                    {
                        "name": "action",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Action performed (e.g., play, pause, like)"
                    },
                    {
                        "name": "target_id",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Target entity ID (e.g., video_id, post_id)"
                    },
                    {
                        "name": "target_type",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Type of target (e.g., video, post, product)"
                    },
                    {
                        "name": "metadata",
                        "type": {
                            "type": "map",
                            "values": "string"
                        },
                        "default": {},
                        "doc": "Additional event metadata (key-value pairs)"
                    }
                ]
            },
            "doc": "Normalized event data structure"
        },
        
        # Video-specific fields (for video recommendation system)
        {
            "name": "video_data",
            "type": ["null", {
                "type": "record",
                "name": "VideoData",
                "fields": [
                    {
                        "name": "video_id",
                        "type": ["null", "string"],
                        "default": None
                    },
                    {
                        "name": "title",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Video title (may be scrubbed for PII)"
                    },
                    {
                        "name": "duration_seconds",
                        "type": ["null", "long"],
                        "default": None
                    },
                    {
                        "name": "play_position_seconds",
                        "type": ["null", "long"],
                        "default": None,
                        "doc": "Playback position in seconds"
                    },
                    {
                        "name": "quality",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Video quality (e.g., 1080p, 4K)"
                    }
                ]
            }],
            "default": None,
            "doc": "Video-specific data (if event is video-related)"
        },
        
        # Device/Platform information
        {
            "name": "device_info",
            "type": ["null", {
                "type": "record",
                "name": "DeviceInfo",
                "fields": [
                    {
                        "name": "device_type",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Device type (e.g., mobile, desktop, tv)"
                    },
                    {
                        "name": "os",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Operating system"
                    },
                    {
                        "name": "browser",
                        "type": ["null", "string"],
                        "default": None
                    },
                    {
                        "name": "ip_address",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "IP address (may be anonymized)"
                    }
                ]
            }],
            "default": None,
            "doc": "Device/platform information"
        },
        
        # Processing metadata (added during bronze ingestion)
        {
            "name": "processing_metadata",
            "type": {
                "type": "record",
                "name": "ProcessingMetadata",
                "fields": [
                    {
                        "name": "ingestion_timestamp_ms",
                        "type": "long",
                        "doc": "When data was ingested into bronze (milliseconds)"
                    },
                    {
                        "name": "ingestion_date",
                        "type": "string",
                        "doc": "Ingestion date (YYYY-MM-DD) for partitioning"
                    },
                    {
                        "name": "ingestion_hour",
                        "type": "int",
                        "doc": "Ingestion hour (0-23) for partitioning"
                    },
                    {
                        "name": "source_topic",
                        "type": "string",
                        "doc": "Source Kafka topic name"
                    },
                    {
                        "name": "source_partition",
                        "type": "int",
                        "doc": "Source Kafka partition"
                    },
                    {
                        "name": "source_offset",
                        "type": "long",
                        "doc": "Source Kafka offset"
                    },
                    {
                        "name": "validation_status",
                        "type": "string",
                        "doc": "Validation status (valid, invalid, warning)"
                    },
                    {
                        "name": "pii_scrubbed",
                        "type": "boolean",
                        "default": False,
                        "doc": "Whether PII was scrubbed from this record"
                    },
                    {
                        "name": "deduplication_key",
                        "type": ["null", "string"],
                        "default": None,
                        "doc": "Key used for deduplication"
                    }
                ]
            },
            "doc": "Metadata added during bronze ingestion"
        }
    ]
}

# Spark StructType schema (for Spark DataFrame operations)
# This is the Spark equivalent of the Avro schema above
BRONZE_SPARK_SCHEMA = """
    event_id STRING,
    event_type STRING,
    timestamp_ms BIGINT,
    timestamp_iso STRING,
    user_id STRING,
    session_id STRING,
    event_data STRUCT<
        action: STRING,
        target_id: STRING,
        target_type: STRING,
        metadata: MAP<STRING, STRING>
    >,
    video_data STRUCT<
        video_id: STRING,
        title: STRING,
        duration_seconds: BIGINT,
        play_position_seconds: BIGINT,
        quality: STRING
    >,
    device_info STRUCT<
        device_type: STRING,
        os: STRING,
        browser: STRING,
        ip_address: STRING
    >,
    processing_metadata STRUCT<
        ingestion_timestamp_ms: BIGINT,
        ingestion_date: STRING,
        ingestion_hour: INT,
        source_topic: STRING,
        source_partition: INT,
        source_offset: BIGINT,
        validation_status: STRING,
        pii_scrubbed: BOOLEAN,
        deduplication_key: STRING
    >
"""

# Partition columns (for partitioning bronze data)
BRONZE_PARTITION_COLUMNS = [
    "processing_metadata.ingestion_date",
    "processing_metadata.ingestion_hour"
]

# Required fields (must be present in bronze data)
BRONZE_REQUIRED_FIELDS = [
    "event_id",
    "event_type",
    "timestamp_ms",
    "processing_metadata"
]

# Optional fields (may be null)
BRONZE_OPTIONAL_FIELDS = [
    "timestamp_iso",
    "user_id",
    "session_id",
    "video_data",
    "device_info"
]


def get_bronze_schema() -> Dict[str, Any]:
    """
    Get bronze schema definition
    
    Returns:
        Avro schema dictionary
    """
    return BRONZE_SCHEMA


def get_bronze_spark_schema() -> str:
    """
    Get bronze schema as Spark SQL DDL string
    
    Returns:
        Spark SQL schema string
    """
    return BRONZE_SPARK_SCHEMA


def get_partition_columns() -> list:
    """
    Get partition columns for bronze data
    
    Returns:
        List of partition column paths
    """
    return BRONZE_PARTITION_COLUMNS


def get_required_fields() -> list:
    """
    Get required fields for bronze data
    
    Returns:
        List of required field names
    """
    return BRONZE_REQUIRED_FIELDS


def get_optional_fields() -> list:
    """
    Get optional fields for bronze data
    
    Returns:
        List of optional field names
    """
    return BRONZE_OPTIONAL_FIELDS

