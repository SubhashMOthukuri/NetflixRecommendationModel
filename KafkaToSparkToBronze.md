# Kafka → Spark → Bronze Pipeline Guide

**Complete guide to the Bronze Ingestion Pipeline - Production ML Data Pipeline**

This guide explains the complete Kafka → Spark Structured Streaming → Bronze Layer pipeline, including all components, their purposes, real-world problems solved, and interview preparation.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [Components Explained](#components-explained)
5. [Real-World Problems Solved](#real-world-problems-solved)
6. [Interview Q&A](#interview-qa)
7. [Component Usage Map](#component-usage-map)

---

## Overview

### What is This Pipeline?

The **Bronze Ingestion Pipeline** reads streaming data from Kafka, processes it through Spark Structured Streaming, validates and transforms it, and stores it in the Bronze layer (object store like S3/MinIO).

**Think of it like:** A factory assembly line that takes raw materials (Kafka events), cleans and inspects them (validation), packages them (transformation), and stores them in a warehouse (Bronze layer) for later use.

### Key Concepts

- **Kafka**: Message broker storing events
- **Spark Structured Streaming**: Real-time data processing engine
- **Bronze Layer**: Raw, validated data storage (first layer in Medallion Architecture)
- **Schema Enforcement**: Ensuring data matches expected structure
- **Data Quality**: Validating data meets business rules
- **PII Scrubbing**: Removing/masking sensitive information
- **Normalization**: Standardizing data formats
- **Deduplication**: Removing duplicate records
- **Checkpointing**: Enabling fault tolerance and recovery

---

## Architecture

```
┌─────────────┐
│   Kafka     │  ← Events from producers
│   Topic      │
└──────┬──────┘
       │
       │ (Spark Structured Streaming)
       ▼
┌─────────────────────────────────────┐
│     Spark Structured Streaming      │
│  ┌───────────────────────────────┐  │
│  │  1. Read from Kafka            │  │
│  │  2. Parse JSON                  │  │
│  │  3. Handle Malformed Data      │  │
│  │  4. Add Processing Metadata    │  │
│  │  5. Enforce Schema             │  │
│  │  6. Validate Data Quality      │  │
│  │  7. Scrub PII                  │  │
│  │  8. Normalize Data             │  │
│  │  9. Deduplicate                 │  │
│  │ 10. Write to Bronze            │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               │ (Parquet files)
               ▼
┌─────────────────────────────────────┐
│      Bronze Layer (MinIO/S3)         │
│  ┌───────────────────────────────┐  │
│  │  bronze/validated/             │  │
│  │    dt=2025-11-24/              │  │
│  │      hr=18/                    │  │
│  │        part-*.parquet          │  │
│  └───────────────────────────────┘  │
└─────────────────────────────────────┘
```

---

## Data Flow

### Step-by-Step Process

1. **Read from Kafka**
   - Spark reads events from Kafka topic
   - Uses Kafka connector for Spark Structured Streaming
   - Configurable batch size and offset management

2. **Parse JSON**
   - Converts Kafka binary messages to JSON
   - Extracts fields using schema
   - Handles parsing errors

3. **Handle Malformed Data**
   - Separates valid and invalid records
   - Sends invalid records to Dead Letter Queue (DLQ)
   - Enriches errors with context

4. **Add Processing Metadata**
   - Adds ingestion timestamp
   - Adds ingestion date/hour for partitioning
   - Adds source topic information

5. **Enforce Schema**
   - Validates data structure matches schema
   - Adds missing optional fields (as null)
   - Casts data to correct types

6. **Validate Data Quality**
   - Checks required fields are not null
   - Validates data ranges (e.g., timestamp > 0)
   - Validates formats (e.g., UUID-like event_id)
   - Adds validation status column

7. **Scrub PII**
   - Hashes sensitive fields (user_id, session_id)
   - Masks or removes PII data
   - Adds PII scrubbed flag

8. **Normalize Data**
   - Standardizes timestamps (to milliseconds, ISO format)
   - Normalizes string formats (trim, lowercase)
   - Handles null values consistently
   - Adds processing metadata

9. **Deduplicate**
   - Removes duplicate records by event_id
   - Keeps latest or earliest record
   - Tracks deduplication statistics

10. **Write to Bronze**
    - Writes Parquet files to object store
    - Partitions by date/hour
    - Uses Snappy compression
    - Creates checkpoints for recovery

---

## Components Explained

### Core Infrastructure

#### 1. `core/spark/spark_session.py`
**Purpose**: Creates and manages SparkSession with production settings

**What it does:**
- Loads configuration from YAML
- Sets up Spark with optimizations (AQE, Kryo serialization)
- Configures memory and resources
- Handles S3/MinIO connectivity

**Real-world problem**: Without this, every job would hardcode Spark settings, making it hard to change configurations and optimize performance.

**Example**:
```python
spark = get_spark_session(environment="prod")
# Spark is configured with production settings automatically
```

---

#### 2. `core/spark/context_manager.py`
**Purpose**: Manages Spark lifecycle and graceful shutdown

**What it does:**
- Registers shutdown handlers (SIGTERM, SIGINT)
- Stops streaming queries gracefully
- Cleans up resources
- Prevents data loss during shutdown

**Real-world problem**: In production (Kubernetes, Docker), jobs get killed. Without graceful shutdown, data can be lost and resources leaked.

**Example**:
```python
manager = SparkContextManager(spark)
query = spark.readStream...start()
manager.register_streaming_query(query)
# On shutdown, query stops gracefully
```

---

#### 3. `core/kafka/kafka_stream_reader.py`
**Purpose**: Reads streaming data from Kafka topics

**What it does:**
- Connects to Kafka brokers
- Subscribes to topics
- Configures consumer settings
- Creates Spark streaming DataFrame

**Real-world problem**: Centralizes Kafka reading logic, making it easy to change topics, offsets, or Kafka settings without modifying every job.

**Example**:
```python
reader = KafkaStreamReader(spark)
df = reader.read_stream("user_events")
# DataFrame streams from Kafka
```

---

#### 4. `core/kafka/offset_manager.py`
**Purpose**: Manages Kafka offsets for recovery

**What it does:**
- Reads offsets from checkpoints
- Determines starting offsets (earliest/latest/checkpoint)
- Validates offset format
- Enables job recovery after failures

**Real-world problem**: Without offset management, jobs restart from beginning (reprocessing all data) or miss data. This enables exactly-once processing.

**Example**:
```python
manager = KafkaOffsetManager(spark)
offset = manager.get_starting_offset("user_events")
# Returns checkpoint offset if exists, else "earliest"
```

---

### Schema Management

#### 5. `schemas/bronze_schema/schema_loader.py`
**Purpose**: Loads schemas from various sources

**What it does:**
- Loads schemas from code, files, or Schema Registry
- Converts Avro schemas to Spark StructType
- Caches schemas for performance
- Supports schema evolution

**Real-world problem**: Schemas change over time. This allows updating schemas without code changes, supporting schema evolution.

**Example**:
```python
schema = load_bronze_schema()
# Returns Spark StructType for bronze data
```

---

#### 6. `schemas/bronze_schema/schema_enforcer.py`
**Purpose**: Enforces schema rules on incoming data

**What it does:**
- Validates required fields exist
- Adds missing optional fields (as null)
- Casts data to correct types
- Drops extra fields (if configured)

**Real-world problem**: Raw data is messy. This ensures data matches the expected structure, preventing downstream errors.

**Example**:
```python
df = enforce_bronze_schema(df, strict=False, add_missing_fields=True)
# Data now matches schema exactly
```

---

#### 7. `schemas/bronze_schema/malformed_handler.py`
**Purpose**: Handles unparseable or invalid data

**What it does:**
- Splits valid and invalid records
- Enriches invalid records with error context
- Writes invalid records to DLQ (Dead Letter Queue)
- Preserves all data (even bad data)

**Real-world problem**: Bad data shouldn't crash the pipeline. This routes bad data to DLQ for investigation while processing good data.

**Example**:
```python
handler = MalformedDataHandler()
valid_df, invalid_df = handler.handle_parse_errors(df)
handler.write_to_dlq(invalid_df)
# Bad data goes to DLQ, good data continues
```

---

### Validation

#### 8. `validation/data_quality/bronze_validator.py`
**Purpose**: Validates data quality against business rules

**What it does:**
- Checks required fields are not null
- Validates data ranges (e.g., timestamp > 0)
- Validates formats (e.g., UUID-like event_id)
- Validates business rules (e.g., event_type in allowed list)
- Adds validation status and score

**Real-world problem**: Data quality issues cause ML model failures. This catches issues early and provides quality metrics.

**Example**:
```python
validator = BronzeValidator()
validated_df = validator.validate(df, add_validation_columns=True)
# Adds validation_status column
```

---

#### 9. `validation/pii_handling/pii_scrubber.py`
**Purpose**: Removes or masks Personally Identifiable Information

**What it does:**
- Hashes PII fields (SHA-256)
- Masks PII (partial visibility)
- Removes PII (set to null)
- Handles nested fields
- Adds PII scrubbed flag

**Real-world problem**: GDPR, CCPA require PII protection. This ensures compliance and reduces breach impact.

**Example**:
```python
scrubber = PIIScrubber(scrub_mode="hash")
df = scrubber.scrub_pii(df, mark_scrubbed=True)
# user_id and session_id are now hashed
```

---

### Transformation

#### 10. `transform/normalization/bronze_normalizer.py`
**Purpose**: Standardizes data format

**What it does:**
- Normalizes timestamps (to milliseconds, ISO 8601)
- Casts data to correct types
- Standardizes strings (trim, lowercase)
- Handles null values consistently
- Adds processing metadata

**Real-world problem**: Data comes in different formats. This standardizes everything, making downstream processing easier.

**Example**:
```python
normalizer = BronzeNormalizer()
df = normalizer.normalize(df, add_processing_metadata=True)
# All data is now standardized
```

---

#### 11. `transform/deduplication/deduplication.py`
**Purpose**: Removes duplicate records

**What it does:**
- Deduplicates by event_id
- Deduplicates by composite key
- Deduplicates within time window
- Keeps latest, earliest, or first occurrence
- Tracks statistics

**Real-world problem**: Duplicates cause inflated metrics and storage waste. This ensures one record per unique event.

**Example**:
```python
dedupe = Deduplication(strategy="event_id")
df = dedupe.remove_duplicates(df, keep="latest")
# Duplicates removed, latest kept
```

---

### Storage

#### 12. `storage/writers/bronze_writer.py`
**Purpose**: Writes processed data to object store

**What it does:**
- Writes Parquet files (compressed)
- Partitions by date/hour
- Handles streaming and batch writes
- Integrates with checkpointing
- Optimizes file sizes

**Real-world problem**: Efficient storage is critical for cost and performance. This writes optimized, partitioned files for fast queries.

**Example**:
```python
writer = BronzeWriter()
query = writer.write_stream(df, checkpoint_location="...", trigger_interval="30 seconds")
query.start()
# Data written to bronze layer
```

---

#### 13. `storage/checkpointing/checkpoint_manager.py`
**Purpose**: Manages Spark checkpoints for recovery

**What it does:**
- Generates checkpoint paths
- Validates checkpoint directories
- Cleans up old checkpoints
- Extracts query statistics
- Enables exactly-once processing

**Real-world problem**: Jobs fail. Checkpoints enable recovery from last successful state, preventing data loss and reprocessing.

**Example**:
```python
manager = CheckpointManager()
path = manager.get_checkpoint_path("user_events")
# Returns checkpoint path for recovery
```

---

#### 14. `storage/writers/manifest_writer.py`
**Purpose**: Writes manifest files for data discovery

**What it does:**
- Scans data directories
- Collects file metadata (path, size, record count)
- Writes manifest files (JSON, Parquet, text)
- Enables fast data discovery

**Real-world problem**: In large data lakes, listing files is slow. Manifests enable quick data discovery without expensive directory listings.

**Example**:
```python
writer = ManifestWriter()
writer.write_manifest("bronze/validated/", "manifest.json")
# Creates manifest for fast queries
```

---

### Observability

#### 15. `observability/metrics/bronze_metrics.py`
**Purpose**: Collects and tracks pipeline metrics

**What it does:**
- Tracks input/output records
- Tracks invalid records
- Tracks batch duration
- Tracks throughput
- Tracks validation success/failure rates

**Real-world problem**: You need visibility into pipeline health. This provides metrics for monitoring and alerting.

**Example**:
```python
metrics = BronzeMetrics()
metrics.record_input_records(1000)
metrics.record_output_records(950)
# Metrics tracked for monitoring
```

---

#### 16. `observability/alerting/alerting.py`
**Purpose**: Sends alerts based on thresholds

**What it does:**
- Monitors metrics (error rate, throughput, latency)
- Compares against thresholds
- Sends alerts (logs, email, Slack, PagerDuty)
- Supports different alert levels (INFO, WARNING, ERROR, CRITICAL)

**Real-world problem**: Issues need immediate attention. This alerts the team when problems occur.

**Example**:
```python
alerting = Alerting(enabled=True)
alerting.check_and_alert(metrics)
# Sends alert if thresholds exceeded
```

---

### Job Orchestration

#### 17. `jobs/streaming/bronze_stream_job.py`
**Purpose**: Main streaming job orchestrating the entire pipeline

**What it does:**
- Initializes all components
- Defines the 10-step processing flow
- Handles errors and retries
- Monitors query health
- Manages graceful shutdown

**Real-world problem**: This is the central orchestrator that combines all components into a working pipeline.

**Example**:
```python
job = BronzeStreamJob(topic="user_events", environment="prod")
job.run()
# Pipeline processes data end-to-end
```

---

#### 18. `jobs/launchers/job_launcher.py`
**Purpose**: Entry point for launching and managing jobs

**What it does:**
- Launches bronze stream job
- Handles auto-restart on failure
- Manages job lifecycle
- Provides command-line interface
- Handles graceful shutdown

**Real-world problem**: Jobs need to be launched and managed. This provides a production-ready entry point with retry logic.

**Example**:
```bash
python -m data_streaming.bronze_ingestion.jobs.launchers.job_launcher \
    --topic user_events \
    --environment prod
```

---

## Real-World Problems Solved

### 1. **Data Loss Prevention**
**Problem**: Jobs crash, data is lost
**Solution**: Checkpointing and offset management enable recovery from last successful state

### 2. **Data Quality Issues**
**Problem**: Bad data causes downstream failures
**Solution**: Validation and DLQ catch issues early, route bad data for investigation

### 3. **PII Compliance**
**Problem**: GDPR/CCPA requires PII protection
**Solution**: PII scrubbing removes/masks sensitive data before storage

### 4. **Schema Evolution**
**Problem**: Schemas change over time
**Solution**: Schema loader and enforcer support schema evolution without code changes

### 5. **Performance Optimization**
**Problem**: Slow queries and high costs
**Solution**: Partitioning, compression, and file optimization improve performance and reduce costs

### 6. **Monitoring and Alerting**
**Problem**: No visibility into pipeline health
**Solution**: Metrics and alerting provide real-time monitoring and proactive alerts

### 7. **Duplicate Data**
**Problem**: Duplicates cause inflated metrics
**Solution**: Deduplication ensures one record per unique event

### 8. **Data Format Inconsistency**
**Problem**: Data comes in different formats
**Solution**: Normalization standardizes all data formats

### 9. **Fault Tolerance**
**Problem**: Jobs fail and need manual restart
**Solution**: Auto-restart and graceful shutdown enable reliable operation

### 10. **Data Discovery**
**Problem**: Finding data in large data lakes is slow
**Solution**: Manifest files enable fast data discovery

---

## Interview Q&A

### Q1: What is the Bronze Layer?
**A**: The Bronze layer is the first layer in the Medallion Architecture (Bronze → Silver → Gold). It stores raw, validated data as it arrives from sources. Data is stored as-is with minimal transformation, preserving original data for debugging and reprocessing.

### Q2: Why use Spark Structured Streaming instead of batch processing?
**A**: Structured Streaming provides:
- **Low latency**: Processes data in near real-time (seconds, not hours)
- **Fault tolerance**: Automatic recovery from failures
- **Exactly-once semantics**: No duplicate or lost data
- **Unified API**: Same code for batch and streaming

### Q3: What is checkpointing and why is it important?
**A**: Checkpointing saves the state of a streaming query (offsets, metadata) to durable storage. It enables:
- **Recovery**: Job can resume from last successful state
- **Exactly-once processing**: Prevents duplicate processing
- **Fault tolerance**: Survives job failures

### Q4: How do you handle schema evolution?
**A**: 
1. **Schema Registry**: Centralized schema management
2. **Schema Loader**: Loads schemas dynamically
3. **Schema Enforcer**: Adds missing optional fields, handles type changes
4. **Backward compatibility**: New fields are optional, old fields remain

### Q5: What is a Dead Letter Queue (DLQ)?
**A**: DLQ stores records that fail processing (malformed, invalid, etc.). It:
- Prevents data loss
- Enables investigation of failures
- Allows reprocessing after fixes
- Provides audit trail

### Q6: How do you ensure data quality?
**A**: Multiple layers:
1. **Schema validation**: Structure matches schema
2. **Data quality checks**: Required fields, ranges, formats
3. **Business rules**: Event types, value constraints
4. **Validation status**: Tracks quality per record

### Q7: Why partition data by date/hour?
**A**: Partitioning improves:
- **Query performance**: Only scan relevant partitions
- **Cost**: Store only needed data
- **Maintenance**: Easy to delete old data
- **Parallelism**: Process partitions in parallel

### Q8: What is PII scrubbing and why is it needed?
**A**: PII scrubbing removes/masks Personally Identifiable Information (user_id, email, IP address). It's needed for:
- **GDPR/CCPA compliance**: Legal requirement
- **Data security**: Reduces breach impact
- **Privacy**: Protects user data

### Q9: How do you handle duplicates in streaming?
**A**: Multiple strategies:
1. **Event ID**: Deduplicate by unique event_id
2. **Composite key**: event_id + timestamp
3. **Time window**: Deduplicate within time window
4. **Stateful processing**: Track seen events in state store

### Q10: What is the difference between Bronze, Silver, and Gold layers?
**A**:
- **Bronze**: Raw, validated data (this pipeline)
- **Silver**: Cleaned, normalized, enriched data
- **Gold**: Feature-engineered data for ML models

---

## Component Usage Map

### How Components Work Together

```
Job Launcher
    ↓
Bronze Stream Job
    ↓
┌─────────────────────────────────────┐
│ 1. Kafka Stream Reader               │ ← Reads from Kafka
│ 2. Parse JSON                        │ ← Extracts fields
│ 3. Malformed Handler                 │ ← Routes bad data to DLQ
│ 4. Add Metadata                      │ ← Adds ingestion info
│ 5. Schema Enforcer                   │ ← Validates structure
│ 6. Bronze Validator                  │ ← Validates quality
│ 7. PII Scrubber                      │ ← Removes PII
│ 8. Bronze Normalizer                 │ ← Standardizes format
│ 9. Deduplication                     │ ← Removes duplicates
│ 10. Bronze Writer                    │ ← Writes to storage
└─────────────────────────────────────┘
    ↓
Checkpoint Manager                     ← Manages recovery
    ↓
Bronze Layer (MinIO/S3)                ← Stores data
    ↓
Manifest Writer                        ← Creates manifests
    ↓
Metrics & Alerting                     ← Monitors health
```

### Configuration Files

- `config/spark_config.yaml`: Spark settings, streaming config, Kafka config
- `config/kafka.yaml`: Kafka producer/consumer settings

### Key Dependencies

- `pyspark`: Spark Structured Streaming
- `confluent-kafka`: Kafka connectivity
- `boto3`: S3/MinIO connectivity
- `pyyaml`: Configuration loading

---

## Summary

The **Kafka → Spark → Bronze Pipeline** is a production-grade data ingestion system that:

✅ **Reads** streaming data from Kafka  
✅ **Validates** data quality and schema  
✅ **Transforms** data (normalize, scrub PII, deduplicate)  
✅ **Stores** data in Bronze layer (partitioned, compressed)  
✅ **Monitors** pipeline health and alerts on issues  
✅ **Recovers** from failures using checkpoints  

This pipeline follows **Netflix/Uber standards** and solves real-world problems in production ML systems.

---

**Next Steps**: After Bronze, data moves to Silver layer for cleaning and enrichment, then to Gold layer for feature engineering and ML model training.

