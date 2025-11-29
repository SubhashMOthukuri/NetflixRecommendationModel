# Production ML Data Pipeline - Complete Guide

**End-to-end data pipeline from Kafka → Bronze → Silver → Gold (Feature Store)**

This repository contains a production-grade ML data pipeline following Netflix/Uber standards, implementing the Medallion Architecture (Bronze/Silver/Gold) with comprehensive feature engineering, validation, monitoring, and feature store integration.

---

## 🏗️ Architecture Overview

```
┌─────────────┐
│   Kafka     │  ← Events from producers
│   Topic      │
└──────┬──────┘
       │
       │ (Spark Structured Streaming)
       ▼
┌─────────────────────────────────────┐
│      BRONZE LAYER                   │
│  Raw, validated, PII-scrubbed data  │
│  Partitioned by: dt, hr             │
└──────┬──────────────────────────────┘
       │
       │ (Batch Processing)
       ▼
┌─────────────────────────────────────┐
│      SILVER LAYER                   │
│  Cleaned, enriched, normalized data │
│  Partitioned by: event_date, event_type │
└──────┬──────────────────────────────┘
       │
       │ (Feature Engineering)
       ▼
┌─────────────────────────────────────┐
│      GOLD LAYER                     │
│  ML-ready features                  │
│  Partitioned by: feature_date, feature_type │
└──────┬──────────────────────────────┘
       │
       │ (Feast Feature Store)
       ▼
┌─────────────────────────────────────┐
│      FEAST                          │
│  Offline Store (Parquet/S3)         │
│  Online Store (Redis)                │
└─────────────────────────────────────┘
```

---

## 📁 Project Structure

```
.
├── config/
│   ├── spark_config.yaml          # Spark and storage configuration
│   ├── kafka.yaml                 # Kafka configuration
│   └── feast_config.yaml          # Feast feature store configuration
│
├── data_streaming/
│   ├── bronze_ingestion/          # Bronze layer processing
│   │   ├── core/                 # Core infrastructure (Spark, Kafka)
│   │   ├── transform/            # Transformations (normalization, deduplication)
│   │   ├── validation/           # Validation (data quality, PII scrubbing)
│   │   ├── storage/              # Storage (writers, checkpointing)
│   │   ├── observability/        # Monitoring (metrics, alerting)
│   │   └── jobs/                 # Job orchestration
│   │
│   ├── silver_processing/        # Silver layer processing
│   │   ├── core/                 # Core (bronze reader)
│   │   ├── transform/             # Transformations (flatten, normalize, type cast)
│   │   ├── enrichment/           # Enrichment (joins with lookup tables)
│   │   ├── validation/           # Validation (quality, schema enforcement)
│   │   ├── storage/              # Storage (silver writer, partition tracker)
│   │   ├── observability/        # Monitoring (metrics, alerting)
│   │   ├── error_handling/       # Error handling (DLQ)
│   │   └── jobs/                 # Job orchestration
│   │
│   └── gold_processing/          # Gold layer processing
│       ├── core/                 # Core (silver reader, Feast client)
│       ├── feature_engineering/ # Feature engineering (user, item, session, etc.)
│       ├── validation/           # Validation (quality checker, schema registry)
│       ├── storage/              # Storage (gold writer)
│       ├── monitoring/           # Monitoring (metrics, feature monitoring)
│       └── jobs/                 # Job orchestration (feature job, backfill)
│
├── schemas/                       # Schema definitions
│   ├── bronze_schema/            # Bronze layer schemas
│   ├── silver_schema/            # Silver layer schemas
│   └── kafka_schema/             # Kafka event schemas
│
├── libs/                          # Shared libraries
│   ├── logger.py                 # Logging utilities
│   ├── exceptions.py             # Custom exceptions
│   └── config_loader.py          # Configuration loader
│
├── services/                      # Producer services
│   ├── producer_service.py       # Event producer service
│   └── producer_main.py          # Producer entry point
│
├── scripts/                       # Utility scripts
│   └── create_bucket_aws.py     # S3 bucket creation
│
└── infra/                         # Infrastructure
    └── docker-compose.yaml       # Docker services (Kafka, MinIO, etc.)
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Java 11+ (for Spark)
- Docker (for Kafka, MinIO)
- Spark 3.5.0

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Start infrastructure (Kafka, MinIO)
docker-compose -f infra/docker-compose.yaml up -d

# Create S3 buckets (MinIO)
python scripts/create_bucket_aws.py
```

### Run Pipeline

```bash
# 1. Start producer (generates events to Kafka)
python services/producer_main.py

# 2. Run bronze ingestion (Kafka → Bronze)
python -m data_streaming.bronze_ingestion.jobs.launchers.job_launcher \
    --topic user_events \
    --environment dev

# 3. Run silver processing (Bronze → Silver)
python -m data_streaming.silver_processing.jobs.launchers.silver_job_launcher \
    --start-date 2024-01-15 \
    --end-date 2024-01-16 \
    --environment dev

# 4. Run gold feature engineering (Silver → Gold)
python -m data_streaming.gold_processing.jobs.launchers.gold_job_launcher \
    feature \
    --start-date 2024-01-15 \
    --end-date 2024-01-16 \
    --environment dev
```

---

## 📚 Documentation

### Complete Guides

- **[PRODUCER_TO_KAFKA_GUIDE.md](PRODUCER_TO_KAFKA_GUIDE.md)** - Producer to Kafka pipeline guide
- **[KafkaToSparkToBronze.md](KafkaToSparkToBronze.md)** - Kafka → Spark → Bronze pipeline guide
- **[BRONZE_TO_SILVER_GUIDE.md](BRONZE_TO_SILVER_GUIDE.md)** - Bronze → Silver pipeline guide
- **[SILVER_TO_GOLD_GUIDE.md](SILVER_TO_GOLD_GUIDE.md)** - Silver → Gold pipeline guide
- **[ADVANCED_TOPICS.md](ADVANCED_TOPICS.md)** - Advanced topics and real-time processing

### Cloud Architecture Guides

- **[AZURE_ARCHITECTURE.md](AZURE_ARCHITECTURE.md)** - Production Azure architecture (Event Hub, Databricks, Data Lake Gen2, Purview, Azure ML)
- **[AWS_ARCHITECTURE.md](AWS_ARCHITECTURE.md)** - Production AWS architecture (Kinesis, EMR, S3, Lake Formation, SageMaker)

### Interview Preparation

- **[INTERVIEW_QA.md](INTERVIEW_QA.md)** - 40+ interview questions with answers

---

## 🎯 Pipeline Layers

### **Bronze Layer**
- **Purpose:** Raw, validated data storage
- **Processing:** Kafka → Spark Streaming → Bronze (Parquet)
- **Features:** Schema validation, PII scrubbing, normalization, deduplication
- **Storage:** `s3a://data-lake/bronze/validated/` (partitioned by dt, hr)

### **Silver Layer**
- **Purpose:** Cleaned, enriched, business-ready data
- **Processing:** Bronze → Spark Batch → Silver (Parquet)
- **Features:** Flattening, type casting, normalization, enrichment, validation
- **Storage:** `s3a://data-lake/silver/silver/` (partitioned by event_date, event_type)

### **Gold Layer**
- **Purpose:** ML-ready features for model training and serving
- **Processing:** Silver → Spark Batch → Gold (Parquet) + Feast
- **Features:** User features, item features, session features, statistical, temporal, embeddings
- **Storage:** 
  - Gold: `s3a://data-lake/gold/features/` (partitioned by feature_date, feature_type)
  - Feast: Offline store (Parquet/S3), Online store (Redis)

---

## 🔧 Key Components

### Bronze Layer
- **Kafka Stream Reader** - Reads from Kafka topics
- **Schema Enforcer** - Enforces bronze schema
- **PII Scrubber** - Removes/masks PII data
- **Bronze Validator** - Validates data quality
- **Bronze Writer** - Writes to bronze layer

### Silver Layer
- **Bronze Reader** - Reads from bronze layer
- **Flattener** - Flattens nested structures
- **Type Caster** - Casts data types
- **Normalizer** - Normalizes values
- **Jointer** - Enriches with lookup tables
- **Silver Writer** - Writes to silver layer

### Gold Layer
- **Silver Reader** - Reads from silver layer
- **Feature Engineers** - Computes features (user, item, session, statistical, temporal, embeddings)
- **Feature Quality Checker** - Validates feature quality
- **Feature Schema Registry** - Manages feature schemas
- **Gold Writer** - Writes to gold layer
- **Feast Client** - Writes to Feast feature store

---

## 📊 Data Flow

### Complete Pipeline Flow

1. **Producer** → Generates events → **Kafka**
2. **Kafka** → Spark Streaming → **Bronze** (raw, validated, PII-scrubbed)
3. **Bronze** → Spark Batch → **Silver** (cleaned, enriched, normalized)
4. **Silver** → Spark Batch → **Gold** (ML-ready features)
5. **Gold** → **Feast** (offline store for training, online store for serving)
6. **Feast** → **ML Model** (training and real-time inference)

---

## 🎓 Key Concepts

### Medallion Architecture
- **Bronze:** Raw, validated data (landing zone)
- **Silver:** Cleaned, enriched data (business-ready)
- **Gold:** Feature-engineered data (ML-ready)

### Point-in-Time Correctness
- Features computed as they existed at event timestamp
- Prevents future data leakage in training data
- Critical for accurate model evaluation

### Feature Store (Feast)
- **Offline Store:** Parquet/S3 for training data generation
- **Online Store:** Redis for real-time feature serving (<10ms)
- **Materialization:** Moves features from offline to online

---

## 📈 Monitoring & Observability

### Metrics Tracked
- **Bronze:** Input/output records, validation rates, PII scrubbing stats
- **Silver:** Processing latency, enrichment stats, quality scores
- **Gold:** Feature computation metrics, quality scores, drift detection

### Alerting
- Error rate thresholds
- Data quality score thresholds
- Consumer lag thresholds
- Feature drift detection

---

## 🔒 Security & Compliance

### PII Handling
- **Bronze:** PII scrubbing (hashing, masking)
- **Silver:** PII masking (IP anonymization, geo-location masking)
- **Gold:** No PII in features (already masked)

### GDPR/CCPA Compliance
- PII scrubbing before storage
- Data retention policies
- Audit trails

---

## 🚀 Performance Optimizations

### Storage
- **Parquet format** (columnar, compressed)
- **Snappy compression** (fast, good ratio)
- **Partitioning** (by date/hour/type for efficient queries)
- **File size optimization** (128MB-1GB per file)

### Processing
- **Adaptive Query Execution (AQE)** - Automatic optimization
- **Broadcast joins** - For small lookup tables
- **Partition pruning** - Only reads relevant partitions
- **Coalesce/repartition** - Optimizes file sizes

---

## 📝 Configuration

### Config Files
- `config/spark_config.yaml` - Spark settings, storage paths
- `config/kafka.yaml` - Kafka broker and topic settings
- `config/feast_config.yaml` - Feast feature store settings

### Environment Variables
- `JAVA_HOME` - Java installation path
- `SPARK_HOME` - Spark installation path (optional)

---

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_bronze_ingestion.py
```

---

## 📖 Interview Preparation

See **[INTERVIEW_QA.md](INTERVIEW_QA.md)** for 40+ interview questions covering:
- Bronze → Silver → Gold pipeline
- Point-in-time correctness
- Feature engineering
- Feature store (Feast)
- Feature drift detection
- And more...

---

## 🛠️ Troubleshooting

### Common Issues

**Issue:** Java gateway process exited
- **Solution:** Set `JAVA_HOME` environment variable

**Issue:** Kafka connection failed
- **Solution:** Check Kafka is running: `docker-compose ps`

**Issue:** S3/MinIO connection failed
- **Solution:** Check MinIO is running and credentials are correct

---

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Feast Documentation](https://docs.feast.dev/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

---

## 🤝 Contributing

This is a production-grade ML data pipeline implementation. All components follow Netflix/Uber standards for:
- Error handling
- Logging
- Monitoring
- Scalability
- Maintainability

---

## 📄 License

This project is for educational purposes and interview preparation.

---

## ✅ Summary

**Complete Production ML Data Pipeline:**
- ✅ Kafka → Bronze (streaming ingestion)
- ✅ Bronze → Silver (batch transformation)
- ✅ Silver → Gold (feature engineering)
- ✅ Gold → Feast (feature store)
- ✅ Comprehensive monitoring and observability
- ✅ Production-grade error handling
- ✅ Point-in-time correctness support
- ✅ Feature quality validation
- ✅ Feature drift detection

**Ready for:** Production deployment, ML model training, real-time feature serving

---

**Built with ❤️ following Netflix/Uber production standards**
