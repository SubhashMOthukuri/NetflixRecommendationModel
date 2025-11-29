# ML Data Pipeline Interview Q&A - TikTok/ByteDance Style

**Quick reference guide for data engineering interviews**

---

## 1. How do you ensure exactly-once processing in Spark Streaming?

**Answer:** Use checkpointing to save query state (offsets, metadata) to durable storage. On restart, Spark resumes from the last checkpoint, preventing duplicate processing. Combine with idempotent writes (e.g., using event_id as primary key) to ensure each record is processed exactly once, even if the job fails and restarts.

---

## 2. How do you handle schema evolution without breaking downstream?

**Answer:** Use Schema Registry with backward/forward compatibility rules. Add new fields as optional (nullable) so old consumers still work. Use schema enforcer to add missing optional fields as null and cast data types. This allows schema changes without breaking existing pipelines or requiring code changes.

---

## 3. What happens if Kafka consumer lag increases?

**Answer:** Consumer lag means consumers can't keep up with producers. Monitor lag metrics and alert when it exceeds thresholds. Solutions include: increase consumer parallelism (more partitions/consumers), optimize processing logic, scale up resources, or temporarily pause producers. High lag can cause data staleness and memory issues.

---

## 4. How do you optimize Spark jobs for cost and performance?

**Answer:** Use Adaptive Query Execution (AQE) for automatic optimization, partition data by date/hour for efficient pruning, use Parquet with Snappy compression, enable broadcast joins for small tables, and tune shuffle partitions. Monitor Spark UI to identify bottlenecks (skew, large shuffles) and optimize accordingly. Cost optimization: right-size executors, use spot instances, and delete old data.

---

## 5. How do you handle PII data in ML pipelines?

**Answer:** Scrub PII before storage using hashing (SHA-256), masking, or removal. Hash sensitive fields like user_id, email, IP address. Add a PII scrubbed flag to track what was processed. This ensures GDPR/CCPA compliance and reduces breach impact. Keep original data encrypted in a separate secure location if needed for debugging.

---

## 6. Explain the difference between Bronze, Silver, and Gold layers.

**Answer:** **Bronze** stores raw, validated data as-is (with PII scrubbed). **Silver** stores cleaned, enriched, business-ready data (nulls removed, joined with reference data). **Gold** stores feature-engineered data optimized for ML models. This Medallion Architecture provides data lineage, enables reprocessing, and separates concerns (raw → clean → features).

---

## 7. How do you debug a slow Spark job in production?

**Answer:** Check Spark UI for bottlenecks: identify slow stages, look for data skew (uneven partition sizes), check shuffle size, and review executor utilization. Common issues: too many small files (coalesce), large shuffles (optimize joins), data skew (salting), or insufficient resources. Use Spark event logs and metrics to pinpoint the problem stage.

---

## 8. What's the difference between batch and streaming processing?

**Answer:** **Batch** processes data in large chunks at scheduled intervals (hours/days), good for historical analysis and cost efficiency. **Streaming** processes data continuously in near real-time (seconds), good for real-time dashboards and alerts. Spark Structured Streaming provides unified API for both, allowing same code to work for batch and streaming.

---

## 9. How do you ensure data quality at scale?

**Answer:** Implement multiple validation layers: schema validation (structure), data quality checks (ranges, formats), and business rule validation (relationships). Route invalid data to Dead Letter Queue (DLQ) for investigation without stopping the pipeline. Track validation metrics (success rate, error types) and alert when quality drops below thresholds.

---

## 10. Design a system to process 1 billion events per day.

**Answer:** Use Kafka for ingestion (high throughput, partitioning), Spark Structured Streaming for processing (scales horizontally), and object store (S3/MinIO) for storage. Partition data by date/hour for parallel processing. Use consumer groups with multiple consumers, scale Spark executors dynamically, and optimize with partitioning, compression, and broadcast joins. Monitor throughput and scale resources based on load.

---

## 11. What is checkpointing and why is it important?

**Answer:** Checkpointing saves streaming query state (offsets, metadata) to durable storage. It enables job recovery from last successful state after failures, preventing data loss and duplicate processing. Without checkpoints, jobs restart from beginning (reprocessing all data) or lose progress, making exactly-once processing impossible.

---

## 12. How do you handle duplicate data in streaming?

**Answer:** Use deduplication strategies: by event_id (primary key), by composite key (event_id + timestamp), or within time windows. Track seen events in state store or use window functions. For exactly-once, combine checkpointing (prevents reprocessing) with idempotent writes (using event_id as key). Keep latest or earliest record based on business needs.

---

## 13. What is a Dead Letter Queue (DLQ) and when do you use it?

**Answer:** DLQ stores records that fail processing (malformed, invalid, errors). It prevents data loss, enables investigation of failures, and allows reprocessing after fixes. Use DLQ for: schema validation failures, data quality violations, parsing errors, or business rule violations. Route bad data to DLQ instead of crashing the pipeline.

---

## 14. How do you optimize Parquet file sizes?

**Answer:** Target 128MB-1GB per file for optimal query performance. Use `coalesce()` or `repartition()` to control file count. Set `maxRecordsPerFile` to limit records per file. Partition by date/hour to distribute data evenly. Too many small files cause slow queries; too few large files cause memory issues. Balance based on query patterns.

---

## 15. What is data skew and how do you handle it?

**Answer:** Data skew is uneven data distribution across partitions (some partitions have much more data). It causes slow stages as some executors work harder. Solutions: use salting (add random key), increase partitions, use broadcast joins for small tables, or filter skewed keys separately. Monitor partition sizes in Spark UI to detect skew.

---

## 16. Explain Kafka consumer groups and rebalancing.

**Answer:** Consumer groups allow multiple consumers to share work (each partition consumed by one consumer). Rebalancing happens when consumers join/leave, reassigning partitions to maintain load balance. Use static group membership to reduce rebalancing. Monitor rebalancing frequency as frequent rebalancing causes processing pauses and lag.

---

## 17. How do you monitor a production data pipeline?

**Answer:** Track key metrics: throughput (events/second), latency (processing time), error rate, consumer lag, and data quality scores. Set up alerts for thresholds (high error rate, low throughput, high lag). Use Spark UI for job monitoring, Kafka metrics for consumer lag, and custom metrics for business KPIs. Log all errors with context for debugging.

---

## 18. What is watermarking in Spark Streaming?

**Answer:** Watermarking defines how late data can arrive and still be processed. It's a time threshold (e.g., "10 minutes") that allows late events within that window. Events older than watermark are dropped. Watermarking enables state cleanup (removes old state) and handles out-of-order data in windowed aggregations. Essential for handling real-world data delays.

---

## 19. How do you choose between batch and streaming?

**Answer:** Use **streaming** for: real-time dashboards, alerts, low-latency requirements (< 1 minute), and continuous processing. Use **batch** for: historical analysis, cost optimization, large-scale ETL, and when latency isn't critical (hours/days). Many systems use both: streaming for real-time, batch for backfills and reprocessing.

---

## 20. How do you ensure data consistency across layers?

**Answer:** Use idempotent operations (same input = same output), implement exactly-once processing with checkpoints, and validate data at each layer. Track data lineage (which source produced which output) and use versioning for schema changes. Implement reconciliation jobs to compare record counts and checksums between layers to detect inconsistencies.

---

## 21. What is the difference between at-least-once and exactly-once?

**Answer:** **At-least-once** guarantees data is processed at least once (may have duplicates if job fails and retries). **Exactly-once** guarantees each record is processed exactly once (no duplicates, no loss). Exactly-once requires: idempotent operations, checkpointing, and transactional writes. At-least-once is simpler but requires deduplication downstream.

---

## 22. How do you handle backpressure in streaming?

**Answer:** Backpressure occurs when processing can't keep up with input rate. Spark Structured Streaming automatically handles it by slowing down reading from source. Solutions: increase processing resources, optimize transformations, use `maxOffsetsPerTrigger` to limit batch size, or add buffering. Monitor processing rate vs input rate to detect backpressure early.

---

## 23. What is schema registry and why use it?

**Answer:** Schema Registry is a centralized service that stores and manages Avro schemas for Kafka topics. It enables schema versioning, compatibility checking (backward/forward), and prevents schema drift. Producers/consumers validate data against registered schemas, ensuring data consistency. Essential for schema evolution in production systems with multiple teams.

---

## 24. How do you optimize Kafka consumer performance?

**Answer:** Increase parallelism (more partitions = more consumers), tune fetch settings (fetch.min.bytes, fetch.max.wait.ms), use appropriate consumer group size, and process in batches. Monitor consumer lag and scale consumers when lag increases. Use async processing and avoid blocking operations in consumer loop. Optimize serialization (use Avro instead of JSON).

---

## 25. Explain the Medallion Architecture (Bronze/Silver/Gold).

**Answer:** **Bronze** = raw, validated data (landing zone, preserves original data). **Silver** = cleaned, enriched data (business-ready, nulls removed, joined with reference data). **Gold** = feature-engineered data (ML-ready, aggregated, optimized for models). Benefits: data lineage, reprocessing capability, separation of concerns, and cost optimization (store only what's needed at each layer).

---

## 26. Explain the complete Bronze → Silver → Gold pipeline flow.

**Answer:** **Bronze** (raw, validated data) → **Silver** (cleaned, enriched data) → **Gold** (ML-ready features). Bronze stores raw events from Kafka (validated, PII-scrubbed). Silver transforms bronze data (flatten, normalize, enrich, validate). Gold computes features from silver (user behavior, item popularity, embeddings) and stores in Feast for ML training and serving.

---

## 27. What is point-in-time correctness and why is it critical for ML?

**Answer:** Point-in-time correctness means computing features as they existed at event timestamp (no future data leakage). Critical because: training data must match production (use only past data), prevents data leakage (no future information), ensures accurate model evaluation. Example: Event on Jan 15 → Use features as they existed on Jan 15, not Jan 20.

---

## 28. How do you compute features in the gold layer?

**Answer:** Multiple feature types: **User features** (watch time, video count, category preferences), **Item features** (view count, completion rate, popularity), **Session features** (duration, engagement, quality), **Statistical features** (mean, std, percentiles), **Temporal features** (recency, frequency, time patterns), **Embeddings** (user/item similarity vectors). All computed with point-in-time correctness for training data.

---

## 29. What is Feast feature store and how do you use it?

**Answer:** Feast is an open-source feature store for ML. **Offline store** (Parquet/S3) for training data generation with point-in-time queries. **Online store** (Redis) for real-time feature serving (<10ms latency). **Materialization** moves features from offline to online. Use for: centralized feature management, consistent features across training/serving, efficient feature retrieval.

---

## 30. How do you detect and handle feature drift?

**Answer:** **Drift detection:** Compare current vs reference feature distributions (mean, std, percentiles). **Monitoring:** Continuously monitor features for distribution changes. **Alerting:** Alert when drift exceeds threshold (e.g., 10% change). **Quality scores:** Track feature quality over time. **Actions:** Investigate root cause, update models if needed, fix data pipeline.

---

## 31. How do you ensure feature quality in gold layer?

**Answer:** Multiple validation layers: **Null checking** (validates null percentage), **Outlier detection** (IQR method), **Distribution checking** (skewness, variance), **Drift detection** (compares distributions), **Schema validation** (validates types and rules). **Quality score** combines all checks. **Component:** FeatureQualityChecker validates features before storing.

---

## 32. What is the difference between offline and online feature stores?

**Answer:** **Offline store** (Parquet/S3): For training data generation, supports point-in-time queries, latency: seconds to minutes. **Online store** (Redis): For real-time feature serving, key-value lookup, latency: <10ms. **Materialization:** Features materialized from offline to online for recent data. **Use case:** Offline for training, online for inference.

---

## 33. How do you handle feature backfilling for training data?

**Answer:** **Feature backfill job** with time travel: Filter silver data up to event timestamp, compute features as they existed at that time, process historical date ranges, support incremental backfill. **Point-in-time correctness:** Ensures no future data leakage. **Example:** Backfill Jan 1-31 features for training data generation.

---

## 34. How do you build embeddings for recommendations?

**Answer:** **ALS (Alternating Least Squares)** matrix factorization: Learn user embeddings from user-item interactions, learn item embeddings from item-user interactions, compute similarity scores (cosine similarity), extract embedding dimensions as features. **Use case:** Collaborative filtering, similarity search, recommendation systems.

---

## 35. How do you monitor feature health in production?

**Answer:** **Feature monitoring:** Freshness (last update time, staleness detection), Completeness (null percentage), Drift detection (distribution changes), Anomaly detection (outliers). **Metrics:** Track freshness, completeness, drift scores, anomaly rates. **Alerting:** Generate alerts when thresholds exceeded. **Component:** FeatureMonitoring continuously monitors features.

---

## 36. What is feature schema registry and why use it?

**Answer:** **Feature schema registry** tracks feature definitions, types, validation rules, and versions. **Why:** Prevents schema drift, enables schema evolution, ensures consistency across pipelines. **Features:** Register schemas, validate features, track versions, compare schemas. **Component:** FeatureSchemaRegistry manages feature schemas.

---

## 37. How do you optimize feature storage in gold layer?

**Answer:** **Parquet format** (columnar storage), **Snappy compression** (fast, good ratio), **Partitioning** (by feature_date and feature_type), **File size optimization** (targets 128MB-1GB per file), **Partition pruning** (only reads relevant partitions). **Result:** Fast queries, cost optimization, efficient storage.

---

## 38. How do you ensure online/offline feature consistency?

**Answer:** **Same computation logic** for offline and online, **Materialization** from offline to online store, **Validation** to ensure consistency, **Monitoring** consistency metrics. **Challenge:** Online store may lag → Monitor and alert on lag. **Solution:** Frequent materialization, validation checks.

---

## 39. What is the complete data flow from Kafka to ML model?

**Answer:** **Kafka** (events) → **Bronze** (raw, validated, PII-scrubbed) → **Silver** (cleaned, enriched, normalized) → **Gold** (ML-ready features) → **Feast** (feature store) → **ML Model** (training/serving). **Bronze:** Stores raw events. **Silver:** Transforms to business-ready data. **Gold:** Computes features. **Feast:** Manages features for ML. **Model:** Trains and serves using features.

---

## 40. How do you handle feature versioning?

**Answer:** **Feature schema registry** tracks feature versions, **Version management** tracks schema changes over time, **Backward compatibility** ensures old models still work, **Schema comparison** tracks changes between versions. **Example:** Feature v1 → v2 (added new field), both versions supported.

---

## Key Concepts Summary

### Must Know:
- ✅ Spark Structured Streaming (exactly-once, checkpointing, watermarking)
- ✅ Kafka (consumer groups, offsets, partitioning, lag)
- ✅ Medallion Architecture (Bronze/Silver/Gold)
- ✅ Data Quality (validation, DLQ, monitoring)
- ✅ Performance Optimization (partitioning, compression, AQE)
- ✅ Feature Engineering (user, item, session, statistical, temporal, embeddings)
- ✅ Point-in-Time Correctness (time travel, no data leakage)
- ✅ Feast Feature Store (offline/online stores, materialization)

### Important:
- ✅ Schema Evolution (Schema Registry, compatibility)
- ✅ PII Handling (GDPR/CCPA compliance)
- ✅ Fault Tolerance (checkpointing, retries, graceful shutdown)
- ✅ Scalability (horizontal scaling, data skew)
- ✅ Monitoring (metrics, alerting, debugging)
- ✅ Feature Drift Detection (distribution monitoring)
- ✅ Feature Quality (validation, quality scores)
- ✅ Feature Serving (online store, real-time inference)

---

**Good luck with your interviews! 🚀**

