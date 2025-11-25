from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Create Spark session (like a huge calculator for big data)
spark = SparkSession.builder \
    .appName("Day1KafkaToS3") \
    .getOrCreate()

# Define schema - tells Spark what structure to expect in JSON
# This is like a template: "I expect user_id (string), action (string), timestamp (number)"
schema = StructType([
    StructField("user_id", StringType()),
    StructField("action", StringType()),
    StructField("timestamp", LongType())  # Changed to LongType for milliseconds
])

# Read streaming data from Kafka
# This creates a "streaming DataFrame" - data flows continuously, not all at once
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "user_events") \
    .load()

# Convert Kafka message (binary) to JSON DataFrame
# Kafka stores data as bytes in "value" column
# Step 1: Take "value" column, convert to string
# Step 2: Parse JSON string using our schema
# Step 3: Extract all fields (user_id, action, timestamp) into columns
json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write streaming data to storage (MinIO/S3)
# This is called "query" because Spark treats it as a streaming query
# It continuously writes data as it arrives
query = json_df.writeStream \
    .format("json") \
    .option("path", "s3a://raw/user_events/") \
    .option("checkpointLocation", "s3a://raw/checkpoints/user_events/") \
    .start()

# Keep the program running and wait for new data
# Without this, program would exit immediately
query.awaitTermination()

