from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *

# ---------------- Spark session ----------------
spark = SparkSession.builder \
    .appName("KafkaToHiveETL") \
    .enableHiveSupport() \
    .getOrCreate()

# ---------------- Kafka source ----------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraudTopic") \  # ✅ FIXED
    .option("startingOffsets", "earliest") \
    .load()

# ---------------- Convert Kafka value ----------------
json_df = kafka_df.selectExpr("CAST(value AS STRING) as raw_data")

# ---------------- UPDATED SCHEMA ----------------
schema = StructType([
    StructField("Time", DoubleType()),
    StructField("Amount", DoubleType()),
    StructField("Class", DoubleType()),
    StructField("event_time", DoubleType()),   # ✅ NEW
    StructField("ingestion_time", StringType()) # ✅ NEW
])

# ---------------- Parse JSON ----------------
parsed_df = json_df.withColumn("data", from_json(col("raw_data"), schema))

# ---------------- BRONZE LAYER ----------------
bronze_df = parsed_df.select("data.*")

bronze_query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", "/data/fraud/bronze/") \
    .option("checkpointLocation", "/data/fraud/checkpoints/bronze/") \
    .outputMode("append") \
    .start()

# ---------------- SILVER LAYER (TABLEAU READY) ----------------

silver_df = bronze_df.select(
    col("Time").alias("event_time"),
    col("Amount").alias("amount"),
    col("Class").cast("int").alias("class"),
    to_timestamp(col("ingestion_time")).alias("ingestion_ts")
)

silver_query = silver_df.writeStream \
    .format("parquet") \
    .partitionBy("class") \   # ✅ PARTITIONING
    .option("path", "/data/fraud/silver/") \
    .option("checkpointLocation", "/data/fraud/checkpoints/silver/") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
