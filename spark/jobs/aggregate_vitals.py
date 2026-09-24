
from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    min,
    max,
    count,
    round as spark_round,
    concat,
    lpad,
    lit
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# -----------------------------------------
# Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("HospitalVitalsAggregation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.sql.session.timeZone", "UTC")


# -----------------------------------------
# Schema
# -----------------------------------------

schema = StructType([
    StructField("event_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("heart_rate", IntegerType()),
    StructField("spo2", IntegerType()),
    StructField("systolic_bp", IntegerType()),
    StructField("diastolic_bp", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("simulated_day", IntegerType()),
    StructField("batch_id", StringType()),
    StructField("ingested_at", StringType())
])


# -----------------------------------------
# Kafka ingestion
# -----------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:9092"
    )
    .option("subscribe", "vitals.raw")
    .option("startingOffsets", "latest")
    .load()
)


# -----------------------------------------
# Parse records
# -----------------------------------------

parsed_df = (
    kafka_df
    .select(
        col("key").cast("string").alias("kafka_key"),
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select(
        "kafka_key",
        "data.*"
    )
    .withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
)


# -----------------------------------------
# Filter valid readings
# -----------------------------------------

expected_batch = concat(
    lit("day_"),
    lpad(col("simulated_day").cast("string"), 3, "0")
)

valid_df = parsed_df.filter(
    col("event_id").isNotNull()
    & col("patient_id").rlike(r"^P(00[1-9]|0[1-9][0-9]|020)$")
    & col("kafka_key").eqNullSafe(col("patient_id"))
    & col("heart_rate").between(30, 220)
    & col("spo2").between(50, 100)
    & col("systolic_bp").between(50, 250)
    & col("diastolic_bp").between(30, 150)
    & col("temperature").between(30.0, 43.0)
    & col("event_time").isNotNull()
    & (col("simulated_day") >= 1)
    & col("batch_id").eqNullSafe(expected_batch)
)


# -----------------------------------------
# Event-time aggregation
# -----------------------------------------

aggregated_df = (
    valid_df
    .withWatermark(
        "event_time",
        "10 minutes"
    )
    .groupBy(
        col("patient_id"),
        window(
            col("event_time"),
            "30 minutes",
            "10 minutes"
        )
    )
    .agg(
        spark_round(
            avg("heart_rate"),
            2
        ).alias("avg_heart_rate"),

        min("spo2").alias("min_spo2"),

        max("temperature").alias(
            "max_temperature"
        ),

        count("*").alias(
            "reading_count"
        )
    )
)


# -----------------------------------------
# Console output
# -----------------------------------------

query = (
    aggregated_df.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/vitals_aggregation"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()