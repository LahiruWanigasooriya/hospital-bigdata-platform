
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from transformations.vitals import (
    parse_vitals,
    validate_vitals,
    add_alerts
)


# -----------------------------------------
# Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("HospitalVitalsDeduplication")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)


# -----------------------------------------
# Kafka source
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
# Shared parsing and validation
# -----------------------------------------

parsed_df = parse_vitals(kafka_df)

validated_df = validate_vitals(parsed_df)

valid_df = validated_df.filter(
    col("is_valid")
)


# -----------------------------------------
# Event-ID deduplication
# -----------------------------------------

unique_df = (
    valid_df
    .withWatermark(
        "event_time",
        "1 day"
    )
    .dropDuplicates(["event_id"])
)


# -----------------------------------------
# Add alert flags
# -----------------------------------------

processed_df = add_alerts(unique_df)


# -----------------------------------------
# Console output
# -----------------------------------------

query = (
    processed_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/vitals_deduplicated"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()