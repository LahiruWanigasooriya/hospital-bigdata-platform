
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    trim,
    length,
    when,
    lit,
    concat,
    lpad,
    regexp_extract
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# -----------------------------------------
# 1. Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("HospitalVitalsProcessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set("spark.sql.session.timeZone", "UTC")


# -----------------------------------------
# 2. Incoming JSON schema
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
# 3. Read Kafka
# -----------------------------------------

raw_df = (
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
# 4. Parse JSON
# -----------------------------------------

parsed_df = (
    raw_df
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("partition"),
        col("offset")
    )
    .withColumn(
        "data",
        from_json(col("raw_json"), schema)
    )
    .select(
        "kafka_key",
        "raw_json",
        "partition",
        "offset",
        "data.*"
    )
)


# -----------------------------------------
# 5. Convert event time
# -----------------------------------------

df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)


# -----------------------------------------
# 6. Validate data
# -----------------------------------------

expected_batch = concat(
    lit("day_"),
    lpad(col("simulated_day").cast("string"), 3, "0")
)

valid_condition = (
    col("event_id").isNotNull()
    & (length(trim(col("event_id"))) > 0)

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

df = df.withColumn(
    "is_valid",
    valid_condition
)


# -----------------------------------------
# 7. Demonstration alert rules
# -----------------------------------------

df = df.withColumn(
    "alert_type",
    when(
        col("heart_rate") > 120,
        lit("HIGH_HEART_RATE")
    )
    .when(
        col("spo2") < 93,
        lit("LOW_SPO2")
    )
    .when(
        col("temperature") >= 38.0,
        lit("HIGH_TEMPERATURE")
    )
    .otherwise(lit("NORMAL"))
)


# -----------------------------------------
# 8. Process each micro-batch
# -----------------------------------------

def process_batch(batch_df, batch_id):

    batch_df.persist()

    try:
        valid_df = batch_df.filter(
            col("is_valid")
        )

        invalid_df = batch_df.filter(
            ~col("is_valid") | col("is_valid").isNull()
        )

        alerts_df = valid_df.filter(
            col("alert_type") != "NORMAL"
        )

        total = batch_df.count()
        valid = valid_df.count()
        invalid = invalid_df.count()
        alerts = alerts_df.count()

        print(
            f"\n===== BATCH {batch_id} ====="
        )

        print(f"Total: {total}")
        print(f"Valid: {valid}")
        print(f"Invalid: {invalid}")
        print(f"Alerts: {alerts}")

        if valid > 0:

            print("\nVALID RECORDS")

            valid_df.select(
                "patient_id",
                "heart_rate",
                "spo2",
                "temperature",
                "simulated_day",
                "event_time"
            ).show(
                10,
                truncate=False
            )

        if alerts > 0:

            print("\nABNORMAL READINGS")

            alerts_df.select(
                "patient_id",
                "heart_rate",
                "spo2",
                "temperature",
                "alert_type"
            ).show(
                10,
                truncate=False
            )

        if invalid > 0:

            print("\nINVALID RECORDS")

            invalid_df.select(
                "kafka_key",
                "raw_json",
                "partition",
                "offset"
            ).show(
                10,
                truncate=False
            )

    finally:
        batch_df.unpersist()


# -----------------------------------------
# 9. Start query
# -----------------------------------------

query = (
    df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/vitals_processing"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()