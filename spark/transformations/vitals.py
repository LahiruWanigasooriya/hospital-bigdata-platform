
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    length,
    trim,
    concat,
    lpad,
    lit,
    when
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# -----------------------------------------
# Shared schema
# -----------------------------------------

VITALS_SCHEMA = StructType([
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
# Parse Kafka messages
# -----------------------------------------

def parse_vitals(kafka_df):

    return (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
            col("partition"),
            col("offset")
        )
        .withColumn(
            "data",
            from_json(
                col("raw_json"),
                VITALS_SCHEMA
            )
        )
        .select(
            "kafka_key",
            "raw_json",
            "partition",
            "offset",
            "data.*"
        )
        .withColumn(
            "event_time",
            to_timestamp(col("timestamp"))
        )
    )


# -----------------------------------------
# Validate records
# -----------------------------------------

def validate_vitals(df):

    expected_batch = concat(
        lit("day_"),
        lpad(
            col("simulated_day").cast("string"),
            3,
            "0"
        )
    )

    valid_condition = (
        col("event_id").isNotNull()
        & (length(trim(col("event_id"))) > 0)

        & col("patient_id").rlike(
            r"^P(00[1-9]|01[0-9]|020)$"
        )

        & col("kafka_key").eqNullSafe(
            col("patient_id")
        )

        & col("heart_rate").between(30, 220)

        & col("spo2").between(50, 100)

        & col("systolic_bp").between(50, 250)

        & col("diastolic_bp").between(30, 150)

        & col("temperature").between(30.0, 43.0)

        & col("event_time").isNotNull()

        & (col("simulated_day") >= 1)

        & col("batch_id").eqNullSafe(
            expected_batch
        )
    )

    return df.withColumn(
        "is_valid",
        valid_condition
    )


# -----------------------------------------
# Add demonstration alert flags
# -----------------------------------------

def add_alerts(df):

    return (
        df
        .withColumn(
            "high_heart_rate",
            col("heart_rate") > 120
        )
        .withColumn(
            "low_spo2",
            col("spo2") < 93
        )
        .withColumn(
            "high_temperature",
            col("temperature") >= 38.0
        )
        .withColumn(
            "has_alert",
            col("high_heart_rate")
            | col("low_spo2")
            | col("high_temperature")
        )
    )