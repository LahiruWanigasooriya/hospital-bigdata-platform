
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    trim,
    length,
    regexp_extract,
    concat,
    lit,
    lpad,
    date_add,
    to_date,
    datediff,
    abs as spark_abs,
    when,
    isnan
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# -----------------------------------------
# 1. Laboratory JSON schema
# -----------------------------------------

LAB_SCHEMA = StructType([
    StructField("lab_id", StringType()),
    StructField("batch_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("test_type", StringType()),
    StructField("result_value", DoubleType()),
    StructField("reference_range", StringType()),
    StructField("unit", StringType()),
    StructField("collected_at", StringType())
])


# -----------------------------------------
# 2. Parse Kafka messages
# -----------------------------------------

def parse_labs(kafka_df):

    return (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_json"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )
        .withColumn(
            "data",
            from_json(
                col("raw_json"),
                LAB_SCHEMA
            )
        )
        .select(
            "kafka_key",
            "raw_json",
            "partition",
            "offset",
            "kafka_timestamp",
            "data.*"
        )
        .withColumn(
            "collection_time",
            to_timestamp(col("collected_at"))
        )
        .withColumn(
            "lab_day",
            regexp_extract(
                col("batch_id"),
                r"^day_([0-9]{3,})$",
                1
            ).cast("integer")
        )
    )


# -----------------------------------------
# 3. Validate laboratory records
# -----------------------------------------

def validate_labs(df):

    expected_date = date_add(
        to_date(lit("2026-09-22")),
        col("lab_day") - 1
    )

    test_is_valid = (
        (
            (col("test_type") == "Hemoglobin")
            & (col("reference_range") == "12.0-16.0")
            & (col("unit") == "g/dL")
        )
        |
        (
            (col("test_type") == "WBC")
            & (col("reference_range") == "4000-11000")
            & (col("unit") == "cells/uL")
        )
        |
        (
            (col("test_type") == "Creatinine")
            & (col("reference_range") == "0.6-1.3")
            & (col("unit") == "mg/dL")
        )
    )

    valid_condition = (
        col("lab_id").isNotNull()
        & (length(trim(col("lab_id"))) > 0)

        & col("patient_id").rlike(
            r"^P(00[1-9]|0[1-9][0-9]|020)$"
        )

        & col("kafka_key").eqNullSafe(
            col("patient_id")
        )

        & col("batch_id").rlike(
            r"^day_[0-9]{3,}$"
        )

        & (col("lab_day") >= 1)

        & col("result_value").isNotNull()
        & ~isnan(col("result_value"))
        & (col("result_value") >= 0)
        & (col("result_value") < float("inf"))

        & col("collection_time").isNotNull()

        & (
            to_date(col("collection_time"))
            == expected_date
        )

        & test_is_valid
    )

    return df.withColumn(
        "is_valid",
        valid_condition
    )


# -----------------------------------------
# 4. Add illustrative range flags
# -----------------------------------------

def add_lab_flags(df):

    lower_bound = (
        when(
            col("test_type") == "Hemoglobin",
            lit(12.0)
        )
        .when(
            col("test_type") == "WBC",
            lit(4000.0)
        )
        .when(
            col("test_type") == "Creatinine",
            lit(0.6)
        )
    )

    upper_bound = (
        when(
            col("test_type") == "Hemoglobin",
            lit(16.0)
        )
        .when(
            col("test_type") == "WBC",
            lit(11000.0)
        )
        .when(
            col("test_type") == "Creatinine",
            lit(1.3)
        )
    )

    return (
        df
        .withColumn("lower_bound", lower_bound)
        .withColumn("upper_bound", upper_bound)
        .withColumn(
            "is_out_of_range",
            (col("result_value") < col("lower_bound"))
            |
            (col("result_value") > col("upper_bound"))
        )
    )