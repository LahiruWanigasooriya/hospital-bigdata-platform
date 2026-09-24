
import os

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    window,
    avg,
    min,
    max,
    count,
    round as spark_round,
    datediff,
    to_date,
    lit
)

from transformations.vitals import (
    parse_vitals,
    validate_vitals
)


# -----------------------------------------
# 1. Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("HospitalVitalAggregationStorage")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)

spark.conf.set(
    "spark.sql.shuffle.partitions",
    "3"
)


# -----------------------------------------
# 2. PostgreSQL configuration
# -----------------------------------------

DB_HOST = os.getenv(
    "POSTGRES_HOST",
    "postgres"
)

DB_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

DB_NAME = os.getenv(
    "POSTGRES_DB",
    "hospital_db"
)

DB_USER = os.getenv(
    "POSTGRES_USER",
    "hospital"
)

DB_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD"
)

if not DB_PASSWORD:
    raise ValueError(
        "POSTGRES_PASSWORD is missing."
    )


# -----------------------------------------
# 3. Kafka source
# -----------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:9092"
    )
    .option("subscribe", "vitals.raw")
    .option("startingOffsets", "earliest")
    .load()
)


# -----------------------------------------
# 4. Shared transformations
# -----------------------------------------

parsed_df = parse_vitals(kafka_df)

validated_df = validate_vitals(parsed_df)

valid_df = validated_df.filter(
    col("is_valid")
)


# -----------------------------------------
# 5. Event-time aggregation
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
# 6. Prepare database columns
# -----------------------------------------

storage_df = (
    aggregated_df
    .select(
        col("patient_id"),

        col("window.start").alias(
            "window_start"
        ),

        col("window.end").alias(
            "window_end"
        ),

        col("avg_heart_rate"),

        col("min_spo2"),

        col("max_temperature"),

        col("reading_count")
    )
    .withColumn(
        "simulated_day",
        datediff(
            to_date(col("window_start")),
            to_date(lit("2026-09-24"))
        ) + 1
    )
    .filter(
        col("simulated_day") >= 1
    )
)


# -----------------------------------------
# 7. PostgreSQL upsert
# -----------------------------------------

def upsert_partition(rows, batch_id):

    import psycopg2

    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    sql = """
        INSERT INTO vital_aggregations (

            patient_id,
            simulated_day,
            window_start,
            window_end,
            avg_heart_rate,
            min_spo2,
            max_temperature,
            reading_count,
            spark_batch_id

        )

        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )

        ON CONFLICT (
            patient_id,
            window_start,
            window_end
        )

        DO UPDATE SET

            simulated_day =
                EXCLUDED.simulated_day,

            avg_heart_rate =
                EXCLUDED.avg_heart_rate,

            min_spo2 =
                EXCLUDED.min_spo2,

            max_temperature =
                EXCLUDED.max_temperature,

            reading_count =
                EXCLUDED.reading_count,

            spark_batch_id =
                EXCLUDED.spark_batch_id,

            updated_at = NOW()

        WHERE
            vital_aggregations.spark_batch_id
            <= EXCLUDED.spark_batch_id
    """

    try:

        with connection:

            with connection.cursor() as cursor:

                for row in rows:

                    cursor.execute(
                        sql,
                        (
                            row.patient_id,
                            row.simulated_day,
                            row.window_start,
                            row.window_end,
                            row.avg_heart_rate,
                            row.min_spo2,
                            row.max_temperature,
                            row.reading_count,
                            batch_id
                        )
                    )

    finally:

        connection.close()


# -----------------------------------------
# 8. foreachBatch
# -----------------------------------------

def process_batch(batch_df, batch_id):

    batch_df.persist()

    try:

        row_count = batch_df.count()

        print(
            f"\n===== AGGREGATION BATCH {batch_id} ====="
        )

        print(
            f"Updated windows: {row_count}"
        )

        if row_count > 0:

            batch_df.show(
                10,
                truncate=False
            )

            batch_df.foreachPartition(
                lambda rows: upsert_partition(
                    rows,
                    batch_id
                )
            )

            print(
                "PostgreSQL upsert completed."
            )

    finally:

        batch_df.unpersist()


# -----------------------------------------
# 9. Start streaming query
# -----------------------------------------

query = (
    storage_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("update")
    .option(
    "checkpointLocation",
    "/opt/project/spark/checkpoints/vitals_aggregation_storage"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()