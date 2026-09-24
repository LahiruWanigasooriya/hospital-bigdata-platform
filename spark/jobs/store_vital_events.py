import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from transformations.vitals import (
    parse_vitals,
    validate_vitals,
    add_alerts
)


spark = (
    SparkSession.builder
    .appName("HospitalVitalEventStorage")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)


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
        "POSTGRES_PASSWORD is missing"
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
    .option(
        "subscribe",
        "vitals.raw"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# -----------------------------------------
# Shared processing
# -----------------------------------------

parsed_df = parse_vitals(kafka_df)

validated_df = validate_vitals(parsed_df)

valid_df = validated_df.filter(
    col("is_valid") == True
)

processed_df = add_alerts(valid_df)


# -----------------------------------------
# PostgreSQL insert
# -----------------------------------------

def insert_partition(rows):

    import psycopg2

    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    sql = """
        INSERT INTO vital_events (

            event_id,
            patient_id,

            heart_rate,
            spo2,
            systolic_bp,
            diastolic_bp,
            temperature,

            event_time,
            simulated_day,
            batch_id,
            ingested_at,

            high_heart_rate,
            low_spo2,
            high_temperature,
            has_alert,

            kafka_partition,
            kafka_offset

        )

        VALUES (

            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s

        )

        ON CONFLICT (event_id)
        DO NOTHING
    """

    try:

        with connection:

            with connection.cursor() as cursor:

                for row in rows:

                    cursor.execute(
                        sql,
                        (
                            row.event_id,
                            row.patient_id,

                            row.heart_rate,
                            row.spo2,
                            row.systolic_bp,
                            row.diastolic_bp,
                            row.temperature,

                            row.event_time,
                            row.simulated_day,
                            row.batch_id,
                            row.ingested_at,

                            row.high_heart_rate,
                            row.low_spo2,
                            row.high_temperature,
                            row.has_alert,

                            row.partition,
                            row.offset
                        )
                    )

    finally:

        connection.close()


# -----------------------------------------
# Micro-batch
# -----------------------------------------

def process_batch(batch_df, batch_id):

    batch_df.persist()

    try:

        total = batch_df.count()

        # Removes duplicates occurring in same
        # Spark micro-batch.
        unique_df = batch_df.dropDuplicates(
            ["event_id"]
        )

        unique_count = unique_df.count()

        print(
            f"\n===== VITAL STORAGE BATCH {batch_id} ====="
        )

        print(
            f"Valid received: {total}"
        )

        print(
            f"Unique in batch: {unique_count}"
        )

        if unique_count > 0:

            unique_df.foreachPartition(
                insert_partition
            )

            print(
                "Vital events persisted."
            )

    finally:

        batch_df.unpersist()


query = (
    processed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/vital_events_storage"
    )
    .trigger(
        processingTime="5 seconds"
    )
    .start()
)

query.awaitTermination()