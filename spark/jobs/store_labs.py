
import os

from pyspark.sql import SparkSession

from pyspark.sql.functions import col

from transformations.labs import (
    parse_labs,
    validate_labs,
    add_lab_flags
)


# -----------------------------------------
# 1. Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("HospitalLaboratoryStorage")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)


# -----------------------------------------
# 2. PostgreSQL settings
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


JDBC_URL = (
    f"jdbc:postgresql://"
    f"{DB_HOST}:{DB_PORT}/{DB_NAME}"
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
    .option("subscribe", "labs.raw")
    .option("startingOffsets", "earliest")
    .load()
)


# -----------------------------------------
# 4. Shared transformations
# -----------------------------------------

parsed_df = parse_labs(kafka_df)

validated_df = validate_labs(parsed_df)

processed_df = add_lab_flags(
    validated_df
)


# -----------------------------------------
# 5. PostgreSQL insert function
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
        INSERT INTO laboratory_results (

            lab_id,
            batch_id,
            patient_id,
            lab_day,
            test_type,
            result_value,
            reference_range,
            unit,
            collected_at,
            is_out_of_range,
            kafka_partition,
            kafka_offset

        )

        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )

        ON CONFLICT (lab_id)
        DO NOTHING
    """

    try:

        with connection:

            with connection.cursor() as cursor:

                for row in rows:

                    cursor.execute(
                        sql,
                        (
                            row.lab_id,
                            row.batch_id,
                            row.patient_id,
                            row.lab_day,
                            row.test_type,
                            row.result_value,
                            row.reference_range,
                            row.unit,
                            row.collection_time,
                            row.is_out_of_range,
                            row.partition,
                            row.offset
                        )
                    )

    finally:

        connection.close()


# -----------------------------------------
# 6. Process each micro-batch
# -----------------------------------------

def process_batch(batch_df, batch_id):

    batch_df.persist()

    try:

        valid_df = batch_df.filter(
            col("is_valid") == True
        )

        invalid_df = batch_df.filter(
            (col("is_valid") == False)
            | col("is_valid").isNull()
        )

        total = batch_df.count()
        invalid = invalid_df.count()

        # Keep one row per lab ID in this batch.
        unique_df = valid_df.dropDuplicates(
            ["lab_id"]
        )

        unique_count = unique_df.count()

        print(
            f"\n===== STORAGE BATCH {batch_id} ====="
        )

        print(f"Total received: {total}")
        print(f"Invalid: {invalid}")
        print(
            f"Unique valid records: {unique_count}"
        )

        if invalid > 0:

            invalid_df.select(
                "raw_json",
                "partition",
                "offset"
            ).show(
                10,
                truncate=False
            )

        if unique_count > 0:

            storage_df = unique_df.select(
                "lab_id",
                "batch_id",
                "patient_id",
                "lab_day",
                "test_type",
                "result_value",
                "reference_range",
                "unit",
                "collection_time",
                "is_out_of_range",
                "partition",
                "offset"
            )

            storage_df.foreachPartition(
                insert_partition
            )

            print(
                "Database insert completed."
            )

    finally:

        batch_df.unpersist()


# -----------------------------------------
# 7. Start streaming query
# -----------------------------------------

query = (
    processed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/labs_storage"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()