
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
    .appName("HospitalLaboratoryProcessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)


# -----------------------------------------
# 2. Read Kafka
# -----------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:9092"
    )
    .option("subscribe", "labs.raw")
    .option("startingOffsets", "latest")
    .load()
)


# -----------------------------------------
# 3. Shared transformations
# -----------------------------------------

parsed_df = parse_labs(kafka_df)

validated_df = validate_labs(parsed_df)

processed_df = add_lab_flags(validated_df)


# -----------------------------------------
# 4. Process each micro-batch
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

        abnormal_df = valid_df.filter(
            col("is_out_of_range") == True
        )

        total = batch_df.count()
        valid = valid_df.count()
        invalid = invalid_df.count()
        abnormal = abnormal_df.count()

        print(
            f"\n===== LAB BATCH {batch_id} ====="
        )

        print(f"Total: {total}")
        print(f"Valid: {valid}")
        print(f"Invalid: {invalid}")
        print(f"Out of range: {abnormal}")

        if valid > 0:

            print("\nVALID LAB RESULTS")

            valid_df.select(
                "lab_id",
                "patient_id",
                "test_type",
                "result_value",
                "unit",
                "lab_day"
            ).show(
                10,
                truncate=False
            )

        if abnormal > 0:

            print("\nOUT-OF-RANGE RESULTS")

            abnormal_df.select(
                "patient_id",
                "test_type",
                "result_value",
                "reference_range",
                "lab_day"
            ).show(
                10,
                truncate=False
            )

        if invalid > 0:

            print("\nINVALID LAB RECORDS")

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
# 5. Start streaming query
# -----------------------------------------

query = (
    processed_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/labs_processing"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()