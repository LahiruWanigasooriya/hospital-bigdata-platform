
from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)


# -----------------------------------------
# 1. Create Spark session
# -----------------------------------------

spark = (
    SparkSession.builder
    .appName("HospitalVitalsStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------------------
# 2. Define JSON schema
# -----------------------------------------

vitals_schema = StructType([
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
# 3. Read Kafka stream
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
# 4. Parse JSON
# -----------------------------------------

parsed_df = (
    kafka_df
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("partition"),
        col("offset")
    )
    .withColumn(
        "data",
        from_json(
            col("json_value"),
            vitals_schema
        )
    )
)

vitals_df = parsed_df.select(
    "kafka_key",
    "partition",
    "offset",
    "data.*"
)


# -----------------------------------------
# 5. Start streaming query
# -----------------------------------------

query = (
    vitals_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option(
        "checkpointLocation",
        "/opt/project/spark/checkpoints/vitals_console"
    )
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()