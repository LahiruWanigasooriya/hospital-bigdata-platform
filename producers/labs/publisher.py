
import csv
import json
import os
import sys
import uuid

from pathlib import Path
from confluent_kafka import Producer
from dotenv import load_dotenv


# ---------------------------------------
# Configuration
# ---------------------------------------

load_dotenv()

KAFKA_SERVER = os.getenv(
    "KAFKA_HOST_BOOTSTRAP_SERVERS",
    "localhost:9094"
)

TOPIC = "labs.raw"

REQUIRED_FIELDS = [
    "batch_id",
    "patient_id",
    "test_type",
    "result_value",
    "reference_range",
    "unit",
    "collected_at"
]


# ---------------------------------------
# Kafka producer
# ---------------------------------------

producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "client.id": "hospital-lab-publisher",
    "enable.idempotence": True,
    "acks": "all"
})


# ---------------------------------------
# Delivery tracking
# ---------------------------------------

delivered_count = 0
failed_count = 0


def delivery_report(err, msg):

    global delivered_count
    global failed_count

    if err is not None:

        failed_count += 1

        print(f"Delivery failed: {err}")

    else:

        delivered_count += 1


# ---------------------------------------
# Read and publish CSV
# ---------------------------------------

def publish_file(file_path):

    global delivered_count
    global failed_count

    delivered_count = 0
    failed_count = 0

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"File not found: {path}"
        )

    records = []

    with open(
        path,
        "r",
        newline="",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        if not reader.fieldnames:
            raise ValueError("CSV header is missing.")

        missing = (
            set(REQUIRED_FIELDS) -
            set(reader.fieldnames)
        )

        if missing:
            raise ValueError(
                f"Missing columns: {missing}"
            )

        for row in reader:

            for field in REQUIRED_FIELDS:

                if not row[field]:
                    raise ValueError(
                        f"Missing value: {field}"
                    )

            row["result_value"] = float(
                row["result_value"]
            )

            # Stable ID for the same lab record
            record_key = (
                f"{row['batch_id']}:"
                f"{row['patient_id']}:"
                f"{row['test_type']}"
            )

            row["lab_id"] = str(
                uuid.uuid5(
                    uuid.NAMESPACE_DNS,
                    record_key
                )
            )

            records.append(row)

    print(
        f"Publishing {len(records)} "
        f"laboratory records..."
    )

    for record in records:

        patient_id = record["patient_id"]

        while True:

            try:

                producer.produce(
                    topic=TOPIC,
                    key=patient_id.encode("utf-8"),
                    value=json.dumps(
                        record
                    ).encode("utf-8"),
                    callback=delivery_report
                )

                break

            except BufferError:

                producer.poll(1)

        producer.poll(0)

    remaining = producer.flush(timeout=30)

    print(f"Delivered: {delivered_count}")
    print(f"Failed: {failed_count}")
    print(f"Remaining: {remaining}")

    if failed_count > 0 or remaining > 0:

        raise RuntimeError(
            "Some laboratory records were "
            "not successfully delivered."
        )

    print("Laboratory ingestion completed.")


# ---------------------------------------
# Main
# ---------------------------------------

if __name__ == "__main__":

    if len(sys.argv) != 2:

        print(
            "Usage: python publisher.py <csv_file>"
        )

        sys.exit(1)

    try:

        publish_file(sys.argv[1])

    except Exception as error:

        print(f"Error: {error}")

        sys.exit(1)