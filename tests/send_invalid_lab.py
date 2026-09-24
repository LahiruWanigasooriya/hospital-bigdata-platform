
import json
import os
import uuid

from confluent_kafka import Producer
from dotenv import load_dotenv


load_dotenv()

producer = Producer({
    "bootstrap.servers": os.getenv(
        "KAFKA_HOST_BOOTSTRAP_SERVERS",
        "localhost:9094"
    )
})


invalid_lab = {
    "lab_id": str(uuid.uuid4()),
    "batch_id": "day_003",
    "patient_id": "P001",
    "test_type": "Hemoglobin",
    "result_value": -50.0,
    "reference_range": "12.0-16.0",
    "unit": "g/dL",
    "collected_at": "2026-09-24T00:00:00+00:00"
}


producer.produce(
    topic="labs.raw",
    key="P001",
    value=json.dumps(invalid_lab)
)

remaining = producer.flush(timeout=30)

if remaining:
    raise RuntimeError(
        "Invalid test event was not delivered."
    )

print("Invalid laboratory event published.")