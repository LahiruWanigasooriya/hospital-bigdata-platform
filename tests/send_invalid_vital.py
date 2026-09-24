
import json

from confluent_kafka import Producer


producer = Producer({
    "bootstrap.servers": "localhost:9094"
})

invalid_event = {
    "event_id": "invalid-test-001",
    "patient_id": "P001",
    "heart_rate": 999,
    "spo2": 98,
    "systolic_bp": 120,
    "diastolic_bp": 80,
    "temperature": 36.8,
    "timestamp": "2026-09-22T00:00:00+00:00",
    "simulated_day": 1,
    "batch_id": "day_001",
    "ingested_at": "2026-09-22T00:00:00+00:00"
}

producer.produce(
    topic="vitals.raw",
    key="P001",
    value=json.dumps(invalid_event)
)

remaining = producer.flush(timeout=30)

if remaining:
    raise RuntimeError("Test event was not delivered.")

print("Invalid test event published.")