
import json
import os
import uuid

from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

from common.simulation_clock import SimulationClock


load_dotenv()

clock = SimulationClock()

producer = Producer({
    "bootstrap.servers": os.getenv(
        "KAFKA_HOST_BOOTSTRAP_SERVERS",
        "localhost:9094"
    )
})


# Same ID for both messages
event_id = str(uuid.uuid4())

simulated_time = clock.now()
day = clock.current_day()

if day < 1:
    raise RuntimeError(
        "Simulation has not started yet."
    )


event = {
    "event_id": event_id,
    "patient_id": "P001",
    "heart_rate": 85,
    "spo2": 98,
    "systolic_bp": 120,
    "diastolic_bp": 80,
    "temperature": 36.8,
    "timestamp": simulated_time.isoformat(),
    "simulated_day": day,
    "batch_id": f"day_{day:03d}",
    "ingested_at": (
        datetime.now(timezone.utc).isoformat()
    )
}


for i in range(2):

    producer.produce(
        topic="vitals.raw",
        key="P001",
        value=json.dumps(event)
    )

    print(
        f"Published copy {i + 1}: {event_id}"
    )


remaining = producer.flush(timeout=30)

if remaining:
    raise RuntimeError(
        f"{remaining} messages remain undelivered."
    )

print("Duplicate test completed.")