
import json
import random
import time
import os
import uuid

from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

from common.simulation_clock import SimulationClock, SIMULATION_START

# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

clock = SimulationClock()

KAFKA_SERVER = os.getenv(
    "KAFKA_HOST_BOOTSTRAP_SERVERS",
    "localhost:9094"
)

TOPIC = "vitals.raw"

PATIENT_COUNT = 20
EMISSION_INTERVAL = 2


producer = Producer({
    "bootstrap.servers": KAFKA_SERVER,
    "client.id": "hospital-vitals-producer"
})


# --------------------------------------------------
# Generate synthetic patient vitals
# --------------------------------------------------

def generate_vitals(patient_id):

    heart_rate = random.randint(60, 100)
    spo2 = random.randint(95, 100)

    systolic_bp = random.randint(100, 130)
    diastolic_bp = random.randint(60, 85)

    temperature = round(
        random.uniform(36.1, 37.2),
        1
    )

    # Occasional synthetic abnormal scenario

    if random.random() < 0.05:

        heart_rate = random.randint(120, 150)
        spo2 = random.randint(85, 92)

        temperature = round(
            random.uniform(38.0, 39.5),
            1
        )

   
    simulated_time = clock.now()

    day_number = (
        simulated_time.date() - SIMULATION_START.date()
    ).days + 1

    return {
        "event_id": str(uuid.uuid4()),
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "spo2": spo2,
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "temperature": temperature,

        "timestamp": (
            simulated_time.isoformat()
        ),

        "simulated_day": day_number,

        "batch_id": (
            f"day_{day_number:03d}"
        ),

        "ingested_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        )
    }


# --------------------------------------------------
# Kafka delivery callback
# --------------------------------------------------

def delivery_report(err, msg):

    if err is not None:

        print(f"Delivery failed: {err}")

    else:

        print(
            f"Delivered to {msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# --------------------------------------------------
# Main producer
# --------------------------------------------------

def main():

    patients = [
        f"P{i:03d}"
        for i in range(1, PATIENT_COUNT + 1)
    ]

    print("Starting hospital vitals producer...")

    try:
        print("Waiting for simulation start...")

        while clock.current_day() < 1:
         time.sleep(1)

        while True:

            for patient_id in patients:

                event = generate_vitals(patient_id)

                producer.produce(
                    topic=TOPIC,
                    key=patient_id.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report
                )

                producer.poll(0)

                print(
                    f"Generated: {patient_id} "
                    f"HR={event['heart_rate']} "
                    f"SpO2={event['spo2']}"
                )

            time.sleep(EMISSION_INTERVAL)

    except KeyboardInterrupt:

        print("Stopping producer...")

    finally:

        remaining = producer.flush(timeout=30)

        if remaining > 0:
            print(
                f"Warning: {remaining} messages "
                "remain undelivered."
            )


if __name__ == "__main__":
    main()