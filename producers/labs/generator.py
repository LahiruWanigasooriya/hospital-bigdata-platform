
import csv
import random
import sys
from pathlib import Path
from datetime import timedelta
from common.simulation_clock import SIMULATION_START


# ---------------------------------------
# Configuration
# ---------------------------------------

PATIENT_COUNT = 20

OUTPUT_DIR = Path("data/labs")


LAB_TESTS = {
    "Hemoglobin": {
        "min": 12.0,
        "max": 16.0,
        "unit": "g/dL"
    },

    "WBC": {
        "min": 4000,
        "max": 11000,
        "unit": "cells/uL"
    },

    "Creatinine": {
        "min": 0.6,
        "max": 1.3,
        "unit": "mg/dL"
    }
}


# ---------------------------------------
# Generate laboratory values
# ---------------------------------------

def generate_lab_value(test):

    minimum = test["min"]
    maximum = test["max"]

    # 10% chance of an abnormal value
    if random.random() < 0.10:

        if random.random() < 0.5:
            return round(minimum * 0.8, 2)

        return round(maximum * 1.2, 2)

    return round(
        random.uniform(minimum, maximum),
        2
    )


# ---------------------------------------
# Generate daily CSV
# ---------------------------------------

def generate_daily_labs(day_number):

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = (
        OUTPUT_DIR /
        f"labs_day_{day_number:03d}.csv"
    )

    if filename.exists():
        raise FileExistsError(
            f"{filename} already exists. "
            "Use a new day number."
        )

    # Same day number produces the same values
    random.seed(day_number)

    simulated_date = (
        SIMULATION_START +
        timedelta(days=day_number - 1)
    )

    batch_id = f"day_{day_number:03d}"

    records = []

    for i in range(1, PATIENT_COUNT + 1):

        patient_id = f"P{i:03d}"

        for test_name, test in LAB_TESTS.items():

            result = generate_lab_value(test)

            record = {
                "batch_id": batch_id,
                "patient_id": patient_id,
                "test_type": test_name,
                "result_value": result,
                "reference_range": (
                    f"{test['min']}-{test['max']}"
                ),
                "unit": test["unit"],
                "collected_at": (
                    simulated_date.isoformat()
                )
            }

            records.append(record)

    fieldnames = [
        "batch_id",
        "patient_id",
        "test_type",
        "result_value",
        "reference_range",
        "unit",
        "collected_at"
    ]

    # Write to a temporary file first.
    # Rename only after writing completes.

    temporary_file = filename.with_suffix(".tmp")

    with open(
        temporary_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(records)

    temporary_file.replace(filename)

    print(f"Generated: {filename}")
    print(f"Batch: {batch_id}")
    print(f"Records: {len(records)}")

    return filename


# ---------------------------------------
# Main
# ---------------------------------------

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(
            "Usage: python generator.py <day_number>"
        )
        sys.exit(1)

    try:
        day_number = int(sys.argv[1])

        if day_number < 1:
            raise ValueError(
                "Day number must be positive."
            )

        generate_daily_labs(day_number)

    except (ValueError, FileExistsError) as error:
        print(f"Error: {error}")
        sys.exit(1)