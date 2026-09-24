from fastapi import (
    APIRouter,
    HTTPException
)

from api.db import get_connection


router = APIRouter(
    prefix="/api/patients",
    tags=["Patients"]
)


@router.get("")
def list_patients():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT DISTINCT patient_id
                FROM vital_events
                ORDER BY patient_id
                """
            )

            rows = cursor.fetchall()

            return {
                "patients": [
                    row["patient_id"]
                    for row in rows
                ]
            }

    finally:

        connection.close()


@router.get("/{patient_id}/latest")
def latest_patient_status(
    patient_id: str
):

    patient_id = patient_id.upper()

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT

                    patient_id,

                    heart_rate,
                    spo2,

                    systolic_bp,
                    diastolic_bp,

                    temperature,

                    event_time,

                    simulated_day,

                    high_heart_rate,
                    low_spo2,
                    high_temperature,
                    has_alert

                FROM vital_events

                WHERE patient_id = %s

                ORDER BY event_time DESC

                LIMIT 1
                """,

                (patient_id,)
            )

            row = cursor.fetchone()

            if row is None:

                raise HTTPException(
                    status_code=404,
                    detail="Patient not found"
                )

            return row

    finally:

        connection.close()


@router.get("/{patient_id}/history")
def patient_history(
    patient_id: str,
    limit: int = 50
):

    patient_id = patient_id.upper()

    limit = max(
        1,
        min(limit, 500)
    )

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT

                    heart_rate,
                    spo2,
                    systolic_bp,
                    diastolic_bp,
                    temperature,
                    event_time,
                    has_alert

                FROM vital_events

                WHERE patient_id = %s

                ORDER BY event_time DESC

                LIMIT %s
                """,

                (
                    patient_id,
                    limit
                )
            )

            rows = cursor.fetchall()

            if not rows:

                raise HTTPException(
                    status_code=404,
                    detail="Patient not found"
                )

            # Display oldest -> newest
            rows.reverse()

            return {
                "patient_id":
                    patient_id,

                "readings":
                    rows
            }

    finally:

        connection.close()