from fastapi import APIRouter

from api.db import get_connection


router = APIRouter(
    prefix="/api/ward",
    tags=["Ward Monitoring"]
)


@router.get("/overview")
def ward_overview():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            # Latest simulated day
            cursor.execute(
                """
                SELECT MAX(simulated_day) AS day
                FROM vital_events
                """
            )

            result = cursor.fetchone()

            day = result["day"]

            if day is None:

                return {
                    "simulated_day": None,
                    "active_patients": 0,
                    "total_readings": 0,
                    "abnormal_readings": 0,
                    "patients_with_alerts": 0
                }


            cursor.execute(
                """
                SELECT

                    COUNT(*) AS total_readings,

                    COUNT(
                        DISTINCT patient_id
                    ) AS active_patients,

                    COUNT(*) FILTER (
                        WHERE has_alert = TRUE
                    ) AS abnormal_readings,

                    COUNT(
                        DISTINCT patient_id
                    ) FILTER (
                        WHERE has_alert = TRUE
                    ) AS patients_with_alerts

                FROM vital_events

                WHERE simulated_day = %s
                """,

                (day,)
            )

            metrics = cursor.fetchone()

            return {

                "simulated_day":
                    day,

                **metrics
            }

    finally:

        connection.close()