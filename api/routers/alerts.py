from fastapi import APIRouter

from api.db import get_connection


router = APIRouter(
    prefix="/api/alerts",
    tags=["Patient Alerts"]
)


@router.get("")
def recent_alerts(
    limit: int = 20
):

    limit = max(
        1,
        min(limit, 100)
    )

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT

                    patient_id,

                    heart_rate,
                    spo2,
                    temperature,

                    high_heart_rate,
                    low_spo2,
                    high_temperature,

                    event_time,
                    simulated_day

                FROM vital_events

                WHERE has_alert = TRUE

                ORDER BY event_time DESC

                LIMIT %s
                """,

                (limit,)
            )

            rows = cursor.fetchall()

            return {
                "count": len(rows),
                "alerts": rows
            }

    finally:

        connection.close()