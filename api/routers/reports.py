from fastapi import (
    APIRouter,
    HTTPException
)

from api.db import get_connection


router = APIRouter(
    prefix="/api/reports",
    tags=["Daily Reports"]
)


@router.get("/daily/{day}")
def daily_report(
    day: int
):

    if day < 1:

        raise HTTPException(
            status_code=400,
            detail="Day must be >= 1"
        )

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT

                    patient_id,

                    simulated_day,

                    avg_heart_rate,

                    min_spo2,

                    max_temperature,

                    reading_count,

                    lab_day,

                    hemoglobin,
                    wbc,
                    creatinine,

                    lab_test_count,
                    abnormal_lab_count,
                    lab_status,

                    concern_level,

                    consolidated_at

                FROM patient_daily_summary

                WHERE simulated_day = %s

                ORDER BY patient_id
                """,

                (day,)
            )

            rows = cursor.fetchall()

            if not rows:

                raise HTTPException(
                    status_code=404,
                    detail=(
                        "No consolidated report "
                        "found for this day"
                    )
                )

            complete = sum(
                row["lab_status"]
                == "COMPLETE"
                for row in rows
            )

            partial = sum(
                row["lab_status"]
                == "PARTIAL"
                for row in rows
            )

            missing = sum(
                row["lab_status"]
                == "MISSING"
                for row in rows
            )

            return {

                "simulated_day":
                    day,

                "patient_count":
                    len(rows),

                "lab_status_summary": {

                    "complete":
                        complete,

                    "partial":
                        partial,

                    "missing":
                        missing
                },

                "patients":
                    rows
            }

    finally:

        connection.close()


@router.get("/daily")
def available_daily_reports():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT

                    simulated_day,

                    COUNT(*) AS patient_count,

                    MAX(
                        consolidated_at
                    ) AS generated_at

                FROM patient_daily_summary

                GROUP BY simulated_day

                ORDER BY simulated_day DESC
                """
            )

            rows = cursor.fetchall()

            return {
                "reports": rows
            }

    finally:

        connection.close()