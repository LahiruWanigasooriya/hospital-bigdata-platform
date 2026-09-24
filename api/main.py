from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import (
    ward,
    patients,
    alerts,
    reports
)

from api.db import get_connection


app = FastAPI(

    title=(
        "Hospital Patient "
        "Monitoring API"
    ),

    description=(
        "Serving API for the "
        "hospital big-data platform"
    ),

    version="1.0.0"
)


app.add_middleware(

    CORSMiddleware,

    allow_origins=[
        "http://localhost:5173"
    ],

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"]
)


app.include_router(
    ward.router
)

app.include_router(
    patients.router
)

app.include_router(
    alerts.router
)

app.include_router(
    reports.router
)


@app.get("/")
def root():

    return {
        "service":
            "Hospital Monitoring API",

        "status":
            "running"
    }


@app.get("/health")
def health():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    EXTRACT(
                        EPOCH FROM (
                            NOW() -
                            MAX(stored_at)
                        )
                    ) AS age_seconds
                FROM vital_events
                """
            )

            result = cursor.fetchone()

            age = result[
                "age_seconds"
            ]

            if age is None:

                return {
                    "status":
                        "unhealthy",

                    "reason":
                        "no vital events"
                }

            age = float(age)

            return {

                "status":
                    "healthy"
                    if age <= 30
                    else "unhealthy",

                "last_vital_age_seconds":
                    round(age, 2)
            }

    finally:

        connection.close()


@app.get("/metrics/current")
def current_metrics():

    connection = get_connection()

    try:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    MAX(simulated_day)
                    AS day
                FROM vital_events
                """
            )

            result = cursor.fetchone()

            day = result["day"]

            if day is None:

                return {
                    "simulated_day": None,
                    "vital_events": 0,
                    "abnormal_events": 0,
                    "active_patients": 0
                }


            cursor.execute(
                """
                SELECT

                    COUNT(*)
                        AS vital_events,

                    COUNT(*) FILTER (
                        WHERE has_alert
                    )
                        AS abnormal_events,

                    COUNT(
                        DISTINCT patient_id
                    )
                        AS active_patients

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