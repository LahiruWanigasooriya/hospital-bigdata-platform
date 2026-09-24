import os

from datetime import datetime, timedelta

import psycopg2

from airflow import DAG
from airflow.decorators import task


DB_CONFIG = {
    "host": os.getenv(
        "HOSPITAL_DB_HOST",
        "postgres"
    ),
    "port": os.getenv(
        "HOSPITAL_DB_PORT",
        "5432"
    ),
    "dbname": os.getenv(
        "HOSPITAL_DB_NAME",
        "hospital_db"
    ),
    "user": os.getenv(
        "HOSPITAL_DB_USER",
        "hospital"
    ),
    "password": os.getenv(
        "HOSPITAL_DB_PASSWORD"
    )
}


def get_connection():

    return psycopg2.connect(
        **DB_CONFIG
    )


default_args = {

    "owner": "hospital-data-team",

    "retries": 2,

    "retry_delay":
        timedelta(seconds=30)
}


with DAG(

    dag_id="hospital_daily_pipeline",

    description=(
        "Daily hospital patient "
        "consolidation pipeline"
    ),

    start_date=datetime(
        2026,
        9,
        22
    ),

    schedule=None,

    catchup=False,

    default_args=default_args,

    tags=[
        "hospital",
        "big-data"
    ]

) as dag:


    @task
    def determine_day():

        connection = get_connection()

        try:

            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT MAX(simulated_day)
                    FROM vital_events
                    """
                )

                day = cursor.fetchone()[0]

                if day is None:

                    raise ValueError(
                        "No vital events available"
                    )

                print(
                    f"Target day: {day}"
                )

                return day

        finally:

            connection.close()


    @task
    def check_vital_data(day):

        connection = get_connection()

        try:

            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM vital_events
                    WHERE simulated_day = %s
                    """,
                    (day,)
                )

                count = cursor.fetchone()[0]

                print(
                    f"Vital events: {count}"
                )

                if count == 0:

                    raise ValueError(
                        "No vital events found"
                    )

                return day

        finally:

            connection.close()


    @task
    def calculate_vitals(day):

        connection = get_connection()

        try:

            with connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        """
                        SELECT
                            calculate_daily_vitals(%s)
                        """,
                        (day,)
                    )

                    affected = (
                        cursor.fetchone()[0]
                    )

                    print(
                        "Daily vital rows: "
                        f"{affected}"
                    )

            return day

        finally:

            connection.close()


    @task
    def check_previous_labs(day):

        previous_day = day - 1

        connection = get_connection()

        try:

            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM laboratory_results
                    WHERE lab_day = %s
                    """,
                    (previous_day,)
                )

                count = cursor.fetchone()[0]

                print(
                    f"Previous-day labs: {count}"
                )

                # Missing labs do not fail pipeline.
                # Consolidation records MISSING status.

                return day

        finally:

            connection.close()


    @task
    def consolidate(day):

        connection = get_connection()

        try:

            with connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        """
                        SELECT
                            consolidate_patient_day(%s)
                        """,
                        (day,)
                    )

                    affected = (
                        cursor.fetchone()[0]
                    )

                    print(
                        "Consolidated rows: "
                        f"{affected}"
                    )

            return day

        finally:

            connection.close()


    @task
    def validate_output(day):

        connection = get_connection()

        try:

            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT
                        COUNT(*),
                        COUNT(*) FILTER (
                            WHERE lab_status =
                            'COMPLETE'
                        ),
                        COUNT(*) FILTER (
                            WHERE lab_status =
                            'PARTIAL'
                        ),
                        COUNT(*) FILTER (
                            WHERE lab_status =
                            'MISSING'
                        )
                    FROM patient_daily_summary
                    WHERE simulated_day = %s
                    """,
                    (day,)
                )

                (
                    total,
                    complete,
                    partial,
                    missing
                ) = cursor.fetchone()

                print(
                    f"Day {day} report:"
                )

                print(
                    f"Total patients: {total}"
                )

                print(
                    f"Complete: {complete}"
                )

                print(
                    f"Partial: {partial}"
                )

                print(
                    f"Missing: {missing}"
                )

                if total == 0:

                    raise ValueError(
                        "Daily report is empty"
                    )

        finally:

            connection.close()


    day = determine_day()

    checked = check_vital_data(day)

    calculated = calculate_vitals(
        checked
    )

    labs_checked = check_previous_labs(
        calculated
    )

    consolidated = consolidate(
        labs_checked
    )

    validate_output(
        consolidated
    )