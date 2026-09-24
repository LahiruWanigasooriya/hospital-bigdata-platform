import os
import json
from datetime import datetime, timezone

import psycopg2


DB_CONFIG = {
    "host": os.getenv(
        "POSTGRES_HOST",
        "localhost"
    ),
    "port": os.getenv(
        "POSTGRES_PORT",
        "5433"
    ),
    "dbname": os.getenv(
        "POSTGRES_DB",
        "hospital_db"
    ),
    "user": os.getenv(
        "POSTGRES_USER",
        "hospital"
    ),
    "password": os.getenv(
        "POSTGRES_PASSWORD",
        "hospital_dev_password"
    )
}


STALE_SECONDS = 30


def log_event(level, event, **details):

    log = {
        "timestamp":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "level": level,

        "component":
            "health_monitor",

        "event": event,

        **details
    }

    print(
        json.dumps(log)
    )


def main():

    connection = psycopg2.connect(
        **DB_CONFIG
    )

    try:

        with connection:

            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT
                        MAX(stored_at),
                        COUNT(*)
                    FROM vital_events
                    """
                )

                (
                    latest_event,
                    total_events
                ) = cursor.fetchone()

                now = datetime.now(
                    timezone.utc
                )

                if latest_event is None:

                    status = "UNHEALTHY"

                    age_seconds = None

                    message = (
                        "No vital events "
                        "have been received."
                    )

                else:

                    age_seconds = (
                        now - latest_event
                    ).total_seconds()

                    if age_seconds > STALE_SECONDS:

                        status = "UNHEALTHY"

                        message = (
                            "Vital stream is stale. "
                            f"Last event stored "
                            f"{age_seconds:.1f} "
                            "seconds ago."
                        )

                        cursor.execute(
                            """
                            SELECT alert_id
                            FROM pipeline_alerts

                            WHERE alert_type =
                                'VITAL_STREAM_STALE'

                            AND resolved = FALSE

                            LIMIT 1
                            """
                        )

                        existing_alert = cursor.fetchone()

                        if existing_alert is None:

                            cursor.execute(
                                """
                                INSERT INTO pipeline_alerts (

                                    alert_type,
                                    severity,
                                    component,
                                    message

                                )

                                VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s
                                )
                                """,

                                (
                                    "VITAL_STREAM_STALE",
                                    "WARNING",
                                    "vital_stream",
                                    message
                                )
                            )

                    else:

                        status = "HEALTHY"

                        message = (
                            "Vital stream is active."
                        )

                        cursor.execute(
                            """
                            UPDATE pipeline_alerts

                            SET resolved = TRUE

                            WHERE alert_type =
                                'VITAL_STREAM_STALE'

                            AND resolved = FALSE
                            """
                        )

            log_event(
                "INFO"
                if status == "HEALTHY"
                else "WARNING",

                "vital_stream_health",

                status=status,

                total_events=total_events,

                age_seconds=age_seconds
            )

            print()
            print(f"Status: {status}")
            print(message)

    finally:

        connection.close()


if __name__ == "__main__":
    main()