import os
import json

from datetime import (
    datetime,
    timezone
)

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
                        MAX(simulated_day)
                    FROM vital_events
                    """
                )

                day = cursor.fetchone()[0]

                if day is None:

                    print(
                        "No vital data available."
                    )

                    return


                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM vital_events
                    WHERE simulated_day = %s
                    """,
                    (day,)
                )

                vital_count = (
                    cursor.fetchone()[0]
                )


                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM vital_events

                    WHERE simulated_day = %s

                    AND has_alert = TRUE
                    """,
                    (day,)
                )

                abnormal_count = (
                    cursor.fetchone()[0]
                )


                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT patient_id)
                    FROM vital_events
                    WHERE simulated_day = %s
                    """,
                    (day,)
                )

                patient_count = (
                    cursor.fetchone()[0]
                )


                metrics = {

                    "vital_events":
                        vital_count,

                    "abnormal_events":
                        abnormal_count,

                    "active_patients":
                        patient_count
                }


                for name, value in metrics.items():

                    cursor.execute(
                        """
                        INSERT INTO pipeline_metrics (

                            component,
                            metric_name,
                            metric_value,
                            simulated_day

                        )

                        VALUES (
                            %s,
                            %s,
                            %s,
                            %s
                        )
                        """,

                        (
                            "vital_pipeline",
                            name,
                            value,
                            day
                        )
                    )


                log = {

                    "timestamp":
                        datetime.now(
                            timezone.utc
                        ).isoformat(),

                    "level": "INFO",

                    "component":
                        "metrics_collector",

                    "event":
                        "metrics_collected",

                    "simulated_day":
                        day,

                    **metrics
                }

                print(
                    json.dumps(log)
                )

    finally:

        connection.close()


if __name__ == "__main__":
    main()