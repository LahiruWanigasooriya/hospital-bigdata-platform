import os

import psycopg2
from psycopg2.extras import RealDictCursor


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


def get_connection():

    return psycopg2.connect(
        **DB_CONFIG,
        cursor_factory=RealDictCursor
    )