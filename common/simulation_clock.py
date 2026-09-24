
import os
import time

from datetime import datetime, timedelta, timezone


SIMULATION_START = datetime(
    2026, 9, 22,
    tzinfo=timezone.utc
)

REAL_SECONDS_PER_DAY = 300

SIMULATED_SECONDS_PER_DAY = 86400

TIME_SCALE = (
    SIMULATED_SECONDS_PER_DAY /
    REAL_SECONDS_PER_DAY
)


class SimulationClock:

    def __init__(self):

        start = os.getenv(
            "SIMULATION_REAL_START"
        )

        if not start:

            raise ValueError(
                "SIMULATION_REAL_START is missing."
            )

        self.real_start = datetime.fromisoformat(
            start
        )

        if (
            self.real_start.tzinfo is None
            or self.real_start.utcoffset() is None
        ):
            raise ValueError(
                "SIMULATION_REAL_START must "
                "include a timezone offset."
            )

    def now(self):

        real_now = datetime.now(timezone.utc)

        elapsed = (
            real_now - self.real_start
        ).total_seconds()

        simulated_seconds = (
            elapsed * TIME_SCALE
        )

        return (
            SIMULATION_START +
            timedelta(seconds=simulated_seconds)
        )

    def current_day(self):

        elapsed = (
            datetime.now(timezone.utc) -
            self.real_start
        ).total_seconds()

        return (
            int(
                elapsed //
                REAL_SECONDS_PER_DAY
            ) + 1
        )

    def batch_id(self):

        return (
            f"day_{self.current_day():03d}"
        )