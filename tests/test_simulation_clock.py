
from dotenv import load_dotenv

from common.simulation_clock import (
    SimulationClock,
    TIME_SCALE
)


load_dotenv()

clock = SimulationClock()

print("Time scale:", TIME_SCALE)

print(
    "Simulated time:",
    clock.now().isoformat()
)

print(
    "Simulated day:",
    clock.current_day()
)

print(
    "Batch ID:",
    clock.batch_id()
)