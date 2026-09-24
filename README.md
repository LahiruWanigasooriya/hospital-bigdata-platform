# Hospital Big Data Platform

An end-to-end data engineering platform for continuous hospital patient
vital-sign monitoring and daily laboratory-result consolidation.

The system simulates bedside monitors for 20 patients, ingests
continuous vital readings through Apache Kafka, processes them with
Apache Spark Structured Streaming, stores validated and deduplicated
data in PostgreSQL, orchestrates daily historical processing with Apache
Airflow, exposes data through FastAPI, and provides a React monitoring
dashboard.

> **Important:** All patient data, thresholds, alerts, and concern
> levels in this project are synthetic and intended for demonstration
> only. They are not clinical rules and must not be used for medical
> decision-making.

------------------------------------------------------------------------

# 1. Complete Setup Guide

This section explains how to run the project from a fresh GitHub clone.

## 1.1 Prerequisites

Install the following before starting:

-   **Git**
-   **Docker Desktop** with Docker Compose
-   **Python 3.10+**
-   **Node.js 18+** and npm
-   A terminal such as PowerShell, Windows Terminal, Bash, or the VS
    Code terminal

Recommended:

-   VS Code
-   At least 8 GB RAM available for Docker and the local applications
-   Sufficient free disk space for Kafka, Spark, PostgreSQL, and Airflow
    images

Check the installations:

``` bash
git --version
docker --version
docker compose version
python --version
node --version
npm --version
```

------------------------------------------------------------------------

## 1.2 Clone the Repository

``` bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd hospital-bigdata-platform
```

Replace `<YOUR_GITHUB_REPOSITORY_URL>` with the actual repository URL.

------------------------------------------------------------------------

## 1.3 Create the Environment File

Create a `.env` file in the project root.

Example:

``` env
POSTGRES_USER=hospital
POSTGRES_PASSWORD=hospital_dev_password
POSTGRES_DB=hospital_db

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_HOST_BOOTSTRAP_SERVERS=localhost:9094

# Set this when starting a new simulation.
# Example:
SIMULATION_REAL_START=2026-09-24T00:00:00+00:00
```

`SIMULATION_REAL_START` represents the real-world starting instant used
by the compressed simulation clock. Use a current/recent UTC timestamp
when beginning a fresh simulation.

Do not commit `.env` to Git.

------------------------------------------------------------------------

## 1.4 Create the Python Virtual Environment

### Windows PowerShell

``` powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### Linux/macOS

``` bash
python3 -m venv .venv
source .venv/bin/activate
```

Upgrade pip:

``` bash
python -m pip install --upgrade pip
```

Install the Python dependencies used by the host-side producers,
monitoring tools, and API:

``` bash
pip install confluent-kafka python-dotenv psycopg2-binary fastapi "uvicorn[standard]"
```

If the repository contains a `requirements.txt`, prefer:

``` bash
pip install -r requirements.txt
```

------------------------------------------------------------------------

## 1.5 Start the Core Docker Infrastructure

Build and start PostgreSQL, Kafka, and Spark:

``` bash
docker compose up -d --build postgres kafka spark
```

Check container status:

``` bash
docker compose ps
```

The main internal/host connections are:

  Service      Container/Internal   Host
  ------------ -------------------- ------------------
  PostgreSQL   `postgres:5432`      `localhost:5433`
  Kafka        `kafka:9092`         `localhost:9094`
  Spark        Docker network       Docker container

Wait until PostgreSQL and Kafka are ready before continuing.

------------------------------------------------------------------------

## 1.6 Verify/Create Kafka Topics

List topics:

``` bash
docker exec hospital-kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

The project requires:

``` text
vitals.raw
labs.raw
```

If they do not already exist, create them:

``` bash
docker exec hospital-kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic vitals.raw \
  --partitions 3 \
  --replication-factor 1
```

``` bash
docker exec hospital-kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic labs.raw \
  --partitions 3 \
  --replication-factor 1
```

On Windows PowerShell, enter each command on one line if backslash line
continuation is not supported.

Kafka records are keyed by `patient_id`, helping preserve patient-level
ordering within a partition.

------------------------------------------------------------------------

## 1.7 Initialize the Hospital Database

For a completely fresh PostgreSQL volume, SQL files mounted under the
PostgreSQL initialization directory may run automatically according to
the Docker Compose configuration.

Verify the tables:

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "\dt"
```

The completed system should contain tables such as:

``` text
laboratory_results
vital_aggregations
vital_events
daily_vital_summary
patient_daily_summary
pipeline_metrics
pipeline_alerts
```

If the database volume already existed before new SQL initialization
files were added, PostgreSQL will **not** automatically rerun those
initialization scripts. Apply the relevant files manually, for example:

``` powershell
Get-Content -Raw database\init\06_vital_events.sql |
  docker exec -i hospital-postgres psql -v ON_ERROR_STOP=1 -U hospital -d hospital_db
```

Use the same pattern for the remaining SQL files in `database/init/` in
their intended numeric order.

For Bash:

``` bash
docker exec -i hospital-postgres psql -v ON_ERROR_STOP=1 -U hospital -d hospital_db < database/init/06_vital_events.sql
```

------------------------------------------------------------------------

## 1.8 Create the Airflow Metadata Database

Airflow uses a separate metadata database and should not store its
internal metadata in `hospital_db`.

Create it once:

``` bash
docker exec hospital-postgres psql -U hospital -d postgres -c "CREATE DATABASE airflow_db;"
```

If `airflow_db` already exists, do not recreate it.

------------------------------------------------------------------------

## 1.9 Initialize and Start Airflow

Initialize Airflow:

``` bash
docker compose up airflow-init
```

Then start the webserver and scheduler:

``` bash
docker compose up -d airflow-webserver airflow-scheduler
```

Verify:

``` bash
docker compose ps
```

Open:

``` text
http://localhost:8081
```

Default local development credentials configured by the project:

``` text
Username: admin
Password: admin
```

These credentials are for local development/demo use only.

Check that the DAG is available:

``` bash
docker exec hospital-airflow-scheduler airflow dags list
```

Check DAG import errors:

``` bash
docker exec hospital-airflow-scheduler airflow dags list-import-errors
```

The expected DAG is:

``` text
hospital_daily_pipeline
```

------------------------------------------------------------------------

## 1.10 Start the Durable Vital-Event Spark Job

The primary storage stream validates vital events and persists unique
events into PostgreSQL.

Run:

``` bash
docker exec -it hospital-spark /opt/spark/bin/spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /opt/project/spark/jobs/store_vital_events.py
```

Keep this terminal running.

The durable `vital_events` table uses `event_id` as its primary key.
This prevents replayed or duplicated Kafka events from producing
duplicate persisted events.

------------------------------------------------------------------------

## 1.11 Start the Vital-Signs Producer

Open another terminal, activate the Python environment, and run:

``` bash
python -m producers.vitals.producer
```

The producer continuously creates synthetic readings for patients `P001`
through `P020`.

Typical fields include:

``` text
event_id
patient_id
heart_rate
spo2
systolic_bp
diastolic_bp
temperature
timestamp
simulated_day
batch_id
ingested_at
```

Verify that data is reaching PostgreSQL:

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT COUNT(*) FROM vital_events;"
```

Run the command again after a few seconds. The count should increase.

------------------------------------------------------------------------

## 1.12 Optional: Start the Rolling Vital Aggregation Job

The rolling aggregation path is used for near-real-time trend analysis.

Run the existing aggregation storage job:

``` bash
docker exec -it hospital-spark /opt/spark/bin/spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /opt/project/spark/jobs/store_vital_aggregations.py
```

This produces rolling window data in:

``` text
vital_aggregations
```

The rolling windows are used for near-real-time monitoring only. Final
whole-day statistics are calculated separately from deduplicated
`vital_events` so overlapping windows cannot inflate daily counts.

------------------------------------------------------------------------

## 1.13 Generate Daily Laboratory Data

Generate a laboratory CSV for a simulated day:

``` bash
python -m producers.labs.generator 1
```

This creates a file similar to:

``` text
data/labs/labs_day_001.csv
```

Each complete day contains:

``` text
20 patients × 3 tests = 60 laboratory records
```

The tests are:

-   Hemoglobin
-   WBC
-   Creatinine

Generate later days by changing the day number:

``` bash
python -m producers.labs.generator 2
python -m producers.labs.generator 3
```

The generator intentionally refuses to silently overwrite an existing
daily file.

------------------------------------------------------------------------

## 1.14 Start the Laboratory Storage Spark Job

Before publishing a new lab batch, start:

``` bash
docker exec -it hospital-spark /opt/spark/bin/spark-submit --master "local[*]" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 /opt/project/spark/jobs/store_labs.py
```

Keep it running.

------------------------------------------------------------------------

## 1.15 Publish the Daily Laboratory File

Example for Day 1:

``` bash
python producers/labs/publisher.py data/labs/labs_day_001.csv
```

Verify:

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT lab_day, COUNT(*) AS records, COUNT(DISTINCT patient_id) AS patients FROM laboratory_results GROUP BY lab_day ORDER BY lab_day;"
```

A complete lab day should contain:

``` text
60 records
20 patients
```

Republishing the same file should not increase the logical record count
because `lab_id` is deterministic and PostgreSQL applies idempotent
insertion.

------------------------------------------------------------------------

## 1.16 Run the Daily Airflow Pipeline

The daily Airflow workflow performs the historical/reporting sequence:

``` text
determine_day
    ↓
check_vital_data
    ↓
calculate_daily_vitals
    ↓
check_previous_labs
    ↓
consolidate
    ↓
validate_output
```

Trigger it from the Airflow UI or CLI:

``` bash
docker exec hospital-airflow-scheduler airflow dags trigger hospital_daily_pipeline
```

The daily consolidation follows:

``` text
Day N vital statistics
        +
Day N-1 laboratory results
        ↓
Day N patient_daily_summary
```

If previous-day laboratory data is unavailable, the report is still
produced and the lab status is represented as `MISSING`. Incomplete
panels are represented as `PARTIAL`.

------------------------------------------------------------------------

## 1.17 Start the Monitoring Tools

### Health check

``` bash
python -m monitoring.health_check
```

When fresh events are being persisted:

``` text
Status: HEALTHY
```

If no new vital event has been persisted for the configured stale
threshold:

``` text
Status: UNHEALTHY
```

### Metrics collection

``` bash
python -m monitoring.collect_metrics
```

Metrics are persisted to:

``` text
pipeline_metrics
```

Health alerts are stored in:

``` text
pipeline_alerts
```

------------------------------------------------------------------------

## 1.18 Start FastAPI

From the project root:

``` bash
uvicorn api.main:app --reload --port 8000
```

Open the interactive API documentation:

``` text
http://localhost:8000/docs
```

Useful endpoints include:

``` text
GET /
GET /health
GET /metrics/current

GET /api/ward/overview

GET /api/patients
GET /api/patients/{patient_id}/latest
GET /api/patients/{patient_id}/history

GET /api/alerts

GET /api/reports/daily
GET /api/reports/daily/{day}
```

------------------------------------------------------------------------

## 1.19 Install and Start the React Dashboard

Open another terminal:

``` bash
cd dashboard
npm install
npm run dev
```

Open the URL shown by Vite, normally:

``` text
http://localhost:5173
```

The dashboard communicates with FastAPI at:

``` text
http://localhost:8000
```

The backend CORS configuration permits the local Vite development
origin.

------------------------------------------------------------------------

## 1.20 Recommended Startup Order

For a normal development/demo run after the initial installation:

``` text
1. docker compose up -d
2. Verify Kafka/PostgreSQL/Spark/Airflow
3. Start store_vital_events.py
4. Optionally start store_vital_aggregations.py
5. Start store_labs.py when a lab batch will be published
6. Start the vital producer
7. Generate/publish required daily lab data
8. Trigger hospital_daily_pipeline when daily consolidation is required
9. Start FastAPI
10. Start React
11. Run monitoring checks/metrics as required
```

------------------------------------------------------------------------

## 1.21 Useful Verification Commands

### Docker

``` bash
docker compose ps
```

### Kafka topics

``` bash
docker exec hospital-kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

### Vital event count

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT simulated_day, COUNT(*) FROM vital_events GROUP BY simulated_day ORDER BY simulated_day;"
```

### Lab count

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT lab_day, COUNT(*) FROM laboratory_results GROUP BY lab_day ORDER BY lab_day;"
```

### Duplicate vital IDs

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT event_id, COUNT(*) FROM vital_events GROUP BY event_id HAVING COUNT(*) > 1;"
```

Expected:

``` text
0 rows
```

### Duplicate patient-day summaries

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT patient_id, simulated_day, COUNT(*) FROM patient_daily_summary GROUP BY patient_id, simulated_day HAVING COUNT(*) > 1;"
```

Expected:

``` text
0 rows
```

### Latest consolidated report

``` bash
docker exec hospital-postgres psql -U hospital -d hospital_db -c "SELECT patient_id, simulated_day, avg_heart_rate, min_spo2, max_temperature, lab_status, concern_level FROM patient_daily_summary ORDER BY simulated_day DESC, patient_id LIMIT 20;"
```

------------------------------------------------------------------------

## 1.22 Stopping the Project

Stop application processes with `Ctrl+C`.

Stop Docker services:

``` bash
docker compose down
```

To stop and remove containers **and project volumes/data**:

``` bash
docker compose down -v
```

> `docker compose down -v` deletes persisted Docker volume data. Use it
> only when you intentionally want a clean environment.

------------------------------------------------------------------------

# 2. Technology Stack and Technical Details

## 2.1 Technology Stack

  -----------------------------------------------------------------------
  Layer                   Technology              Responsibility
  ----------------------- ----------------------- -----------------------
  Data simulation         Python                  Generates continuous
                                                  vitals and daily lab
                                                  data

  Event ingestion         Apache Kafka            Decoupled, partitioned
                                                  event ingestion

  Stream processing       Apache Spark Structured Parsing, validation,
                          Streaming               transformations, alert
                                                  flags, windows

  Durable/serving storage PostgreSQL 16           Raw validated events,
                                                  aggregates, labs,
                                                  reports, metrics

  Workflow orchestration  Apache Airflow          Daily dependencies,
                                                  retries, aggregation
                                                  and consolidation

  Backend API             FastAPI                 REST API for monitoring
                                                  and reports

  Frontend                React + Vite            Interactive hospital
                                                  monitoring dashboard

  Charts                  Recharts                Patient vital trend
                                                  visualization

  Containerization        Docker + Docker Compose Reproducible local
                                                  multi-service
                                                  environment

  Python PostgreSQL       psycopg2                PostgreSQL
  client                                          writes/queries from
                                                  Spark helpers,
                                                  monitoring and API

  Kafka Python client     confluent-kafka         Producer-side Kafka
                                                  communication
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 2.2 Apache Kafka

Kafka is the ingestion backbone.

Topics:

``` text
vitals.raw
labs.raw
```

Both use multiple partitions, and records are keyed by `patient_id`.

### Why Kafka is used

-   Separates data producers from downstream processors.
-   Handles continuous event ingestion.
-   Supports partitioned processing.
-   Allows Spark consumers to recover using offsets/checkpoints.
-   Allows multiple downstream consumers without tightly coupling them
    to producers.

Host access:

``` text
localhost:9094
```

Docker-network access:

``` text
kafka:9092
```

------------------------------------------------------------------------

## 2.3 Apache Spark Structured Streaming

Spark is the main stream-processing engine.

Main responsibilities include:

-   Kafka consumption
-   JSON parsing
-   schema enforcement
-   data validation
-   invalid-event rejection
-   abnormal vital detection
-   event-time processing
-   watermarking
-   event deduplication
-   rolling aggregation
-   PostgreSQL persistence

Spark version used by the project:

``` text
Apache Spark 3.5.6
```

Kafka connector:

``` text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
```

Shared transformation logic is organized under:

``` text
spark/transformations/
```

rather than duplicating parsing and validation logic across every Spark
job.

------------------------------------------------------------------------

## 2.4 PostgreSQL

PostgreSQL is both the durable processing state store and serving
database.

Important tables include:

### `vital_events`

Validated continuous vital events.

Important property:

``` text
event_id PRIMARY KEY
```

This is the durable deduplication boundary.

Even if Kafka replays the same event, the same `event_id` cannot be
inserted twice.

### `laboratory_results`

Validated daily laboratory records.

Stores:

-   patient
-   simulated lab day
-   test type
-   result
-   reference range
-   unit
-   out-of-range status
-   Kafka metadata

### `vital_aggregations`

Near-real-time rolling vital windows.

Used for monitoring trends rather than final daily totals.

### `daily_vital_summary`

Non-overlapping whole-day statistics calculated directly from unique
`vital_events`.

Examples:

-   average/min/max heart rate
-   average/min SpO₂
-   average blood pressure
-   average/max temperature
-   reading count
-   alert count

### `patient_daily_summary`

Final patient/day consolidation.

Combines:

``` text
Day N whole-day vitals
+
Day N-1 laboratory data
```

It includes laboratory completeness and the synthetic concern level.

### `pipeline_metrics`

Stores observability metrics.

### `pipeline_alerts`

Stores pipeline-health alerts such as stale vital-stream detection.

------------------------------------------------------------------------

## 2.5 Apache Airflow

Airflow orchestrates daily historical/reporting work.

It does **not** process the continuous Kafka stream.

Spark handles streaming; Airflow handles workflow dependencies.

DAG:

``` text
hospital_daily_pipeline
```

Workflow:

``` text
determine_day
      ↓
check_vital_data
      ↓
calculate_daily_vitals
      ↓
check_previous_labs
      ↓
consolidate
      ↓
validate_output
```

The workflow is safe to rerun because daily result tables use
deterministic keys and upserts.

------------------------------------------------------------------------

## 2.6 FastAPI

FastAPI is the serving layer between PostgreSQL and the frontend.

Main responsibilities:

-   health endpoint
-   current pipeline/ward metrics
-   patient listing
-   latest patient state
-   patient history
-   abnormal-event API
-   available daily reports
-   consolidated daily patient report

Swagger/OpenAPI documentation is automatically available at:

``` text
http://localhost:8000/docs
```

------------------------------------------------------------------------

## 2.7 React and Vite

React provides the user-facing monitoring dashboard.

The dashboard includes:

-   active patient count
-   total reading count
-   abnormal reading count
-   patients with alerts
-   patient selector
-   latest vital readings
-   vital-history chart
-   recent alert feed
-   consolidated daily report
-   laboratory completeness status
-   synthetic concern level
-   LIVE/STALE pipeline indicator

The frontend polls the API periodically so live values update without
manually refreshing the page.

------------------------------------------------------------------------

## 2.8 Docker and Docker Compose

Docker is used to run infrastructure consistently without requiring
Kafka, Spark, PostgreSQL, and Airflow to be installed directly on the
host.

Typical containers include:

``` text
hospital-kafka
hospital-postgres
hospital-spark
hospital-airflow-webserver
hospital-airflow-scheduler
```

Docker Compose defines:

-   service networking
-   ports
-   environment variables
-   mounted project files
-   persistent storage
-   startup dependencies

This makes the environment reproducible across development machines.

------------------------------------------------------------------------

## 2.9 Simulation Clock

The platform compresses time so multiple logical hospital days can be
demonstrated quickly.

Core configuration:

``` text
Simulation start date: 2026-09-22 UTC
Real seconds per simulated day: 300
Simulated seconds per day: 86400
Time scale: 288×
```

Therefore:

``` text
5 real minutes = 1 simulated day
```

The shared simulation clock is implemented in:

``` text
common/simulation_clock.py
```

Both the streaming and daily data paths use the same logical timeline.

------------------------------------------------------------------------

# 3. Project Details --- End to End

## 3.1 Project Purpose

The Hospital Big Data Platform demonstrates how continuous patient
sensor readings can be combined with once-daily laboratory results in a
single data platform.

The system is designed around two different data-arrival patterns:

### Continuous stream

Bedside monitors continuously produce patient vital readings.

### Daily batch

A pathology/laboratory system produces one daily file containing patient
test results.

The platform provides both:

``` text
near-real-time monitoring
+
historical daily consolidation
```

------------------------------------------------------------------------

## 3.2 High-Level Architecture

``` text
                         ┌───────────────────────┐
                         │ Python Vital Producer │
                         └───────────┬───────────┘
                                     │
                                     ▼
                              Kafka: vitals.raw
                                     │
                                     ▼
                           Spark Structured Streaming
                            │                    │
                            │                    │
                            ▼                    ▼
                    vital_events        vital_aggregations
                  (durable unique)      (rolling trends)
                            │
                            ▼
                    daily_vital_summary
                            │
                            │
                            │
┌──────────────────┐        │
│ Daily Lab CSV    │        │
└────────┬─────────┘        │
         │                  │
         ▼                  │
   Kafka: labs.raw          │
         │                  │
         ▼                  │
 Spark Validation           │
         │                  │
         ▼                  │
 laboratory_results ────────┘
         │
         ▼
      Airflow
         │
         ▼
 patient_daily_summary
         │
         ▼
      FastAPI
         │
         ▼
 React Monitoring Dashboard


Observability:
pipeline_metrics
pipeline_alerts
structured logs
health checks
LIVE/STALE status
```

------------------------------------------------------------------------

## 3.3 Continuous Vital-Sign Pipeline

The vital producer simulates 20 patients:

``` text
P001 ... P020
```

Each patient produces readings containing:

``` text
heart_rate
spo2
systolic_bp
diastolic_bp
temperature
timestamp
```

Additional engineering metadata includes:

``` text
event_id
simulated_day
batch_id
ingested_at
```

Occasional synthetic abnormal values are generated to exercise
alert-processing logic.

Flow:

``` text
Python producer
      ↓
Kafka vitals.raw
      ↓
Spark
      ↓
Parse
      ↓
Validate
      ↓
Flag abnormal values
      ↓
Persist
      ↓
PostgreSQL vital_events
```

------------------------------------------------------------------------

## 3.4 Vital Validation

Spark validates incoming data before it becomes part of the serving
dataset.

Patient IDs are restricted to:

``` text
P001-P020
```

using the exact pattern:

``` text
^P(00[1-9]|01[0-9]|020)$
```

The processing layer also validates required fields and numeric values.

Invalid events are not treated as valid patient readings.

------------------------------------------------------------------------

## 3.5 Synthetic Vital Alerts

The project generates independent synthetic alert flags such as:

``` text
high_heart_rate
low_spo2
high_temperature
has_alert
```

Example demonstration thresholds include:

``` text
heart rate > 120
SpO₂ < 93
temperature >= 38°C
```

These thresholds exist only to demonstrate streaming alert processing.

The API does not recalculate these rules. Spark performs the processing
once and persists the result.

That creates a clear separation:

``` text
Spark = processing/business rules
FastAPI = serving
React = presentation
```

------------------------------------------------------------------------

## 3.6 Durable Vital Deduplication

Each vital event contains a UUID `event_id`.

Before final reporting, validated events are stored in:

``` text
vital_events
```

with:

``` text
PRIMARY KEY(event_id)
```

There are two levels of protection:

``` text
same Spark micro-batch
      ↓
dropDuplicates(event_id)

across micro-batches/replays/restarts
      ↓
PostgreSQL PRIMARY KEY(event_id)
```

This means final daily calculations operate on unique persisted source
events.

------------------------------------------------------------------------

## 3.7 Near-Real-Time Rolling Aggregation

The system also calculates patient-level event-time windows.

Current design:

``` text
Window length: 30 simulated minutes
Slide:         10 simulated minutes
Watermark:     10 simulated minutes
```

Example metrics include:

``` text
average heart rate
minimum SpO₂
maximum temperature
reading count
```

These windows intentionally overlap and are useful for recent trends.

They are **not summed to produce daily totals**, because doing so would
count the same event in multiple windows.

------------------------------------------------------------------------

## 3.8 Whole-Day Vital Aggregation

Daily statistics are calculated directly from:

``` text
vital_events
```

rather than from overlapping rolling windows.

Flow:

``` text
unique vital_events
       ↓
GROUP BY patient_id, simulated_day
       ↓
daily_vital_summary
```

This ensures:

``` text
one persisted event
=
one contribution to the daily aggregate
```

The daily table contains metrics such as:

``` text
avg_heart_rate
min_heart_rate
max_heart_rate
avg_spo2
min_spo2
avg_systolic_bp
avg_diastolic_bp
avg_temperature
max_temperature
reading_count
alert_count
high_heart_rate_count
low_spo2_count
high_temperature_count
```

------------------------------------------------------------------------

## 3.9 Daily Laboratory Pipeline

The laboratory generator creates one CSV per simulated day.

For every patient:

``` text
Hemoglobin
WBC
Creatinine
```

Therefore:

``` text
20 patients × 3 tests = 60 rows/day
```

Each laboratory event includes:

``` text
batch_id
patient_id
test_type
result_value
reference_range
unit
collected_at
```

The publisher adds a deterministic `lab_id`.

Flow:

``` text
Daily CSV
   ↓
Python publisher
   ↓
Kafka labs.raw
   ↓
Spark
   ↓
validation
   ↓
out-of-range detection
   ↓
PostgreSQL laboratory_results
```

------------------------------------------------------------------------

## 3.10 Laboratory Idempotency

A laboratory ID is derived deterministically from the logical test
identity:

``` text
batch/day
+
patient
+
test type
```

PostgreSQL prevents duplicate `lab_id` records.

Therefore republishing the same daily CSV does not continuously increase
the logical lab count.

------------------------------------------------------------------------

## 3.11 Previous-Day Laboratory Enrichment

The final daily patient summary uses:

``` text
Day N vitals
+
Day N-1 labs
```

Example:

``` text
Day 4 vital trends
+
Day 3 laboratory results
=
Day 4 consolidated patient summary
```

This models a situation where the latest available completed pathology
batch informs the following day's monitoring context.

------------------------------------------------------------------------

## 3.12 Missing Laboratory Data

The consolidation does not fail simply because laboratory data is
missing.

The summary records one of:

``` text
COMPLETE
PARTIAL
MISSING
```

### COMPLETE

All three expected laboratory tests are available.

### PARTIAL

Only part of the expected laboratory panel is available.

### MISSING

No previous-day laboratory data is available for the patient.

This preserves the vital report while making data completeness explicit.

------------------------------------------------------------------------

## 3.13 Recomputation

Daily calculations are designed to be rerunnable.

If late laboratory data arrives:

``` text
late Day N lab
      ↓
recompute Day N+1
      ↓
same patient/day key updated
```

The primary key:

``` text
(patient_id, simulated_day)
```

prevents duplicate patient-day report rows.

This supports:

-   retries
-   late data
-   manual reruns
-   Airflow reruns
-   deterministic consolidation

------------------------------------------------------------------------

## 3.14 Synthetic Concern Level

The final consolidated report includes a demonstration-only:

``` text
concern_level
```

Values:

``` text
NORMAL
WATCH
HIGH
```

The classification combines synthetic vital trends with abnormal
previous-day laboratory results.

Example project logic:

### HIGH

Triggered by conditions such as:

``` text
min SpO₂ < 90
OR max temperature >= 39
OR multiple abnormal lab results
```

### WATCH

Triggered by conditions such as:

``` text
min SpO₂ < 93
OR max temperature >= 38
OR average heart rate > 110
OR at least one abnormal lab result
```

### NORMAL

Used when none of the synthetic concern conditions are present.

Again, this is an engineering demonstration feature and is **not a
validated medical score**.

------------------------------------------------------------------------

## 3.15 Airflow Daily Orchestration

The daily DAG coordinates the reporting workflow rather than the
continuous stream.

``` text
determine_day
      ↓
check_vital_data
      ↓
calculate_daily_vitals
      ↓
check_previous_labs
      ↓
consolidate
      ↓
validate_output
```

Airflow provides:

-   task dependencies
-   retries
-   task-level logs
-   execution history
-   manual reruns
-   report validation
-   visible workflow state

Missing previous-day labs are treated as valid data-quality state rather
than an automatic pipeline crash.

------------------------------------------------------------------------

## 3.16 Serving API

FastAPI exposes PostgreSQL data to external consumers.

### Ward overview

``` text
GET /api/ward/overview
```

Returns information such as:

``` text
simulated day
active patients
total readings
abnormal readings
patients with alerts
```

### Patient list

``` text
GET /api/patients
```

### Latest patient reading

``` text
GET /api/patients/P001/latest
```

### Patient history

``` text
GET /api/patients/P001/history?limit=30
```

### Recent abnormal readings

``` text
GET /api/alerts?limit=20
```

### Available reports

``` text
GET /api/reports/daily
```

### Consolidated report

``` text
GET /api/reports/daily/{day}
```

------------------------------------------------------------------------

## 3.17 React Monitoring Dashboard

The React dashboard presents the system as an operational monitoring
interface.

### Ward overview

Displays:

``` text
Active Patients
Total Readings
Abnormal Readings
Patients with Alerts
```

### Patient monitoring

A user can select a patient and view:

``` text
heart rate
SpO₂
blood pressure
temperature
current alert state
recent vital history
```

### Vital trend visualization

Recharts displays recent patient readings over time.

### Recent alerts

Shows abnormal readings and which synthetic threshold was triggered.

### Daily consolidated report

Displays:

``` text
patient
daily vital trends
reading count
previous-day labs
lab completeness
synthetic concern level
```

------------------------------------------------------------------------

## 3.18 Observability

The platform includes observability at several layers.

### Structured logging

Monitoring utilities produce machine-readable log entries containing
fields such as:

``` text
timestamp
level
component
event
status
```

### Pipeline metrics

Stored metrics include:

``` text
vital_events
abnormal_events
active_patients
```

### Health rule

The vital pipeline becomes stale when no new event has been persisted
within the configured real-time threshold.

The health check intentionally uses:

``` text
vital_events.stored_at
```

rather than simulated event timestamps because simulated time is
accelerated.

### Alert lifecycle

``` text
events flowing
      ↓
HEALTHY

no new stored events
      ↓
VITAL_STREAM_STALE
      ↓
unresolved warning

events resume
      ↓
HEALTHY
      ↓
warning resolved
```

### API health

``` text
GET /health
```

returns the current freshness status.

### Dashboard status

The frontend displays:

``` text
LIVE
```

or:

``` text
STALE
```

based on backend freshness.

------------------------------------------------------------------------

## 3.19 Architecture Style

The platform follows a **Kappa-oriented architecture with an
orchestrated daily reporting path**.

The primary event-processing path is:

``` text
Kafka
  ↓
Spark Structured Streaming
  ↓
PostgreSQL
```

Historical daily calculations reuse the durable validated event state
rather than maintaining an entirely separate implementation of the same
processing logic.

Airflow then orchestrates:

``` text
daily aggregation
      ↓
previous-day lab enrichment
      ↓
consolidation
      ↓
validation/reporting
```

This keeps continuous processing centered around one streaming path
while still supporting scheduled historical reporting and recomputation.

------------------------------------------------------------------------

## 3.20 Reliability and Data-Quality Features

The platform includes several protections:

-   schema-based Spark parsing
-   patient-ID validation
-   invalid vital rejection
-   invalid laboratory rejection
-   deterministic lab IDs
-   UUID vital event IDs
-   durable event deduplication
-   Kafka/Spark checkpoints
-   PostgreSQL primary keys
-   idempotent inserts
-   idempotent daily upserts
-   explicit missing-lab status
-   Airflow retries
-   health monitoring
-   stale-stream alerts
-   structured logging
-   recomputation support

------------------------------------------------------------------------

## 3.21 Project Structure

``` text
hospital-bigdata-platform/
│
├── airflow/
│   ├── dags/
│   │   └── hospital_daily_pipeline.py
│   ├── logs/
│   └── plugins/
│
├── api/
│   ├── __init__.py
│   ├── db.py
│   ├── main.py
│   └── routers/
│       ├── __init__.py
│       ├── alerts.py
│       ├── patients.py
│       ├── reports.py
│       └── ward.py
│
├── common/
│   └── simulation_clock.py
│
├── dashboard/
│   ├── src/
│   │   ├── components/
│   │   ├── api.js
│   │   ├── App.jsx
│   │   ├── index.css
│   │   └── main.jsx
│   └── package.json
│
├── data/
│   └── labs/
│
├── database/
│   └── init/
│
├── monitoring/
│   ├── __init__.py
│   ├── collect_metrics.py
│   └── health_check.py
│
├── producers/
│   ├── labs/
│   │   ├── generator.py
│   │   └── publisher.py
│   └── vitals/
│       └── producer.py
│
├── spark/
│   ├── checkpoints/
│   ├── jobs/
│   ├── transformations/
│   └── Dockerfile
│
├── tests/
│
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

Generated files, checkpoints, logs, virtual environments, node modules,
and secrets should not be committed.

------------------------------------------------------------------------

## 3.22 Main Data Flow Summary

### Real-time path

``` text
Vital Producer
    ↓
Kafka
    ↓
Spark
    ↓
Validation
    ↓
Synthetic alert detection
    ↓
Durable deduplication
    ↓
PostgreSQL
    ↓
FastAPI
    ↓
React Dashboard
```

### Daily path

``` text
Lab CSV
   ↓
Kafka
   ↓
Spark
   ↓
laboratory_results

Unique vital_events
   ↓
daily_vital_summary

daily_vital_summary
       +
previous-day laboratory_results
       ↓
Airflow-orchestrated consolidation
       ↓
patient_daily_summary
       ↓
FastAPI
       ↓
React Daily Report
```

------------------------------------------------------------------------

## 3.23 Known Simplifications

This project intentionally uses several practical simplifications:

-   Data is synthetic.
-   One simulated day is compressed into a short real-time interval.
-   The environment is designed primarily for local Docker execution.
-   PostgreSQL is used as the durable event/report store instead of a
    distributed analytical warehouse.
-   The Spark-to-PostgreSQL sink uses idempotent database
    constraints/upserts rather than a distributed transactional
    exactly-once sink.
-   Rolling windows and daily aggregates serve different purposes.
-   The synthetic concern level is not clinically validated.
-   The local Airflow setup uses simple development credentials.
-   Horizontal scaling, authentication/authorization, TLS, secrets
    management, HA Kafka/PostgreSQL, and production-grade monitoring are
    outside the current implementation.

------------------------------------------------------------------------

## 3.24 Production Improvements

For a production-scale system, possible improvements include:

-   Kafka multi-broker replication
-   schema registry and formal event contracts
-   managed Spark/Kafka infrastructure
-   object storage such as S3/HDFS with Parquet for long-term event
    history
-   data lake/lakehouse architecture
-   stronger exactly-once sink strategy
-   PostgreSQL connection pooling
-   API authentication and authorization
-   TLS
-   secret management
-   Prometheus metrics
-   Grafana dashboards
-   centralized structured logs
-   OpenTelemetry tracing
-   Airflow secrets/connections instead of plain environment credentials
-   automated CI/CD
-   unit/integration/end-to-end tests in CI
-   dead-letter handling for permanently invalid events
-   retention/archival policies
-   clinically validated rules designed and reviewed by appropriate
    healthcare professionals

------------------------------------------------------------------------

## 3.25 Quick End-to-End Test

After the complete environment is running:

1.  Start `store_vital_events.py`.
2.  Start the vital producer.
3.  Confirm `vital_events` increases.
4.  Generate a lab day.
5.  Start `store_labs.py`.
6.  Publish the lab file.
7.  Confirm 60 lab records for the complete day.
8.  Trigger `hospital_daily_pipeline`.
9.  Confirm `daily_vital_summary`.
10. Confirm `patient_daily_summary`.
11. Start FastAPI.
12. Start React.
13. Verify the dashboard updates.
14. Stop the vital producer.
15. Wait beyond the stale threshold.
16. Verify `/health` and the dashboard report `STALE`.
17. Restart the producer.
18. Verify the platform returns to `LIVE`.

------------------------------------------------------------------------

## 3.26 License

Add the license selected for your repository here.

Example:

``` text
MIT License
```


------------------------------------------------------------------------

## Final Note

This repository is an end-to-end demonstration of a modern event-driven
data platform combining streaming ingestion, structured stream
processing, durable storage, scheduled historical computation, REST
serving, frontend visualization, and pipeline observability.

All generated patient records and monitoring rules are synthetic and
intended solely for software/data-engineering demonstration.
