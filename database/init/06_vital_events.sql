CREATE TABLE IF NOT EXISTS vital_events (

    event_id TEXT PRIMARY KEY,

    patient_id VARCHAR(4) NOT NULL,

    heart_rate INTEGER NOT NULL,

    spo2 INTEGER NOT NULL,

    systolic_bp INTEGER NOT NULL,

    diastolic_bp INTEGER NOT NULL,

    temperature DOUBLE PRECISION NOT NULL,

    event_time TIMESTAMPTZ NOT NULL,

    simulated_day INTEGER NOT NULL,

    batch_id TEXT NOT NULL,

    ingested_at TIMESTAMPTZ,

    high_heart_rate BOOLEAN NOT NULL,

    low_spo2 BOOLEAN NOT NULL,

    high_temperature BOOLEAN NOT NULL,

    has_alert BOOLEAN NOT NULL,

    kafka_partition INTEGER NOT NULL,

    kafka_offset BIGINT NOT NULL,

    stored_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_vital_event_day
        CHECK (simulated_day >= 1),

    CONSTRAINT chk_vital_event_patient
        CHECK (
            patient_id ~
            '^P(00[1-9]|01[0-9]|020)$'
        )
);

CREATE INDEX IF NOT EXISTS
idx_vital_events_patient_day
ON vital_events (
    patient_id,
    simulated_day
);

CREATE INDEX IF NOT EXISTS
idx_vital_events_time
ON vital_events (
    event_time
);