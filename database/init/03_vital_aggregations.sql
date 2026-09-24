
CREATE TABLE IF NOT EXISTS vital_aggregations (

    patient_id VARCHAR(4) NOT NULL,

    simulated_day INTEGER NOT NULL,

    window_start TIMESTAMPTZ NOT NULL,

    window_end TIMESTAMPTZ NOT NULL,

    avg_heart_rate DOUBLE PRECISION NOT NULL,

    min_spo2 INTEGER NOT NULL,

    max_temperature DOUBLE PRECISION NOT NULL,

    reading_count BIGINT NOT NULL,

    spark_batch_id BIGINT NOT NULL DEFAULT -1,

    updated_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_vital_aggregations
        PRIMARY KEY (
            patient_id,
            window_start,
            window_end
        ),

    CONSTRAINT chk_vital_patient
        CHECK (
            patient_id ~
            '^P(00[1-9]|0[1-9][0-9]|020)$'
        ),

    CONSTRAINT chk_vital_day
        CHECK (simulated_day >= 1),

    CONSTRAINT chk_vital_window
        CHECK (window_end > window_start),

    CONSTRAINT chk_vital_count
        CHECK (reading_count > 0)

);


CREATE INDEX IF NOT EXISTS idx_vital_patient_day
ON vital_aggregations (
    patient_id,
    simulated_day
);


CREATE INDEX IF NOT EXISTS idx_vital_window_end
ON vital_aggregations (
    window_end
);