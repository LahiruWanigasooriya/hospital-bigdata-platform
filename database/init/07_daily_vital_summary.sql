CREATE TABLE IF NOT EXISTS daily_vital_summary (

    patient_id VARCHAR(4) NOT NULL,

    simulated_day INTEGER NOT NULL,

    avg_heart_rate DOUBLE PRECISION NOT NULL,

    min_heart_rate INTEGER NOT NULL,

    max_heart_rate INTEGER NOT NULL,

    avg_spo2 DOUBLE PRECISION NOT NULL,

    min_spo2 INTEGER NOT NULL,

    avg_systolic_bp DOUBLE PRECISION NOT NULL,

    avg_diastolic_bp DOUBLE PRECISION NOT NULL,

    avg_temperature DOUBLE PRECISION NOT NULL,

    max_temperature DOUBLE PRECISION NOT NULL,

    reading_count BIGINT NOT NULL,

    alert_count BIGINT NOT NULL,

    high_heart_rate_count BIGINT NOT NULL,

    low_spo2_count BIGINT NOT NULL,

    high_temperature_count BIGINT NOT NULL,

    calculated_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        patient_id,
        simulated_day
    )
);