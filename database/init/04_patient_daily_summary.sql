
CREATE TABLE IF NOT EXISTS patient_daily_summary (

    patient_id VARCHAR(4) NOT NULL,

    simulated_day INTEGER NOT NULL,

    window_start TIMESTAMPTZ NOT NULL,

    window_end TIMESTAMPTZ NOT NULL,

    avg_heart_rate DOUBLE PRECISION NOT NULL,

    min_spo2 INTEGER NOT NULL,

    max_temperature DOUBLE PRECISION NOT NULL,

    reading_count BIGINT NOT NULL,

    lab_day INTEGER,

    hemoglobin DOUBLE PRECISION,

    wbc DOUBLE PRECISION,

    creatinine DOUBLE PRECISION,

    lab_test_count INTEGER NOT NULL DEFAULT 0,

    lab_status TEXT NOT NULL,

    consolidated_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        patient_id,
        simulated_day
    ),

    CONSTRAINT chk_summary_day
        CHECK (simulated_day >= 1),

    CONSTRAINT chk_summary_lab_status
        CHECK (
            lab_status IN (
                'COMPLETE',
                'PARTIAL',
                'MISSING'
            )
        ),

    CONSTRAINT chk_summary_lab_count
        CHECK (
            lab_test_count BETWEEN 0 AND 3
        )

);


CREATE INDEX IF NOT EXISTS idx_summary_day
ON patient_daily_summary (
    simulated_day
);