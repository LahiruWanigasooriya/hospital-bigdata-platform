
CREATE TABLE IF NOT EXISTS laboratory_results (

    lab_id TEXT PRIMARY KEY,

    batch_id TEXT NOT NULL,

    patient_id VARCHAR(4) NOT NULL,

    lab_day INTEGER NOT NULL,

    test_type TEXT NOT NULL,

    result_value DOUBLE PRECISION NOT NULL,

    reference_range TEXT NOT NULL,

    unit TEXT NOT NULL,

    collected_at TIMESTAMPTZ NOT NULL,

    is_out_of_range BOOLEAN NOT NULL,

    kafka_partition INTEGER NOT NULL,

    kafka_offset BIGINT NOT NULL,

    created_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_lab_day
        CHECK (lab_day >= 1),

    CONSTRAINT valid_patient_id
        CHECK (
            patient_id ~
            '^P(00[1-9]|0[1-9][0-9]|020)$'
        ),

    CONSTRAINT valid_lab_result
        CHECK (
            result_value >= 0
            AND result_value < 'Infinity'::float8
        ),

    CONSTRAINT valid_test_type
        CHECK (
            test_type IN (
                'Hemoglobin',
                'WBC',
                'Creatinine'
            )
        )

);


CREATE INDEX IF NOT EXISTS idx_labs_patient_day
ON laboratory_results (
    patient_id,
    lab_day
);


CREATE INDEX IF NOT EXISTS idx_labs_batch
ON laboratory_results (
    batch_id
);