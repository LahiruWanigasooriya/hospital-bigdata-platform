CREATE TABLE IF NOT EXISTS pipeline_metrics (

    metric_id BIGSERIAL PRIMARY KEY,

    component VARCHAR(50) NOT NULL,

    metric_name VARCHAR(100) NOT NULL,

    metric_value DOUBLE PRECISION NOT NULL,

    simulated_day INTEGER,

    recorded_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS pipeline_alerts (

    alert_id BIGSERIAL PRIMARY KEY,

    alert_type VARCHAR(100) NOT NULL,

    severity VARCHAR(20) NOT NULL,

    component VARCHAR(50) NOT NULL,

    message TEXT NOT NULL,

    patient_id VARCHAR(4),

    simulated_day INTEGER,

    created_at TIMESTAMPTZ
        NOT NULL DEFAULT NOW(),

    resolved BOOLEAN
        NOT NULL DEFAULT FALSE,

    CONSTRAINT chk_alert_severity
        CHECK (
            severity IN (
                'INFO',
                'WARNING',
                'CRITICAL'
            )
        )
);


CREATE INDEX IF NOT EXISTS
idx_pipeline_metrics_component
ON pipeline_metrics (
    component,
    recorded_at
);


CREATE INDEX IF NOT EXISTS
idx_pipeline_alerts_active
ON pipeline_alerts (
    resolved,
    created_at
);