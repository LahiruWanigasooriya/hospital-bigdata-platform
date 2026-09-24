ALTER TABLE patient_daily_summary
ADD COLUMN IF NOT EXISTS concern_level
VARCHAR(10) NOT NULL DEFAULT 'NORMAL';

ALTER TABLE patient_daily_summary
DROP CONSTRAINT IF EXISTS chk_concern_level;

ALTER TABLE patient_daily_summary
ADD CONSTRAINT chk_concern_level
CHECK (
    concern_level IN (
        'NORMAL',
        'WATCH',
        'HIGH'
    )
);


ALTER TABLE patient_daily_summary
ADD COLUMN IF NOT EXISTS
abnormal_lab_count INTEGER
NOT NULL DEFAULT 0;