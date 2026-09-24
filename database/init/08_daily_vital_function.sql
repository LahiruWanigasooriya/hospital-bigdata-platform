CREATE OR REPLACE FUNCTION
calculate_daily_vitals(target_day INTEGER)

RETURNS INTEGER

LANGUAGE plpgsql

AS $$

DECLARE

    affected_rows INTEGER;

BEGIN

    IF target_day < 1 THEN

        RAISE EXCEPTION
            'Invalid simulated day: %',
            target_day;

    END IF;


    INSERT INTO daily_vital_summary (

        patient_id,
        simulated_day,

        avg_heart_rate,
        min_heart_rate,
        max_heart_rate,

        avg_spo2,
        min_spo2,

        avg_systolic_bp,
        avg_diastolic_bp,

        avg_temperature,
        max_temperature,

        reading_count,

        alert_count,
        high_heart_rate_count,
        low_spo2_count,
        high_temperature_count,

        calculated_at

    )

    SELECT

        patient_id,

        simulated_day,

        ROUND(
            AVG(heart_rate)::numeric,
            2
        ),

        MIN(heart_rate),

        MAX(heart_rate),

        ROUND(
            AVG(spo2)::numeric,
            2
        ),

        MIN(spo2),

        ROUND(
            AVG(systolic_bp)::numeric,
            2
        ),

        ROUND(
            AVG(diastolic_bp)::numeric,
            2
        ),

        ROUND(
            AVG(temperature)::numeric,
            2
        ),

        MAX(temperature),

        COUNT(*),

        COUNT(*) FILTER (
            WHERE has_alert
        ),

        COUNT(*) FILTER (
            WHERE high_heart_rate
        ),

        COUNT(*) FILTER (
            WHERE low_spo2
        ),

        COUNT(*) FILTER (
            WHERE high_temperature
        ),

        NOW()

    FROM vital_events

    WHERE simulated_day = target_day

    GROUP BY
        patient_id,
        simulated_day


    ON CONFLICT (
        patient_id,
        simulated_day
    )

    DO UPDATE SET

        avg_heart_rate =
            EXCLUDED.avg_heart_rate,

        min_heart_rate =
            EXCLUDED.min_heart_rate,

        max_heart_rate =
            EXCLUDED.max_heart_rate,

        avg_spo2 =
            EXCLUDED.avg_spo2,

        min_spo2 =
            EXCLUDED.min_spo2,

        avg_systolic_bp =
            EXCLUDED.avg_systolic_bp,

        avg_diastolic_bp =
            EXCLUDED.avg_diastolic_bp,

        avg_temperature =
            EXCLUDED.avg_temperature,

        max_temperature =
            EXCLUDED.max_temperature,

        reading_count =
            EXCLUDED.reading_count,

        alert_count =
            EXCLUDED.alert_count,

        high_heart_rate_count =
            EXCLUDED.high_heart_rate_count,

        low_spo2_count =
            EXCLUDED.low_spo2_count,

        high_temperature_count =
            EXCLUDED.high_temperature_count,

        calculated_at =
            NOW();


    GET DIAGNOSTICS
        affected_rows = ROW_COUNT;

    RETURN affected_rows;

END;

$$;