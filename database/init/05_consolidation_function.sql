CREATE OR REPLACE FUNCTION
consolidate_patient_day(target_day INTEGER)

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


    WITH previous_labs AS (

        SELECT

            patient_id,

            lab_day,

            MAX(result_value) FILTER (
                WHERE test_type = 'Hemoglobin'
            ) AS hemoglobin,

            MAX(result_value) FILTER (
                WHERE test_type = 'WBC'
            ) AS wbc,

            MAX(result_value) FILTER (
                WHERE test_type = 'Creatinine'
            ) AS creatinine,

            COUNT(DISTINCT test_type)
                AS lab_test_count,

            COUNT(*) FILTER (
                WHERE is_out_of_range = TRUE
            ) AS abnormal_lab_count

        FROM laboratory_results

        WHERE lab_day = target_day - 1

        GROUP BY
            patient_id,
            lab_day
    ),

    consolidated AS (

        SELECT

            v.patient_id,

            v.simulated_day,

            v.avg_heart_rate,

            v.min_spo2,

            v.max_temperature,

            v.reading_count,

            l.lab_day,

            l.hemoglobin,

            l.wbc,

            l.creatinine,

            COALESCE(
                l.lab_test_count,
                0
            ) AS lab_test_count,

            COALESCE(
                l.abnormal_lab_count,
                0
            ) AS abnormal_lab_count,

            CASE

                WHEN COALESCE(
                    l.lab_test_count,
                    0
                ) = 3

                    THEN 'COMPLETE'

                WHEN COALESCE(
                    l.lab_test_count,
                    0
                ) = 0

                    THEN 'MISSING'

                ELSE 'PARTIAL'

            END AS lab_status

        FROM daily_vital_summary v

        LEFT JOIN previous_labs l

            ON v.patient_id =
               l.patient_id

        WHERE
            v.simulated_day =
            target_day
    )

    INSERT INTO patient_daily_summary (

        patient_id,
        simulated_day,

        window_start,
        window_end,

        avg_heart_rate,
        min_spo2,
        max_temperature,
        reading_count,

        lab_day,

        hemoglobin,
        wbc,
        creatinine,

        lab_test_count,
        abnormal_lab_count,
        concern_level,
        lab_status,

        consolidated_at
    )

    SELECT

        patient_id,
        simulated_day,

        (
            DATE '2026-09-22'
            + (simulated_day - 1)
        )::timestamp AT TIME ZONE 'UTC',

        (
            DATE '2026-09-22'
            + simulated_day
        )::timestamp AT TIME ZONE 'UTC',

        avg_heart_rate,
        min_spo2,
        max_temperature,
        reading_count,

        lab_day,

        hemoglobin,
        wbc,
        creatinine,

        lab_test_count,
        abnormal_lab_count,

        CASE

            WHEN min_spo2 < 90
                 OR max_temperature >= 39
                 OR abnormal_lab_count >= 2
            THEN 'HIGH'

            WHEN min_spo2 < 93
                 OR max_temperature >= 38
                 OR avg_heart_rate > 110
                 OR abnormal_lab_count >= 1
            THEN 'WATCH'

            ELSE 'NORMAL'

        END AS concern_level,

        lab_status,

        NOW()

    FROM consolidated

    ON CONFLICT (
        patient_id,
        simulated_day
    )

    DO UPDATE SET

        window_start =
            EXCLUDED.window_start,

        window_end =
            EXCLUDED.window_end,

        avg_heart_rate =
            EXCLUDED.avg_heart_rate,

        min_spo2 =
            EXCLUDED.min_spo2,

        max_temperature =
            EXCLUDED.max_temperature,

        reading_count =
            EXCLUDED.reading_count,

        lab_day =
            EXCLUDED.lab_day,

        hemoglobin =
            EXCLUDED.hemoglobin,

        wbc =
            EXCLUDED.wbc,

        creatinine =
            EXCLUDED.creatinine,

        lab_test_count =
            EXCLUDED.lab_test_count,

        abnormal_lab_count =
            EXCLUDED.abnormal_lab_count,

        concern_level =
            EXCLUDED.concern_level,

        lab_status =
            EXCLUDED.lab_status,

        consolidated_at =
            NOW();


    GET DIAGNOSTICS
        affected_rows = ROW_COUNT;

    RETURN affected_rows;

END;

$$;