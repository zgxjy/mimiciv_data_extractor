-- ------------------------------------------------------------------
-- Title: Acute Physiology and Chronic Health Evaluation IV (APACHE IV) Score
-- This query extracts components for the APACHE IV score.
-- The score is calculated on the first day of each ICU patient's stay.
--
-- Reference for APACHE IV:
--    Zimmerman, J. E., Kramer, A. A., McNair, D. S., & Malila, F. M. (2006).
--    Acute Physiology and Chronic Health Evaluation (APACHE) IV:
--    hospital mortality assessment for today's critically ill patients.
--    Critical care medicine, 34(5), 1297-1310.
--
-- NOTES:
-- 1. This query focuses on the Acute Physiology Score (APS), Age Points, and Chronic Health Points.
--    The full APACHE IV model also includes weights based on the primary ICU admission diagnosis,
--    patient location before ICU, and length of hospital stay before ICU, which are NOT implemented here
--    due to their complexity and reliance on extensive mapping tables.
-- 2. "Worst" values in the first 24 hours are used (value that gives the most points).
-- 3. Chronic health definitions are simplified based on ICD codes and may not perfectly match
--    the detailed clinical criteria in the original APACHE IV.
-- 4. FiO2 for oxygenation calculations: This query uses bg.fio2 assuming it's the FiO2
--    corresponding to the PaO2 measurement. If a more precise chart-derived FiO2 is needed
--    at the exact time of PaO2, further logic would be required.
-- 5. GCS: If the patient is sedated, the pre-sedation GCS should be used. This query uses the
--    minimum GCS recorded in the first 24 hours, which might be affected by sedation.
-- ------------------------------------------------------------------

WITH co AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        intime AS starttime,
        DATETIME_ADD(intime, INTERVAL '24' HOUR) AS endtime
    FROM `physionet-data.mimiciv_icu.icustays`
)

-- Age
, age AS (
    SELECT
        co.stay_id,
        a.age
    FROM co
    INNER JOIN `physionet-data.mimiciv_derived.age` a
        ON co.hadm_id = a.hadm_id
)

-- GCS
, gcs AS (
    SELECT
        co.stay_id,
        MIN(gcs.gcs) AS gcs_min
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.gcs` gcs
        ON co.stay_id = gcs.stay_id
        AND gcs.charttime >= co.starttime AND gcs.charttime <= co.endtime
    GROUP BY co.stay_id
)

-- Vitals
, vital AS (
    SELECT
        co.stay_id,
        MIN(vital.temperature) AS temp_min,
        MAX(vital.temperature) AS temp_max,
        MIN(vital.heart_rate) AS heartrate_min,
        MAX(vital.heart_rate) AS heartrate_max,
        MIN(vital.mbp) AS map_min, -- Mean Arterial Pressure
        MAX(vital.mbp) AS map_max,
        MIN(vital.resp_rate) AS resprate_min,
        MAX(vital.resp_rate) AS resprate_max
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.vitalsign` vital
        ON co.stay_id = vital.stay_id
        AND vital.charttime >= co.starttime AND vital.charttime <= co.endtime
    GROUP BY co.stay_id
)

-- Urine Output
, uo AS (
    SELECT
        co.stay_id,
        SUM(uod.urineoutput) AS urineoutput_24hr
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.urine_output` uod
        ON co.stay_id = uod.stay_id
        AND uod.charttime >= co.starttime AND uod.charttime <= co.endtime
    GROUP BY co.stay_id
)

-- Labs: PaO2, PaCO2, pH, Sodium, Potassium, Creatinine, Hematocrit, WBC, Bicarbonate, Bilirubin, Albumin, Glucose
, labs_bg AS ( -- Blood Gas values
    SELECT
        co.stay_id,
        MIN(bg.pao2) AS pao2_min, -- for PaO2 < 0.5 FiO2 scoring
        MAX(bg.pao2) AS pao2_max,
        MIN(bg.paco2) AS paco2_min,
        MAX(bg.paco2) AS paco2_max,
        MIN(bg.ph) AS ph_min,
        MAX(bg.ph) AS ph_max,
        MIN(bg.fio2) AS fio2_min_for_pao2, -- FiO2 at time of worst PaO2 (approx)
        MAX(bg.fio2) AS fio2_max_for_pao2  -- FiO2 at time of worst PaO2 (approx)
        -- For A-a gradient, we need paired PaO2, PaCO2, FiO2.
        -- This CTE gets overall min/max, next one will get values for A-a gradient
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.bg` bg -- Assuming ART. for arterial
        ON co.stay_id = bg.stay_id AND bg.specimen_pred = 'ART.' -- or bg.specimen = 'ART.' if available
        AND bg.charttime >= co.starttime AND bg.charttime <= co.endtime
    GROUP BY co.stay_id
)

, oxygenation_cte AS (
    -- Select PaO2 and FiO2 pairs to calculate A-a gradient or select appropriate PaO2.
    -- Prioritize FiO2 from chartevents if available and reliable, else use FiO2 from BG.
    -- For simplicity, using bg.fio2 directly here.
    -- Worst A-a gradient or PaO2 is chosen.
    SELECT
        co.stay_id,
        bg.pao2,
        bg.paco2,
        bg.fio2 -- FiO2 from blood gas analysis machine (usually in range 21-100 or 0.21-1.0)
               -- Ensure this is correctly interpreted (fraction or percentage)
               -- Assuming it's a fraction (0.21-1.0) for calculation. If it's %, divide by 100.
               -- MIMIC-IV bg derived view usually has fio2 as fraction.
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.bg` bg
        ON co.stay_id = bg.stay_id AND bg.specimen_pred = 'ART.'
        AND bg.charttime >= co.starttime AND bg.charttime <= co.endtime
    WHERE bg.pao2 IS NOT NULL AND bg.fio2 IS NOT NULL
)

, aa_gradient_calc AS (
    SELECT
        stay_id,
        pao2,
        fio2,
        (fio2 * (760 - 47)) - (paco2 / 0.8) - pao2 AS aa_do2 -- Patm=760, PH2O=47, R=0.8
    FROM oxygenation_cte
    WHERE fio2 >= 0.5 -- Calculate A-aDO2 only if FiO2 >= 0.5
)

, worst_oxygenation AS (
    SELECT
        stay_id,
        MAX(CASE WHEN fio2 >= 0.5 THEN aa_do2 ELSE NULL END) AS aa_do2_worst, -- Highest A-aDO2 is worst
        MIN(CASE WHEN fio2 < 0.5 THEN pao2 ELSE NULL END) AS pao2_lowfio2_worst -- Lowest PaO2 is worst (when FiO2 < 0.5)
    FROM aa_gradient_calc
    GROUP BY stay_id
)


, labs_chem AS (
    SELECT
        co.stay_id,
        MIN(chem.sodium) AS sodium_min,
        MAX(chem.sodium) AS sodium_max,
        MIN(chem.potassium) AS potassium_min,
        MAX(chem.potassium) AS potassium_max,
        MIN(chem.creatinine) AS creatinine_min, -- For ARF check
        MAX(chem.creatinine) AS creatinine_max, -- For scoring
        MIN(chem.hematocrit) AS hematocrit_min,
        MAX(chem.hematocrit) AS hematocrit_max,
        MIN(chem.wbc) AS wbc_min,
        MAX(chem.wbc) AS wbc_max,
        MIN(chem.bicarbonate) AS bicarbonate_min, -- Ensure this is venous if possible, but MIMIC derived chemistry might be arterial
        MAX(chem.bicarbonate) AS bicarbonate_max,
        MIN(chem.albumin) AS albumin_min,
        MAX(chem.albumin) AS albumin_max,
        MIN(chem.glucose) AS glucose_min,
        MAX(chem.glucose) AS glucose_max
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.chemistry` chem  -- This view combines labs, be mindful of specimen types if critical
        ON co.stay_id = chem.stay_id
        AND chem.charttime >= co.starttime AND chem.charttime <= co.endtime
    LEFT JOIN `physionet-data.mimiciv_derived.complete_blood_count` cbc
        ON co.stay_id = cbc.stay_id
        AND cbc.charttime >= co.starttime AND cbc.charttime <= co.endtime
    -- Note: `chemistry` might already include hematocrit and wbc for some systems,
    -- but explicitly joining `complete_blood_count` is safer for these.
    -- Glucose can also be in `bg`. Assuming `chemistry` has the broader set for APACHE IV.
    GROUP BY co.stay_id
)

, labs_enzyme AS (
    SELECT
        co.stay_id,
        MIN(enz.bilirubin_total) AS bilirubin_min,
        MAX(enz.bilirubin_total) AS bilirubin_max
    FROM co
    LEFT JOIN `physionet-data.mimiciv_derived.enzyme` enz
        ON co.stay_id = enz.stay_id
        AND enz.charttime >= co.starttime AND enz.charttime <= co.endtime
    GROUP BY co.stay_id
)

-- Acute Renal Failure (ARF) Flag for Creatinine Point Doubling
-- ARF: Urine Output < 410ml/day (original APACHE IV used 8h blocks, simplified here to 24h sum)
--      OR Serum Creatinine >=1.5 mg/dL. (Points for Cr are doubled if ARF and NOT chronic dialysis)
--      AND NOT on chronic dialysis
, arf_flag AS (
    SELECT
        co.stay_id,
        MAX(CASE
            WHEN (uo.urineoutput_24hr < 410 OR labs_chem.creatinine_max >= 1.5)
            -- AND chronic_dialysis_flag = 0 -- (Need a chronic dialysis flag here)
            THEN 1
            ELSE 0
        END) AS arf_present_acute_phase -- Flag indicating ARF criteria met in the acute phase
    FROM co
    LEFT JOIN uo ON co.stay_id = uo.stay_id
    LEFT JOIN labs_chem ON co.stay_id = labs_chem.stay_id
    -- Placeholder for chronic_dialysis_flag:
    -- This would typically come from a comorbidity check or procedures indicating chronic dialysis prior to admission.
    -- For this query, we'll assume no chronic dialysis if this flag is not easily derivable.
    -- A more complete implementation would join to a chronic dialysis flag from a comorbidity CTE.
    GROUP BY co.stay_id
)


-- Chronic Health Conditions (Simplified)
-- Points: Cirrhosis (biopsy proven or portal HTN & hx of variceal bleed or hepatic encephalopathy) = 5
--         NYHA Class IV CHF = 5
--         Severe Resp Disease (chronic restrictive, obstructive, or vascular disease with severe exercise limitation OR documented chronic hypoxia, hypercapnia, severe pulm HTN, resp dependency) = 5
--         Chronic Renal Dialysis = 5 (Note: If present, ARF bonus for Cr might not apply or be interpreted differently)
--         Immunocompromised (immunosuppression, chemo, radiation, chronic steroids, leukemia, lymphoma, AIDS) = 5
-- This `comorb` CTE is a simplified version based on ICD codes, similar to SAPS II but with attempts to map to APACHE IV concepts.
, comorb AS (
    SELECT
        hadm_id,
        MAX(CASE -- AIDS (Simplified from SAPS II, aligns with APACHE IV general concept)
            WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 3) BETWEEN '042' AND '044' THEN 1
            WHEN icd_version = 10 AND (SUBSTR(icd_code, 1, 3) BETWEEN 'B20' AND 'B22' OR SUBSTR(icd_code, 1, 3) = 'B24') THEN 1
            ELSE 0
        END) AS aids,

        MAX(CASE -- Hematologic Malignancy (Simplified from SAPS II)
            WHEN icd_version = 9 AND (
                (SUBSTR(icd_code, 1, 5) BETWEEN '20000' AND '20302') OR -- Lymphoma, Multiple Myeloma
                (SUBSTR(icd_code, 1, 5) BETWEEN '20310' AND '20892') OR -- Leukemia (various types)
                SUBSTR(icd_code, 1, 4) IN ('2386', '2733') -- Other related
            ) THEN 1
            WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) BETWEEN 'C81' AND 'C96' THEN 1 -- Malignant neoplasms of lymphoid, hematopoietic and related tissue
            ELSE 0
        END) AS hem_malignancy,

        MAX(CASE -- Metastatic Cancer (Simplified from SAPS II)
            WHEN icd_version = 9 AND (
                (SUBSTR(icd_code, 1, 4) BETWEEN '1960' AND '1991') OR -- Secondary and unspecified malignant neoplasms of lymph nodes, respiratory and digestive organs, other specified sites
                (SUBSTR(icd_code, 1, 5) IN ('20970', '20971', '20972', '20973', '20974', '20975', '20979')) OR -- Various neuroendocrine tumors often metastatic
                (SUBSTR(icd_code, 1, 5) = '78951') -- Malignant ascites
            ) THEN 1
            WHEN icd_version = 10 AND (
                (SUBSTR(icd_code, 1, 3) BETWEEN 'C77' AND 'C79') OR -- Secondary and unspecified malignant neoplasm of lymph nodes / Secondary malignant neoplasm of respiratory and digestive organs / Secondary malignant neoplasm of other and unspecified sites
                 SUBSTR(icd_code, 1, 4) = 'C800'                   -- Malignant neoplasm, unspecified, disseminated
            ) THEN 1
            ELSE 0
        END) AS mets_cancer,

        MAX(CASE -- Liver Cirrhosis (Simplified)
            WHEN icd_version = 9 AND (
                SUBSTR(icd_code, 1, 4) = '5712' OR -- Alcoholic cirrhosis of liver
                SUBSTR(icd_code, 1, 4) = '5715' OR -- Cirrhosis of liver without mention of alcohol
                SUBSTR(icd_code, 1, 4) = '5716'    -- Biliary cirrhosis
            ) THEN 1
            WHEN icd_version = 10 AND (
                SUBSTR(icd_code, 1, 4) = 'K703' OR -- Alcoholic cirrhosis of liver
                SUBSTR(icd_code, 1, 3) = 'K74'     -- Fibrosis and cirrhosis of liver (K74.3-K74.6 are various cirrhosis types)
            ) THEN 1
            ELSE 0
        END) AS cirrhosis,

        MAX(CASE -- General Immunosuppression (other than AIDS/Hem Malignancy) based on CCMDB Wiki for ICD-10
            WHEN icd_version = 10 AND (
                SUBSTR(icd_code, 1, 3) IN ('D80', 'D81', 'D82', 'D83', 'D84') OR -- Immunodeficiencies
                SUBSTR(icd_code, 1, 4) IN ('T860', 'T861', 'T862', 'T863', 'T864', 'T865', 'T868', 'T869') -- Complications of transplanted organs and tissues
            ) THEN 1
            -- ICD-9 for general immunosuppression is harder to map broadly here
            ELSE 0
        END) AS other_immunosuppression,

        MAX(CASE -- Chronic Renal Failure (Simplified - looking for ESRD type codes)
            WHEN icd_version = 9 AND SUBSTR(icd_code, 1, 4) = '5856' THEN 1 -- ESRD ICD-9
            WHEN icd_version = 10 AND SUBSTR(icd_code, 1, 3) = 'N18' AND SUBSTR(icd_code, 4,1) = '6' THEN 1 -- ESRD ICD-10 (N18.6)
            -- This doesn't capture "on chronic dialysis" directly from procedures, which is more accurate.
            ELSE 0
        END) AS chronic_renal_failure_esrd

    FROM `physionet-data.mimiciv_hosp.diagnoses_icd`
    GROUP BY hadm_id
)
-- Admission Type (NonOperative, Elective PostOp, Emergency PostOp)
, adm_type AS (
    SELECT
        adm.hadm_id,
        CASE
            -- Elective surgery: ELECTIVE admission type and a surgical service
            WHEN adm.admission_type = 'ELECTIVE' AND LOWER(ser.curr_service) LIKE '%surg%' THEN 'ElectiveSurgical'
            -- Emergency surgery: Non-elective admission type and a surgical service
            WHEN adm.admission_type != 'ELECTIVE' AND LOWER(ser.curr_service) LIKE '%surg%' THEN 'EmergencySurgical'
            -- Non-operative: All others
            ELSE 'NonOperative'
        END AS admission_category,
        ROW_NUMBER() OVER (PARTITION BY adm.hadm_id ORDER BY ser.transfertime ASC) as service_order -- first service
    FROM `physionet-data.mimiciv_hosp.admissions` adm
    LEFT JOIN `physionet-data.mimiciv_hosp.services` ser
        ON adm.hadm_id = ser.hadm_id
)

-- Combine all components
, cohort AS (
    SELECT
        co.subject_id,
        co.hadm_id,
        co.stay_id,
        co.starttime,
        co.endtime,
        a.age,
        gcs.gcs_min,
        vital.temp_min, vital.temp_max,
        vital.heartrate_min, vital.heartrate_max,
        vital.map_min, vital.map_max,
        vital.resprate_min, vital.resprate_max,
        oxy.aa_do2_worst,
        oxy.pao2_lowfio2_worst,
        labs_bg.fio2_min_for_pao2, -- Need the FIO2 that corresponds to the PaO2 used for scoring
        labs_bg.fio2_max_for_pao2, -- Potentially, or the FIO2 when aa_do2_worst occurred
        labs_bg.ph_min, labs_bg.ph_max,
        labs_chem.sodium_min, labs_chem.sodium_max,
        labs_chem.potassium_min, labs_chem.potassium_max,
        labs_chem.creatinine_max, -- Max Cr for scoring
        arf.arf_present_acute_phase,
        cm.chronic_renal_failure_esrd, -- To check if ARF bonus applies
        labs_chem.hematocrit_min, labs_chem.hematocrit_max,
        labs_chem.wbc_min, labs_chem.wbc_max,
        labs_chem.bicarbonate_min, labs_chem.bicarbonate_max, -- APACHE IV uses actual bicarb
        labs_enzyme.bilirubin_max,
        labs_chem.albumin_min,
        labs_chem.glucose_min, labs_chem.glucose_max,
        cm.aids,
        cm.hem_malignancy,
        cm.mets_cancer,
        cm.cirrhosis,
        cm.other_immunosuppression,
        at.admission_category
    FROM co
    LEFT JOIN age a ON co.stay_id = a.stay_id
    LEFT JOIN gcs ON co.stay_id = gcs.stay_id
    LEFT JOIN vital ON co.stay_id = vital.stay_id
    LEFT JOIN worst_oxygenation oxy ON co.stay_id = oxy.stay_id
    LEFT JOIN labs_bg ON co.stay_id = labs_bg.stay_id
    LEFT JOIN labs_chem ON co.stay_id = labs_chem.stay_id
    LEFT JOIN labs_enzyme ON co.stay_id = labs_enzyme.stay_id
    LEFT JOIN arf_flag arf ON co.stay_id = arf.stay_id
    LEFT JOIN comorb cm ON co.hadm_id = cm.hadm_id
    LEFT JOIN adm_type at ON co.hadm_id = at.hadm_id AND at.service_order = 1
)

-- Calculate APACHE IV component points
, scorecomp AS (
    SELECT
        c.*,
        -- Temperature Points (deg C)
        CASE
            WHEN temp_max >= 41 OR temp_min <= 29.9 THEN 4
            WHEN temp_max >= 39 OR temp_min <= 31.9 THEN 3
            WHEN temp_min <= 33.9 THEN 2
            WHEN temp_max >= 38.5 OR temp_min <= 35.9 THEN 1
            WHEN temp_max < 38.5 AND temp_min > 35.9 THEN 0
            ELSE NULL
        END AS temp_points,

        -- MAP Points (mmHg)
        CASE
            WHEN map_max >= 160 OR map_min <= 49 THEN 4
            WHEN map_max >= 130 THEN 3
            WHEN map_max >= 110 OR map_min <= 69 THEN 2
            WHEN map_max < 110 AND map_min > 69 THEN 0
            ELSE NULL
        END AS map_points,

        -- Heart Rate Points (beats/min)
        CASE
            WHEN heartrate_max >= 180 OR heartrate_min <= 39 THEN 4
            WHEN heartrate_max >= 140 OR heartrate_min <= 54 THEN 3
            WHEN heartrate_max >= 110 OR heartrate_min <= 69 THEN 2
            WHEN heartrate_max < 110 AND heartrate_min > 69 THEN 0
            ELSE NULL
        END AS hr_points,

        -- Respiratory Rate Points (breaths/min) (Ventilated or Non-ventilated scoring is different in APACHE IV)
        -- This is simplified: uses the same scale. A full implementation would check vent status.
        CASE
            WHEN resprate_max >= 50 OR resprate_min <= 5 THEN 4
            WHEN resprate_max >= 35 THEN 3
            WHEN resprate_min <= 9 THEN 2
            WHEN resprate_max >= 25 OR resprate_min <= 11 THEN 1
            WHEN resprate_max < 25 AND resprate_min > 11 THEN 0
            ELSE NULL
        END AS rr_points,

        -- Oxygenation Points
        -- If FiO2 >= 0.5, use A-aDO2. If FiO2 < 0.5, use PaO2.
        CASE
            WHEN fio2_max_for_pao2 >= 0.5 AND aa_do2_worst IS NOT NULL THEN -- Higher FIO2 was used at some point for PaO2
                CASE
                    WHEN aa_do2_worst >= 500 THEN 4
                    WHEN aa_do2_worst >= 350 THEN 3
                    WHEN aa_do2_worst >= 200 THEN 2
                    ELSE 0
                END
            WHEN fio2_min_for_pao2 < 0.5 AND pao2_lowfio2_worst IS NOT NULL THEN
                CASE
                    WHEN pao2_lowfio2_worst < 55 THEN 4
                    WHEN pao2_lowfio2_worst <= 60 THEN 3
                    WHEN pao2_lowfio2_worst <= 70 THEN 1
                    ELSE 0
                END
            ELSE 0 -- Default to 0 if no valid oxygenation data for scoring
        END AS oxygenation_points,

        -- Arterial pH Points
        CASE
            WHEN ph_max >= 7.7 OR ph_min < 7.15 THEN 4
            WHEN ph_max >= 7.6 OR ph_min < 7.25 THEN 3
            WHEN ph_min < 7.33 THEN 2
            WHEN ph_max >= 7.5 THEN 1
            WHEN ph_max < 7.5 AND ph_min >= 7.33 THEN 0
            ELSE NULL
        END AS ph_points,

        -- Serum Sodium Points (mmol/L)
        CASE
            WHEN sodium_max >= 180 OR sodium_min <= 110 THEN 4
            WHEN sodium_max >= 160 OR sodium_min <= 119 THEN 3
            WHEN sodium_max >= 155 OR sodium_min <= 129 THEN 2
            WHEN sodium_max >= 150 OR sodium_min <= 134 THEN 1
            WHEN sodium_max < 150 AND sodium_min > 134 THEN 0
            ELSE NULL
        END AS sodium_points,

        -- Serum Potassium Points (mmol/L)
        CASE
            WHEN potassium_max >= 7.0 OR potassium_min < 2.5 THEN 4
            WHEN potassium_max >= 6.0 THEN 3
            WHEN potassium_min < 3.0 THEN 2
            WHEN potassium_max >= 5.5 THEN 1
            WHEN potassium_max < 5.5 AND potassium_min >= 3.0 THEN 0
            ELSE NULL
        END AS potassium_points,

        -- Serum Creatinine Points (mg/dL) - Points doubled for ARF if NOT on chronic dialysis
        CASE
            WHEN creatinine_max >= 3.5 THEN 3
            WHEN creatinine_max >= 2.0 THEN 2
            WHEN creatinine_max >= 1.5 THEN 1 -- This means Cr >= 1.5 is the threshold for ARF point consideration
            WHEN creatinine_max >= 0.6 AND creatinine_max < 1.5 THEN 0
            WHEN creatinine_max < 0.6 THEN 2 -- Low creatinine also gets points in some versions, APACHE IV generally focuses on high
            ELSE NULL
        END AS creatinine_base_points,

        -- Hematocrit Points (%)
        CASE
            WHEN hematocrit_max >= 60 OR hematocrit_min < 20 THEN 4
            WHEN hematocrit_max >= 50 OR hematocrit_min < 30 THEN 2
            WHEN hematocrit_max < 50 AND hematocrit_min >= 30 THEN 0
            ELSE NULL
        END AS hct_points,

        -- WBC Points (x10^3/uL)
        CASE
            WHEN wbc_max >= 40 OR wbc_min < 1 THEN 4
            WHEN wbc_max >= 20 OR wbc_min < 3 THEN 2
            WHEN wbc_max >= 15 THEN 1
            WHEN wbc_max < 15 AND wbc_min >=3 THEN 0
            ELSE NULL
        END AS wbc_points,

        -- Serum Bicarbonate Points (mmol/L) (actual, not base excess)
        CASE
            WHEN bicarbonate_max >= 52 OR bicarbonate_min < 15 THEN 4
            WHEN bicarbonate_max >= 41 OR bicarbonate_min < 18 THEN 3
            WHEN bicarbonate_min < 22 THEN 2
            WHEN bicarbonate_max >= 32 THEN 1 -- Original APACHE IV might have slightly different ranges
            WHEN bicarbonate_max < 32 AND bicarbonate_min >= 22 THEN 0
            ELSE NULL
        END AS bicarbonate_points,

        -- Serum Bilirubin Points (mg/dL) - For non-operative or emergency post-op; elective post-op has different table (not implemented here)
        CASE
            WHEN bilirubin_max >= 6.0 THEN 4
            WHEN bilirubin_max >= 4.0 THEN 3
            WHEN bilirubin_max >= 2.0 THEN 2
            WHEN bilirubin_max < 2.0 THEN 0
            ELSE NULL
        END AS bilirubin_points,

        -- Serum Albumin Points (g/dL) - For non-operative or emergency post-op
        CASE
            WHEN albumin_min < 2.0 THEN 4
            WHEN albumin_min < 2.5 THEN 3
            WHEN albumin_min < 3.5 THEN 2 -- APACHE IV uses ranges like <2.0, 2.0-2.4, 2.5-2.9 etc. Simplified.
            WHEN albumin_min >= 3.5 THEN 0
            ELSE NULL
        END AS albumin_points,

        -- Serum Glucose Points (mg/dL) - For non-operative or emergency post-op
        CASE
            WHEN glucose_max >= 500 OR glucose_min < 40 THEN 4
            WHEN glucose_max >= 350 OR glucose_min < 60 THEN 3
            WHEN glucose_max >= 200 THEN 2
            -- WHEN glucose_min < 70 THEN 1 -- Original APACHE IV doesn't score moderately low glucose
            WHEN glucose_max < 200 AND glucose_min >=60 THEN 0
            ELSE NULL
        END AS glucose_points,

        -- GCS Points
        CASE
            WHEN gcs_min IS NULL THEN NULL -- Should ideally use pre-sedation GCS
            ELSE 15 - gcs_min -- APACHE IV scores (15-GCS) for points.
                               -- If GCS is 3, points are 12. If 15, points are 0.
                               -- The direct point mapping for GCS in APACHE IV is complex and depends on motor/verbal if sum is low.
                               -- A simple (15-GCS) is a common proxy for the GCS component contribution.
                               -- Original tables are: 3-4 (8pts), 5 (7pts), 6 (6pts), 7 (5pts), 8-10 (3pts), 11-13 (1pt), 14-15 (0pt)
                               -- Let's use the proper mapping if possible
        END AS gcs_raw_points_value, -- Placeholder for direct mapping below

        -- Age Points
        CASE
            WHEN age <= 44 THEN 0
            WHEN age <= 54 THEN 5
            WHEN age <= 64 THEN 11
            WHEN age <= 74 THEN 13
            WHEN age >= 75 THEN 16
            ELSE NULL
        END AS age_points,

        -- Chronic Health Points (Simplified)
        -- Max 5 points for any of these conditions. APACHE IV has specific points for specific conditions.
        -- Cirrhosis (5), NYHA IV (5), Severe Resp (5), Chronic Renal Dialysis (5), Immunocompromise (5)
        -- This simplified version gives 5 if ANY relevant flag is true from comorb.
        -- A true APACHE IV would sum points if multiple distinct chronic health categories are met.
        CASE
            WHEN cirrhosis = 1 OR aids = 1 OR hem_malignancy = 1 OR mets_cancer = 1 OR other_immunosuppression = 1 OR chronic_renal_failure_esrd = 1 THEN 5
            -- Add other chronic health conditions here if defined and scored separately
            ELSE 0
        END AS chronic_health_points

    FROM cohort c
)
, final_scores AS (
    SELECT sc.*,
    (CASE -- More accurate GCS points based on sum
        WHEN gcs_min IS NULL THEN 0 -- Default to 0 if GCS unknown, or handle as per APACHE IV rules (e.g., assume normal if sedated and no prior)
        WHEN gcs_min <= 4 THEN 8 -- Combines 3 and 4
        WHEN gcs_min = 5 THEN 7
        WHEN gcs_min = 6 THEN 6
        WHEN gcs_min = 7 THEN 5
        WHEN gcs_min >= 8 AND gcs_min <=10 THEN 3
        WHEN gcs_min >= 11 AND gcs_min <=13 THEN 1
        WHEN gcs_min >= 14 THEN 0
        ELSE 0
    END) AS gcs_points_final,

    (CASE -- Creatinine points with ARF bonus
        WHEN creatinine_base_points IS NULL THEN 0
        WHEN arf_present_acute_phase = 1 AND chronic_renal_failure_esrd = 0 THEN creatinine_base_points * 2 -- Double points for ARF if not on chronic dialysis
        ELSE creatinine_base_points
    END) as creatinine_points_final

    FROM scorecomp sc
)
-- Sum of points for APACHE IV APS (Acute Physiology Score)
SELECT
    fs.subject_id, fs.hadm_id, fs.stay_id,
    fs.starttime, fs.endtime,
    COALESCE(fs.temp_points,0) AS temp_points,
    COALESCE(fs.map_points,0) AS map_points,
    COALESCE(fs.hr_points,0) AS hr_points,
    COALESCE(fs.rr_points,0) AS rr_points,
    COALESCE(fs.oxygenation_points,0) AS oxygenation_points,
    COALESCE(fs.ph_points,0) AS ph_points,
    COALESCE(fs.sodium_points,0) AS sodium_points,
    COALESCE(fs.potassium_points,0) AS potassium_points,
    COALESCE(fs.creatinine_points_final,0) AS creatinine_points,
    COALESCE(fs.hct_points,0) AS hct_points,
    COALESCE(fs.wbc_points,0) AS wbc_points,
    COALESCE(fs.bicarbonate_points,0) AS bicarbonate_points,
    COALESCE(fs.gcs_points_final,0) AS gcs_points,
    -- Bilirubin, Albumin, Glucose are part of APS for Non-op/Emergency post-op
    -- For Elective post-op, these are scored differently or not at all.
    -- Assuming Non-op / Emergency Post-op for these points:
    CASE
        WHEN fs.admission_category = 'ElectiveSurgical' THEN 0 -- Elective post-op might not score these or score them differently
        ELSE COALESCE(fs.bilirubin_points,0)
    END AS bilirubin_points_final,
    CASE
        WHEN fs.admission_category = 'ElectiveSurgical' THEN 0
        ELSE COALESCE(fs.albumin_points,0)
    END AS albumin_points_final,
    CASE
        WHEN fs.admission_category = 'ElectiveSurgical' THEN 0
        ELSE COALESCE(fs.glucose_points,0)
    END AS glucose_points_final,

    COALESCE(fs.age_points,0) AS age_points_final,
    COALESCE(fs.chronic_health_points,0) AS chronic_health_points_final,

    -- APACHE IV Acute Physiology Score (APS)
    (
        COALESCE(fs.temp_points,0) +
        COALESCE(fs.map_points,0) +
        COALESCE(fs.hr_points,0) +
        COALESCE(fs.rr_points,0) +
        COALESCE(fs.oxygenation_points,0) +
        COALESCE(fs.ph_points,0) +
        COALESCE(fs.sodium_points,0) +
        COALESCE(fs.potassium_points,0) +
        COALESCE(fs.creatinine_points_final,0) +
        COALESCE(fs.hct_points,0) +
        COALESCE(fs.wbc_points,0) +
        COALESCE(fs.bicarbonate_points,0) +
        COALESCE(fs.gcs_points_final,0) +
        (CASE WHEN fs.admission_category != 'ElectiveSurgical' THEN COALESCE(fs.bilirubin_points,0) ELSE 0 END) +
        (CASE WHEN fs.admission_category != 'ElectiveSurgical' THEN COALESCE(fs.albumin_points,0) ELSE 0 END) +
        (CASE WHEN fs.admission_category != 'ElectiveSurgical' THEN COALESCE(fs.glucose_points,0) ELSE 0 END)
    ) AS apache_iv_aps,

    -- Total APACHE IV Score (APS + Age + Chronic Health)
    -- This is the score before diagnosis-specific weighting.
    (
        COALESCE(fs.temp_points,0) +
        COALESCE(fs.map_points,0) +
        COALESCE(fs.hr_points,0) +
        COALESCE(fs.rr_points,0) +
        COALESCE(fs.oxygenation_points,0) +
        COALESCE(fs.ph_points,0) +
        COALESCE(fs.sodium_points,0) +
        COALESCE(fs.potassium_points,0) +
        COALESCE(fs.creatinine_points_final,0) +
        COALESCE(fs.hct_points,0) +
        COALESCE(fs.wbc_points,0) +
        COALESCE(fs.bicarbonate_points,0) +
        COALESCE(fs.gcs_points_final,0) +
        (CASE WHEN fs.admission_category != 'ElectiveSurgical' THEN COALESCE(fs.bilirubin_points,0) ELSE 0 END) +
        (CASE WHEN fs.admission_category != 'ElectiveSurgical' THEN COALESCE(fs.albumin_points,0) ELSE 0 END) +
        (CASE WHEN fs.admission_category != 'ElectiveSurgical' THEN COALESCE(fs.glucose_points,0) ELSE 0 END) +
        COALESCE(fs.age_points,0) +
        COALESCE(fs.chronic_health_points,0)
    ) AS apache_iv_score_pre_dx_weighting,
    fs.admission_category
FROM final_scores fs
ORDER BY fs.subject_id, fs.stay_id;