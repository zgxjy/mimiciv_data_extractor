from psycopg2 import sql


def add_demography(table_name, sql):
    sql += "-- 添加人口学信息\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gender character; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS dod date; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS admittime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS dischtime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS los_hospital numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS admission_age numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS race character varying; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hospital_expire_flag smallint; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hospstay_seq bigint; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_hosp_stay boolean; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS icu_intime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS icu_outtime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS los_icu numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS icustay_seq bigint; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_icu_stay boolean; 
UPDATE {table_name} af 
SET 
    gender = i.gender, 
    dod = i.dod, 
    admittime = i.admittime, 
    dischtime = i.dischtime, 
    los_hospital = i.los_hospital, 
    admission_age = i.admission_age, 
    race = i.race, 
    hospital_expire_flag = i.hospital_expire_flag, 
    hospstay_seq = i.hospstay_seq, 
    first_hosp_stay = i.first_hosp_stay, 
    icu_intime = i.icu_intime, 
    icu_outtime = i.icu_outtime, 
    los_icu = i.los_icu, 
    icustay_seq = i.icustay_seq, 
    first_icu_stay = i.first_icu_stay
FROM mimiciv_derived.icustay_detail i 
WHERE af.stay_id = i.stay_id;
    """.format(table_name=table_name)
        
    sql +="\n"
    sql += "-- 婚姻状态\n"
    sql += """
-- 添加缺失的列
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS marital_status CHARACTER(100);

-- 更新 'marital_status' 字段
UPDATE {table_name} af
SET 
    marital_status = ad.marital_status
FROM mimiciv_hosp.admissions ad
WHERE af.subject_id = ad.subject_id AND af.hadm_id = ad.hadm_id;
    """.format(table_name=table_name)
        
    sql +="\n"
    sql += "-- 身高\n"
    sql += """
-- 添加缺失的列
ALTER TABLE {table_name} 
ADD COLUMN IF NOT EXISTS height NUMERIC;

-- 更新'height' 字段
UPDATE {table_name} af
SET 
    height = ht.height
FROM mimiciv_derived.first_day_height ht
WHERE af.subject_id = ht.subject_id AND af.stay_id = ht.stay_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 体重\n"
    sql += """
-- 添加缺失的列，如果尚不存在
ALTER TABLE {table_name} 
ADD COLUMN IF NOT EXISTS weight NUMERIC;

-- 更新 'weight' 字段
UPDATE {table_name} af
SET weight = (
    SELECT avg(wt.weight)
    FROM mimiciv_derived.first_day_weight wt
    WHERE af.subject_id = wt.subject_id AND af.stay_id = wt.stay_id 
    GROUP BY wt.subject_id
)
WHERE EXISTS (
    SELECT 1
    FROM mimiciv_derived.first_day_weight wt
    WHERE af.subject_id = wt.subject_id AND af.stay_id = wt.stay_id 
);
""".format(table_name=table_name)
    sql +="\n"
    sql += "-- BMI\n"
    sql += """
-- 添加缺失的列，如果尚不存在
ALTER TABLE {table_name} 
ADD COLUMN IF NOT EXISTS bmi NUMERIC;

-- 更新 'bmi' 字段
UPDATE {table_name} af
SET bmi = weight / (height / 100)^2
WHERE height IS NOT NULL AND weight IS NOT NULL;
""".format(table_name=table_name)
    
    sql += "\n"
    sql += "-- 死亡时间计算\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gender character; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS dod date; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS admittime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS dischtime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS los_hospital numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS admission_age numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS race character varying; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hospital_expire_flag smallint; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hospstay_seq bigint; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_hosp_stay boolean; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS icu_intime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS icu_outtime timestamp without time zone; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS los_icu numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS icustay_seq bigint; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_icu_stay boolean; 
UPDATE {table_name} af 
SET 
    gender = i.gender, 
    dod = i.dod, 
    admittime = i.admittime, 
    dischtime = i.dischtime, 
    los_hospital = i.los_hospital, 
    admission_age = i.admission_age, 
    race = i.race, 
    hospital_expire_flag = i.hospital_expire_flag, 
    hospstay_seq = i.hospstay_seq, 
    first_hosp_stay = i.first_hosp_stay, 
    icu_intime = i.icu_intime, 
    icu_outtime = i.icu_outtime, 
    los_icu = i.los_icu, 
    icustay_seq = i.icustay_seq, 
    first_icu_stay = i.first_icu_stay
FROM mimiciv_derived.icustay_detail i 
WHERE af.stay_id = i.stay_id;
""".format(table_name=table_name)
    return sql

def add_antecedent(table_name, sql):
    sql += "\n"
    sql += "-- 既往史\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS age_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS myocardial_infarct integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS congestive_heart_failure integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS peripheral_vascular_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS cerebrovascular_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS dementia integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS chronic_pulmonary_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS rheumatic_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS peptic_ulcer_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mild_liver_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS diabetes_without_cc integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS diabetes_with_cc integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS paraplegia integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS renal_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS malignant_cancer integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS severe_liver_disease integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS metastatic_solid_tumor integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aids integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS charlson_comorbidity_index integer; 
UPDATE {table_name} af 
SET 
    age_score = i.age_score, 
    myocardial_infarct = i.myocardial_infarct, 
    congestive_heart_failure = i.congestive_heart_failure, 
    peripheral_vascular_disease = i.peripheral_vascular_disease, 
    cerebrovascular_disease = i.cerebrovascular_disease, 
    dementia = i.dementia, 
    chronic_pulmonary_disease = i.chronic_pulmonary_disease, 
    rheumatic_disease = i.rheumatic_disease, 
    peptic_ulcer_disease = i.peptic_ulcer_disease, 
    mild_liver_disease = i.mild_liver_disease, 
    diabetes_without_cc = i.diabetes_without_cc, 
    diabetes_with_cc = i.diabetes_with_cc, 
    paraplegia = i.paraplegia, 
    renal_disease = i.renal_disease, 
    malignant_cancer = i.malignant_cancer, 
    severe_liver_disease = i.severe_liver_disease, 
    metastatic_solid_tumor = i.metastatic_solid_tumor, 
    aids = i.aids, 
    charlson_comorbidity_index = i.charlson_comorbidity_index
FROM mimiciv_derived.charlson i 
WHERE af.subject_id = i.subject_id and af.hadm_id = i.hadm_id;
""".format(table_name=table_name)
    return sql
        
def add_vital_sign(table_name, sql):
    sql += "\n"
    sql += "--患者住院第一天生命体征:心率、血压、血糖\n"
    sql +="""
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS heart_rate_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS heart_rate_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS heart_rate_mean double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS sbp_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS sbp_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS sbp_mean double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS dbp_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS dbp_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS dbp_mean double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS mbp_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS mbp_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS mbp_mean double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS resp_rate_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS resp_rate_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS resp_rate_mean double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS temperature_min numeric; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS temperature_max numeric; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS temperature_mean numeric; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS spo2_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS spo2_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS spo2_mean double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS glucose_min double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS glucose_max double precision; 
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS glucose_mean double precision; 
UPDATE {table_name} af 
SET 
    heart_rate_min = i.heart_rate_min, 
    heart_rate_max = i.heart_rate_max, 
    heart_rate_mean = i.heart_rate_mean, 
    sbp_min = i.sbp_min, 
    sbp_max = i.sbp_max, 
    sbp_mean = i.sbp_mean, 
    dbp_min = i.dbp_min, 
    dbp_max = i.dbp_max, 
    dbp_mean = i.dbp_mean, 
    mbp_min = i.mbp_min, 
    mbp_max = i.mbp_max, 
    mbp_mean = i.mbp_mean, 
    resp_rate_min = i.resp_rate_min, 
    resp_rate_max = i.resp_rate_max, 
    resp_rate_mean = i.resp_rate_mean, 
    temperature_min = i.temperature_min, 
    temperature_max = i.temperature_max, 
    temperature_mean = i.temperature_mean, 
    spo2_min = i.spo2_min, 
    spo2_max = i.spo2_max, 
    spo2_mean = i.spo2_mean, 
    glucose_min = i.glucose_min, 
    glucose_max = i.glucose_max, 
    glucose_mean = i.glucose_mean
FROM mimiciv_derived.first_day_vitalsign i 
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
            """.format(table_name=table_name)
        
    sql += "\n"
    sql += "-- 血气指标\r\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS lactate_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS lactate_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ph_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ph_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS so2_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS so2_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS po2_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS po2_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pco2_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pco2_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aado2_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aado2_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aado2_calc_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aado2_calc_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pao2fio2ratio_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pao2fio2ratio_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS baseexcess_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS baseexcess_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bicarbonate_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bicarbonate_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS totalco2_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS totalco2_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hematocrit_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hematocrit_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hemoglobin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hemoglobin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS carboxyhemoglobin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS carboxyhemoglobin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS methemoglobin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS methemoglobin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS temperature_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS temperature_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS chloride_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS chloride_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS calcium_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS calcium_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS glucose_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS glucose_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS potassium_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS potassium_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sodium_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sodium_max double precision; 
UPDATE {table_name} af 
SET 
    lactate_min = i.lactate_min, 
    lactate_max = i.lactate_max, 
    ph_min = i.ph_min, 
    ph_max = i.ph_max, 
    so2_min = i.so2_min, 
    so2_max = i.so2_max, 
    po2_min = i.po2_min, 
    po2_max = i.po2_max, 
    pco2_min = i.pco2_min, 
    pco2_max = i.pco2_max, 
    aado2_min = i.aado2_min, 
    aado2_max = i.aado2_max, 
    aado2_calc_min = i.aado2_calc_min, 
    aado2_calc_max = i.aado2_calc_max, 
    pao2fio2ratio_min = i.pao2fio2ratio_min, 
    pao2fio2ratio_max = i.pao2fio2ratio_max, 
    baseexcess_min = i.baseexcess_min, 
    baseexcess_max = i.baseexcess_max, 
    bicarbonate_min = i.bicarbonate_min, 
    bicarbonate_max = i.bicarbonate_max, 
    totalco2_min = i.totalco2_min, 
    totalco2_max = i.totalco2_max, 
    hematocrit_min = i.hematocrit_min, 
    hematocrit_max = i.hematocrit_max, 
    hemoglobin_min = i.hemoglobin_min, 
    hemoglobin_max = i.hemoglobin_max, 
    carboxyhemoglobin_min = i.carboxyhemoglobin_min, 
    carboxyhemoglobin_max = i.carboxyhemoglobin_max, 
    methemoglobin_min = i.methemoglobin_min, 
    methemoglobin_max = i.methemoglobin_max, 
    temperature_min = i.temperature_min, 
    temperature_max = i.temperature_max, 
    chloride_min = i.chloride_min, 
    chloride_max = i.chloride_max, 
    calcium_min = i.calcium_min, 
    calcium_max = i.calcium_max, 
    glucose_min = i.glucose_min, 
    glucose_max = i.glucose_max, 
    potassium_min = i.potassium_min, 
    potassium_max = i.potassium_max, 
    sodium_min = i.sodium_min, 
    sodium_max = i.sodium_max
FROM mimiciv_derived.first_day_bg i 
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
""".format(table_name=table_name)

    sql+="\n"
    sql+="-- 化学元素、血细胞指标\r\n"
    sql+="""
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hematocrit_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hematocrit_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hemoglobin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hemoglobin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS platelets_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS platelets_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS wbc_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS wbc_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS albumin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS albumin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS globulin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS globulin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS total_protein_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS total_protein_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aniongap_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS aniongap_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bicarbonate_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bicarbonate_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bun_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bun_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS calcium_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS calcium_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS chloride_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS chloride_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS creatinine_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS creatinine_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS glucose_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS glucose_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sodium_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sodium_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS potassium_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS potassium_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_basophils_min numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_basophils_max numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_eosinophils_min numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_eosinophils_max numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_lymphocytes_min numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_lymphocytes_max numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_monocytes_min numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_monocytes_max numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_neutrophils_min numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS abs_neutrophils_max numeric; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS atyps_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS atyps_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bands_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bands_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS imm_granulocytes_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS imm_granulocytes_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS metas_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS metas_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS nrbc_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS nrbc_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS d_dimer_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS d_dimer_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS fibrinogen_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS fibrinogen_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS thrombin_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS thrombin_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS inr_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS inr_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pt_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pt_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ptt_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ptt_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS alt_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS alt_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS alp_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS alp_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ast_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ast_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS amylase_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS amylase_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_total_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_total_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_direct_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_direct_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_indirect_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_indirect_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ck_cpk_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ck_cpk_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ck_mb_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ck_mb_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ggt_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ggt_max double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ld_ldh_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS ld_ldh_max double precision; 
UPDATE {table_name} af 
SET 
    hematocrit_min = i.hematocrit_min, 
    hematocrit_max = i.hematocrit_max, 
    hemoglobin_min = i.hemoglobin_min, 
    hemoglobin_max = i.hemoglobin_max, 
    platelets_min = i.platelets_min, 
    platelets_max = i.platelets_max, 
    wbc_min = i.wbc_min, 
    wbc_max = i.wbc_max, 
    albumin_min = i.albumin_min, 
    albumin_max = i.albumin_max, 
    globulin_min = i.globulin_min, 
    globulin_max = i.globulin_max, 
    total_protein_min = i.total_protein_min, 
    total_protein_max = i.total_protein_max, 
    aniongap_min = i.aniongap_min, 
    aniongap_max = i.aniongap_max, 
    bicarbonate_min = i.bicarbonate_min, 
    bicarbonate_max = i.bicarbonate_max, 
    bun_min = i.bun_min, 
    bun_max = i.bun_max, 
    calcium_min = i.calcium_min, 
    calcium_max = i.calcium_max, 
    chloride_min = i.chloride_min, 
    chloride_max = i.chloride_max, 
    creatinine_min = i.creatinine_min, 
    creatinine_max = i.creatinine_max, 
    glucose_min = i.glucose_min, 
    glucose_max = i.glucose_max, 
    sodium_min = i.sodium_min, 
    sodium_max = i.sodium_max, 
    potassium_min = i.potassium_min, 
    potassium_max = i.potassium_max, 
    abs_basophils_min = i.abs_basophils_min, 
    abs_basophils_max = i.abs_basophils_max, 
    abs_eosinophils_min = i.abs_eosinophils_min, 
    abs_eosinophils_max = i.abs_eosinophils_max, 
    abs_lymphocytes_min = i.abs_lymphocytes_min, 
    abs_lymphocytes_max = i.abs_lymphocytes_max, 
    abs_monocytes_min = i.abs_monocytes_min, 
    abs_monocytes_max = i.abs_monocytes_max, 
    abs_neutrophils_min = i.abs_neutrophils_min, 
    abs_neutrophils_max = i.abs_neutrophils_max, 
    atyps_min = i.atyps_min, 
    atyps_max = i.atyps_max, 
    bands_min = i.bands_min, 
    bands_max = i.bands_max, 
    imm_granulocytes_min = i.imm_granulocytes_min, 
    imm_granulocytes_max = i.imm_granulocytes_max, 
    metas_min = i.metas_min, 
    metas_max = i.metas_max, 
    nrbc_min = i.nrbc_min, 
    nrbc_max = i.nrbc_max, 
    d_dimer_min = i.d_dimer_min, 
    d_dimer_max = i.d_dimer_max, 
    fibrinogen_min = i.fibrinogen_min, 
    fibrinogen_max = i.fibrinogen_max, 
    thrombin_min = i.thrombin_min, 
    thrombin_max = i.thrombin_max, 
    inr_min = i.inr_min, 
    inr_max = i.inr_max, 
    pt_min = i.pt_min, 
    pt_max = i.pt_max, 
    ptt_min = i.ptt_min, 
    ptt_max = i.ptt_max, 
    alt_min = i.alt_min, 
    alt_max = i.alt_max, 
    alp_min = i.alp_min, 
    alp_max = i.alp_max, 
    ast_min = i.ast_min, 
    ast_max = i.ast_max, 
    amylase_min = i.amylase_min, 
    amylase_max = i.amylase_max, 
    bilirubin_total_min = i.bilirubin_total_min, 
    bilirubin_total_max = i.bilirubin_total_max, 
    bilirubin_direct_min = i.bilirubin_direct_min, 
    bilirubin_direct_max = i.bilirubin_direct_max, 
    bilirubin_indirect_min = i.bilirubin_indirect_min, 
    bilirubin_indirect_max = i.bilirubin_indirect_max, 
    ck_cpk_min = i.ck_cpk_min, 
    ck_cpk_max = i.ck_cpk_max, 
    ck_mb_min = i.ck_mb_min, 
    ck_mb_max = i.ck_mb_max, 
    ggt_min = i.ggt_min, 
    ggt_max = i.ggt_max, 
    ld_ldh_min = i.ld_ldh_min, 
    ld_ldh_max = i.ld_ldh_max
FROM mimiciv_derived.first_day_lab i 
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 患者评分：获取患者住院第一天评分：GCS"
    sql += "\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gcs_min double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gcs_motor double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gcs_verbal double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gcs_eyes double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gcs_unable integer; 
UPDATE {table_name} af 
SET 
    gcs_min = i.gcs_min, 
    gcs_motor = i.gcs_motor, 
    gcs_verbal = i.gcs_verbal, 
    gcs_eyes = i.gcs_eyes, 
    gcs_unable = i.gcs_unable
FROM mimiciv_derived.first_day_gcs i 
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 患者评分：获取患者住院第一天评分：sofa"
    sql += "\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sofa integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS respiration integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS coagulation integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS liver integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS cardiovascular integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS cns integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS renal integer; 
UPDATE {table_name} af 
SET 
    sofa = i.sofa, 
    respiration = i.respiration, 
    coagulation = i.coagulation, 
    liver = i.liver, 
    cardiovascular = i.cardiovascular, 
    cns = i.cns, 
    renal = i.renal
FROM mimiciv_derived.first_day_sofa i 
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 患者评分：获取患者住院第一天评分：saps3"
    sql += "\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sapsii integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sapsii_prob double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS age_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS hr_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sysbp_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS temp_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS pao2fio2_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS uo_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bun_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS wbc_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS potassium_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS sodium_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bicarbonate_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS bilirubin_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS gcs_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS comorbidity_score integer; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS admissiontype_score integer; 
UPDATE {table_name} af 
SET 
    sapsii = i.sapsii, 
    sapsii_prob = i.sapsii_prob, 
    age_score = i.age_score, 
    hr_score = i.hr_score, 
    sysbp_score = i.sysbp_score, 
    temp_score = i.temp_score, 
    pao2fio2_score = i.pao2fio2_score, 
    uo_score = i.uo_score, 
    bun_score = i.bun_score, 
    wbc_score = i.wbc_score, 
    potassium_score = i.potassium_score, 
    sodium_score = i.sodium_score, 
    bicarbonate_score = i.bicarbonate_score, 
    bilirubin_score = i.bilirubin_score, 
    gcs_score = i.gcs_score, 
    comorbidity_score = i.comorbidity_score, 
    admissiontype_score = i.admissiontype_score
FROM mimiciv_derived.sapsii i 
WHERE af.subject_id = i.subject_id and af.stay_id = i.stay_id;
""".format(table_name=table_name)

    
    sql += "\n"
    sql += "-- 获取患者住院:心率heart_rate_arv"
    sql += """
DROP TABLE IF EXISTS mimiciv_data.heart_rate_arv;
CREATE TABLE mimiciv_data.heart_rate_arv AS
SELECT
  derived.subject_id,
  derived.stay_id,
  AVG(derived.abs_diff) AS heart_rate_arv
FROM (
    SELECT 
      ie.subject_id, 
      ie.stay_id, 
      ce.heart_rate,
      LAG(ce.heart_rate) OVER(PARTITION BY ie.stay_id ORDER BY ce.charttime) as prev_heart_rate,
      ABS(ce.heart_rate - LAG(ce.heart_rate) OVER(PARTITION BY ie.stay_id ORDER BY ce.charttime)) as abs_diff
    FROM mimiciv_icu.icustays AS ie
    INNER JOIN mimiciv_derived.vitalsign AS ce
      ON ie.stay_id = ce.stay_id --筛选时间范围的心率数据，计算第一次icu_stay期间内的心率变异--AND ce.charttime >= ie.intime - INTERVAL '6 HOUR'--AND ce.charttime <= ie.intime + INTERVAL '1 DAY'
  ) AS derived
GROUP BY
  derived.subject_id,
  derived.stay_id;
"""
    sql += "\n"
    sql += "-- 获取患者住院:心率heart_rate_arv"
    sql += "\n"
    sql += """
-- 添加缺失的列，如果尚不存在
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS heart_rate_arv NUMERIC;

-- 更新 'heart_rate_arv' 字段
UPDATE {table_name} af
SET heart_rate_arv = hr.heart_rate_arv
FROM mimiciv_data.heart_rate_arv hr
WHERE af.subject_id = hr.subject_id;
DROP TABLE IF EXISTS mimiciv_data.heart_rate_arv;
    """.format(table_name=table_name)
    
    return sql

def add_blood_info(table_name, sql):
    sql += "\n"
    sql += "-- 获取患者住院：红细胞相关指标"
    sql += "\n"
    sql += """
DROP TABLE IF EXISTS mimiciv_data.blood_mean;
CREATE TABLE mimiciv_data.blood_mean AS
SELECT
  derived.subject_id,
  derived.hadm_id,

  AVG(derived.hematocrit) AS mean_hematocrit,
  AVG(derived.hemoglobin) AS mean_hemoglobin,
  AVG(derived.mch) AS mean_mch,
  AVG(derived.mchc) AS mean_mchc,
  AVG(derived.mcv) AS mean_mcv,
  AVG(derived.platelet) AS mean_platelet,
  AVG(derived.rbc) AS mean_rbc,
  AVG(derived.rdw) AS mean_rdw,
  AVG(derived.rdwsd) AS mean_rdwsd,
  AVG(derived.wbc) AS mean_wbc

FROM (
    SELECT 
      ce.*
    FROM mimiciv_icu.icustays AS ie
    INNER JOIN mimiciv_derived.complete_blood_count AS ce
      ON ie.hadm_id = ce.hadm_id
      AND ce.charttime >= ie.intime - INTERVAL '6 HOUR'
      AND ce.charttime <= ie.intime + INTERVAL '1 DAY'
  ) AS derived
GROUP BY
  derived.subject_id,
  derived.hadm_id;
"""
    sql += "\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_hematocrit double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_hemoglobin double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_mch double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_mchc double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_mcv double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_platelet double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_rbc double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_rdw double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_rdwsd double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS mean_wbc double precision; 
UPDATE {table_name} af 
SET 
    mean_hematocrit = i.mean_hematocrit, 
    mean_hemoglobin = i.mean_hemoglobin, 
    mean_mch = i.mean_mch, 
    mean_mchc = i.mean_mchc, 
    mean_mcv = i.mean_mcv, 
    mean_platelet = i.mean_platelet, 
    mean_rbc = i.mean_rbc, 
    mean_rdw = i.mean_rdw, 
    mean_rdwsd = i.mean_rdwsd, 
    mean_wbc = i.mean_wbc
FROM mimiciv_data.blood_mean i 
WHERE af.hadm_id = i.hadm_id;
DROP TABLE IF EXISTS mimiciv_data.blood_mean;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 获取患者住院：血清指标"
    sql += "\n"
    sql += """
DROP TABLE IF EXISTS mimiciv_data.blood_first;
CREATE TABLE mimiciv_data.blood_first AS (
SELECT
cbc.*,
ROW_NUMBER() OVER(PARTITION BY cbc.subject_id, cbc.hadm_id ORDER BY cbc.charttime ASC) as lab_rank
from mimiciv_derived.complete_blood_count cbc
WHERE cbc.hadm_id IN (SELECT hadm_id FROM {table_name})
);
    """.format(table_name=table_name)

    sql += "\n"
    sql += """
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_hematocrit double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_hemoglobin double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_mch double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_mchc double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_mcv double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_platelet double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_rbc double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_rdw double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_rdwsd double precision; 
ALTER TABLE {table_name} ADD COLUMN  IF NOT EXISTS first_wbc double precision; 
UPDATE {table_name} af 
SET 
    first_hematocrit = i.hematocrit, 
    first_hemoglobin = i.hemoglobin, 
    first_mch = i.mch, 
    first_mchc = i.mchc, 
    first_mcv = i.mcv, 
    first_platelet = i.platelet, 
    first_rbc = i.rbc, 
    first_rdw = i.rdw, 
    first_rdwsd = i.rdwsd, 
    first_wbc = i.wbc
FROM mimiciv_data.blood_first i 
WHERE i.lab_rank=1 and af.hadm_id = i.hadm_id;
DROP TABLE IF EXISTS mimiciv_data.blood_first;
    """.format(table_name=table_name)

    return sql

def add_cardiovascular_lab(table_name, sql):
    sql += "\n"
    sql += "-- 获取患者住院：首次住院的第一次化验指标"
    sql += "\n"
    sql += """-- 创建甘油三酯查询临时表
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_Triglyceride NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (51000) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_Triglyceride = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;
    """.format(table_name=table_name)

    sql += "\n"
    sql += """-- 低密度脂蛋白
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_LDL NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (50905,50906) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_LDL = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;   
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 糖化血红蛋白"
    sql += "\n"
    sql += """
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_hba1c NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (50852) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_hba1c = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 高密度脂蛋白"
    sql += "\n"
    sql += """
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_HDL NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (50904) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_HDL = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 血钾水平"
    sql += "\n"
    sql += """
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_Potassium NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (50822, 50833, 52452, 52610,50971) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_Potassium = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 建立NT-Pro 脑利钠肽临时表并更新"
    sql += "\n"
    sql += """
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_NTproBNP NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (50963) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_NTproBNP = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;
""".format(table_name=table_name)

    sql += "\n"
    sql += "-- 建立glucose临时表并更新"
    sql += "\n"
    sql += """
ALTER TABLE {table_name}
ADD COLUMN IF NOT EXISTS first_glucose NUMERIC;

WITH tg AS (
    SELECT 
        lab.subject_id, 
        lab.hadm_id, 
        lab.charttime, 
        lab.valuenum,
        ROW_NUMBER() OVER(PARTITION BY lab.subject_id, lab.hadm_id ORDER BY lab.charttime ASC) AS lab_rank
    FROM 
        mimiciv_hosp.labevents lab
    WHERE 
        lab.itemid IN (50809, 50931, 51478, 51981, 52027, 52569) 
        AND lab.valuenum IS NOT NULL 
        AND lab.hadm_id IN (SELECT hadm_id FROM {table_name})
)

UPDATE {table_name} af
SET 
    first_glucose = hr.valuenum
FROM 
    tg hr
WHERE 
    hr.lab_rank = 1 
    AND af.hadm_id = hr.hadm_id;
""".format(table_name=table_name)

    return sql


def add_medicine(table_name, sql):
    sql += "\n"
    sql += "-- 获取患者住院：首次住院的第一次用药"
    sql += "\n"
    sql += """
-- 添加缺失的列，如果尚不存在
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_aspirin integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_clopidogrel integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_furosemide integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_lisinopril integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_metoprolol integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_losartan integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_amlodipine integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_diltiazem integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_digoxin integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_amiodarone integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_insulin integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_statin integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_dabigatran integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_rivaroxaban integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_heparin integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_warfarin integer;
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS used_sacubactril_valsartan integer;

-- 建立临时用药表
with used_drugs as (
SELECT 
    pre.hadm_id,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%aspirin%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过阿司匹林
        ELSE 0
    END AS used_aspirin,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%clopidogrel%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过氯吡格雷
        ELSE 0
    END AS used_clopidogrel,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%furosemide%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过呋塞米
        ELSE 0
    END AS used_furosemide,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%lisinopril%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过依普利
        ELSE 0
    END AS used_lisinopril,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%metoprolol%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过美托洛尔
        ELSE 0
    END AS used_metoprolol,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%losartan%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过洛卡特普
        ELSE 0
    END AS used_losartan,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%amlodipine%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过氨氯地平
        ELSE 0
    END AS used_amlodipine,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%diltiazem%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过地尔硫卓
        ELSE 0
    END AS used_diltiazem,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%digoxin%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过地高辛
        ELSE 0
    END AS used_digoxin,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%amiodarone%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过胺碘酮
        ELSE 0
    END AS used_amiodarone,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%insulin%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过胰岛素
        ELSE 0
    END AS used_insulin,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%statin%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过他汀
		ELSE 0
    END AS used_statin,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%dabigatran%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过达比加群
		ELSE 0
    END AS used_dabigatran,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%rivaroxaban%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过利伐沙班
		ELSE 0
    END AS used_rivaroxaban,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%heparin%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过肝素
		ELSE 0
    END AS used_heparin,
    CASE 
        WHEN MAX(CASE WHEN drug ILIKE '%warfarin%' THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过华法林
		ELSE 0
    END AS used_warfarin,
    CASE 
        WHEN MAX(CASE WHEN LOWER(drug) LIKE '%sacubitril%' AND LOWER(drug) LIKE '%valsartan%'  THEN 1 ELSE 0 END) = 1 THEN 1 -- 用过沙库巴替利/缬沙坦
		ELSE 0
    END AS used_sacubactril_valsartan
    
FROM 
	mimiciv_hosp.prescriptions pre
WHERE 
	pre.hadm_id IN (SELECT hadm_id FROM {table_name})
GROUP BY 
    pre.hadm_id
)

-- 更新用药情况
UPDATE {table_name} af
SET 
    used_aspirin = ud.used_aspirin,
    used_clopidogrel = ud.used_clopidogrel,
    used_furosemide = ud.used_furosemide,
    used_lisinopril = ud.used_lisinopril,
    used_metoprolol = ud.used_metoprolol,
    used_losartan = ud.used_losartan,
    used_amlodipine = ud.used_amlodipine,
    used_diltiazem = ud.used_diltiazem,
    used_digoxin = ud.used_digoxin,
    used_amiodarone = ud.used_amiodarone,
    used_insulin = ud.used_insulin,
    used_statin = ud.used_statin,
    used_dabigatran = ud.used_dabigatran,
    used_rivaroxaban = ud.used_rivaroxaban,
    used_heparin = ud.used_heparin,
    used_warfarin = ud.used_warfarin
FROM
    used_drugs ud
WHERE 
    af.hadm_id = ud.hadm_id;
""".format(table_name=table_name)
    return sql

def add_surgeries(table_name, sql):
    sql += "\n"
    sql += "-- 获取患者手术史情况"
    sql += "\n"
    sql += """
-- Step 1: Add the new column 'cardiac_surgery_before' to the table
ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS cardiac_surgery_before INT DEFAULT 0;

-- Step 2: Update the 'cardiac_surgery' column based on the existence of prior heart surgery
WITH temp_heart_sur AS (
    SELECT 
        p.subject_id
    FROM 
        mimiciv_hosp.procedures_icd AS p
    JOIN 
        mimiciv_hosp.d_icd_procedures AS d 
        ON p.icd_code = d.icd_code
    JOIN 
        {table_name} AS a 
        ON p.subject_id = a.subject_id
    WHERE 
        (d.long_title ILIKE '%heart%' OR d.long_title ILIKE '%cardiac%')
        AND p.seq_num < a.seq_num
    GROUP BY 
        p.subject_id
)
UPDATE 
    {table_name} AS a
SET 
    cardiac_surgery_before = 1
FROM 
    temp_heart_sur AS t
WHERE 
    a.subject_id = t.subject_id;
""".format(table_name=table_name)
    return sql



def add_past_diagnostic(table_name,sql):
    sql += "\n"
    sql += "-- 获取患者过往疾病诊断史"
    sql += "\n"
    sql += """
-- Step 1: Add the new columns for prior diagnosis, icd codes, and long titles of anxiolytic
    ALTER TABLE {table_name} 
    ADD COLUMN IF NOT EXISTS prior_anxiolytic INT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prior_anxiolytic_icd_codes TEXT DEFAULT '',
    ADD COLUMN IF NOT EXISTS prior_anxiolytic_long_titles TEXT DEFAULT '';

    -- Step 2: Update the new columns based on prior diagnosis records
    WITH temp_diag AS (
        SELECT 
            d.subject_id,
            STRING_AGG(DISTINCT d.icd_code, ',') AS icd_codes, -- 以逗号连接ICD代码
            STRING_AGG(DISTINCT i.long_title, '; ') AS long_titles -- 以分号连接long_title
        FROM 
            mimiciv_hosp.diagnoses_icd AS d
        JOIN 
            mimiciv_hosp.d_icd_diagnoses AS i 
            ON d.icd_code = i.icd_code
        JOIN 
            {table_name} AS a 
            ON d.subject_id = a.subject_id
        WHERE 
            d.icd_code IN ('30410  ', '30411  ', '30412  ', '30413  ', '30540  ', '30541  ', '30542  ', '30543  ', 'F13    ', 'F131   ', 'F1310  ', 'F1311  ', 'F1312  ', 'F13120 ', 'F13121 ', 'F13129 ', 'F1313  ', 'F13130 ', 'F13131 ', 'F13132 ', 'F13139 ', 'F1314  ', 'F1315  ', 'F13150 ', 'F13151 ', 'F13159 ', 'F1318  ', 'F13180 ', 'F13181 ', 'F13182 ', 'F13188 ', 'F1319  ', 'F132   ', 'F1320  ', 'F1321  ', 'F1322  ', 'F13220 ', 'F13221 ', 'F13229 ', 'F1323  ', 'F13230 ', 'F13231 ', 'F13232 ', 'F13239 ', 'F1324  ', 'F1325  ', 'F13250 ', 'F13251 ', 'F13259 ', 'F1326  ', 'F1327  ', 'F1328  ', 'F13280 ', 'F13281 ', 'F13282 ', 'F13288 ', 'F1329  ', 'F139   ', 'F1390  ', 'F1391  ', 'F1392  ', 'F13920 ', 'F13921 ', 'F13929 ', 'F1393  ', 'F13930 ', 'F13931 ', 'F13932 ', 'F13939 ', 'F1394  ', 'F1395  ', 'F13950 ', 'F13951 ', 'F13959 ', 'F1396  ', 'F1397  ', 'F1398  ', 'F13980 ', 'F13981 ', 'F13982 ', 'F13988 ', 'F1399  ', 'P041A  ')
            AND d.seq_num < a.seq_num
        GROUP BY 
            d.subject_id
    )
    UPDATE 
        {table_name} AS a
    SET 
        prior_anxiolytic = 1,
        prior_anxiolytic_icd_codes = t.icd_codes,
        prior_anxiolytic_long_titles = t.long_titles
    FROM 
        temp_diag AS t
    WHERE 
        a.subject_id = t.subject_id;
""".format(table_name=table_name)
    return sql
