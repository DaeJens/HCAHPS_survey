-- CMS HCAHPS (Hospital) Data Cleaning

-- First, we will look at the Hospital General Information dataset
-- 1. Duplicate raw dataset before proceeding
CREATE TABLE hospital_geninfo_staging1
LIKE hospital_geninfo;

INSERT hospital_geninfo_staging1
SELECT *
FROM hospital_geninfo;

SELECT * FROM hospital_geninfo_staging1;

-- 2. Remove duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
		facility_id, 
    facility_name, 
    city,
    state,
    zip_code,
    county_or_parish,
    hospital_type,
    hospital_ownership,
    emergency_services,
    birthing_friendly_designation,
    hospital_overall_rating,
    MORT_group_measure_count,
    count_of_facility_MORT_measures,
    count_of_MORT_measures_better,
    count_of_MORT_measures_no_diff,
    count_of_MORT_measures_worse,
    safety_group_measure_count,
    count_of_safety_measures_better,
    count_of_safety_measures_no_diff,
    count_of_safety_measures_worse,
    READM_group_measure_count,
    count_of_READM_measures_better,
    count_of_READM_measures_no_diff,
    count_of_READM_measures_worse,
    pt_exp_group_measure_count,
    count_of_facility_pt_exp_measures,
    TE_group_measure_count,
    count_of_facility_TE_measures
    ) AS row_num
FROM hospital_geninfo_staging1;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
		facility_id, 
    facility_name, 
    city,
    state,
    zip_code,
    county_or_parish,
    hospital_type,
    hospital_ownership,
    emergency_services,
    birthing_friendly_designation,
    hospital_overall_rating,
    MORT_group_measure_count,
    count_of_facility_MORT_measures,
    count_of_MORT_measures_better,
    count_of_MORT_measures_no_diff,
    count_of_MORT_measures_worse,
    safety_group_measure_count,
    count_of_safety_measures_better,
    count_of_safety_measures_no_diff,
    count_of_safety_measures_worse,
    READM_group_measure_count,
    count_of_READM_measures_better,
    count_of_READM_measures_no_diff,
    count_of_READM_measures_worse,
    pt_exp_group_measure_count,
    count_of_facility_pt_exp_measures,
    TE_group_measure_count,
    count_of_facility_TE_measures
    ) AS row_num
FROM hospital_geninfo_staging1
) SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- There are no duplicate entries in this dataset. 

-- 3. Standardize the data
SELECT DISTINCT(facility_name)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(address)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(city)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(state)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(zip_code)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(county_or_parish)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(telephone_number)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(hospital_type)
FROM hospital_geninfo_staging
ORDER BY 1;

SELECT DISTINCT(hospital_ownership)
FROM hospital_geninfo_staging
ORDER BY 1;

-- Add region to the dataset
ALTER TABLE hospital_geninfo_staging1
ADD COLUMN region VARCHAR(50);

UPDATE hospital_geninfo_staging1
SET region =
	CASE
	WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT') THEN 'New England'
	WHEN state IN ('NY', 'PA', 'NJ') THEN 'Mid-Atlantic'
	WHEN state IN ('OH', 'IN', 'IL', 'MI', 'WI') THEN 'East North Central'
	WHEN state IN ('MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'West North Central'
	WHEN state IN ('DE', 'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL') THEN 'South Atlantic'
	WHEN state IN ('AL', 'MS', 'TN', 'KY') THEN 'East South Central'
	WHEN state IN ('TX', 'OK', 'AR', 'LA') THEN 'West South Central'
	WHEN state IN ('MT', 'ID', 'WY', 'CO', 'UT', 'NV', 'AZ', 'NM') THEN 'Mountain West'
	WHEN state IN ('WA', 'OR', 'CA', 'HI', 'AK') THEN 'Pacific'
	ELSE 'Other/Uncategorized'
	END;

-- 4. Null values or blank values
SELECT *
FROM hospital_geninfo_staging1
WHERE hospital_overall_rating IS NULL;
-- 2,572 entries (~48%) do not have a hospital rating.

-- Let's examine which records have only footnotes
SELECT *
FROM hospital_geninfo_staging1
WHERE hospital_overall_rating IS NULL
	AND MORT_group_footnote IS NOT NULL
    AND safety_group_footnote IS NOT NULL
    AND READM_group_footnote IS NOT NULL
    AND pt_exp_group_footnote IS NOT NULL
    AND TE_group_footnote IS NOT NULL;
-- 859 entries are NULL across all measures, with footnotes corresponding to reasons such as 
-- "Results are not available for this reporting period" or "too few measures or measure groups 
-- reported to calculate a score". 
-- For this reason, these entries are not useful in downstream analyses and will be removed.
-- Let's duplicate our table and remove these entries.
CREATE TABLE hospital_geninfo_staging2
LIKE hospital_geninfo_staging1;

INSERT hospital_geninfo_staging2
SELECT *
FROM hospital_geninfo_staging1;

SELECT * FROM hospital_geninfo_staging2;

DELETE FROM hospital_geninfo_staging2
WHERE hospital_overall_rating IS NULL
	AND MORT_group_footnote IS NOT NULL
    AND safety_group_footnote IS NOT NULL
    AND READM_group_footnote IS NOT NULL
    AND pt_exp_group_footnote IS NOT NULL
    AND TE_group_footnote IS NOT NULL;

-- Next, we will look at the HCAHPS datasets
-- HCAHPS 2024 (Hospital)
-- 1. Duplicate raw dataset before proceeding
CREATE TABLE hcahps_hospital_2024_staging1
LIKE hcahps_hospital_2024;

INSERT hcahps_hospital_2024_staging1
SELECT *
FROM hcahps_hospital_2024;

SELECT * FROM hcahps_hospital_2024_staging1;

-- 2. Remove duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2024_staging1;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2024_staging1
) SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- There are no duplicate records in the HCAHPS 2024 dataset.

-- 3. Standardize the data
SELECT * FROM hcahps_hospital_2024_staging1;

SELECT DISTINCT(facility_name)
FROM hcahps_hospital_2024_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(address)
FROM hcahps_hospital_2024_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(city)
FROM hcahps_hospital_2024_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(state)
FROM hcahps_hospital_2024_staging1
ORDER BY 1 ASC;
-- HCAHPS dataset includes all 50 states plus 6 territories (AS, DC, GU, MP, PR, VI)

SELECT DISTINCT(zip_code)
FROM hcahps_hospital_2024_staging1
ORDER BY 1 ASC;

SELECT *
FROM hcahps_hospital_2024_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

UPDATE hcahps_hospital_2024_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "YES, ", "YES ");
    
UPDATE hcahps_hospital_2024_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "NO, ", "NO ");

UPDATE hcahps_hospital_2024_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "YES, ", "YES ");

UPDATE hcahps_hospital_2024_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "NO, ", "NO ");
    
SELECT *
FROM hcahps_hospital_2024_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

-- Change start/end date from VARCHAR to DATE
UPDATE hcahps_hospital_2024_staging1
SET start_date =
	STR_TO_DATE(start_date, '%m/%d/%Y');

UPDATE hcahps_hospital_2024_staging1
SET end_date =
	STR_TO_DATE(end_date, '%m/%d/%Y');

-- 4. Null values or blank values
SELECT * FROM hcahps_hospital_2024_staging1;

-- There are a mix of 'NULL', 'Not Applicable', and 'Not Available' entries in our dataset. 
-- Let's replace 'Not Applicable' and 'Not Available' with 'NULL'.
SELECT DISTINCT(patient_survey_star_rating) FROM hcahps_hospital_2024_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2024_staging1
SET patient_survey_star_rating =
	CASE
		WHEN patient_survey_star_rating = 'Not Applicable' OR patient_survey_star_rating = 'Not Available' THEN NULL
		ELSE patient_survey_star_rating
	END;

SELECT DISTINCT(hcahps_answer_percent) FROM hcahps_hospital_2024_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2024_staging1
SET hcahps_answer_percent =
	CASE
		WHEN hcahps_answer_percent = 'Not Applicable' OR hcahps_answer_percent = 'Not Available' THEN NULL
		ELSE hcahps_answer_percent
	END;

SELECT DISTINCT(hcahps_linear_mean) FROM hcahps_hospital_2024_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2024_staging1
SET hcahps_linear_mean =
	CASE
		WHEN hcahps_linear_mean = 'Not Applicable' OR hcahps_linear_mean = 'Not Available' THEN NULL
		ELSE hcahps_linear_mean
	END;

SELECT DISTINCT(num_completed_surveys) FROM hcahps_hospital_2024_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2024_staging1
SET num_completed_surveys =
	CASE
		WHEN num_completed_surveys = 'Not Available' THEN NULL
        ELSE num_completed_surveys
	END;

SELECT DISTINCT(survey_response_rate_percent) FROM hcahps_hospital_2024_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2024_staging1
SET survey_response_rate_percent =
	CASE
		WHEN survey_response_rate_percent = 'Not Available' THEN NULL
        ELSE survey_response_rate_percent
	END;

SELECT *
FROM hcahps_hospital_2024_staging1
WHERE patient_survey_star_rating IS NULL
	AND hcahps_answer_percent IS NULL
    AND hcahps_linear_mean IS NULL
    AND num_completed_surveys IS NULL
    AND survey_response_rate_percent IS NULL
;
-- No entries are null in all measurements.

-- HCAHPS 2023 (Hospital)
-- 1. Duplicate raw dataset before proceeding
CREATE TABLE hcahps_hospital_2023_staging1
LIKE hcahps_hospital_2023;

INSERT hcahps_hospital_2023_staging1
SELECT *
FROM hcahps_hospital_2023;

SELECT * FROM hcahps_hospital_2023_staging1;

-- 2. Remove duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2023_staging1;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2023_staging1
) SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- There are no duplicate records in the HCAHPS 2023 dataset.

-- 3. Standardize the data
SELECT * FROM hcahps_hospital_2023_staging1;

SELECT DISTINCT(facility_name)
FROM hcahps_hospital_2023_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(address)
FROM hcahps_hospital_2023_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(city)
FROM hcahps_hospital_2023_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(state)
FROM hcahps_hospital_2023_staging1
ORDER BY 1 ASC;
-- HCAHPS dataset includes all 50 states plus 6 territories (AS, DC, GU, MP, PR, VI)

SELECT DISTINCT(zip_code)
FROM hcahps_hospital_2023_staging1
ORDER BY 1 ASC;

SELECT *
FROM hcahps_hospital_2023_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

UPDATE hcahps_hospital_2023_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "YES, ", "YES ");
    
UPDATE hcahps_hospital_2023_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "NO, ", "NO ");

UPDATE hcahps_hospital_2023_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "YES, ", "YES ");

UPDATE hcahps_hospital_2023_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "NO, ", "NO ");
    
SELECT *
FROM hcahps_hospital_2023_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

-- Change start/end date from VARCHAR to DATE
UPDATE hcahps_hospital_2023_staging1
SET start_date =
	STR_TO_DATE(start_date, '%m/%d/%Y');

UPDATE hcahps_hospital_2023_staging1
SET end_date =
	STR_TO_DATE(end_date, '%m/%d/%Y');

-- 4. Null values or blank values
SELECT * FROM hcahps_hospital_2023_staging1;

-- There are a mix of 'NULL', 'Not Applicable', and 'Not Available' entries in our dataset. 
-- Let's replace 'Not Applicable' and 'Not Available' with 'NULL'.
SELECT DISTINCT(patient_survey_star_rating) FROM hcahps_hospital_2023_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2023_staging1
SET patient_survey_star_rating =
	CASE
		WHEN patient_survey_star_rating = 'Not Applicable' OR patient_survey_star_rating = 'Not Available' THEN NULL
		ELSE patient_survey_star_rating
	END;

SELECT DISTINCT(hcahps_answer_percent) FROM hcahps_hospital_2023_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2023_staging1
SET hcahps_answer_percent =
	CASE
		WHEN hcahps_answer_percent = 'Not Applicable' OR hcahps_answer_percent = 'Not Available' THEN NULL
		ELSE hcahps_answer_percent
	END;

SELECT DISTINCT(hcahps_linear_mean) FROM hcahps_hospital_2023_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2023_staging1
SET hcahps_linear_mean =
	CASE
		WHEN hcahps_linear_mean = 'Not Applicable' OR hcahps_linear_mean = 'Not Available' THEN NULL
		ELSE hcahps_linear_mean
	END;

SELECT DISTINCT(num_completed_surveys) FROM hcahps_hospital_2023_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2023_staging1
SET num_completed_surveys =
	CASE
		WHEN num_completed_surveys = 'Not Available' THEN NULL
        ELSE num_completed_surveys
	END;

SELECT DISTINCT(survey_response_rate_percent) FROM hcahps_hospital_2023_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2023_staging1
SET survey_response_rate_percent =
	CASE
		WHEN survey_response_rate_percent = 'Not Available' THEN NULL
        ELSE survey_response_rate_percent
	END;

SELECT COUNT(*)
FROM hcahps_hospital_2023_staging1
WHERE patient_survey_star_rating IS NULL
	AND hcahps_answer_percent IS NULL
    AND hcahps_linear_mean IS NULL
    AND num_completed_surveys IS NULL
    AND survey_response_rate_percent IS NULL
;
-- There are 66,216 entries where every measurement is NULL across the board. 

-- HCAHPS 2022 (Hospital)
-- 1. Duplicate raw dataset before proceeding
CREATE TABLE hcahps_hospital_2022_staging1
LIKE hcahps_hospital_2022;

INSERT hcahps_hospital_2022_staging1
SELECT *
FROM hcahps_hospital_2022;

SELECT * FROM hcahps_hospital_2022_staging1;

-- 2. Remove duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2022_staging1;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2022_staging1
) SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- There are no duplicate records in the HCAHPS 2022 dataset.

-- 3. Standardize the data
SELECT * FROM hcahps_hospital_2022_staging1;

SELECT DISTINCT(facility_name)
FROM hcahps_hospital_2022_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(address)
FROM hcahps_hospital_2022_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(city)
FROM hcahps_hospital_2022_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(state)
FROM hcahps_hospital_2022_staging1
ORDER BY 1 ASC;
-- HCAHPS dataset includes all 50 states plus 6 territories (AS, DC, GU, MP, PR, VI)

SELECT DISTINCT(zip_code)
FROM hcahps_hospital_2022_staging1
ORDER BY 1 ASC;

SELECT *
FROM hcahps_hospital_2022_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

UPDATE hcahps_hospital_2022_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "YES, ", "YES ");
    
UPDATE hcahps_hospital_2022_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "NO, ", "NO ");

UPDATE hcahps_hospital_2022_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "yes, ", "YES ");

UPDATE hcahps_hospital_2022_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "no, ", "NO ");
    
SELECT *
FROM hcahps_hospital_2022_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

-- Change start/end date from VARCHAR to DATE
UPDATE hcahps_hospital_2022_staging1
SET start_date =
	STR_TO_DATE(start_date, '%m/%d/%Y');

UPDATE hcahps_hospital_2022_staging1
SET end_date =
	STR_TO_DATE(end_date, '%m/%d/%Y');

-- 4. Null values or blank values
SELECT * FROM hcahps_hospital_2022_staging1;

-- There are a mix of 'NULL', 'Not Applicable', and 'Not Available' entries in our dataset. 
-- Let's replace 'Not Applicable' and 'Not Available' with 'NULL'.
SELECT DISTINCT(patient_survey_star_rating) FROM hcahps_hospital_2022_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2022_staging1
SET patient_survey_star_rating =
	CASE
		WHEN patient_survey_star_rating = 'Not Applicable' OR patient_survey_star_rating = 'Not Available' THEN NULL
		ELSE patient_survey_star_rating
	END;

SELECT DISTINCT(hcahps_answer_percent) FROM hcahps_hospital_2022_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2022_staging1
SET hcahps_answer_percent =
	CASE
		WHEN hcahps_answer_percent = 'Not Applicable' OR hcahps_answer_percent = 'Not Available' THEN NULL
		ELSE hcahps_answer_percent
	END;

SELECT DISTINCT(hcahps_linear_mean) FROM hcahps_hospital_2022_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2022_staging1
SET hcahps_linear_mean =
	CASE
		WHEN hcahps_linear_mean = 'Not Applicable' OR hcahps_linear_mean = 'Not Available' THEN NULL
		ELSE hcahps_linear_mean
	END;

SELECT DISTINCT(num_completed_surveys) FROM hcahps_hospital_2022_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2022_staging1
SET num_completed_surveys =
	CASE
		WHEN num_completed_surveys = 'Not Available' THEN NULL
        ELSE num_completed_surveys
	END;

SELECT DISTINCT(survey_response_rate_percent) FROM hcahps_hospital_2022_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2022_staging1
SET survey_response_rate_percent =
	CASE
		WHEN survey_response_rate_percent = 'Not Available' THEN NULL
        ELSE survey_response_rate_percent
	END;

SELECT COUNT(*)
FROM hcahps_hospital_2022_staging1
WHERE patient_survey_star_rating IS NULL
	AND hcahps_answer_percent IS NULL
    AND hcahps_linear_mean IS NULL
    AND num_completed_surveys IS NULL
    AND survey_response_rate_percent IS NULL
;
-- There are 65,379 entries that are NULL across all measurements in the 2022 dataset.

-- HCAHPS 2021 (Hospital)
-- 1. Duplicate raw dataset before proceeding
CREATE TABLE hcahps_hospital_2021_staging1
LIKE hcahps_hospital_2021;

INSERT hcahps_hospital_2021_staging1
SELECT *
FROM hcahps_hospital_2021;

SELECT * FROM hcahps_hospital_2021_staging1;

-- 2. Remove duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2021_staging1;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
	facility_id,
    facility_name,
    address,
    city,
    state,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date) AS row_num
FROM hcahps_hospital_2021_staging1
) SELECT *
FROM duplicate_cte
WHERE row_num > 1;
-- There are no duplicate records in the HCAHPS 2021 dataset.

-- 3. Standardize the data
SELECT * FROM hcahps_hospital_2021_staging1;

SELECT DISTINCT(facility_name)
FROM hcahps_hospital_2021_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(address)
FROM hcahps_hospital_2021_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(city)
FROM hcahps_hospital_2021_staging1
ORDER BY 1 ASC;

SELECT DISTINCT(state)
FROM hcahps_hospital_2021_staging1
ORDER BY 1 ASC;
-- HCAHPS dataset includes all 50 states plus 6 territories (AS, DC, GU, MP, PR, VI)

SELECT DISTINCT(zip_code)
FROM hcahps_hospital_2021_staging1
ORDER BY 1 ASC;

SELECT *
FROM hcahps_hospital_2021_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

UPDATE hcahps_hospital_2021_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "YES, ", "YES ");
    
UPDATE hcahps_hospital_2021_staging1
SET hcahps_question =
	REPLACE(hcahps_question, "NO, ", "NO ");

UPDATE hcahps_hospital_2021_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "yes, ", "YES ");

UPDATE hcahps_hospital_2021_staging1
SET hcahps_answer_description =
	REPLACE(hcahps_answer_description, "no, ", "NO ");
    
SELECT *
FROM hcahps_hospital_2021_staging1
WHERE hcahps_measure_id LIKE "H_RECMND_%";

-- Change start/end date from VARCHAR to DATE
UPDATE hcahps_hospital_2021_staging1
SET start_date =
	STR_TO_DATE(start_date, '%m/%d/%Y');

UPDATE hcahps_hospital_2021_staging1
SET end_date =
	STR_TO_DATE(end_date, '%m/%d/%Y');

-- 4. Null values or blank values
SELECT * FROM hcahps_hospital_2021_staging1;

-- There are a mix of 'NULL', 'Not Applicable', and 'Not Available' entries in our dataset. 
-- Let's replace 'Not Applicable' and 'Not Available' with 'NULL'.
SELECT DISTINCT(patient_survey_star_rating) FROM hcahps_hospital_2021_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2021_staging1
SET patient_survey_star_rating =
	CASE
		WHEN patient_survey_star_rating = 'Not Applicable' OR patient_survey_star_rating = 'Not Available' THEN NULL
		ELSE patient_survey_star_rating
	END;

SELECT DISTINCT(hcahps_answer_percent) FROM hcahps_hospital_2021_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2021_staging1
SET hcahps_answer_percent =
	CASE
		WHEN hcahps_answer_percent = 'Not Applicable' OR hcahps_answer_percent = 'Not Available' THEN NULL
		ELSE hcahps_answer_percent
	END;

SELECT DISTINCT(hcahps_linear_mean) FROM hcahps_hospital_2021_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2021_staging1
SET hcahps_linear_mean =
	CASE
		WHEN hcahps_linear_mean = 'Not Applicable' OR hcahps_linear_mean = 'Not Available' THEN NULL
		ELSE hcahps_linear_mean
	END;

SELECT DISTINCT(num_completed_surveys) FROM hcahps_hospital_2021_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2021_staging1
SET num_completed_surveys =
	CASE
		WHEN num_completed_surveys = 'Not Available' THEN NULL
        ELSE num_completed_surveys
	END;

SELECT DISTINCT(survey_response_rate_percent) FROM hcahps_hospital_2021_staging1 ORDER BY 1 DESC;

UPDATE hcahps_hospital_2021_staging1
SET survey_response_rate_percent =
	CASE
		WHEN survey_response_rate_percent = 'Not Available' THEN NULL
        ELSE survey_response_rate_percent
	END;

SELECT *
FROM hcahps_hospital_2021_staging1
WHERE patient_survey_star_rating IS NULL
	AND hcahps_answer_percent IS NULL
    AND hcahps_linear_mean IS NULL
    AND num_completed_surveys IS NULL
    AND survey_response_rate_percent IS NULL
;
-- There are 96,069 entries that are NULL across all measures in the 2021 dataset.

-- Join the HCAHPS datasets
-- The HCAHPS datasets have the same columns, so a UNION will be the most appropriate join.
CREATE TABLE hcahps_combined AS (
SELECT *, 2023 AS `year`
FROM hcahps_hospital_2024_staging1
UNION ALL
SELECT *, 2022 AS `year`
FROM hcahps_hospital_2023_staging1
UNION ALL
SELECT *, 2021 AS `year`
FROM hcahps_hospital_2022_staging1
UNION ALL
SELECT *, 2020hcahps_combinedhcahps_combined AS `year`
FROM hcahps_hospital_2021_staging1
);

-- Export tables to .csv
SELECT 
	'facility_id',
    'facility_name',
    'address',
    'city',
    'state',
    'zip_code',
    'county_or_parish',
    'telephone_number',
    'hospital_type',
    'hospital_ownership',
    'emergency_services',
    'birthing_friendly_designation',
    'hospital_overall_rating',
    'hospital_overall_rating_footnote',
    'MORT_group_measure_count',
    'count_of_facility_MORT_measures',
    'count_of_MORT_measures_better',
    'count_of_MORT_measures_no_diff',
    'count_of_MORT_measures_worse',
    'MORT_group_footnote',
    'safety_group_measure_count',
    'count_of_facility_safety_measures',
    'count_of_safety_measures_better',
    'count_of_safety_measures_no_diff',
    'count_of_safety_measures_worse',
    'safety_group_footnote',
    'READM_group_measure_count',
    'count_of_facility_READM_measures',
    'count_of_READM_measures_better',
    'count_of_READM_measures_no_diff',
    'count_of_READM_measures_worse',
    'READM_group_footnote',
    'pt_exp_group_measure_count',
    'count_of_facility_pt_exp_measures',
    'pt_exp_group_footnote',
    'TE_group_measure_count',
    'count_of_facility_TE_measures',
    'TE_group_footnote',
    'region'
UNION ALL
SELECT * FROM hospital_geninfo_staging2
INTO OUTFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/cleaned_data/Hospital_General_Info/hospital_geninfo_clean.csv' 
FIELDS ENCLOSED BY '"' 
TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

-- HCAHPS combined dataset
SELECT
	'facility_id',
    'facility_name',
    'city',
    'state',
    'zip_code',
    'county_or_parish',
    'hcahps_measure_id',
    'hcahps_question',
	'hcahps_answer_description',
    'patient_survey_star_rating',
    'hcahps_answer_percent',
    'hcahps_linear_mean',
    'num_completed_surveys',
    'survey_response_rate_percent',
    'start_date',
    'end_date',
    'year'
UNION ALL
SELECT 
	facility_id,
    facility_name,
    city,
    state,
    zip_code,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    hcahps_answer_percent,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date,
    `year`
FROM hcahps_combined
WHERE hcahps_answer_percent IS NOT NULL
INTO OUTFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/cleaned_data/Hospital_HCAHPS/hcahps_combined_clean.csv' 
FIELDS ENCLOSED BY '"' 
TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

-- HCAHPS patient survey star rating
SELECT
	'facility_id',
    'facility_name',
    'city',
    'state',
    'zip_code',
    'county_or_parish',
    'hcahps_measure_id',
    'hcahps_question',
	'hcahps_answer_description',
    'patient_survey_star_rating',
    'num_completed_surveys',
    'survey_response_rate_percent',
    'start_date',
    'end_date',
    'year'
UNION ALL
SELECT 
	facility_id,
    facility_name,
    city,
    state,
    zip_code,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    patient_survey_star_rating,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date,
    `year`
FROM hcahps_combined
WHERE patient_survey_star_rating IS NOT NULL
INTO OUTFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/cleaned_data/Hospital_HCAHPS/hcahps_combined_ps_clean.csv' 
FIELDS ENCLOSED BY '"' 
TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

-- HCAHPS answer percent
SELECT
	'facility_id',
    'facility_name',
    'city',
    'state',
    'zip_code',
    'county_or_parish',
    'hcahps_measure_id',
    'hcahps_question',
	'hcahps_answer_description',
    'hcahps_answer_percent',
    'num_completed_surveys',
    'survey_response_rate_percent',
    'start_date',
    'end_date',
    'year'
UNION ALL
SELECT 
	facility_id,
    facility_name,
    city,
    state,
    zip_code,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    hcahps_answer_percent,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date,
    `year`
FROM hcahps_combined
WHERE hcahps_answer_percent IS NOT NULL
INTO OUTFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/cleaned_data/Hospital_HCAHPS/hcahps_combined_pa_clean.csv' 
FIELDS ENCLOSED BY '"' 
TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

-- HCAHPS linear mean
SELECT
	'facility_id',
    'facility_name',
    'city',
    'state',
    'zip_code',
    'county_or_parish',
    'hcahps_measure_id',
    'hcahps_question',
	'hcahps_answer_description',
    'hcahps_linear_mean',
    'num_completed_surveys',
    'survey_response_rate_percent',
    'start_date',
    'end_date',
    'year'
UNION ALL
SELECT 
	facility_id,
    facility_name,
    city,
    state,
    zip_code,
    county_or_parish,
    hcahps_measure_id,
    hcahps_question,
    hcahps_answer_description,
    hcahps_linear_mean,
    num_completed_surveys,
    survey_response_rate_percent,
    start_date,
    end_date,
    `year`
FROM hcahps_combined
WHERE hcahps_linear_mean IS NOT NULL
INTO OUTFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/cleaned_data/Hospital_HCAHPS/hcahps_combined_lm_clean.csv' 
FIELDS ENCLOSED BY '"' 
TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';