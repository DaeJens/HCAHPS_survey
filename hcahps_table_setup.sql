-- CMS Hospital General Information table setup
CREATE TABLE IF NOT EXISTS cms_hospital.hospital_geninfo
(
    facility_id VARCHAR(15),
    facility_name VARCHAR(100),
    address VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(6),
    county_or_parish VARCHAR(100),
    telephone_number VARCHAR(50),
    hospital_type VARCHAR(100),
    hospital_ownership VARCHAR(150),
    emergency_services VARCHAR(4),
    birthing_friendly_designation BOOL,
    hospital_overall_rating INTEGER,
    hospital_overall_rating_footnote VARCHAR(100),
    MORT_group_measure_count INTEGER,
    count_of_facility_MORT_measures INTEGER,
    count_of_MORT_measures_better INTEGER,
    count_of_MORT_measures_no_diff INTEGER,
    count_of_MORT_measures_worse INTEGER,
    MORT_group_footnote VARCHAR(100),
    safety_group_measure_count INTEGER,
    count_of_facility_safety_measures INTEGER,
    count_of_safety_measures_better INTEGER,
    count_of_safety_measures_no_diff INTEGER,
    count_of_safety_measures_worse INTEGER,
    safety_group_footnote VARCHAR(100),
    READM_group_measure_count INTEGER,
    count_of_facility_READM_measures INTEGER,
    count_of_READM_measures_better INTEGER,
    count_of_READM_measures_no_diff INTEGER,
    count_of_READM_measures_worse INTEGER,
    READM_group_footnote VARCHAR(100),
    pt_exp_group_measure_count INTEGER,
    count_of_facility_pt_exp_measures INTEGER,
    pt_exp_group_footnote VARCHAR(100),
    TE_group_measure_count INTEGER,
    count_of_facility_TE_measures INTEGER,
    TE_group_footnote VARCHAR(100)
);

LOAD DATA INFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/raw_data/Hospital_General_Information.csv'
INTO TABLE hospital_geninfo
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
;

-- HCAHPS 2024 (Hospital) table setup
CREATE TABLE IF NOT EXISTS cms_hospital.hcahps_hospital_2024
(
    facility_id VARCHAR(15),
    facility_name VARCHAR(100),
    address VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    county_or_parish VARCHAR(100),
    telephone_number VARCHAR(50),
    hcahps_measure_id VARCHAR(100),
    hcahps_question VARCHAR(255),
    hcahps_answer_description VARCHAR(255),
    patient_survey_star_rating VARCHAR(50),
    patient_survey_star_footnote VARCHAR(255),
    hcahps_answer_percent VARCHAR(50),
    hcahps_answer_footnote VARCHAR(255),
    hcahps_linear_mean VARCHAR(50),
    num_completed_surveys VARCHAR(50),
    num_completed_surveys_footnote VARCHAR(255),
    survey_response_rate_percent VARCHAR(50),
    survey_response_rate_footnote VARCHAR(255),
    start_date VARCHAR(10),
    end_date VARCHAR(10)
);

LOAD DATA INFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/archived_data/hospitals_10_2024/HCAHPS-Hospital.csv'
INTO TABLE hcahps_hospital_2024
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
;

-- HCAHPS 2023 (Hospital) table setup
CREATE TABLE IF NOT EXISTS cms_hospital.hcahps_hospital_2023
(
    facility_id VARCHAR(15),
    facility_name VARCHAR(100),
    address VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    county_or_parish VARCHAR(100),
    telephone_number VARCHAR(50),
    hcahps_measure_id VARCHAR(100),
    hcahps_question VARCHAR(255),
    hcahps_answer_description VARCHAR(255),
    patient_survey_star_rating VARCHAR(50),
    patient_survey_star_footnote VARCHAR(255),
    hcahps_answer_percent VARCHAR(50),
    hcahps_answer_footnote VARCHAR(255),
    hcahps_linear_mean VARCHAR(50),
    num_completed_surveys VARCHAR(50),
    num_completed_surveys_footnote VARCHAR(255),
    survey_response_rate_percent VARCHAR(50),
    survey_response_rate_footnote VARCHAR(255),
    start_date VARCHAR(10),
    end_date VARCHAR(10)
);

LOAD DATA INFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/archived_data/hospitals_11_2023/hospitals_11_2023/HCAHPS-Hospital.csv'
INTO TABLE hcahps_hospital_2023
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
;

-- HCAHPS 2022 (Hospital) table setup
CREATE TABLE IF NOT EXISTS cms_hospital.hcahps_hospital_2022
(
    facility_id VARCHAR(15),
    facility_name VARCHAR(100),
    address VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    county_or_parish VARCHAR(100),
    telephone_number VARCHAR(50),
    hcahps_measure_id VARCHAR(100),
    hcahps_question VARCHAR(255),
    hcahps_answer_description VARCHAR(255),
    patient_survey_star_rating VARCHAR(50),
    patient_survey_star_footnote VARCHAR(255),
    hcahps_answer_percent VARCHAR(50),
    hcahps_answer_footnote VARCHAR(255),
    hcahps_linear_mean VARCHAR(50),
    num_completed_surveys VARCHAR(50),
    num_completed_surveys_footnote VARCHAR(255),
    survey_response_rate_percent VARCHAR(50),
    survey_response_rate_footnote VARCHAR(255),
    start_date VARCHAR(10),
    end_date VARCHAR(10)
);

LOAD DATA INFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/archived_data/hospitals_10_2022/hospitals_10_2022/HCAHPS-Hospital.csv'
INTO TABLE hcahps_hospital_2022
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
;

-- HCAHPS 2021 (Hospital) table setup
CREATE TABLE IF NOT EXISTS cms_hospital.hcahps_hospital_2021
(
    facility_id VARCHAR(15),
    facility_name VARCHAR(100),
    address VARCHAR(150),
    city VARCHAR(50),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    county_or_parish VARCHAR(100),
    telephone_number VARCHAR(50),
    hcahps_measure_id VARCHAR(100),
    hcahps_question VARCHAR(255),
    hcahps_answer_description VARCHAR(255),
    patient_survey_star_rating VARCHAR(50),
    patient_survey_star_footnote VARCHAR(255),
    hcahps_answer_percent VARCHAR(50),
    hcahps_answer_footnote VARCHAR(255),
    hcahps_linear_mean VARCHAR(50),
    num_completed_surveys VARCHAR(50),
    num_completed_surveys_footnote VARCHAR(255),
    survey_response_rate_percent VARCHAR(50),
    survey_response_rate_footnote VARCHAR(255),
    start_date VARCHAR(10),
    end_date VARCHAR(10)
);

LOAD DATA INFILE 'C:/Users/Daelin/Documents/Workspace/DataAnalyst_PortfolioPrep/Hospital/archived_data/hospitals_10_2021/HCAHPS-Hospital.csv'
INTO TABLE hcahps_hospital_2021
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
;