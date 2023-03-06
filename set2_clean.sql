---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Create schema and set search path
---------------------------------------------------------------------------------------
-- (1) Create a new schema named clean_data.
--DROP SCHEMA IF EXISTS clean_data CASCADE;
---CREATE SCHEMA clean_data;

-- (2) Set search path to clean_data.
SET SEARCH_PATH TO clean_data; 
--SET SEARCH_PATH TO raw_data; 



-- (1) Create table diabetic_patients_re_admission_prediction like raw_data.diabetic_patients_re_admission_prediction.
CREATE TABLE diabetes_dataset_2 (LIKE raw_data.diabetes_dataset_2 INCLUDING ALL);


--  Insert all data from raw_data.diabetic_patients_re_admission_prediction into diabetic_patients_re_admission_prediction.
--DELETE FROM diabetic_patients_re_admission_prediction;
INSERT INTO diabetes_dataset_2
SELECT *
FROM raw_data.diabetes_dataset_2
ORDER BY 1;




/* SELECT DISTINCT(pregnancies)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(gender)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(glucose)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(bloodpressure)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(skinthickness)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(insulin)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(bmi)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(diabetespedigreefunction)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(age)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(outcome)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(calorieintake)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(exercise)
FROM diabetes_dataset_2
ORDER by 1;  */

/* SELECT DISTINCT(sleepduration)
FROM diabetes_dataset_2
ORDER by 1;  */


