

-- (2) Set search path to clean_data.
SET SEARCH_PATH TO clean_data; 
--SET SEARCH_PATH TO raw_data; 



-- (1) Create table diabetic_patients_re_admission_prediction like raw_data.diabetic_patients_re_admission_prediction.
CREATE TABLE diabetes_dataset_1and2 (LIKE raw_data.diabetes_dataset INCLUDING ALL);


--  Insert all data from raw_data.diabetic_patients_re_admission_prediction into diabetic_patients_re_admission_prediction.
--DELETE FROM diabetic_patients_re_admission_prediction;
INSERT INTO diabetes_dataset_1and2
SELECT *
FROM raw_data.diabetes_dataset
ORDER BY 1;



INSERT INTO diabetes_dataset_1and2
SELECT pregnancies,
    glucose,
    bloodpressure,
    skinthickness,
    insulin,
    bmi,
    diabetespedigreefunction,
    age,
    outcome 
FROM raw_data.diabetes_dataset_2
ORDER BY 1;


SELECT * FROM diabetes_dataset_1and2;









