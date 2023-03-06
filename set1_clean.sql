---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Create schema and set search path
---------------------------------------------------------------------------------------


-- (2) Set search path to clean_data.
SET SEARCH_PATH TO clean_data; 



-- (1) Create table 
CREATE TABLE diabetes_dataset (LIKE raw_data.diabetes_dataset INCLUDING ALL);


INSERT INTO diabetes_dataset
SELECT *
FROM raw_data.diabetes_dataset
ORDER BY 1;


--SELECT * FROM diabetes_dataset;



/* SELECT DISTINCT(pregnancies)
FROM raw_data.diabetes_dataset
ORDER by 1;  */

/* SELECT DISTINCT(glucose)
FROM raw_data.diabetes_dataset
ORDER by 1; 

SELECT glucose, count(glucose)
FROM raw_data.diabetes_dataset
GROUP BY glucose
ORDER by 1;*/

/* SELECT DISTINCT(bloodpressure)
FROM raw_data.diabetes_dataset
ORDER by 1;

SELECT bloodpressure, count(bloodpressure)
FROM raw_data.diabetes_dataset
GROUP BY bloodpressure
ORDER by 1;
*/

/* SELECT DISTINCT(skinthickness)
FROM raw_data.diabetes_dataset
ORDER by 1;  */

/* SELECT DISTINCT(insulin)
FROM raw_data.diabetes_dataset
ORDER by 1;

SELECT insulin, count(insulin)
FROM raw_data.diabetes_dataset
GROUP BY insulin
ORDER by 1; */

/* SELECT DISTINCT(bmi)
FROM raw_data.diabetes_dataset
ORDER by 1; 

SELECT bmi, count(bmi)
FROM raw_data.diabetes_dataset
GROUP BY bmi
ORDER by 1; */

/* SELECT DISTINCT(diabetespedigreefunction)
FROM raw_data.diabetes_dataset
ORDER by 1;  */

/* SELECT DISTINCT(age)
FROM raw_data.diabetes_dataset
ORDER by 1;  */

/* SELECT DISTINCT(outcome)
FROM raw_data.diabetes_dataset
ORDER by 1;  */



