---------------------------------------------------------------------------------------
-- Execute on cohe_6590
---------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------
-- Create schema and set search path
---------------------------------------------------------------------------------------
-- (1) Create a new schema named clean_data.
--DROP SCHEMA IF EXISTS clean_data CASCADE;
--CREATE SCHEMA clean_data;

-- (2) Set search path to clean_data.
SET SEARCH_PATH TO clean_data; 


---------------------------------------------------------------------------------------
-- Profile and prepare transformation - raw_data.diabetic_patients_re_admission_prediction.patient_nbr
---------------------------------------------------------------------------------------
--  Create a deidentified map (patient_nbr_map: idd, id) - do NOT populate. Set id as UNIQUE.
--DROP TABLE IF EXISTS patient_nbr_map; 
CREATE TABLE patient_nbr_map (
	idd SERIAL,
	id  INTEGER UNIQUE
);


---------------------------------------------------------------------------------------
-- Profile and prepare transformation - raw_data.diabetic_patients_re_admission_prediction.gender
---------------------------------------------------------------------------------------


--  functions to clean gender. It takes as a parameter the uncleanded
--     gender value and returns the cleaned. Also, call UPPER(gender) once.
CREATE OR REPLACE FUNCTION clean_gender (gender TEXT) RETURNS TEXT AS $$
DECLARE
	up TEXT := UPPER(gender);
	ret TEXT;
BEGIN
	ret := 
		CASE 
			WHEN up = 'F' OR up = 'FEMALE' THEN 'F'
			WHEN up = 'M' OR up = 'MALE'   THEN 'M'
			ELSE                                'U'
		END;
	RETURN  ret;
END
$$ LANGUAGE plpgsql;



---------------------------------------------------------------------------------------
-- Profile and prepare transformation - raw_data.diabetic_patients_re_admission_prediction.race
---------------------------------------------------------------------------------------
--  functions to clean race.
--     (a) Uppercase
--     (b) Combine WHITE and CAUCASIAN -> CAUCASIAN
--     (c) Combine  'AFRICAN AMERICAN' and 'BLACK' -> 'AFRICAN AMERICAN/BLACK'
CREATE OR REPLACE FUNCTION clean_race (race TEXT) RETURNS TEXT AS $$
DECLARE
	up TEXT := UPPER(race);
	ret TEXT;
BEGIN
	ret := 
		CASE 
			WHEN up = 'WHITE' THEN 'CAUCASIAN'
			WHEN up = 'AFRICAN AMERICAN' OR up = 'BLACK' THEN 'AFRICAN AMERICAN/BLACK'
			WHEN up = '?' THEN 'UNKNOWN'
			ELSE up
		END;
	RETURN  ret;
END
$$ LANGUAGE plpgsql;


-- clean brackets function
-- returns null if ?
CREATE OR REPLACE FUNCTION clean_brackets(value TEXT) RETURNS TEXT AS $$
DECLARE
	vc text;
BEGIN
	IF value = '?' THEN 
		RETURN null;
	ELSIF value = '>200' THEN 
		RETURN '200 +';
	END IF;
	vc := regexp_replace(regexp_replace(value, E'\\[', ''), E'\\)', '');
	RETURN split_part(vc, '-', 1) || '-' || (split_part(vc, '-', 2)::int - 1)::text;
END; $$ 
LANGUAGE plpgsql;
---------------------------

--function to replace ? to null

CREATE OR REPLACE FUNCTION clean_questions(value TEXT) RETURNS TEXT AS $$

BEGIN
	IF value = '?' THEN 
		RETURN null;
	
	END IF;
	
	RETURN value;
END; $$ 
LANGUAGE plpgsql;



---------------------------------------------------------------------------------------
-- Transform - raw_data.diabetic_patients_re_admission_prediction
---------------------------------------------------------------------------------------
-- (1) Create table diabetic_patients_re_admission_prediction like raw_data.diabetic_patients_re_admission_prediction.
CREATE TABLE diabetic_patients_re_admission_prediction (LIKE raw_data.diabetic_patients_re_admission_prediction INCLUDING ALL);


CREATE OR REPLACE FUNCTION clean_diabetic_patients_re_admission_prediction() RETURNS TRIGGER AS $$
BEGIN
	-- deidentify primary key
	INSERT INTO clean_data.patient_nbr_map (id) VALUES (NEW.patient_nbr)
		ON CONFLICT DO NOTHING RETURNING idd INTO NEW.patient_nbr;
	
	-- clean gender
	SELECT clean_data.clean_gender(NEW.gender) INTO NEW.gender;
	
	-- clean age
	SELECT clean_brackets(NEW.age)    INTO NEW.age;
	
	-- clean weight
	SELECT clean_brackets(NEW.weight) INTO NEW.weight;

	-- clean race
	SELECT clean_data.clean_race(NEW.race) INTO NEW.race;
	
	-- clean "?"
	SELECT clean_data.clean_questions(NEW.payer_code) INTO NEW.payer_code;
	SELECT clean_data.clean_questions(NEW.medical_specialty) INTO NEW.medical_specialty;
	SELECT clean_data.clean_questions(NEW.diag_1) INTO NEW.diag_1;
	SELECT clean_data.clean_questions(NEW.diag_2) INTO NEW.diag_2;
	SELECT clean_data.clean_questions(NEW.diag_3) INTO NEW.diag_3;

	-- return updated tuple for insertion
	RETURN NEW;

END;
$$ LANGUAGE plpgsql;

-- Write a BEFORE INSERT trigger (clean_diabetic_patients_re_admission_prediction_trigger) for clean_data.diabetic_patients_re_admission_prediction 
--     (include schema) that, for each row, exectutes clean_diabetic_patients_re_admission_prediction().
--DROP TRIGGER IF EXISTS clean_diabetic_patients_re_admission_prediction_trigger ON diabetic_patients_re_admission_prediction;
CREATE TRIGGER clean_diabetic_patients_re_admission_prediction_trigger 
	BEFORE INSERT
	ON clean_data.diabetic_patients_re_admission_prediction
	FOR EACH ROW
	EXECUTE PROCEDURE clean_diabetic_patients_re_admission_prediction();

--  Insert all data from raw_data.diabetic_patients_re_admission_prediction into diabetic_patients_re_admission_prediction.
--DELETE FROM diabetic_patients_re_admission_prediction;
INSERT INTO diabetic_patients_re_admission_prediction
SELECT *
FROM raw_data.diabetic_patients_re_admission_prediction
ORDER BY 1;


DROP TABLE patient_nbr_map;

-- Quick comparison (d = dirty, c = cleaned, m = map)
SELECT d.patient_nbr, c.patient_nbr, d.gender, c.gender, d.race, c.race, d.age, c.age, d.weight, c.weight, d.payer_code, c.payer_code,
 d.medical_specialty, c. medical_specialty, d.diag_1, c.diag_1, d.diag_2, c.diag_2, d.diag_3, c.diag_3
FROM raw_data.diabetic_patients_re_admission_prediction d 
	JOIN patient_nbr_map m ON d.patient_nbr = m.id
	JOIN diabetic_patients_re_admission_prediction c ON m.idd = c.patient_nbr
LIMIT 10000;





/*
SELECT DISTINCT(weight)
FROM raw_data.diabetic_patients_re_admission_prediction
ORDER by 1;
*/

/*SELECT *
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(race)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(gender)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(age)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(weight)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(admission_type_id)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(admission_source_id)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;*/

/*SELECT DISTINCT(payer_code)        
FROM diabetic_patients_re_admission_prediction
ORDER by 1;

SELECT DISTINCT(medical_specialty)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;*/

/* SELECT DISTINCT(num_lab_procedures)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(num_lab_procedures)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(num_lab_procedures)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(num_procedures)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(num_medications)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(number_outpatient)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(number_emergency)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(number_inpatient)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/*SELECT DISTINCT(diag_1)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  

SELECT DISTINCT(diag_2)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  

SELECT DISTINCT(diag_3)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;*/  

/* SELECT DISTINCT(number_diagnoses)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(max_glu_serum)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(a1cresult)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(metformin)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(repaglinide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(nateglinide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(chlorpropamide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(glimepiride)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(acetohexamide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(glipizide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(glyburide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(tolbutamide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(pioglitazone)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(rosiglitazone)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(acarbose)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(miglitol)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(troglitazone)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(tolazamide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(examide)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(citoglipton)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(insulin)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(glyburide_metformin)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(glipizide_metformin)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(glimepiride_pioglitazone)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(metformin_rosiglitazone)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(metformin_pioglitazone)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(change)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(diabetesmed)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */

/* SELECT DISTINCT(readmitted)
FROM diabetic_patients_re_admission_prediction
ORDER by 1;  */


