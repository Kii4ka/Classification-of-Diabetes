SET SEARCH_PATH TO clean_data; 


CREATE TABLE early_classification_of_diabetes (LIKE raw_data.early_classification_of_diabetes INCLUDING ALL);

CREATE TABLE early_diabetes_classification (LIKE raw_data.early_diabetes_classification INCLUDING ALL);


INSERT INTO early_classification_of_diabetes
SELECT *
FROM raw_data.early_classification_of_diabetes
ORDER BY 1;

INSERT INTO early_diabetes_classification
SELECT *
FROM raw_data.early_diabetes_classification
ORDER BY 1;
