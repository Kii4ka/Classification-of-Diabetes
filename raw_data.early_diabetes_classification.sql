--raw_data.early_diabetes_classification

SELECT highbp , COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--0 = no high BP 1 = high BP

SELECT highchol, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--0 = no high cholesterol 1 = high cholesterol

SELECT cholcheck, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--0 = no cholesterol check in 5 years 1 = yes cholesterol check in 5 years

SELECT bmi, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;


SELECT smoker, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--Have you smoked at least 100 cigarettes in your entire life? [Note: 5 packs = 100 cigarettes] 0 = no 1 = yes

SELECT stroke, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--(Ever told) you had a stroke. 0 = no 1 = yes
  
SELECT heartdiseaseorattack, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--coronary heart disease (CHD) or myocardial infarction (MI) 0 = no 1 = yes
     
SELECT physactivity, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--physical activity in past 30 days - not including job 0 = no 1 = yes


SELECT fruits, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--Consume Fruit 1 or more times per day 0 = no 1 = yes

SELECT fruits, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--Consume Vegetables 1 or more times per day 0 = no 1 = yes

SELECT hvyalcoholconsump, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--Heavy drinkers (adult men having more than 14 drinks per week and adult women having more than 7 drinks per week) 0 = no


SELECT anyhealthcare, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--Have any kind of health care coverage, including health insurance, prepaid plans such as HMO, etc. 0 = no 1 = yes


SELECT nodocbccost, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;  

--Was there a time in the past 12 months when you needed to see a doctor but could not because of cost? 0 = no 1 = yes

    
SELECT genhlth, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;  

--Would you say that in general your health is: scale 1-5 1 = excellent 2 = very good 3 = good 4 = fair 5 = poor
      
SELECT menthlth, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;  

--Now thinking about your mental health, which includes stress, depression, and problems with emotions, for how


SELECT physhlth, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;    

--Now thinking about your physical health, which includes physical illness and injury, for how many days during the past 30
      
SELECT diffwalk, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;    
       
--Do you have serious difficulty walking or climbing stairs? 0 = no 1 = yes
	   
	   
SELECT sex, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;  

--0 = female 1 = male


SELECT age, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;

--13-level age category (_AGEG5YR see codebook) 1 = 18-24 9 = 60-64 13 = 80 or older

SELECT education, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;    

--Education level (EDUCA see codebook) scale 1-6 1 = Never attended school or only kindergarten 2 = Grades 1 through 8

SELECT income, COUNT(*)
FROM raw_data.early_diabetes_classification
GROUP BY 1
ORDER BY 1;  

--Income scale (INCOME2 see codebook) scale 1-8 1 = less than $10,000 5 = less than $35,000 8 = $75,000 or more
    
       

    
   

  

   
