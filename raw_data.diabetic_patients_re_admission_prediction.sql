--raw_data.diabetic_patients_re_admission_prediction--
--suggest to clean race, weight, max_glu_serum, a1cresult and exclude all other attributes in set 



SELECT race, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--? in race 

SELECT gender, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--catergory unknown has a count of 3

SELECT age, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--age is grouped by sets of 10


SELECT weight, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--weight is grouped by 25 pounds by a large number of unknowns 


SELECT discharge_disposition_id, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--added this for possible reimbursement concerns 


--discharge_disposition_id	description
--1	Discharged to home
--2	Discharged/transferred to another short term hospital
--3	Discharged/transferred to SNF
--4	Discharged/transferred to ICF
--5	Discharged/transferred to another type of inpatient care institution
--6	Discharged/transferred to home with home health service
--7	Left AMA
--8	Discharged/transferred to home under care of Home IV provider
--9	Admitted as an inpatient to this hospital
--10	Neonate discharged to another hospital for neonatal aftercare
--11	Expired
--12	Still patient or expected to return for outpatient services
--13	Hospice / home
--14	Hospice / medical facility
--15	Discharged/transferred within this institution to Medicare approved swing bed
--16	Discharged/transferred/referred another institution for outpatient services
--17	Discharged/transferred/referred to this institution for outpatient services
--18	NULL
--19	Expired at home. Medicaid only, hospice.
--20	Expired in a medical facility. Medicaid only, hospice.
--21	Expired, place unknown. Medicaid only, hospice.
--22	Discharged/transferred to another rehab fac including rehab units of a hospital .
--23	Discharged/transferred to a long term care hospital.
--24	Discharged/transferred to a nursing facility certified under Medicaid but not certified under Medicare.
--25	Not Mapped
--26	Unknown/Invalid
--30	Discharged/transferred to another Type of Health Care Institution not Defined Elsewhere
--27	Discharged/transferred to a federal health care facility.
--28	Discharged/transferred/referred to a psychiatric hospital of psychiatric distinct part unit of a hospital
--29	Discharged/transferred to a Critical Access Hospital (CAH).




SELECT time_in_hospital, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--


SELECT max_glu_serum, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;


--lots of none values 

SELECT a1cresult, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;

--lots of none values 


SELECT readmitted, COUNT(*)
FROM raw_data.diabetic_patients_re_admission_prediction
GROUP BY 1
ORDER BY 1;








