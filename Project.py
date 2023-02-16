

# %%
import databaseUtils as db      # database utilities
import ioFunctions as fnc       # I/O functions
import pandas as pd             # pandas
import sys
sys.path.append('../../python_utils/')
import time



# Directories
dataDir = '../../data/'
outputDir = dataDir+'output/'

# Set each variable in the dictionary.
connDetails = {
    'host'      : 'localhost',
    'port'      : 5432,
    'dbname'    : 'cohe_6590_project',
    'user'      : 'cohe_project',
    'password'  : 'cohe_project_password'
}

schema = 'raw_data'


#############################################################################


# %%


table = 'diabetes_dataset'

db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (     
    pregnancies                  INTEGER,
    glucose                      INTEGER,
    bloodpressure                INTEGER,
    skinthickness                INTEGER,
    insulin                      INTEGER,
    bmi                          NUMERIC,
    diabetespedigreefunction     NUMERIC,
    age                          INTEGER ,
    outcome                      TEXT

);
""".format(schema, table), connDetails) 
#  Download the data into the table.

url = 'https://storage.googleapis.com/kagglesdsdata/datasets/2267722/3804604/diabetes.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230215%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230215T174637Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=6fbd229ae18fd3626e55c55aa9ac0558eb2b55620c2864c7600ea4e412a60fcce79fd75d299e18862746f4e778a8bcf615e92dca64489b41ad10872b22ef83a4d3597336c7a609c32bf38a3b67217532bec320c32beb8c44afcb6efdfc9bfbace91457eddfd3e77c571cb657eb8518dff1b051192737d4c6eae0fcb7839676989d1562189340b769576db27d4ebd071314112543c11cf627b366b827048d07afcad7f2f5ede874a38b53143a3d40c4ed106186c70e53bb6c4e159a5265ee95b56dcec9a7189840c1a0fa9aa1586ebf2bc44724bc174ffe0d71fa4ad7493fd73b953f6994459f2435c00a18ae71e513b51441de37dfc4394edd9443a2dc8c1a23'
fnc.downloadAndCopyFromCSV(schema, table, url, connDetails)
 

print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))


# %%


table = 'diabetes_dataset_2'

db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (     

    Pregnancies                  INTEGER,
    Gender                       TEXT,
    Glucose                      INTEGER,
    BloodPressure                INTEGER,
    SkinThickness                INTEGER,
    Insulin                      INTEGER,
    BMI                          NUMERIC,
    DiabetesPedigreeFunction     NUMERIC,
    Age                          INTEGER,
    Outcome                      INTEGER,
    CalorieIntake                NUMERIC,
    Exercise                     TEXT,
    SleepDuration                INTEGER

);
""".format(schema, table), connDetails) 
#  Download the data into the table.

url = 'https://storage.googleapis.com/kagglesdsdata/datasets/1288395/2147104/diabetes.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230215%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230215T181157Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=6998623dbd9c62a9ee18d450a1f724f937dfb50bc219bf40c078e568b59881fa392c1dee7f9dc9a905e9ad150caac9ef8eec267e65b876ac2840b6dd20481906a37d8929e2fe498c6e89cc5fa05ae087d2d4c82b9c478e50baff424d7410b26480313a93fd632888cf19bc8eb3bab3a4dd88e100a2ae63e7dbbf69928a9b90ff8bd48affd5297da6bd0f4c6052262f53274889a5ea1e3e685dd6d639eab3e1814708c2eeba03d8a0c5d91e529779f794f6d7e3f66fd331960e584641ab615fdf03f0c9528c21f39d6e2a3837616197561d31d4e03cee1bca017f49c637db41515ccbabb9dd26d71735bde9fdccb53d05e499c26e862b10c86e8acc0f6766ba4e'
fnc.downloadAndCopyFromCSV(schema, table, url, connDetails)
 

print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))

#############################################################################




# %%


table = 'early_diabetes_classification'


db.execute("""
DROP TABLE IF EXISTS {0}.{1};

""".format(schema, table), connDetails)

url = 'https://storage.googleapis.com/kagglesdsdata/datasets/1772261/2892974/diabetes_data.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230215%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230215T182357Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=23cc903d104d82d4dd944afad9fa638fe70ad91f9debfc44096cd983332fe6e562a5c59d84a6bf238968bc47c64b1ee3eb0ffabc833fd1a1ea8d3a2cfd6b9214b9258df797ef7f2bb5ec0362f94ae56ac6ed3e596e068f8a8e500dd968e4ed3039028ed8a9a1b88a2bb9736820cd81cea894878ed1ae02a3e47d0dac38e972e49304b3145e5b0323c7a2793e7ff46b5cbf99711e4b31b84baac364423c7caaad6c1f161504e8de2bf46fa442d20c41c02397b794fe418dbe97cb329b70a7b9212bcfccf07224ae1baf35b0b14e1fc8e7bcf265c8ce592270c6905e55a9bfc95142e415e97b161ac259113a56a95a921a88c5ec4aa754c6c130f2ad74a23a1c53'

delim = pd.read_csv(url, delimiter=';')

fnc.copyFromDF(schema, table, delim, connDetails)

print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))


#############################################################################




# %%



#############################################################################
table = 'Early_Classification_of_Diabetes'

import requests, zipfile, io
r = requests.get('https://storage.googleapis.com/kaggle-data-sets/841033/1445892/compressed/diabetic_data.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230215%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230215T191037Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=853da6d82a763c1898f9c7cb4f1ebfa57d673072a1e3ef63ef42925dafaf89c2937f5445e20a432f45a2b0c303ab5dbc2dbc8bf57bdcc78a9e27cc43efd66b79f59be4f2b9adcf44abe340079722e597286357e4af2bf995df527b86f385f1c61c219de9ef2dbed5db13954e7d68fd423c38a6a7858b4965a7142fdfbc8240a77753dca258ad8b7cffc1c7a6b052192e090a14feca0f6e19124f93f0499e3c4c1202f003c7344e26b25a9e1a84071da8280c07823f4a13536346e7750baf58f1944cdb9c3cb573c10e435559095a4e0808a1b85a3698c8f8fffda4999957892ee3880a1cbd46a175e88361b42bd77e9206c17dbb75573533a40983d59aa93aa0')
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("/Users/Shared/cohe_6590_exercises/Project")


db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (
    encounter_id                   INTEGER,
    patient_nbr                    INTEGER,
    race                           TEXT,
    gender                         TEXT,
    age                            TEXT,
    weight                         TEXT,
    admission_type_id              INTEGER,
    discharge_disposition_id       INTEGER,
    admission_source_id            INTEGER,
    time_in_hospital               INTEGER,
    payer_code                     TEXT,
    medical_specialty              TEXT,
    num_lab_procedures             INTEGER,
    num_procedures                 INTEGER,
    num_medications                INTEGER,
    number_outpatient              INTEGER,
    number_emergency               INTEGER,
    number_inpatient               INTEGER,
    diag_1                         TEXT,
    diag_2                         TEXT,
    diag_3                         TEXT,
    number_diagnoses               INTEGER,
    max_glu_serum                  TEXT,
    A1Cresult                      TEXT,
    metformin                      TEXT,
    repaglinide                    TEXT,
    nateglinide                    TEXT,
    chlorpropamide                 TEXT,
    glimepiride                    TEXT,
    acetohexamide                  TEXT,
    glipizide                      TEXT,
    glyburide                      TEXT,
    tolbutamide                    TEXT,
    pioglitazone                   TEXT,
    rosiglitazone                  TEXT,
    acarbose                       TEXT,
    miglitol                       TEXT,
    troglitazone                   TEXT,
    tolazamide                     TEXT,
    examide                        TEXT,
    citoglipton                    TEXT,
    insulin                        TEXT,
    glyburide_metformin            TEXT,
    glipizide_metformin            TEXT,
    glimepiride_pioglitazone       TEXT,
    metformin_rosiglitazone        TEXT,
    metformin_pioglitazone         TEXT,
    change                         TEXT, 
    diabetesMed                    TEXT,
    readmitted                     TEXT
);
""".format(schema, table), connDetails)

fnc.copyFromCSV(schema, table,'/Users/Shared/cohe_6590_exercises/Project/diabetic_data.csv', connDetails)


print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))


# %%



#############################################################################


import requests, zipfile, io
r = requests.get('https://storage.googleapis.com/kaggle-data-sets/1703281/2789260/compressed/diabetes_binary_health_indicators_BRFSS2015.csv.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20230215%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20230215T194526Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=b961e3d6f7f0bf7fe63c9a2c86e900d8e1e326e1340007b52c864d536b62a1fd8dc6c0361ba960525500753dae793a07398a6ceb6c10bcb477ae131c92ba5c37e1ef127b01ce72ec4f10dc51c1316d9dd6de4a6011157fd6b4837dafde049d8b9d964524b0fa60421d4765f0a395c93aa811ea082012c65f9f634a77a87b847d95e20c18e7b93bb41fbe0d727cb498012d9e31590abdab2c659c210bae5c72d3bad31ba97271cb16402b9338f034227d30c0b8b482060a7211f2bac8b112f898bdf1970e0770885ba6cc966438f5d1b96c5540e8af99390cacbd24a55b5ac1be135510d39564b9425b52d5a61fcaf438ddda9262461a90d6569e494b55d3277d')
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall("/Users/Shared/cohe_6590_exercises/Project")


table = 'Diabetic_Patients_Re_admission_Prediction'

db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (
       Diabetes_binary             NUMERIC,
       HighBP                      NUMERIC,
       HighChol                    NUMERIC,
       CholCheck                   NUMERIC,
       BMI                         NUMERIC,
       Smoker                      NUMERIC,
       Stroke                      NUMERIC,
       HeartDiseaseorAttack        NUMERIC,
       PhysActivity                NUMERIC,
       Fruits                      NUMERIC,
       Veggies                     NUMERIC,
       HvyAlcoholConsump           NUMERIC,
       AnyHealthcare               NUMERIC,
       NoDocbcCost                 NUMERIC,
       GenHlth                     NUMERIC,
       MentHlth                    NUMERIC,
       PhysHlth                    NUMERIC,
       DiffWalk                    NUMERIC,
       Sex                         NUMERIC,
       Age                         NUMERIC,
       Education                   NUMERIC,
       Income                      NUMERIC
);
""".format(schema, table), connDetails)
fnc.copyFromCSV(schema, table,'/Users/Shared/cohe_6590_exercises/Project/diabetes_binary_health_indicators_BRFSS2015.csv', connDetails)


print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))


# %%



#############################################################################


