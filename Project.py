

# %%
# Install Kaggle before using this script
# !pip install kaggle

import databaseUtils as db      # database utilities
import ioFunctions as fnc       # I/O functions
import pandas as pd             # pandas
import sys
sys.path.append('../../python_utils/')
import time

# You need to use your Kaggle key.
# To get it, go to your account on Kaggle. open Account tab.
# Under API section, you will see Create New API Token.
# Download the kaggle.json file to .kaggle folder in your user's home directory
# Go to your user home directory:
# Mac: Open Terminal and use the Go dropdown menu, Home will be shown there
# Windows: C:\Users\<Your user name>
# Create .kaggle folder in your home
# Download the kaggle.json file to .kaggle folder in your user's home directory



from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

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

url = 'https://raw.githubusercontent.com/plotly/datasets/master/diabetes.csv'
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
api.dataset_download_files('dslearner0406/diabetes-dataset', path=".", unzip=True)

url = 'diabetes.csv'

fnc.copyFromCSV(schema, table, url, connDetails)

print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))

#############################################################################




# %%


table = 'early_diabetes_classification'
table2 = 'early_diabetes_classification_2'
table3 = 'early_diabetes_classification_3'


db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (     

    Diabetes_012            NUMERIC,
    HighBP                  NUMERIC,
    HighChol               NUMERIC,
    CholCheck              NUMERIC,
    BMI                    NUMERIC,
    Smoker                 NUMERIC,
    Stroke                 NUMERIC,                  
    HeartDiseaseorAttack  NUMERIC,
    PhysActivity          NUMERIC,
    Fruits                NUMERIC,
    Veggies              NUMERIC,
    HvyAlcoholConsump    NUMERIC,
    AnyHealthcare        NUMERIC,
    NoDocbcCost          NUMERIC,
    GenHlth              NUMERIC,
    MentHlth             NUMERIC,
    PhysHlth             NUMERIC,
    DiffWalk             NUMERIC,
    Sex                  NUMERIC,
    Age                  NUMERIC,
    Education            NUMERIC,
    Income               NUMERIC
);

""".format(schema, table), connDetails)




db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (     

    Diabetes_012            NUMERIC,
    HighBP                  NUMERIC,
    HighChol               NUMERIC,
    CholCheck              NUMERIC,
    BMI                    NUMERIC,
    Smoker                 NUMERIC,
    Stroke                 NUMERIC,                  
    HeartDiseaseorAttack  NUMERIC,
    PhysActivity          NUMERIC,
    Fruits                NUMERIC,
    Veggies              NUMERIC,
    HvyAlcoholConsump    NUMERIC,
    AnyHealthcare        NUMERIC,
    NoDocbcCost          NUMERIC,
    GenHlth              NUMERIC,
    MentHlth             NUMERIC,
    PhysHlth             NUMERIC,
    DiffWalk             NUMERIC,
    Sex                  NUMERIC,
    Age                  NUMERIC,
    Education            NUMERIC,
    Income               NUMERIC
);

""".format(schema, table2), connDetails)




db.execute("""
DROP TABLE IF EXISTS {0}.{1};
CREATE TABLE {0}.{1} (     

    Diabetes_012            NUMERIC,
    HighBP                  NUMERIC,
    HighChol               NUMERIC,
    CholCheck              NUMERIC,
    BMI                    NUMERIC,
    Smoker                 NUMERIC,
    Stroke                 NUMERIC,                  
    HeartDiseaseorAttack  NUMERIC,
    PhysActivity          NUMERIC,
    Fruits                NUMERIC,
    Veggies              NUMERIC,
    HvyAlcoholConsump    NUMERIC,
    AnyHealthcare        NUMERIC,
    NoDocbcCost          NUMERIC,
    GenHlth              NUMERIC,
    MentHlth             NUMERIC,
    PhysHlth             NUMERIC,
    DiffWalk             NUMERIC,
    Sex                  NUMERIC,
    Age                  NUMERIC,
    Education            NUMERIC,
    Income               NUMERIC
);

""".format(schema, table3), connDetails)


api.dataset_download_files('alexteboul/diabetes-health-indicators-dataset', path=".", unzip=True)

url = 'diabetes_012_health_indicators_BRFSS2015.csv'
url2 = 'diabetes_binary_5050split_health_indicators_BRFSS2015.csv'
url3 = 'diabetes_binary_health_indicators_BRFSS2015.csv'


fnc.copyFromCSV(schema, table, url, connDetails)
fnc.copyFromCSV(schema, table2, url2, connDetails)
fnc.copyFromCSV(schema, table3, url3, connDetails)


print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))
print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table2),connDetails))
print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table3),connDetails))



#############################################################################




# %%



#############################################################################
table = 'Early_Classification_of_Diabetes'

db.execute("""
DROP TABLE IF EXISTS {0}.{1};


CREATE TABLE {0}.{1} (     

    age                  INTEGER,
    gender               TEXT,
    polyuria             INTEGER,
    polydipsia           INTEGER,
    sudden_weight_loss   INTEGER,
    weakness             INTEGER,
    polyphagia           INTEGER,
    genital_thrush       INTEGER,
    visual_blurring      INTEGER,
    itching              INTEGER,
    irritability         INTEGER,
    delayed_healing      INTEGER,
    partial_paresis      INTEGER,
    muscle_stiffness     INTEGER,
    alopecia             INTEGER,
    obesity              INTEGER,
    class                INTEGER
    
    );


""".format(schema, table), connDetails)


api.dataset_download_files('andrewmvd/early-diabetes-classification', path=".", unzip=True)

url = 'diabetes_data.csv'

df = pd.read_csv(url, sep = ';')


df.to_csv('diabetes_data.csv', index = False)

fnc.copyFromCSV(schema, table, url, connDetails)


print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))


# %%



#############################################################################


table = 'Diabetic_Patients_Re_admission_Prediction'

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


api.dataset_download_files('saurabhtayal/diabetic-patients-readmission-prediction', path=".", unzip=True)

url = 'diabetic_data.csv'


fnc.copyFromCSV(schema, table, url, connDetails)


print(db.executeQuery('SELECT * FROM {0}.{1}'.format(schema, table),connDetails))


# %%



#############################################################################

