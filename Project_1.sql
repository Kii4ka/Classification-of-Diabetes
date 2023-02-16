--  Create the database 
CREATE DATABASE cohe_6590_project;


---------------------------------------------------------------------------------------
-- Please,  close the file and open it again in cohe_6590_project database.
---------------------------------------------------------------------------------------

--  Create schema raw_data and set search path
--DROP SCHEMA IF EXISTS raw_data CASCADE;
CREATE SCHEMA IF NOT EXISTS raw_data;
SET SEARCH_PATH TO raw_data;


--  Create the user cohe_project and grant privileges
CREATE ROLE cohe_project WITH LOGIN PASSWORD 'cohe_project_password';
GRANT CREATE ON DATABASE cohe_6590_project TO cohe_project;
GRANT ALL PRIVILEGES ON SCHEMA raw_data TO cohe_project;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO cohe_project;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw_data TO cohe_project;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL PRIVILEGES ON TABLES TO cohe_project;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL PRIVILEGES ON SEQUENCES TO cohe_project;
GRANT pg_read_server_files TO cohe_project;

-- Go to python file.



