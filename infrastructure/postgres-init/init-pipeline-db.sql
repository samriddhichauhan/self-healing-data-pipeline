-- Runs once, automatically, on first Postgres volume init (docker-entrypoint-initdb.d).
-- Creates the pipeline's own business database, separate from Airflow's
-- internal metadata database (POSTGRES_DB=airflow), inside the same
-- lightweight single-container Postgres instance.
CREATE DATABASE pipeline OWNER airflow;
