"""
Postgres connection for the pipeline's own business database (separate from
Airflow's internal metadata database). Fails loudly and clearly if the
database is unreachable — callers must not swallow this into a fake success.
"""
import os
import psycopg2
import psycopg2.extras


def get_connection():
    host = os.environ.get("PIPELINE_DB_HOST", "localhost")
    port = os.environ.get("PIPELINE_DB_PORT", "5432")
    dbname = os.environ.get("PIPELINE_DB_NAME", "pipeline")
    user = os.environ.get("PIPELINE_DB_USER", "airflow")
    password = os.environ.get("PIPELINE_DB_PASSWORD", "airflow")

    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password, connect_timeout=5
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        raise ConnectionError(
            f"Could not connect to pipeline Postgres database at {host}:{port}/{dbname}: {e}"
        ) from e


def dict_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
