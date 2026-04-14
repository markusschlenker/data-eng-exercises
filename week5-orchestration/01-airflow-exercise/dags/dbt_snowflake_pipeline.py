"""
Week 5, Weekly Project: dbt pipeline orchestration with Airflow

Train usage of BashOperator

Based on instructions in 
   https://github.com/markusschlenker/nf-de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/
   05_weekly_project/instructions.md
"""
import os
import shutil
import logging
import typing
import pendulum
import pandas as pd
from pathlib import Path
import kagglehub
from contextlib import contextmanager
from tqdm import tqdm
from sqlalchemy import create_engine, text
from dotenv import dotenv_values
from urllib.parse import quote_plus

from airflow.sdk import dag, task, setup, teardown
from airflow.providers.standard.operators.bash import BashOperator


logger = logging.getLogger("airflow.task")

########################################################################################################################
# utility
#
SNOWFLAKE_DATABASE = "SNOWFLAKE_LEARNING_DB"
SNOWFLAKE_SCHEMA = "RAW_OLIST"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

def _snowflake_engine(schema=SNOWFLAKE_SCHEMA):
    """
    Build a SQLAlchemy engine for Snowflake.

    Get credentials from Snowflake "Account Details" section
    """
    base_dir = Path(__file__).resolve().parent  # dags folder
    credentials = dotenv_values(base_dir / ".env.snowflake.credentials", verbose=True)

    account = credentials["ACCOUNT"]                # "ABCDEF-GH12345"
    user = credentials["USERNAME"]                  # "USERNAME"
    password = quote_plus(credentials["PASSWORD"])  # quote_plus("the-secret-password") - HTML quoted

    database = SNOWFLAKE_DATABASE
    # schema = SNOWFLAKE_SCHEMA
    warehouse = SNOWFLAKE_WAREHOUSE
    role = "ACCOUNTADMIN"

    url = (
        f"snowflake://{user}:{password}@{account}"
        f"?warehouse={warehouse}&role={role}"
    )
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"USE WAREHOUSE {warehouse}")
        conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        conn.exec_driver_sql(f"USE DATABASE {database}")
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS  {schema}")

    return engine

########################################################################################################################
# business logic
#
DAGS_DIR = Path(__file__).resolve().parent  # dags dir  
SF_DBT_PROJECT_DIR = "/opt/airflow/sf_dbt_weekly_proj"

########################################################################################################################

@dag(
    dag_id="dbt_snowflake_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "snowflake", "etl", "olist", "week5"],
)
def dbt_snowflake_pipeline():

    dummy = BashOperator(
        task_id="dummy",
        bash_command="echo 'hello' && "
            " pwd && echo 'break' &&"
            " ls /opt/airflow/sf_dbt_weekly_proj && echo 'break'"
        ,
        do_xcom_push=False,
    )

    dbt_init = BashOperator(
        task_id="dbt_init",
        bash_command="echo 'doing inits' && "
            f"cd {SF_DBT_PROJECT_DIR} &&"
            "pwd && ls && "
            #" rm -rf dbt_packages"
            "type dbt && "
            f"dbt deps --profiles-dir {SF_DBT_PROJECT_DIR} > dbt_deps.log 2>&1 && "
            "cat dbt_deps.log"
        ,
        do_xcom_push=False,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="echo 'doing run' && "
            f"cd {SF_DBT_PROJECT_DIR} &&"
            "pwd && "
            f"dbt run --profiles-dir {SF_DBT_PROJECT_DIR} > dbt_run.log 2>&1 && "
            "cat dbt_run.log"
        ,
        do_xcom_push=False,
    )

    dummy >> dbt_init >> dbt_run


dbt_snowflake_pipeline_dag = dbt_snowflake_pipeline()

if __name__ == "__main__":
    import time
    print("run business logic locally")
    time.sleep(0.5)
    print("done")
