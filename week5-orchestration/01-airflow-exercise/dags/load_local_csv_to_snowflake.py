import os
import pendulum
import pandas as pd
from pathlib import Path
from airflow.decorators import dag, task
from sqlalchemy import create_engine

from urllib.parse import quote_plus
def _snowflake_engine():
    """
    Build a SQLAlchemy engine for Snowflake.
    """
    account = "ABCDEF-GH12345"
    user = "USERNAME"
    password = quote_plus("the-secret-password")
    database = "SNOWFLAKE_LEARNING_DB"
    schema = "PUBLIC"
    warehouse = "COMPUTE_WH"
    role = "ACCOUNTADMIN"

    url = (
        f"snowflake://{user}:{password}@{account}/{database}/{schema}"
        f"?warehouse={warehouse}&role={role}"
    )
    return create_engine(url)


@dag(
    dag_id="load_local_csv_to_snowflake",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "etl", "local-file"],
)
def load_local_csv_to_snowflake():

    @task
    def load():
        # CSV path (same folder as DAG)
        base_dir = Path(__file__).resolve().parent
        csv_path = base_dir / "daily_sales_report.csv"

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path}")

        df = pd.read_csv(csv_path)
        if df.empty:
            raise ValueError(f"CSV at {csv_path} is empty.")

        # Standardize column names for Snowflake
        df.columns = [c.upper() for c in df.columns]

        table_name = "DAILY_SALES"
        engine = _snowflake_engine()

        with engine.begin() as conn:
            conn.exec_driver_sql("USE WAREHOUSE COMPUTE_WH")
            conn.exec_driver_sql("USE DATABASE SNOWFLAKE_LEARNING_DB")
            conn.exec_driver_sql("USE SCHEMA PUBLIC")
            conn.exec_driver_sql("DROP TABLE IF EXISTS DAILY_SALES")

            # Load data
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="replace",   # replace each run
                index=False,
                method="multi",
            )

    load()


# Instantiate the DAG
load_local_csv_to_snowflake()
