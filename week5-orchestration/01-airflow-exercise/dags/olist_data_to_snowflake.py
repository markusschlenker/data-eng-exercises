"""
Week 5, Activity 2: Load Olist Dataset into Snowflake with Airflow

Based on instructions in 
    https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/
    main/02_airflow_dbt_snowflake_introduction/exercise/activity_2_load_olist_data_to_snowflake.md
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


logger = logging.getLogger("airflow.task")

########################################################################################################################
# utility
#
SNOWFLAKE_DATABASE = "SNOWFLAKE_LEARNING_DB"
# SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_SCHEMA = "RAW_OLIST"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

def _snowflake_engine():
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
    schema = SNOWFLAKE_SCHEMA
    warehouse = SNOWFLAKE_WAREHOUSE
    role = "ACCOUNTADMIN"

    url = (
        f"snowflake://{user}:{password}@{account}/{database}"
        f"?warehouse={warehouse}&role={role}"
    )
    engine = create_engine(url)
    with engine.begin() as conn:
        # conn.execute(
        #     text("CREATE SCHEMA IF NOT EXISTS :schema_name"),
        #     {"schema_name": schema},
        # )
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS  {schema}")

    return engine

########################################################################################################################
# business logic
#
base_dir = Path(__file__).resolve().parent  # dags dir
PATH_OLIST = path_olist = base_dir / "data_olist"
EXPECTED_OLIST_FILES = [
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_customers_dataset.csv",
]
def _download_olist_files():
    """
    Download Kaggle Olist dataset
    """
    # Download latest version
    path = kagglehub.dataset_download("olistbr/brazilian-ecommerce", output_dir=PATH_OLIST)
    logger.info(f"Path to dataset files: {path}")

    # check all files are there
    for f in EXPECTED_OLIST_FILES:
        _f = Path(path) / f
        logger.info(f"Verifying download of {f}...")
        if not _f.exists():
            raise FileNotFoundError(f"Missing required file in kagglehub download: {_f}")
        logger.info("ok")
        
    return str(path)

def _clean_olist_files():
    if PATH_OLIST.is_dir():
        shutil.rmtree(PATH_OLIST)
    logger.info(f"Deleted path: {PATH_OLIST}")
    logger.info(f"Verify removal: path still exists? {PATH_OLIST.exists()}")
    return None

def _etl_single_csv(csv_file: typing.Union[str, bytes, os.PathLike]):
    csv_path = PATH_OLIST / csv_file

    if not csv_path.exists() or csv_path.lstat().st_size == 0:
        return f"File {csv_path} missing or empty."
    
    logging.info(f"Extracting {csv_path}")
    iterchunks = pd.read_csv(csv_path, chunksize=5000)

    chunk = next(iterchunks)
    columns = [c.upper() for c in chunk.columns]
    chunk.columns = columns

    engine = _snowflake_engine()
    table_name = Path(csv_file).stem.upper()
    logging.info(f"Laoding into snowflake warehause in table {table_name}")

    with engine.begin() as conn:
        conn.exec_driver_sql(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
        conn.exec_driver_sql(f"USE DATABASE {SNOWFLAKE_DATABASE}")
        conn.exec_driver_sql(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        
        # # only for debugging !!
        # logging.info(f"DEBUG: dropping table {table_name}")
        # logging.debug(f"DEBUG: dropping table {table_name}")
        # conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")
        # conn.exec_driver_sql(f"DROP TABLE IF EXISTS LOADED_FILES")
        # return ""

        # Create control table if not exists
        conn.exec_driver_sql("""
            CREATE TABLE IF NOT EXISTS  LOADED_FILES (
                FILE_NAME STRING PRIMARY KEY,
                LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # check of data already loaded for this csv file
        result = conn.execute(
            text("SELECT COUNT(*) FROM  LOADED_FILES  WHERE FILE_NAME = :file"),
            {"file": csv_file},
        )
        if result.scalar() > 0:
            logger.info(f"Skipping loading for {csv_file}. Already loaded.")
            return "skipped"

        num_chunk = 1
        logging.info(f"Adding chunk {num_chunk}")
        params = {
            "name": table_name.lower(),  # use lower() to avoid pandas/sqlalchemy UserWarning, table create as upper case in Snowflake
            "con": conn,
            "if_exists": "replace",
            "index": False,
            "method": "multi",
        }
        params.update({"if_exists": "append"})
        chunk.to_sql(**params)

        for chunk in iterchunks:
            num_chunk += 1
            logging.info(f"Adding chunk {num_chunk}")
            chunk.columns = columns
            chunk.to_sql(**params)
        logging.info(f"Completed adding data from {csv_file} to {table_name}")

        # conn.exec_driver_sql(f"""
        #     INSERT INTO  LOADED_FILES(FILE_NAME)  VALUES('{csv_file}')
        # """)
        conn.execute(
            text("INSERT INTO LOADED_FILES(FILE_NAME) VALUES(:file)"),
            {"file": csv_file},
        )


    return "loaded"    

########################################################################################################################

@dag(
    dag_id="olist_data_to_snowflake",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["olist", "snowflake", "etl", "kaggle", "week5"],
)
def olist_data_to_snowflake():

    @setup
    def download_olist_files():
        """Download Kaggle Olist dataset"""
        return _download_olist_files()
    
    @task
    def extract_single_csv(csv_file: typing.Union[str, bytes, os.PathLike]):
        _etl_single_csv(csv_file)
        return
    
    @teardown
    def clean_olist_files():
        """Remove Kaggle Olist dataset after loading to warehouse"""
        _clean_olist_files()
        return None
    
    setup_obj = download_olist_files()
    teardown_obj = clean_olist_files()

    extractors = [extract_single_csv(f) for f in EXPECTED_OLIST_FILES]

    setup_obj >> extractors >> teardown_obj
    setup_obj >> teardown_obj


olist_data_to_snowflake_dag = olist_data_to_snowflake()

if __name__ == "__main__":
    import time
    #olist_data_to_snowflake_dag.test()
    olist_path =_download_olist_files()
    time.sleep(0.5)
    _etl_single_csv(EXPECTED_OLIST_FILES[0])
    _etl_single_csv(EXPECTED_OLIST_FILES[-1])
    time.sleep(0.5)
    #_clean_olist_files()
