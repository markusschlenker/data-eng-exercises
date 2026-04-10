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

from airflow.sdk import dag, task, setup, teardown


logger = logging.getLogger("airflow.task")

########################################################################################################################
# utility
#

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

def _extract_single_csv(csv_file: typing.Union[str, bytes, os.PathLike]):
    csv_path = PATH_OLIST / csv_file

    if not csv_path.exists() or csv_path.lstat().st_size == 0:
        return f"File {csv_path} missing or empty."
    
    iterchunks = pd.read_csv(csv_path, chunksize=50_000)

    df = next(iterchunks)
    columns = [c.upper() for c in df.columns]


    with open(PATH_OLIST / (csv_file + ".df.import.test"), "w") as f:
        f.write(df.head(10).to_string())
        logger.info(f"dumped df.head to {f.name}")
    pass    

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
        _extract_single_csv(csv_file)
        return
    
    @task
    def dummy(xxx):
        print(f"dummy {xxx}")
        for f in os.listdir(xxx):
            print(f)
        return None


    @teardown
    def clean_olist_files():
        """Remove Kaggle Olist dataset after loading to warehouse"""
        _clean_olist_files()
        return None
    
    setup_obj = download_olist_files()
    teardown_obj = clean_olist_files()

    setup_obj >> dummy(PATH_OLIST) >> extract_single_csv(EXPECTED_OLIST_FILES[0]) >> teardown_obj
    setup_obj >> teardown_obj


olist_data_to_snowflake_dag = olist_data_to_snowflake()

if __name__ == "__main__":
    import time
    #olist_data_to_snowflake_dag.test()
    olist_path =_download_olist_files()
    time.sleep(0.5)
    _extract_single_csv(EXPECTED_OLIST_FILES[0])
    _extract_single_csv(EXPECTED_OLIST_FILES[-1])
    time.sleep(0.5)
    #_clean_olist_files()
