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
    base_dir = Path(__file__).resolve().parent  # dags dir
    path_olist = base_dir / "data_olist"

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

def _clean_olist_files(path_olist: typing.Union[str, bytes, os.PathLike]):
    path_olist = Path(path_olist)
    if PATH_OLIST.is_dir():
        shutil.rmtree(PATH_OLIST)

    logger.info(f"Deleted path: {PATH_OLIST}")
    logger.info(f"Still exists? {PATH_OLIST.exists()}")
    return None

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
        """
        Download Kaggle Olist dataset
        """
        return _download_olist_files()
    
    @task
    def extract():
        pass
    
    @task
    def dummy(xxx):
        print(f"dummy {xxx}")
        for f in os.listdir(xxx):
            print(f)
        return None


    @teardown
    def clean_olist_files():
        """
        Remove Kaggle Olist dataset after loading to warehouse
        """
        _clean_olist_files(PATH_OLIST)
        raise FileNotFoundError("asdasd")
        return None
    
    setup_obj = download_olist_files()
    teardown_obj = clean_olist_files()
    setup_obj >> dummy(PATH_OLIST) >> teardown_obj
    setup_obj >> teardown_obj


olist_data_to_snowflake_dag = olist_data_to_snowflake()

if __name__ == "__main__":
    import time
    #olist_data_to_snowflake_dag.test()
    olist_path =_download_olist_files()
    time.sleep(0.5)
    _clean_olist_files(olist_path)
