import os
import logging
import pendulum
import pandas as pd
from pathlib import Path
import glob
from airflow.decorators import dag, task
from airflow.models import XCom
from sqlalchemy import create_engine
from dotenv import dotenv_values
import requests
import json
from urllib.parse import quote_plus

# logger = logging.getLogger(__name__)  # results in logger name like "unusual_prefix_c3993553fb0abb7e191c8230049053384b74bae2_weather_data_to_snowflake"
task_logger = logging.getLogger("task")

SNOWFLAKE_DATABASE = "SNOWFLAKE_LEARNING_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
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
        f"snowflake://{user}:{password}@{account}/{database}/{schema}"
        f"?warehouse={warehouse}&role={role}"
    )
    return create_engine(url)


@dag(
    dag_id="weather_data_to_snowflake",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,  # no backfilling
    tags=["snowflake", "etl", "weather-api"],
)
def weather_data_to_snowflake():

    @task
    def api_call(ti=None):
        """
        Get weather data
        """
        # get API key from .env file
        base_dir = Path(__file__).resolve().parent  # dags folder
        credentials = dotenv_values(base_dir / ".env.openweathermap", verbose=True)
        apikey = credentials["APIKEY_OPENWEATHERMAP"]

        # get lat/lon for city via geo API
        city = "Würzburg"
        endpoint = "http://api.openweathermap.org/geo/1.0/direct"
        params = {
            "q": city,
            "appid": apikey,
        }
        resp = requests.get(endpoint, params=params)
        if resp.status_code == 200:
            geo_dict = resp.json()[0]
            task_logger.warning(f"For testing.")
        else:
            task_logger.warning(f"Unexpected response status {resp.status_code} "
                                "from OpenWeather Geocoding API. Using default location.")
            geo_dict = {'name': 'Würzburg', 'lat': 49.79245, 'lon': 9.932966,
                        'country': 'DE', 'state': 'Bavaria'}

        # get weather data for city
        endpoint = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "lat": geo_dict["lat"],
            "lon": geo_dict["lon"],
            "appid": apikey,
        }
        resp = requests.get(endpoint, params=params)
        print(f"""{json.dumps(resp.json(), indent=2)}""")

        # explicit XCom push for testing
        ti.xcom_push(key="weather_data", value=resp.json())

        return resp.json()  # implicit XCom push as key='return_value'

    @task
    def load():
        #table_name = "WEATHER_DATA"
        table_name = "weather_data"  # airflow logs complain about all caps table name and recommends lower case table name
        engine = _snowflake_engine()

        with engine.begin() as conn:
            conn.exec_driver_sql(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            conn.exec_driver_sql(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            conn.exec_driver_sql(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")

            # Load data
            df = pd.DataFrame({"id": [1,2,3], "price": [1.2, 3.1, 5.0]})  # test data
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="replace",
                index=False,
                method="multi",
            )

    api_call() >> load()


# Instantiate the DAG
weather_data_to_snowflake()
