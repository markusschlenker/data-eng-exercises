"""
Titanic DAG

Based on instructions in 
    https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/
    main/01_introduction_to_airflow_3/exercise/activity_1_titanic_data_etl_with_taskflow_api.md

Example from Airflow tutorial used as template:
    https://airflow.apache.org/docs/apache-airflow/3.2.0/tutorial/taskflow.html
"""
import pendulum
import pandas as pd
from pathlib import Path

from airflow.sdk import dag, task

default_args = {
    'owner': 'airflow',
}
@dag(
    dag_id="titanic_etl",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),  # some date in the past
    catchup=False,
    tags=["etl", "example"],
    access_control={
        'team_alpha_role': {'can_read', 'can_edit'}
    },
)
def titanic_taskflow_api_etl():
    """
    ### Titanic ETL with Airflow DAG using TaskFlow API

    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.

    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """

    @task()
    def extract():
        """
        #### Extract task

        Load data from csv file
        """
        base_dir = Path(__file__).resolve().parent
        csv_path = base_dir / "titanic.csv"

        # Prefer to raise early if file not present on worker:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path} on worker")
        
        df = pd.read_csv(csv_path)
        print("Extracting task complete")
        return df
    

    @task()
    def transform(data: pd.DataFrame):
        """
        #### Transform task

        - Keep columns: Name, Sex, Age, Survived
        - Fill missing ages with the mean
        - Convert Survived (0 → "No", 1 → "Yes")

        """
        columns_to_keep = ["Name", "Sex", "Age", "Survived"]
        data.Age = data.Age.fillna(data.Age.mean())
        data.Survived = data.Survived.map(lambda x: {1: "Yes", 0: "No"}[x])
        print("Transforming task complete")
        return data[columns_to_keep]
    

    @task()
    def load(data: pd.DataFrame):
        """
        #### Load task

        Save cleaned data into a new CSV file
        """
        print("PWD=", Path.cwd())
        base_dir = Path(__file__).resolve().parent
        data.to_csv(base_dir / 'fct_titanic.csv')
        print("Loading task complete")
        return None


    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

titanic_etl_dag = titanic_taskflow_api_etl()
