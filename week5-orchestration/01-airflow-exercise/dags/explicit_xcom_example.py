from airflow.decorators import dag, task
from airflow.models import XCom
from datetime import datetime

@dag(
    dag_id="explicit_xcom_example",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def explicit_xcom_example():

    @task
    def push_task(ti=None):
        # ti = task_instance
        ti.xcom_push(key="my_key", value="Custom XCom Data")  # storage in metadata database via key:value

    @task
    def pull_task(ti=None):
        data = ti.xcom_pull(task_ids="push_task", key="my_key")  # retrieval from metadata database via key:value
        print(f"Explicitly pulled: {data}")

    push_task() >> pull_task()  # mandatory, because Airflow cannot determine the order implicitely

explicit_xcom_example()