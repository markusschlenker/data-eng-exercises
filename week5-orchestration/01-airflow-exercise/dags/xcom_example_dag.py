from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def xcom_example_dag():

    @task
    def push_task():
        return "Hello from Task 1"

    @task
    def pull_task(msg):
        print(f"Received via XCom: {msg}")

    msg = push_task()  # DAG-level task instantiation
    pull_task(msg)     # DAG-level task instantiation

xcom_example_dag()  # DAG must be called
