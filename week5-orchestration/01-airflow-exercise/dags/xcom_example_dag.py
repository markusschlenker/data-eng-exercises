from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="xcom_example_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    )
def xcom_example_dag():

    @task
    def push_task():
        return "Hello from Task 1"

    @task
    def pull_task(msg):
        print(f"Received via XCom: {msg}")

    # option A
    # msg = push_task()  # DAG-level task instantiation
    # pull_task(msg)     # DAG-level task instantiation

    # option A.1
    # t1 = push_task()
    # t2 = pull_task(t1)
    ##t1 >> t2  # this is optional here, because t1 is fed into pull_task and Airflow will determine the execution order

    # option B
    # (msg:=push_task()) >> (pull_task(msg))

    # option C
    pull_task(push_task())


xcom_example_dag()  # DAG must be called
