from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import random

def check_sales(**kwargs):
    sales = random.randint(50000, 150000)
    print(f"Today's sales: {sales}")

    if sales > 100000:
        return "high_value_task"
    else:
        return "low_value_task"

def high_value_processing():
    print("Running high value processing...")

def low_value_processing():
    print("Running low value processing...")

with DAG(
    dag_id="branching_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    branch_task = BranchPythonOperator(
        task_id="check_sales_branch",
        python_callable=check_sales
    )

    high_value_task = PythonOperator(
        task_id="high_value_task",
        python_callable=high_value_processing
    )

    low_value_task = PythonOperator(
        task_id="low_value_task",
        python_callable=low_value_processing
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    # Flow
    start >> branch_task
    branch_task >> high_value_task >> end
    branch_task >> low_value_task >> end