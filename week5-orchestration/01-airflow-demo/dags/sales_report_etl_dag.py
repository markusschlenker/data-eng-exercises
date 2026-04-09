import pendulum
import pandas as pd
from pathlib import Path
from airflow.decorators import dag, task


# Define DAG
@dag(
    dag_id="daily_sales_report_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,     
    catchup=False,
    tags=["etl", "example"],
)
def sales_report_etl_dag():

    # Task 1: Extract
    @task
    def extract():
        base_dir = Path(__file__).resolve().parent
        csv_path = base_dir / "raw_sales.csv"

        # Prefer to raise early if file not present on worker:
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV not found at {csv_path} on worker")
        
        df = pd.read_csv(csv_path)
        return df

    # Task 2: Transform
    @task
    def transform(df: pd.DataFrame):
        df.dropna(inplace=True)
        df["total_price"] = df["price"] * df["quantity"]
        category_revenue = df.groupby("category")["total_price"].sum().reset_index()
        return category_revenue

    # Task 3: Load
    @task
    def load(final_df: pd.DataFrame):
        final_path = Path(__file__).resolve().parent / "daily_sales_report.csv"
        final_df.to_csv(final_path, index=False)
        print(f"Report saved to {final_path}")

    # DAG Task Pipeline
    raw_data = extract()
    transformed = transform(raw_data)
    load(transformed)


# Instantiate DAG
sales_report_etl_dag()
