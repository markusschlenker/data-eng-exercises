# Airflow pipeline demo project

[Instructions](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/01_introduction_to_airflow_3/01_introduction_to_airflow_3.md)

## Setup

Setup is done based on [*instructions* Section 3](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/01_introduction_to_airflow_3/01_introduction_to_airflow_3.md#3-airflow-installation-with-docker).

Prerequisites:

- Run Docker Desktop

Steps overview in working directory:

- Download [docker-compose.yaml](https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml)
- make folders  `mkdir dags logs plugins config`
- create .env with `AIRFLOW_UID=50000`
- initialize: `docker compose up airflow-init`
- start: `docker compose up`
- go to Airflow dashboard at http://localhost:8080 (user=airflow, pw=airflow)

## Demo exercise

Exercise is based on [*instructions* Section 9](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/01_introduction_to_airflow_3/01_introduction_to_airflow_3.md#9-creating-your-first-dag-in-airflow).

Steps overview:

- Copy [raw_sales.csv](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/01_introduction_to_airflow_3/data/raw_sales.csv) to directory `dags`
- Create file `dags/sales_report_etl_dag.py` with *complete code* from *instructions* Section 9
- Do Airflow setup described above or re-run `docker compose up` if Airflow containers are already running to update files in containers with the newly added local files.
- In Airflow dashboard at http://localhost:8080 got to dags and search our DAG by name `daily_sales_report_etl` or by tag `etl`
- Click *Trigger* > then *Single Run*
  