# Demo project

Following instructions from [here](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/02_airflow_dbt_snowflake_introduction/02_airflow_dbt_snowflake_introduction.md#3-dbt-core)

Configure and setup project

* edit ~/.dbt/profiles.yml
* run `dbt init sf_dbt_activity0`  # project name changed for folder sorting order
* check project by running in project folder `dbt debug`

Create files as described (prefix files/models with `dbt_demo_`)
* models/sources.yml
* models/staging/stg_orders.sql
* models/intermediate/int_orders_clean.sql
* models/marts/fact_orders.sql

Run the fact model
* `dbt run --select +dbt_demo_fact_orders.sql`


# Activity 1

[activity1](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/03_dbt_in_depth/exercise/activity_1_dbt_model_order_geography.md)

## Pre-Steps

Configure and setup project

* edit ~/.dbt/profiles.yml
* run `dbt init sf_dbt_activity1`

## Activity steps


