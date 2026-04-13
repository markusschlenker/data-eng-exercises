# Demo project

Following instructions from [here](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/02_airflow_dbt_snowflake_introduction/02_airflow_dbt_snowflake_introduction.md#3-dbt-core)

Configure and setup project

* edit ~/.dbt/profiles.yml
* run `dbt init sf_dbt_activity0`  # project name changed for folder sorting order
* check project by running in project folder `dbt debug`

Create files as described (prefix files/models with `dbt_demo_`)
* models/sources.yml
  
    Note that my Airflow DAG is configured to put the OLIST data into the 'RAW_OLIST' schema instead of 'PUBLIC'

* models/staging/stg_orders.sql
* models/intermediate/int_orders_clean.sql
* models/marts/fact_orders.sql

Add custom schema in `dbt_project.yml`

* Add custom schema to get result tables in specific sub-schema.
  Note that this uses the schema name from the global project config in `~/.dbt/profiles.yml`
  as a prefix and only adds the configured schema name per model to the global one

  * configure in dbt_project.yml for all models

  ```sql
  models:
    sf_dbt_activity0:
      +schema: dbt_demo_activity0
  ```

  * configure in dbt_project.yml for any model individually, overwriting above global suffix

  ```sql
  models:
    sf_dbt_activity0:
      staging:
        +schema: dbt_demo_activity0__STG
      intermed:
        +schema: dbt_demo_activity0__STG
      marts:
        +schema: dbt_demo_activity0__MARTS
  ```

  * configure in model file

  ```sql
  {{ config(
    schema='MY_MARTS',
  ) }}
  ```
  


* ref: https://docs.getdbt.com/docs/build/custom-schemas?version=1.12


Run the fact model
* `dbt run --select +dbt_demo_fact_orders.sql`


# Activity 1

[activity1](https://github.com/neuefische/de-week-5-Orchestrating-Modern-Data-Workflows/blob/main/03_dbt_in_depth/exercise/activity_1_dbt_model_order_geography.md)

## Pre-Steps

Configure and setup project

* edit ~/.dbt/profiles.yml
* run `dbt init sf_dbt_activity1`

## Activity steps


