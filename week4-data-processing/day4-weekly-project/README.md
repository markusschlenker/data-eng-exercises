# Week 4 weekly project

Ref: https://github.com/neuefische/de-week-4-Data-Processing-Fundamentals/blob/main/04_weekly_project/instructions.md

## Preparation to have a clean baseline

1. Copy reference solution from https://github.com/neuefische/de-week-4-Data-Processing-Fundamentals/tree/solution/03_data_pipeline_design_%26_data_quality/solution
   - northwind_project_docker/
   - data_pipeline_with_quality_checks.ipynb

1. Convert the notebook to python script ...

    ```bash
    jupyter nbconvert --to python data_pipeline_with_quality_checks.ipynb
    ```

1. ... and add conditional table drop statements for all tables if missing
   
    ```python
    sql_stmt = """DROP TABLE IF EXISTS public.tblnorthwind_error"""
    with engine.begin() as conn:
    conn.execute(text(sql_stmt))

    sql_stmt = """DROP TABLE IF EXISTS public.fct_tblnorthwind"""
    with engine.begin() as conn:
    conn.execute(text(sql_stmt))
    ```

1. Start docker 
   
   ```bash
   cd northwind_project_docker
   docker compose up --build
   ```


1. run the converted script

    `python data_pipeline_with_quality_checks.py`


## Start with the exercise based on instructions in Ref at top of this readme

