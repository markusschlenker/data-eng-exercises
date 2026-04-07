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


## Exercise part 4 steps - Metabase

1. Follow instructions [here](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker)

    ```bash
    docker pull metabase/metabase:latest
    docker run -d -p 3000:3000 --name metabase metabase/metabase
    ```

1. Connect to metabase container in browser at http://localhost:3000/

1. Setup
   - user: abs@niceto.com
   - PW: 1metabase
   - host: host.docker.internal 
     Note that metabase tries to connect to the database from within the container and therefore
     `localhost` referes to the container internals. If the metabase container is added in the
     docker-compose.yml as a separate service, it should be connectable with 

### Alternatively connect at container startup

```bash
docker run -d -p 3001:3000 \
  -e "MB_DB_TYPE=postgres" \
  -e "MB_DB_DBNAME=db_northwind" \
  -e "MB_DB_PORT=5434" \
  -e "MB_DB_USER=postgres" \
  -e "MB_DB_PASS=admin" \
  -e "MB_DB_HOST=host.docker.internal" \
   --name metabase_cli_startup metabase/metabase
```