"""
Data loader for e-commerce data stored in data.csv using Core approach with SQLAlchemy.

Pre-Requisite: delete database 'ecommerce_db' if it exists before running this
script to see the full process of DB creation (e.g. in pgadmin).

Source of data: https://www.kaggle.com/datasets/carrie1/ecommerce-data
"""
import csv
import os
from dotenv import load_dotenv  # .env file 
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, Float, String, select, func, insert, text
)

from logger import get_logger

# Initialize logger
log = get_logger("CSV-LOADER")

# Load .env config
load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("DB_NAME")
CSV_PATH = os.path.join(os.getcwd(), os.getenv("CSV_PATH"))
TABLE_NAME = os.getenv("TABLE_NAME")


# ===============================================================
# SAFE CSV OPEN
# ===============================================================
def open_csv_safely(path):
    return open(path, newline="", encoding="utf-8", errors="replace")


# ===============================================================
# CREATE DATABASE
# ===============================================================
def create_database():
    try:
        admin_engine = create_engine(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/postgres",
            isolation_level="AUTOCOMMIT"
        )

        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname=:d"),
                {"d": DB_NAME}
            ).fetchone()

            if exists:
                log.info(f"Database '{DB_NAME}' already exists.")
            else:
                conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
                log.info(f"Database '{DB_NAME}' created successfully.")
    except Exception as e:
        log.error(f"Error creating database: {e}")


# ===============================================================
# GET DB ENGINE
# ===============================================================
def get_engine():
    return create_engine(
        f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}"
    )


# ===============================================================
# DEFINE TABLE (Based on your provided structure)
# ===============================================================
def create_table_from_csv(engine):
    metadata = MetaData()

    table = Table(
        TABLE_NAME,
        metadata,
        Column("InvoiceNo", String),
        Column("StockCode", String),
        Column("Description", String),
        Column("Quantity", Integer),
        Column("InvoiceDate", String),
        Column("UnitPrice", Float),
        Column("CustomerID", String),
        Column("Country", String),
    )

    metadata.create_all(engine, checkfirst=True)
    log.info(f"Table '{TABLE_NAME}' is ready.")
    return table


# ===============================================================
# CLEAN + INSERT
# ===============================================================
def insert_csv_data(table, engine):
    try:
        with open_csv_safely(CSV_PATH) as f:
            rdr = csv.DictReader(f)
            header_len = len(rdr.fieldnames)
            cleaned_rows = []

            for row in rdr:

                if len(row) != header_len:
                    log.warning(f"Skipping malformed row: {row}")
                    continue

                # Clean string values
                row = {k: (v.strip() if v else "") for k, v in row.items()}

                # Convert numeric fields
                try:
                    row["Quantity"] = int(row["Quantity"])
                except:
                    row["Quantity"] = 0

                try:
                    row["UnitPrice"] = float(row["UnitPrice"])
                except:
                    row["UnitPrice"] = 0.0

                cleaned_rows.append(row)

        if not cleaned_rows:
            log.warning("No valid rows found in CSV.")
            return

        with engine.connect() as conn:
            conn.execute(insert(table), cleaned_rows)
            conn.commit()

        log.info(f"Inserted {len(cleaned_rows)} rows.")

    except Exception as e:
        log.error(f"Error inserting CSV data: {e}")


# ===============================================================
# INSIGHTS
# ===============================================================
def describe_data(table, engine):
    try:
        with engine.connect() as conn:
            total = conn.execute(select(func.count()).select_from(table)).scalar()
            log.info(f"Total rows: {total}")

            # Print first 5 rows
            for row in conn.execute(select(table).limit(5)):
                log.info(f"Row: {dict(row._mapping)}")

            # Stats
            for col in ["Quantity", "UnitPrice"]:
                mn, mx, avg = conn.execute(
                    select(
                        func.min(table.c[col]),
                        func.max(table.c[col]),
                        func.avg(table.c[col])
                    )
                ).first()

                log.info(f"{col}: min={mn}, max={mx}, avg={avg}")

    except Exception as e:
        log.error(f"Error describing data: {e}")


# ===============================================================
# MAIN
# ===============================================================
if __name__ == "__main__":

    log.info("Starting data load process...")

    create_database()

    engine = get_engine()

    table = create_table_from_csv(engine)

    insert_csv_data(table, engine)

    describe_data(table, engine)

    log.info("Process completed.")
