"""
Data loader for e-commerce data stored in data.csv using ORM approach with SQLAlchemy.

Pre-Requisite: delete database 'ecommerce_db' if it exists before running this
script to see the full process of DB creation (e.g. in pgadmin).

Source of data: https://www.kaggle.com/datasets/carrie1/ecommerce-data
"""
import csv
import os
from dotenv import load_dotenv
from sqlalchemy import func
from db import engine, SessionLocal, Base
from models import Ecommerce
from logger import get_logger  # import your logger function

# Initialize logger
log = get_logger("ORM-LOADER")

load_dotenv()
CSV_PATH = os.path.join(os.getcwd(), os.getenv("CSV_PATH"))

# Safe type conversion
def safe_int(val):
    try:
        return int(val)
    except:
        return 0

def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0

def create_tables():
    Base.metadata.create_all(engine)
    log.info("Tables created.")

def load_csv_rows():
    """Load all rows from CSV and return ORM objects."""
    rows = []

    with open(CSV_PATH, encoding="utf-8", errors="replace") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            try:
                rows.append(
                    Ecommerce(
                        InvoiceNo=r.get("InvoiceNo") or None,
                        StockCode=r.get("StockCode") or None,
                        Description=r.get("Description") or None,
                        Quantity=safe_int(r.get("Quantity")),
                        InvoiceDate=r.get("InvoiceDate") or None,
                        UnitPrice=safe_float(r.get("UnitPrice")),
                        CustomerID=r.get("CustomerID") or None,
                        Country=r.get("Country") or None,
                    )
                )
            except Exception as e:
                log.error(f"Failed to process row: {r}")
                log.exception(e)
    return rows

def insert_data():
    session = SessionLocal()
    items = load_csv_rows()

    if items:
        session.bulk_save_objects(items)
        session.commit()
        log.info(f"Inserted {len(items)} rows into DB.")
    else:
        log.warning("No rows to insert.")

    session.close()

def show_insights():
    session = SessionLocal()
    log.info("===== INSIGHTS =====")

    # Total rows
    total = session.query(Ecommerce).count()
    log.info("Total Rows: %s", total)

    # First 5 rows
    log.info("First 5 rows:")
    for row in session.query(Ecommerce).limit(5):
        log.info("%s | %s | %s", row.InvoiceNo, row.Description, row.Quantity)

    # Numeric insights for Quantity
    q_min, q_max, q_avg = session.query(
        func.min(Ecommerce.Quantity),
        func.max(Ecommerce.Quantity),
        func.avg(Ecommerce.Quantity)
    ).first()
    log.info("Quantity -> min=%s, max=%s, avg=%s", q_min, q_max, q_avg)

    session.close()

if __name__ == "__main__":
    create_tables()
    insert_data()
    show_insights()
