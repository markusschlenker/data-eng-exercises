from dotenv import load_dotenv
import os
from logger import get_logger
from orm_csv import create_tables, insert_data, show_insights
from db import create_database_if_not_exists

load_dotenv()
log = get_logger("APP-PIPELINE")

def validate_env():
    required = ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "DB_NAME", "CSV_PATH"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        log.error("Missing environment variables: %s", missing)
        raise SystemExit(f"Missing environment variables: {missing}")
    log.info("Environment validated.")

def main():
    log.info("=== STARTING PIPELINE ===")
    validate_env()

    # Create DB (AFTER Postgres is healthy)
    log.info("Checking database...")
    create_database_if_not_exists()

    # Create tables
    log.info("Creating tables (if not exists)...")
    create_tables()

    # Insert CSV
    log.info("Inserting data from CSV...")
    insert_data()

    # Show stats
    show_insights()

    log.info("=== PIPELINE COMPLETED ===")

if __name__ == "__main__":
    main()
