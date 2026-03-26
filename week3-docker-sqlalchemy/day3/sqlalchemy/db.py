import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import text
from logger import get_logger  # import your logger function

# Initialize logger
log = get_logger("DB-SETUP")

# Load .env
load_dotenv()

# Read environment variables
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
DB_NAME = os.getenv("DB_NAME")

if DB_NAME is None:
    raise ValueError("ERROR: DB_NAME is missing from .env file!")

# Create database if not exists
def create_database_if_not_exists():
    admin_url = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/postgres"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    with admin_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname=:d"),
            {"d": DB_NAME}
        ).fetchone()

        if exists:
            log.info(f"Database '{DB_NAME}' already exists.")
        else:
            conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
            log.info(f"Database '{DB_NAME}' created.")

create_database_if_not_exists()

# Connect to target database
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()
