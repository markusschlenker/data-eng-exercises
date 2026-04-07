import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def create_database(db_user, db_password, db_host, db_name):
    """
    Connects to the default 'postgres' database and creates the specified
    database using SQLAlchemy.
    """
    try:
        # Create a connection engine to the default 'postgres' database
        # This is necessary to issue the CREATE DATABASE command
        temp_engine = create_engine(
            f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/postgres')

        # Connect to the database and set the isolation level to AUTOCOMMIT
        with temp_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            # Check if the database exists
            result = conn.execute(
                text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"))
            exists = result.scalar()

            if not exists:
                # Create the database using a raw SQL command
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                print(f"Database '{db_name}' created successfully.")
            else:
                print(f"Database '{db_name}' already exists.")

    except Exception as e:
        print(f"Error creating database: {e}")


def load_data_from_csv(csv_file, table_name):
    """
    Connects to the specified database using SQLAlchemy and loads data from a CSV file.
    """
    try:
        # Get database connection details from environment variables
        db_host = os.environ.get('DB_HOST')
        db_user = os.environ.get('DB_USER')
        db_password = os.environ.get('DB_PASSWORD')
        db_name = os.environ.get('DB_NAME')

        # Create the SQLAlchemy engine for the target database
        engine = create_engine(
            f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}/{db_name}')

        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file, encoding="latin1")
        print(f"Successfully read {len(df)} rows from {csv_file}")

        # Use the pandas to_sql method to create the table and load data
        # 'if_exists' can be 'fail', 'replace', or 'append'
        # 'index=False' prevents pandas from writing the DataFrame index as a column
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(
            f"Successfully loaded {len(df)} rows into '{table_name}' using SQLAlchemy.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    # Get database connection details from environment variables
    db_host = os.environ.get('DB_HOST')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_name = os.environ.get('DB_NAME')

    # Create the database first
    create_database(db_user, db_password, db_host, db_name)

    # Then, load the data into the newly created database
    load_data_from_csv('Northwind_errors.csv', 'tblnorthwind')
