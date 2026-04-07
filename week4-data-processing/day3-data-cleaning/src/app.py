from pathlib import Path
import os
from sqlalchemy import Column, Date, Float, Integer, Boolean, MetaData, String, Table, create_engine, text
import pandas as pd

def main():
    print("starting data cleaning Northwind_error.csv")
    print(f"{Path.cwd()=}")

    # check data is available where expected and encoding matches
    with open("data/Northwind_errors.csv", "r", encoding='ISO-8859-1') as file:
        data = file.readlines()
        print("Data from nortNorthwind_errorshwind.csv:")
        print(data[:2])
        print(data[5])  # special encoding

    # this gets the variables set via docker-compose.yml environment section
    PG_USER=os.getenv("POSTGRES_USER", "pg_user")
    PG_PASSWORD=os.getenv("POSTGRES_PASSWORD", "pg_password")
    PG_HOST=os.getenv("POSTGRES_HOST", "pg_host")
    PG_PORT=os.getenv("POSTGRES_PORT", "5432")
    DB_NAME=os.getenv("POSTGRES_DB", "db_name")

    DATABASE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{DB_NAME}"

    engine = create_engine(DATABASE_URL)
    metadata = MetaData(schema="public")  # optional schema, default is 'public'

    """
    orderID,customerID,employeeID,orderDate,requiredDate,shippedDate,shipVia,Freight,
    productID,unitPrice,quantity,discount,companyName,contactName,contactTitle,
    employees.lastName,employees.firstName,employees.title,productName,supplierID,
    categoryID,quantityPerUnit,unitPrice.1,unitsInStock,unitsOnOrder,reorderLevel,discontinued,
    categoryName,suppliers.companyName,suppliers.contactName,suppliers.contactTitle
    """

    NAME_TBL_NORTHWIND = "northwindtbl"
    northwindtbl = Table(
        NAME_TBL_NORTHWIND,
        metadata,
        Column("orderID", Integer, primary_key=True),
        Column("customerID", String),
        Column("employeeID", Integer),
        Column("orderDate", Date),
        Column("requiredDate", Date),
        Column("shippedDate", Date),
        Column("shipVia", Integer),
        Column("freight", Float),
        Column("productID", Integer),
        Column("unitPriceAsOrdered", Float),
        Column("quantity", Integer),
        Column("discount", Float),
        Column("companyName", String),
        Column("contactName", String),
        Column("contactTitle", String),
        Column("employeesLastName", String),
        Column("employeesFirstName", String),
        Column("employeesTitle", String),
        Column("productName", String),
        Column("supplierID", Integer),
        Column("categoryID", Integer),
        Column("quantityPerUnit", String),
        Column("unitPriceCurrent", Float),
        Column("unitsInStock", Integer),
        Column("unitsOnOrder", Integer),
        Column("reorderLevel", Integer),
        Column("discontinued", Boolean),
        Column("categoryName", String),
        Column("suppliersCompanyName", String),
        Column("suppliersContactName", String),
        Column("suppliersContactTitle", String),
    )

    metadata.create_all(engine, checkfirst=True)
    print(f"[INFO] Table '{NAME_TBL_NORTHWIND}' created or already exists.")
    
    with engine.connect() as conn:
        conn.execute(text(f"SELECT 1"))

    col_names = northwindtbl.columns.keys()

    df = pd.read_csv("data/Northwind_errors.csv", skiprows=1, names=col_names, encoding='ISO-8859-1')
    print(df.info())
    print(df.head())

    df.to_sql(NAME_TBL_NORTHWIND, con=engine, if_exists="replace", index=False)

    df1 = pd.read_sql(f"""SELECT "orderID", "orderDate"  FROM {NAME_TBL_NORTHWIND}""", con=engine)
    print(df1.info())
    print(df1.head())

    with engine.connect() as conn:
        print(conn.execute(text(f"SELECT * FROM {NAME_TBL_NORTHWIND} LIMIT 2")).fetchall())

    print("data cleaning Northwind_error.csv finished")
    
if __name__ == "__main__":
    main()
