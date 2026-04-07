#!/usr/bin/env python
# coding: utf-8

# ## Make sure your container northwind_project_docker is up and running to perform below code

# In[11]:


#pip install sqlalchemy


# In[12]:


#pip install psycopg2-binary


# In[1]:


from sqlalchemy import create_engine,text
import psycopg2


# ## Connect to the PostgreSQL database running in the Docker container on port 5434

# In[2]:


username = "postgres"     
password = "admin"     
host = "localhost"            
port = "5434"                   
database = "db_northwind"


# Create the SQLAlchemy engine
engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{database}")

# Test the connection
with engine.begin() as conn:
    result = conn.execute(text("SELECT version();"))
    print(result.fetchone())


# ## Drop landing table in case it exists

# In[3]:


sql_stmt = """DROP TABLE IF EXISTS public.land_tblnorthwind"""


# In[4]:


with engine.begin() as conn:
    conn.execute(text(sql_stmt))


# ## Create landing table public.land_tblnorthwind

# In[5]:


sql_stmt = """
create table public.land_tblnorthwind as
select
"orderID" as orderid,
"customerID" as customerid,
"employeeID" as employeeid,
cast("orderDate" as date) as orderdate,
cast("requiredDate" as date) as requireddate,
cast("shippedDate" as date) as shippeddate,
"shipVia" as shipvia,
"Freight" as freight,
"productID" as productid,
"unitPrice" as unitprice,
"quantity" as quantity,
"discount" as discount,
"companyName" as companyname,
"contactName" as contactname,
"contactTitle" as contacttitle,
"employees.lastName" as employeeslastname,
"employees.firstName" as employeesfirstname,
"employees.title" as employeestitle,
"productName" as productname,
"supplierID" as supplierid,
"categoryID" as categoryid,
"quantityPerUnit" as quantityperunit,
"unitPrice.1" as unitprice1,
"unitsInStock" as unitsinstock,
"unitsOnOrder" as unitsonorder,
"reorderLevel" as reorderlevel,
"discontinued" as discontinued,
"categoryName" as categoryname,
"suppliers.companyName" as supplierscompanyname,
"suppliers.contactName" as supplierscontactname,
"suppliers.contactTitle" as supplierscontacttitle
from tblnorthwind;
"""


# In[6]:


with engine.begin() as conn:
    conn.execute(text(sql_stmt))


print("Landing Table with name and data type created successfully")


# ## Match count between source table and landing table. Raise error in case erro count don't match
# 
# This can be done after the 2 tables `tblnorthwind` and `land_tblnorthwind` are loaded into the postgres db running in docker container, refer to `app.py`

# In[7]:


with engine.begin() as conn:
    # Get counts from both tables
    src_count = conn.execute(text("SELECT COUNT(*) FROM public.tblnorthwind")).scalar()
    land_count = conn.execute(text("SELECT COUNT(*) FROM public.land_tblnorthwind")).scalar()

    print(f"tblnorthwind count: {src_count}, land_tblnorthwind count: {land_count}")

    # Compare and raise error if mismatch
    if src_count != land_count:
        raise ValueError(f"Row count mismatch! tblnorthwind={src_count}, land_tblnorthwind={land_count}")
    else:
        print("Row counts match!")


# ## Create table tblnorthwind_error to store erroneous records 

# In[8]:


sql_stmt = """DROP TABLE IF EXISTS public.tblnorthwind_error"""
with engine.begin() as conn:
    conn.execute(text(sql_stmt))

error_table = """CREATE TABLE IF NOT EXISTS public.tblnorthwind_error
(
    orderid bigint,
    customerid text,
    employeeid bigint,
    orderdate date,
    requireddate date,
    shippeddate date,
    shipvia bigint,
    freight double precision,
    productid bigint,
    unitprice double precision,
    quantity bigint,
    discount double precision,
    companyname text ,
    contactname text ,
    contacttitle text ,
    employeeslastname text ,
    employeesfirstname text ,
    employeestitle text ,
    productname text ,
    supplierid bigint,
    categoryid bigint,
    quantityperunit text  ,
    unitprice1 double precision,
    unitsinstock bigint,
    unitsonorder bigint,
    reorderlevel bigint,
    discontinued bigint,
    categoryname text ,
    supplierscompanyname text ,
    supplierscontactname text ,
    supplierscontacttitle text,
    error_reason text
)
"""


# In[9]:


with engine.begin() as conn:
    conn.execute(text(error_table))

print("Error Table created successfully")


# ## Find all the records where orderid is null and store it in error table

# In[10]:


null_orderid_sql = """insert into public.tblnorthwind_error
select a.*,'orderid is null in source' as error_reason from public.land_tblnorthwind a where orderid is null;"""


# In[11]:


with engine.begin() as conn:
    conn.execute(text(null_orderid_sql))

print("Null orderid QC processed successfully")


# ## Find all the records where customerid is null and store it in error table

# In[12]:


null_customerid_sql = """insert into public.tblnorthwind_error
select a.*,'customerid is null in source' as error_reason from public.land_tblnorthwind a where customerid is null;"""


# In[13]:


with engine.begin() as conn:
    conn.execute(text(null_customerid_sql))

print("Null customerid QC processed successfully")


# ## Find all the records where orderdate is before 1990 and store it in error table

# In[14]:


old_orders = """insert into public.tblnorthwind_error
select a.*,'old orders received' as error_reason from public.land_tblnorthwind a where EXTRACT(YEAR FROM orderdate) <'1990';"""


# In[15]:


with engine.begin() as conn:
    conn.execute(text(old_orders))

print("Old orders QC processed successfully")


# ## Find all records where quantity is negative

# In[16]:


quantity_negative = """insert into public.tblnorthwind_error
select a.*,'quantity is negative' as error_reason from public.land_tblnorthwind a where quantity < 0;"""


# In[17]:


with engine.begin() as conn:
    conn.execute(text(quantity_negative))

print("Negative quantity QC processed successfully")


# ## Find all records where supplierscontacttitle column only contains digits

# In[18]:


qsupplierscontacttitle_check = """insert into public.tblnorthwind_error
select a.*,'supplierscontacttitle is invalid' as error_reason from public.land_tblnorthwind a where supplierscontacttitle ~ '^[0-9]+$';"""


# In[19]:


with engine.begin() as conn:
    conn.execute(text(qsupplierscontacttitle_check))

print("Invalid supplierscontacttitle QC processed successfully")


# ## Create final fact table to store correct records

# In[20]:



sql_stmt = """DROP TABLE IF EXISTS public.fct_tblnorthwind"""
with engine.begin() as conn:
    conn.execute(text(sql_stmt))
    
fact_table = """CREATE TABLE IF NOT EXISTS public.fct_tblnorthwind
(
    orderid bigint,
    customerid text,
    employeeid bigint,
    orderdate date,
    requireddate date,
    shippeddate date,
    shipvia bigint,
    freight double precision,
    productid bigint,
    unitprice double precision,
    quantity bigint,
    discount double precision,
    companyname text ,
    contactname text ,
    contacttitle text ,
    employeeslastname text ,
    employeesfirstname text ,
    employeestitle text ,
    productname text ,
    supplierid bigint,
    categoryid bigint,
    quantityperunit text  ,
    unitprice1 double precision,
    unitsinstock bigint,
    unitsonorder bigint,
    reorderlevel bigint,
    discontinued bigint,
    categoryname text ,
    supplierscompanyname text ,
    supplierscontactname text ,
    supplierscontacttitle text,
    error_reason text,
    load_date timestamp with time zone DEFAULT CURRENT_TIMESTAMP
)
"""


# In[21]:


with engine.begin() as conn:
    conn.execute(text(fact_table))

print("Fact table created successfully")


# ## Load all the correct records into the fact table

# In[22]:


correct_records_sql = """
INSERT INTO public.fct_tblnorthwind
SELECT distinct a.*,b.error_reason,current_timestamp as load_date
FROM public.land_tblnorthwind a
LEFT JOIN public.tblnorthwind_error b
  ON COALESCE(a.orderid, -999) = COALESCE(b.orderid, -999)
 AND COALESCE(a.productid, -999) = COALESCE(b.productid, -999)
WHERE b.orderid IS NULL and b.productid is null;"""


# In[23]:


with engine.begin() as conn:
    conn.execute(text(correct_records_sql))

print("Load completed successfully. Fact table populated with correct records.")


# ## Add primary key CONSTRAINT on fact table fct_tblnorthwind

# In[24]:


sql_stmt_pk = """
ALTER TABLE public.fct_tblnorthwind
ADD CONSTRAINT pk_fct_tblnorthwind PRIMARY KEY (orderid, productid);
"""


# In[25]:


with engine.begin() as conn:
    conn.execute(text(sql_stmt_pk))

print("Primary key created on fact Table")


# 
