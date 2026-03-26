# Exercise: Use sqlalchemy, PostgresQL, Pandas to interact with the table `tblnorthwind` 

## Objective

Create a python program which uses sqlalchemy module to interact with PostgresQL db and perform various analysis.

## Instructions

1. Install required library.
2. Connect to training database used in week 2 weekly project and check version of the PostgresQL.
3. Count total number of records from the table tblnorthwind.
4. Create user table and display inserted records. User table's ddl is provided in theory part.
5. Execute: Full ORM Workflow by pointing to tblnorthwind table. (Optional)
6. Store tblnorthwind table content in pandas dataframe and perform below opertaions:
   - Find the total number of orders placed by each customer.
   - Calculate the total revenue (unitprice × quantity) for each product.
   - Find the average discount given per category.
   - Identify the employees who have handled more than 50 orders.
   - List all orders where the shipped date is later than the required date.

7. Create customers table which was created in week 2 project.


