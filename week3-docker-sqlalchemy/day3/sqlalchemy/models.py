from sqlalchemy import Column, Integer, String, Float
from db import Base

class Ecommerce(Base):
    __tablename__ = "ecommerce_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    InvoiceNo = Column(String, nullable=True)
    StockCode = Column(String, nullable=True)
    Description = Column(String, nullable=True)
    Quantity = Column(Integer, nullable=True)
    InvoiceDate = Column(String, nullable=True)
    UnitPrice = Column(Float, nullable=True)
    CustomerID = Column(String, nullable=True)
    Country = Column(String, nullable=True)
