import pyodbc
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, Column, MetaData, Table, column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, String, Date, Float, Numeric, DECIMAL
from sqlalchemy.sql.expression import insert, update, delete
from datetime import date

class SQL:
    
    def __init__(self, query, conxn):
        self.query=query
        self.conxn=conxn
        
    def create_connection(self):
        engine=sqlalchemy.create_engine(self.conxn)
        connection=engine.connect()
        return connection
    
    def read_sql(self):
        df=pd.read_sql_query(
            sql=self.query, con=self.conxn
        )
        return df