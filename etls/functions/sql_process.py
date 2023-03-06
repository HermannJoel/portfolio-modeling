import pyodbc, psycopg2
import pandas as pd
import sqlalchemy 
import pymongo

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql.expression import insert, update, delete
from datetime import date    
    
def read_sql(query, conn):
    engine = sqlalchemy.create_engine(conn)
    connection=engine.connect()
    df = pd.read_sql_query(
         sql=query, con=conn
    )
    return df

def write_sql(dataset, table, conn):
     engine=sqlalchemy.create_engine(conn)
     connection=engine.connect()
     dataset.to_sql(name=table, con=connection, if_exists='append', index=False)
     
