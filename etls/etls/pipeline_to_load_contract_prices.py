# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 17:19:02 2022

@author: hermann.ngayap
"""
#==============================================================================
#==========This script is to load contracts prices(OA, CR & PPA)=============== 
#==========   of all assets(in planification & in production)  ================
#==============================================================================

import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
import os

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)

ncwd='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
try:
    os.chdir(ncwd)
    print('the working directory has been changed!')
    print('wd: %s ' % os.getcwd())
except NotADirectoryError():
    print('you have not chosen directory!')
except FileNotFoundError():
    print('the folder was not found. the path is incorect!')
except PermissionError():
    print('you do not have access to this folder/file!')

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'


#To imports Excel files containing contracts prices dfs of assets in planif, in production and assets under ppa  
prices_prod=pd.read_excel(path_dir_temp+'contracts_prices_oa_cr_vmr.xlsx')
prices_planif=pd.read_excel(path_dir_temp+'contracts_prices_eol_sol.xlsx')
prices_ppa=pd.read_excel(path_dir_temp+'contracts_prices_ppa.xlsx')

#Merge contracts prices dfs of assets in planif, in production and assets under ppa  
frame=[prices_prod, prices_planif, prices_ppa]
prices_oa_cr_ppa= pd.concat(frame, axis=0, ignore_index=True)
prices_oa_cr_ppa.reset_index(inplace=True, drop=True)

prices_oa_cr_ppa=prices_oa_cr_ppa.assign(rw_id=[1 + i for i in xrange(len(prices_oa_cr_ppa))])[['rw_id'] + prices_oa_cr_ppa.columns.tolist()]
prices_oa_cr_ppa=prices_oa_cr_ppa[['rw_id', 'hedge_id', 'projet_id', 'projet', 'date', 'année', 'trimestre', 'mois', 'price']]

prices_oa_cr_ppa.to_excel(path_dir_in+'contracts_prices_oa_cr_ppa.xlsx', index=False)

#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, Column
from server_credentials import server_credentials
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, String, Date, Float, DECIMAL

def open_database():
    print('Connecting to SQL Server with ODBC driver')
    connection_string = 'DRIVER={SQL Server};SERVER='+server_credentials['server']+';DATABASE='+server_credentials['database']+';UID='+server_credentials['username']+';Trusted_Connection='+server_credentials['yes']
    cnxn = pyodbc.connect(connection_string)
    print('connected!')

    return cnxn

#windows authentication 
def mssql_engine(): 
    engine = create_engine('mssql+pyodbc://BLX186-SQ1PRO01/StarDust?driver=SQL+Server+Native+Client+11.0',
                           fast_executemany=True) 
    return engine

#Fix data type
prices_oa_cr_ppa['date']=pd.to_datetime(prices_oa_cr_ppa.date)
prices_oa_cr_ppa['date']=prices_oa_cr_ppa['date'].dt.date
#Fill N/A 
prices_oa_cr_ppa.fillna(prices_oa_cr_ppa.dtypes.replace({'float64': 0.0, 'object':'None'}), inplace=True)

#Insert hedge template data in DB in hedge table
table_name='contracts_prices'
prices_oa_cr_ppa.to_sql(table_name, 
                   con=mssql_engine(), 
                   index=False, 
                   if_exists='append',
                   schema='dbo',
                   chunksize=1000,
                   dtype={
                       'surr_id': Integer,
                       'hedge_id':Integer,
                       'projet_id':String(100),
                       'projet':String(250),
                       'dates':Date,
                       'année':Integer,
                       'trimestre':Integer,
                       'mois':Integer, 
                       'prix_contrat':DECIMAL(10, 3),
                       })

#To update new records into the destination table
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('contracts_prices', 
                           metadata,
                           Column('surr_id', Integer, nullable=False),
                           Column('hedge_id', Integer , nullable=False),
                           Column('projet_id', String(100), nullable=False),
                           Column('projet', String(250)),
                           Column('dates', Date),
                           Column('année', Integer),
                           Column('trimestre', String(50)),
                           Column('mois', Integer),
                           Column('prix_contrat', DECIMAL(10, 3), nullable=True)
                           )
session=sessionmaker(bind=mssql_engine())
session=session()
#Loop over the target df and update to update records
cnx=open_database()
cursor = cnx.cursor()
for ind, row in prices_oa_cr_ppa.iterrows():
    ins=sqlalchemy.sql.Insert(datatable).values({'surr_id':row.rw_id, 'hedge_id':row.hedge_id, 'projet_id':row.projet_id, 'projet':row.projet, 'dates':row.date,
                                                 'année':row.année, 'trimestre':row.trimestre, 'mois':row.mois, 'prix_contrat':row.price
                                                 })
    session.execute(ins)
session.flush()
session.commit()
cnx.close()