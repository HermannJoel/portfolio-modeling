# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 12:25:32 2022

@author: hermann.ngayap
"""
#==============This script is to load the template hedge in the DB in hedge table(first time load)

import pandas as pd
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

#==============================================================================
#============    Load template hedge into hedge table      ====================
#=======================        SCD type 2       ==============================
#==============================================================================
#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, Column
from sqlalchemy.types import Integer, String, Date, Float, DECIMAL
from server_credentials import server_credentials
from sqlalchemy.orm import sessionmaker

def open_database():
    print('Connecting to SQL Server with ODBC driver')
    connection_string = 'DRIVER={SQL Server};SERVER='+server_credentials['server']+';DATABASE='+server_credentials['database']+';UID='+server_credentials['uid']+';Trusted_Connection='+server_credentials['yes']
    cnxn = pyodbc.connect(connection_string)
    print('connected!')

    return cnxn

#windows authentication 
def mssql_engine(): 
    engine = create_engine('mssql+pyodbc://BLX186-SQ1PRO01/StarDust?driver=SQL+Server+Native+Client+11.0',
                           fast_executemany=True) 
    return engine

# =============================================================================
# #import temporary hedge data
# #data were added manualy and the resulting file is saved as template_hedge
# hedge_template=pd.read_excel(path_dir_temp+'hedge_vmr_planif.xlsx')
# #Fix data type
# hedge_template['date_debut']=pd.to_datetime(hedge_template.date_debut)
# hedge_template['date_debut']=hedge_template['date_debut'].dt.date
# 
# hedge_template['date_fin']=pd.to_datetime(hedge_template.date_fin)
# hedge_template['date_fin']=hedge_template['date_fin'].dt.date
# 
# hedge_template['date_dementelement']=pd.to_datetime(hedge_template.date_dementelement)
# hedge_template['date_dementelement']=hedge_template['date_dementelement'].dt.date
# #Export the temporary hedge template as excel file
# #hedge_template.to_excel(path_dir_in+"template_hedge.xlsx", index=False)
# =============================================================================

#To import tamplate hedge as source stream. 
flux_source=pd.read_excel(path_dir_in+'template_hedge.xlsx')

#Fix data type
flux_source['date_debut']=pd.to_datetime(flux_source.date_debut)
#flux_source['date_debut']=flux_source['date_debut'].dt.date

flux_source['date_fin']=pd.to_datetime(flux_source.date_fin)
#flux_source['date_fin']=flux_source['date_fin'].dt.date

flux_source['date_dementelement']=pd.to_datetime(flux_source.date_dementelement)
#flux_source['date_dementelement']=flux_source['date_dementelement'].dt.date

#for the the fist time loading, versionning is 1
flux_source['version']=1
#Fill N/A 
flux_source.fillna(flux_source.dtypes.replace({'float64': 0.0, 'datetime64[ns]':'1901-01-01', 'object': 'N/A'}), inplace=True)


#Insert hedge template data in DB in hedge table (1st load)
table_name='hedge_scd2'
flux_source.to_sql(table_name, 
                   con=mssql_engine(), 
                   index=False, 
                   if_exists='append',
                   schema='dbo',
                   chunksize=1000,
                   dtype={
                       'rw_id': Integer,
                       'hedge_id':Integer,
                       'projet_id':String(50),
                       'projet':String(250),
                       'technologie':String(100),
                       'type_hedge':String(50),
                       'date_debut':Date,
                       'date_fin':Date, 
                       'date_dementelement':Date,
                       'puissance_installée':DECIMAL(10, 5),
                       'profil':String(100), 
                       'pct_couverture':DECIMAL(5, 2), 
                       'contrepartie':String(100), 
                       'pays_contrepartie':String(100),
                       'en_planif':String(50),
                       'version':Integer
                       })


#Second method to insert template hedge data in the table hedge
# =============================================================================
#To insert template hedge into the destination table hedge
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('hedge_scd2', 
                           metadata,
                           Column('rw_id', Integer, nullable=False),
                           Column('hedge_id', Integer, nullable=False),
                           Column('projet_id', String(100), nullable=False),
                           Column('projet', String(250)),
                           Column('technologie', String(100)),
                           Column('type_hedge', String(100), nullable=True),
                           Column('date_debut', Date, nullable=True),
                           Column('date_fin', Date, nullable=True),
                           Column('puissance_installée', DECIMAL(10, 5), nullable=True),
                           Column('date_dementelement', Date, nullable=True),
                           Column('profil', String(100), nullable=True),
                           Column('pct_couverture', DECIMAL(5, 2), nullable=True),
                           Column('contrepartie', String(100), nullable=True),
                           Column('pays_contrepartie', String(100), nullable=True),
                           Column('en_planif', String(50)),
                           Column('version', Integer)
                           )
session=sessionmaker(bind=mssql_engine())
session=session()
#Loop over the target df and update to update records
cnx=open_database()
cursor = cnx.cursor()
for ind, row in flux_source.iterrows():
     ins=sqlalchemy.sql.Insert(datatable).values({'rw_id':row.rw_id, 'hedge_id':row.hedge_id, 'projet_id':row.projet_id, 'projet':row.projet,'technologie':row.technologie,
                                                  'type_hedge':row.type_hedge, 'date_debut':row.date_debut, 'date_fin':row.date_fin, 'puissance_installée':row.puissance_installée,
                                                  'date_dementelement':row.date_dementelement, 'profil':row.profil, 'pct_couverture':row.pct_couverture, 'contrepartie':row.contrepartie,  
                                                  'pays_contrepartie':row.pays_contrepartie, 'en_planif':row.en_planif, 'version':row.version 
                                                  })
     session.execute(ins)
session.flush()
session.commit()
cnx.close()
# =============================================================================
