# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 11:24:42 2022

@author: hermann.ngayap
"""
#==============This script is to load the template asset in the DB in asset table(first time load)
import pandas as pd
import numpy as np
xrange = range
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
import os
cwd=os.getcwd()
ncwd='D:/blx_mdp/cwd/'
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

path_dir_in='D:/blx_mdp/cwd/in/'
path_dir_temp='D:/blx_mdp/cwd/temp/'

#==============================================================================
#==========      Load template asset into asset table      ====================
#==========                 SCD type 1                     ====================
#==============================================================================

#==============================================================================
#========== To join annual volume to temporary asset template  ================
#==============================================================================
# =============================================================================
# #Load temporary asset template
# asset_vmr_planif=pd.read_excel(path_dir_temp+"asset_vmr_planif.xlsx")
# #To load template prod and select only projet_id, p50 and p90
# df_prod=pd.read_excel(path_dir_in+"template_prod.xlsx")
# prod_id=df_prod.iloc[:,np.r_[0, 2, 3]]
# #To join annual production to the temprary asset template 
# asset_template=pd.merge(asset_vmr_planif, prod_id, how="left", on=['projet_id'])
# =============================================================================

#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import psycopg2
from psycopg2._psycopg import connection
import sqlalchemy
from sqlalchemy import create_engine, Column
from server_credentials import server_credentials
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, String, Date, Float, Numeric, DECIMAL

def open_database():
    print('Connecting to Postgres')
    connection_string = "host='localhost' dbname='blxmdpdwdev' user='postgres' password='24Fe1988'"
    cnxn = psycopg2.connect(connection_string)
    print("Connecting to database\n	->%s" % connection_string)
    cursor = cnxn.cursor()
    print( "Connected!\n")
    return cnxn

#windows authentication 
def postgressql_engine(): 
    engine = create_engine("postgresql+psycopg2://postgres:24Fe1988@localhost:5432/blxmdpdwdev") 
    return engine


import sys
def main():
	#Define our connection string
	conn_string = "host='localhost' dbname='blxmdpdwdev' user='postgres' password='24Fe1988'"

	# print the connection string we will use to connect
	print("Connecting to database\n	->%s" % conn_string)

	# get a connection, if a connect cannot be made an exception will be raised here
	cnx = psycopg2.connect(conn_string)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = cnx.cursor()
	print( "Connected!\n")

if __name__ == "__main__":
	main()

#To import asset template
asset_template=pd.read_excel(path_dir_in+'template_asset.xlsx')

#To import asset template
asset_template=asset_template.iloc[:,1:]
#Fix data type
#asset_template['rw_id']=asset_template['rw_id'].astype(pd.Int64Dtype())
asset_template['asset_id']=asset_template['asset_id'].astype(pd.Int64Dtype())

asset_template['cod']=pd.to_datetime(asset_template.cod)
asset_template['cod']=asset_template['cod'].dt.date

asset_template['date_merchant']=pd.to_datetime(asset_template.date_merchant)
asset_template['date_merchant']=asset_template['date_merchant'].dt.date

asset_template['date_dementelement']=pd.to_datetime(asset_template.date_dementelement)
asset_template['date_dementelement']=asset_template['date_dementelement'].dt.date

asset_template['date_msi']=pd.to_datetime(asset_template.date_msi)
asset_template['date_msi']=asset_template['date_msi'].dt.date

#Fill N/A
asset_template.fillna(asset_template.dtypes.replace({'float64': 0.0, 'object': 'None', 'Int64 ':0}), inplace=True)

#Insert template asset into asset table in the DB
cnx=open_database()
cursor = cnxn.cursor()
table_name='asset'
asset_template.to_sql(table_name, 
                   con=open_database(), 
                   index=False, 
                   if_exists='append',
                   schema='dash',
                   chunksize=1000,
                   dtype={
                       'AssetId':Integer,
                       'ProjectId':String(100),
                       'Project':String(250),
                       'Technology':String(200),
                       'Cod':Date,
                       'Mw':DECIMAL(5, 2),
                       'SuccessPct':DECIMAL(7, 5), 
                       'InstalledPower':DECIMAL(7, 5),
                       'Eoh':DECIMAL(10, 3),
                       'MerchantDate':Date, 
                       'DismentleDate':Date, 
                       'Repowering':String(100), 
                       'MsiDate':Date,
                       'InPlanif':String(50),
                       'P50':DECIMAL(10, 3),
                       'P90':DECIMAL(10, 3)
                       })


#To update new records into the destination table
metadata=sqlalchemy.MetaData(bind=open_database(), schema='dash')
datatable=sqlalchemy.Table('asset',
                           metadata,
                           Column('AssetId', Integer),
                           Column('ProjectId', String(100)),
                           Column('Project', String(250)),
                           Column('Technology', String(200)),
                           Column('Cod', Date),
                           Column('Mw', DECIMAL(5, 2)),
                           Column('SuccessPct', DECIMAL(7, 5)),
                           Column('InstalledPower', DECIMAL(7, 5)),
                           Column('Eoh', Numeric(10, 3)),
                           Column('MerchantDate', Date),
                           Column('DismentleDate', Date),
                           Column('Repowering', String(50)),
                           Column('MsiDate', Date,),
                           Column('InPlanif', String(50)),
                           Column('P50', DECIMAL(10, 3)),
                           Column('P90', DECIMAL(10, 3)),
                           extend_existing=True
                           )
session=sessionmaker(bind=open_database())
session=session()
#Loop over the target df and update to update records
#cnx=open_database()
#cursor = cnxn.cursor()
for ind, row in asset_template.iterrows():
    ins=sqlalchemy.sql.Insert(datatable).values({'AssetId':row.asset_id, 'ProjectId':row.projet_id, 'Project':row.projet, 'Technology':row.technologie,
                                                 'Cod':row.cod, 'Mw':row.mw, 'SuccessPct':row.taux_succès, 'InstalledPower':row.puissance_installée,
                                                 'Eoh':row.eoh, 'MerchantDate':row.date_merchant, 'DismenteleDate':row.date_dementelement,
                                                 'Repowering':row.repowering, 'MsiDate':row.date_msi, 'InPlanif':row.en_planif, 'P50':row.p50, 
                                                 'P90':row.p90})  
    session.execute(ins)
session.flush()
session.commit()
cnx.close()

import psycopg2, pprint
pprint.pprint(dir(psycopg2))