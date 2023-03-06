# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 13:56:06 2022

@author: hermann.ngayap
"""
#==================This sript is to update the table asset with the source stream from asset_template 
import pandas as pd
xrange = range
import numpy as np
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
import os
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

#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, Column 
from sqlalchemy.orm import sessionmaker
from server_credentials import server_credentials
from sqlalchemy.types import Integer, String, Date, Float, Numeric, DECIMAL
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

#==============================================================================
#==========   Update asset table with data from template asset ================
#==========                 SCD type 1                     ====================
#==============================================================================
#To import original asset template 
df_asset=pd.read_excel(path_dir_in+"template_asset_test.xlsx")

#This is to transform only data of asset in planification.
#because the data of asset in productions are already in the DB
#============================================================
#===============     Asset Planif     =======================
#============================================================
#To import only asset data of assets in planification
df_asset_planif=pd.read_excel(path_dir_temp+"template_asset_planif_test.xlsx")

#To join original template_asset data with df_asset_planif data 
frames = [df_asset, df_asset_planif]
stream_src = pd.concat(frames)
stream_src.reset_index(inplace=True, drop=True)

#Fix data type
stream_src['rw_id']=stream_src['rw_id'].astype(pd.Int64Dtype())
stream_src['asset_id']=stream_src['asset_id'].astype(pd.Int64Dtype())
stream_src['cod']=pd.to_datetime(stream_src.cod)
stream_src['cod']=stream_src['cod'].dt.date

stream_src['date_merchant']=pd.to_datetime(stream_src.date_merchant)
stream_src['date_merchant']=stream_src['date_merchant'].dt.date

stream_src['date_dementelement']=pd.to_datetime(stream_src.date_dementelement)
stream_src['date_dementelement']=stream_src['date_dementelement'].dt.date

stream_src['date_msi']=pd.to_datetime(stream_src.date_msi)
stream_src['date_msi']=stream_src['date_msi'].dt.date

#rename columns stream source columns and add sufix '_src'
stream_src.columns=[str(col) + '_src' for col in stream_src.columns]

#Load destination stream data from asset table
cnx=open_database()
cursor = cnx.cursor()
stream_tbl=pd.read_sql_query('''
                            SELECT *
                            FROM asset;
                            ''', cnx)

#Fix data type
stream_tbl['rw_id']=stream_tbl['rw_id'].astype(pd.Int64Dtype())
stream_tbl['asset_id']=stream_tbl['asset_id'].astype(pd.Int64Dtype())
stream_tbl['cod']=pd.to_datetime(stream_tbl.cod)
stream_tbl['cod']=stream_tbl['cod'].dt.date

stream_tbl['date_merchant']=pd.to_datetime(stream_tbl.date_merchant)
stream_tbl['date_merchant']=stream_tbl['date_merchant'].dt.date

stream_tbl['date_dementelement']=pd.to_datetime(stream_tbl.date_dementelement)
stream_tbl['date_dementelement']=stream_tbl['date_dementelement'].dt.date

stream_tbl['date_msi']=pd.to_datetime(stream_tbl.date_msi)
stream_tbl['date_msi']=stream_tbl['date_msi'].dt.date

#Rename target df
stream_tbl.columns=[str(col) + '_tgt' for col in stream_tbl.columns]

#Merge source and target df
stream_join=pd.merge(stream_src, stream_tbl, left_on='projet_id_src', 
                   right_on='projet_id_tgt', how='left')

#To fill NA with nan otherwise will return 'TypeError: boolean value of NA is ambiguous'
stream_join=stream_join.fillna(value=np.nan)

#To create an Insert flag
#When the fields asset_id (indice 17) and projet_id (indice 18) in the source stream are not present in the destination stream, flag the record with "I" (Insert).
#Compare the fields projet_id and asset_id from source and destination streams. when the fields are
#equal, flag the records with "N" (Not Insert) 
stream_join['ins_flag']=stream_join.apply(lambda x:'I' if (pd.isnull(x[18]) and pd.isnull(x[19])) else 'N', axis=1)
#stream_join['ins_flag']=stream_join.apply(lambda x:'I' if (x[1]!=x[18] and x[2]!=x[19]) else 'N', axis=1)

#To fill nan with 0(without this, will return an error: "TypeError: boolean value of NA is ambiguous")
stream_join=stream_join.fillna(value=0)

#To create an Update Flag
#Compare the fields projet_id and asset_id from source and destination streams. when the fields are
#equal and the value of at least one fields of the source stream is not equal to the value of the same field in 
#the destination stream, flag the record with "U" (Update).
#Likewise.
#When the fields projet_id and asset_id from source and destination streams are equal 
#and all the values of other fields in the two streams are also equal, flag the records with "N"(Not Update).  
stream_join['upd_flag']=stream_join.apply(lambda x:'U' if (
    (x[1]==x[18] and x[2]==x[19]) and ((x[3]!=x[20]) or (x[4]!=x[21]) or (str(x[5])!=str(x[22])) 
                                       or (x[6]!=x[23]) or (x[7]!=x[24]) or (x[8]!=x[25]) 
                                       or (x[9]!=x[26]) or (str(x[10])!=str(x[27]))
                                       or (str(x[11])!=str(x[28])) or (x[12]!=x[29]) or (str(x[13])!=str(x[30]))
                                       or (x[14]!=x[31]) or (x[15]!=x[32]) or (x[16]!=x[33])
                                       )                                     
    ) else 'N', axis=1)



#Prepare and insert updated df
#Selet only records with the flag 'I'
ins_rec=stream_join.loc[stream_join['ins_flag']=='I']
ins_upd=ins_rec[['rw_id_src', 'asset_id_src', 'projet_id_src', 'projet_src',
                 'technologie_src', 'cod_src', 'mw_src', 'taux_succès_src',
                 'puissance_installée_src', 'eoh_src', 'date_merchant_src',
                 'date_dementelement_src', 'repowering_src', 'date_msi_src',
                 'en_planif_src', 'p50_src', 'p90_src']]

#To rename columns by removing suffix "_src" from columns label
ins_upd.columns = ins_upd.columns.map(lambda x: x.removesuffix('_src'))

#Prepare df for updatd records
#Selet only records with the flag 'U'
upd_rec=stream_join.loc[stream_join['upd_flag']=='U']
upd_df=upd_rec[['rw_id_src', 'asset_id_src', 'projet_id_src', 'projet_src',
                 'technologie_src', 'cod_src', 'mw_src', 'taux_succès_src',
                 'puissance_installée_src', 'eoh_src', 'date_merchant_src',
                 'date_dementelement_src', 'repowering_src', 'date_msi_src',
                 'en_planif_src', 'p50_src', 'p90_src']]
#To rename columns
upd_df.columns = upd_df.columns.map(lambda x: x.removesuffix('_src'))

#Insert new records in to destination table asset in DB
table_name='asset'
ins_upd.to_sql(table_name, 
                   con=mssql_engine(), 
                   index=False, 
                   if_exists='append',
                   schema='dbo',
                   chunksize=1000,
                   dtype={
                       'rw_id': Integer,
                       'asset_id':Integer,
                       'projet_id':String(100),
                       'projet':String(250),
                       'technologie':String(100),
                       'cod':Date,
                       'mw':DECIMAL(10, 5),
                       'taux_succès':DECIMAL(5, 2), 
                       'puissance_installée':DECIMAL(10, 5),
                       'eoh':DECIMAL(10, 5),
                       'date_merchant':Date, 
                       'date_dementelement':Date, 
                       'repowering':String(100), 
                       'date_msi':Date,
                       'en_planif':String(50),
                       'p50':DECIMAL(10, 5),
                       'p90':DECIMAL(10, 5)
                       })

#To update new records into the destination table
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('asset', 
                           metadata,
                           Column('rw_id', Integer, nullable=False),
                           Column('asset_id', Integer, nullable=False),
                           Column('projet_id', String(100), nullable=False),
                           Column('projet', String(250)),
                           Column('technologie', String(100)),
                           Column('cod', Date),
                           Column('mw', DECIMAL(10, 5), nullable=True),
                           Column('taux_succès', DECIMAL(5, 2), nullable=True),
                           Column('puissance_installée', Numeric(10, 3), nullable=True),
                           Column('eoh', DECIMAL(10, 5), nullable=True),
                           Column('date_merchant', Date, nullable=True),
                           Column('date_dementelement', Date, nullable=True),
                           Column('repowering', String(50), nullable=True),
                           Column('date_msi', Date, nullable=True),
                           Column('en_planif', String(50)),
                           Column('p50', DECIMAL(10, 5), nullable=True),
                           Column('p90', DECIMAL(10, 5), nullable=True),
                           )
session=sessionmaker(bind=mssql_engine())
session=session()
#Loop over the target df and update to update records
cnx=open_database()
cursor = cnx.cursor()
for ind, row in upd_df.iterrows():
    upd=sqlalchemy.sql.update(datatable).values({'rw_id':row.rw_id, 'asset_id':row.asset_id, 'projet_id':row.projet_id, 'projet':row.projet, 'technologie':row.technologie,
                                                 'cod':row.cod, 'mw':row.mw, 'taux_succès':row.taux_succès, 'puissance_installée':row.puissance_installée,
                                                 'eoh':row.eoh, 'date_merchant':row.date_merchant, 'date_dementelement':row.date_dementelement,
                                                 'repowering':row.repowering, 'date_msi':row.date_msi, 'en_planif':row.en_planif, 'p50':row.p50, 
                                                 'p90':row.p90}) \
        .where(sqlalchemy.and_(datatable.c.projet_id==row.projet_id and datatable.c.asset_id==row.asset_id))   
    session.execute(upd)
session.flush()
session.commit()
cnx.close()


