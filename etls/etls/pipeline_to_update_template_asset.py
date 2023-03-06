# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 13:56:06 2022

@author: hermann.ngayap
"""
#==================This sript is to update the table asset with the source stream from asset_template 

import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)

import os
print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/hjBoralex/etl/gitcwd/blx_mdp_front-end/etls/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'


#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, event, update
from sqlalchemy.orm import sessionmaker
from server_credentials import server_credentials



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
#import source data stream
stream_src=pd.read_excel(path_dir_in+"template_asset.xlsx")

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

#rename columns
stream_src.rename(columns={'rw_id':'rw_id_src', 'asset_id':'asset_id_src', 'projet_id':'projet_id_src', 
                          'projet':'projet_src', 'technologie':'technologie_src', 'cod':'cod_src', 'mw':"mw_src", 
                          'taux_succès':'taux_succès_src', 'puissance_installée':'puissance_installée_src', 'eoh':'eoh_src',
                          'date_merchant':'date_merchant_src', 'date_dementelement':'date_dementelement_src', 'repowering':'repowering_src', 
                          'date_msi':'date_msi_src', 'en_planif':'en_planif_src', 'p50':'p50_src', 'p90':'p90_src'},  inplace=True)

#Load destination stream from asset table
cnx=open_database()
cursor = cnx.cursor()
stream_tbl=pd.read_sql_query('''
                            SELECT *
                            FROM asset_scd1;
                            ''', cnx)
#Rename target df
stream_tbl.rename(columns={'rw_id':'rw_id_tgt', 'asset_id':'asset_id_tgt', 'projet_id':'projet_id_tgt', 
                          'projet':'projet_tgt', 'technologie':'technologie_tgt', 'cod':'cod_tgt', 'mw':"mw_tgt", 
                          'taux_succès':'taux_succès_tgt', 'puissance_installée':'puissance_installée_tgt', 'eoh':'eoh_tgt',
                          'date_merchant':'date_merchant_tgt', 'date_dementelement':'date_dementelement_tgt', 'repowering':'repowering_tgt', 
                          'date_msi':'date_msi_tgt', 'en_planif':'en_planif_tgt', 'p50':'p50_tgt', 'p90':'p90_tgt'},  inplace=True)

#Merge source anr target df
stream_join=pd.merge(stream_src, stream_tbl, left_on='projet_id_src', 
                   right_on='projet_id_tgt', how='left')

#To create an Insert flag
#When the field in the source stream are not present in the destination stream, flag the record with "I" (Insert)
#Compare the fields projet_id and asset_id from source and destination streams. when the field are
#identical, flag the records with "N" (Not Insert) 
stream_join['ins_flag']=stream_join.apply(lambda x:'I' if (pd.isnull(x[18]) and pd.isnull(x[19])) else 'N', axis=1)

#To create an Update Flag
#Compare the fields projet_id and asset_id from source and destination streams. when the field are
#identical and the values of at least one other field does is not identical to values the same field in 
#the other stream, flag the record with "U" (Update).

#When the fields projet_id and asset_id from source and destination streams are identical 
#as well as all values in the other fields of the streams, flag the records with "N" (Not Update).  
stream_join['upd_flag']=stream_join.apply(lambda x:'U' if (
    (x[1]==x[18] and x[2]==x[19]) and ((x[3]!=x[20]) or (x[4]!=x[21]) or (str(x[5])!=str(x[22])) 
                                       or (x[6]!=x[23]) or (x[7]!=x[24]) or (x[8]!=x[25]) 
                                       or (x[9]!=x[26]) or (str(x[10])!=str(x[27]))
                                       or (str(x[11])!=str(x[28])) or (x[12]!=x[29]) or (str(x[13])!=str(x[30]))
                                       or (x[14]!=x[31]) or (x[15]!=x[32]) or (x[16]!=x[33])
                                       )                                     
    ) else 'N', axis=1)

#Prepare and insert updated df
ins_rec=stream_join.loc[stream_join['ins_flag']=='I']
ins_upd=ins_rec[['rw_id_src', 'asset_id_src', 'projet_id_src', 'projet_src',
                 'technologie_src', 'cod_src', 'mw_src', 'taux_succès_src',
                 'puissance_installée_src', 'eoh_src', 'date_merchant_src',
                 'date_dementelement_src', 'repowering_src', 'date_msi_src',
                 'en_planif_src', 'p50_src', 'p90_src']]
#To rename columns
ins_upd.rename(columns={'rw_id_src':'rw_id', 'asset_id_src':'asset_id', 'projet_id_src':'projet_id', 
                          'projet_src':'projet', 'technologie_src':'technologie', 'cod_src':'cod', 
                          'mw_src':'mw', 'taux_succès_src':'taux_succès', 'puissance_installée_src':'puissance_installée',
                          'eoh_src':'eoh', 'date_merchant_src':'date_merchant', 'date_dementelement_src':'date_dementelement', 
                          'repowering_src':'repowering', 'date_msi_src':'date_msi', 'en_planif_src':'en_planif', 
                          'p50_src':'p50', 'p90_src':'p90'
                          }, inplace=True)


#Prepare df for updatd records
upd_rec=stream_join.loc[stream_join['upd_flag']=='U']
upd_df=upd_rec[['rw_id_src', 'asset_id_src', 'projet_id_src', 'projet_src',
                 'technologie_src', 'cod_src', 'mw_src', 'taux_succès_src',
                 'puissance_installée_src', 'eoh_src', 'date_merchant_src',
                 'date_dementelement_src', 'repowering_src', 'date_msi_src',
                 'en_planif_src', 'p50_src', 'p90_src']]
#To rename columns
upd_df.rename(columns={'rw_id_src':'rw_id', 'asset_id_src':'asset_id', 'projet_id_src':'projet_id', 
                          'projet_src':'projet', 'technologie_src':'technologie', 'cod_src':'cod', 
                          'mw_src':'mw', 'taux_succès_src':'taux_succès', 'puissance_installée_src':'puissance_installée',
                          'eoh_src':'eoh', 'date_merchant_src':'date_merchant', 'date_dementelement_src':'date_dementelement', 
                          'repowering_src':'repowering', 'date_msi_src':'date_msi', 'en_planif_src':'en_planif', 
                          'p50_src':'p50', 'p90_src':'p90'
                          }, inplace=True)

#Insert new records in to destination table asset in DB
table_name='asset'
ins_upd.to_sql(table_name, con=mssql_engine(), index=False, if_exists='append')


#To update new records into the destination table
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('asset', metadata, autoload=True)
session=sessionmaker(bind=mssql_engine())
session=session()
#Loop over the target df and update to update records
cnx=open_database()
cursor = cnx.cursor()
for ind, row in upd_df.iterrows():
    upd=sqlalchemy.sql.update(datatable).values({'rw_id':row.rw_id, 'projet':row.projet, 'technologie':row.technologie,
                                                 'cod':row.cod, 'mw':row.mw, 'taux_succès':row.taux_succès, 'puissance_installée':row.puissance_installée,
                                                 'eoh':row.eoh, 'date_merchant':row.date_merchant, 'date_dementelement':row.date_dementelement,
                                                 'repowering':row.repowering, 'date_msi':row.date_msi, 'en_planif':row.en_planif, 'p50':row.p50, 
                                                 'p90':row.p90}) \
        .where(sqlalchemy.and_(datatable.c.projet_id==row.projet_id and datatable.c.asset_id==row.asset_id))   
    # specify that parameters are DECIMAL(10,3) columns
    cursor.setinputsizes([(pyodbc.SQL_DECIMAL, 10, 3)])
    session.execute(upd)
session.flush()
session.commit()
cnx.close()


