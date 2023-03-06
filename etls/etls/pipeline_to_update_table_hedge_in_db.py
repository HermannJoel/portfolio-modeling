# -*- coding: utf-8 -*-
"""
Created on Wed Sep 14 11:22:21 2022

@author: hermann.ngayap
"""

#==============This script is to update the hedge table in the DB==============

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
#====  update table hedge in the DB with data from template hedge      ========
#=======================        SCD type 2       ==============================
#==============================================================================
#Open SQL connection
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, Column
from server_credentials import server_credentials
from sqlalchemy.orm import sessionmaker
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

#To load the data from hedge template
df_hedge=pd.read_excel(path_dir_in+'template_hedge.xlsx')

#This is to transform only data of asset in planification.
#because the data of asset in productions are already in the DB
#============================================================
#===============     Hedge Planif     =======================
#============================================================
#To import only hedge data of asset in planification
df_hedge_planif=pd.read_excel(path_dir_temp+"template_hedge_planif.xlsx")

#To join original template_hedge data with df_hedge_planif data 
#because the data of asset in production are already in the DB
frames = [df_hedge, df_hedge_planif]
stream_src = pd.concat(frames)
stream_src.reset_index(inplace=True, drop=True)

stream_src.drop("rw_id", axis=1, inplace=True)
stream_src=stream_src.assign(rw_id=[1 + i for i in xrange(len(stream_src))])[['rw_id'] + stream_src.columns.tolist()]
stream_src=stream_src[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                      "date_debut", "date_fin", "date_dementelement", "puissance_installée", 
                      "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

#Fix data type
stream_src['date_debut']=pd.to_datetime(stream_src.date_debut)
stream_src['date_debut']=stream_src['date_debut'].dt.date

stream_src['date_fin']=pd.to_datetime(stream_src.date_fin)
stream_src['date_fin']=stream_src['date_fin'].dt.date

stream_src['date_dementelement']=pd.to_datetime(stream_src.date_dementelement)
stream_src['date_dementelement']=stream_src['date_dementelement'].dt.date

#rename columns
stream_src.columns=[str(col) + '_src' for col in stream_src.columns]

#Load data from destination stream hedge (hedge table)
cnx=open_database()
cursor = cnx.cursor()
stream_tbl=pd.read_sql_query('''
                            SELECT *
                            FROM hedge;
                            ''', cnx)
#Fix data type
stream_tbl['date_debut']=pd.to_datetime(stream_tbl.date_debut)
stream_tbl['date_debut']=stream_tbl['date_debut'].dt.date

stream_tbl['date_fin']=pd.to_datetime(stream_tbl.date_fin)
stream_tbl['date_fin']=stream_tbl['date_fin'].dt.date

stream_tbl['date_dementelement']=pd.to_datetime(stream_tbl.date_dementelement)
stream_tbl['date_dementelement']=stream_tbl['date_dementelement'].dt.date
#To rename columns label
stream_tbl.columns=[str(col) + '_tgt' for col in stream_tbl.columns]


#Left join source df and target df
stream_join=pd.merge(stream_src, stream_tbl, left_on=['projet_id_src', 'hedge_id_src'], 
                   right_on=['projet_id_tgt', 'hedge_id_tgt'], how='left')

#Fill N/A 
stream_join.fillna(stream_join.dtypes.replace({'float64': 0.0, 'object':'None'}), inplace=True)

#Identify new records and create a separate df
#i.e. columns hedge_id_tgt(indice 17) and projet_id_tgt (indice(18) with empty rows
#Flag these new records with I (Insert)  
stream_join['ins_flag']=stream_join.apply(lambda x:'I' if (pd.isnull(x[17]) and pd.isnull(x[18])) else 'N', axis=1)

#Create a df of records withe flag I.
ins_rec=stream_join.loc[stream_join['ins_flag']=='I']
ins_rec=ins_rec[['rw_id_src', 'hedge_id_src', 'projet_id_src', 
                 'projet_src', 'technologie_src', 'type_hedge_src', 
                 'date_debut_src', 'date_fin_src', 'puissance_installée_src', 
                 'date_dementelement_src', 'profil_src', 'pct_couverture_src', 
                 'contrepartie_src', 'pays_contrepartie_src', 'en_planif_src']]
#To rename columns by removing suffix "_src" of columns label
ins_rec.columns = ins_rec.columns.map(lambda x: x.removesuffix('_src')) 
ins_df=ins_rec[['rw_id', 'hedge_id', 'projet_id', 
                 'projet', 'technologie', 'type_hedge', 
                 'date_debut', 'date_fin', 'puissance_installée', 
                 'date_dementelement', 'profil', 'pct_couverture', 
                 'contrepartie', 'pays_contrepartie', 'en_planif']]
#flag=1 for 1st version
ins_df['version']=1

#Insert data in DB in hedge table
table_name='hedge'
ins_df.to_sql(     table_name, 
                   con=mssql_engine(), 
                   index=False, 
                   if_exists='append',
                   schema='dbo',
                   chunksize=1000,
                   dtype={
                       'rw_id': Integer,
                       'hedge_id':Integer,
                       'projet_id':String(100),
                       'projet':String(250),
                       'technologie':String(100),
                       'type_hedge':String(50),
                       'date_debut':Date,
                       'date_fin':Date, 
                       'date_dementelement':Date,
                       'puissance_installée':DECIMAL(10, 3),
                       'profil':String(100), 
                       'pct_couverture':DECIMAL(5, 2), 
                       'contrepartie':String(100), 
                       'pays_contrepartie':String(100),
                       'en_planif':String(50),
                       'version':Integer
                       })

#===================== Second method to load data  ============
#To insert template hedge into the destination table hedge
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('hedge', 
                           metadata,
                           Column('rw_id', Integer, nullable=False),
                           Column('hedge_id', Integer, nullable=False),
                           Column('projet_id', String(100), nullable=False, default=None),
                           Column('projet', String(250), default=None),
                           Column('technologie', String(100), default=None),
                           Column('type_hedge', String(100), nullable=True, default=None),
                           Column('date_debut', Date, nullable=True),
                           Column('data_fin', Date, nullable=True),
                           Column('puissance_installée', DECIMAL(10, 3), nullable=True),
                           Column('date_dementelement', Date, nullable=True),
                           Column('profil', String(100), nullable=True, default=None),
                           Column('pct_couverture', DECIMAL(5, 1), nullable=True),
                           Column('contrepartie', String(100), nullable=True, default=None),
                           Column('pays_contrepartie', String(100), nullable=True, Default=None),
                           Column('en_planif', String(50)),
                           Column('en_planif', Integer),
                           )
session=sessionmaker(bind=mssql_engine())
session=session()
#Loop over the target df and update to update records
for ind, row in ins_df.iterrows():
     ins=sqlalchemy.sql.insert(datatable).values({'rw_id':row.rw_id, 'hedge_id':row.hedge_id, 'projet_id':row.projet_id, 'projet':row.projet,'technologie':row.technologie,
                                                  'type_hedge':row.type_hedge, 'date_debut':row.date_debut, 'date_fin':row.date_fin, 'puissance_installée':row.puissance_installée,  
                                                  'date_dementelement':row.date_dementelement, 'profil':row.profil, 'pct_couverture':row.pct_couverture,  
                                                  'contrepartie':row.contrepartie, 'pays_contrepartie':row.pays_contrepartie, 'en_planif':row.en_planif, 'version':row.version 
                                                  }) 
     session.execute(ins)
session.flush()
session.commit()
#=============================================================
#Identify records where there is a change in data, update their previous version and 
#insert new records.
#To create an Update Flag
stream_join['ins_upd_flag']=stream_join.apply(lambda x:'UI' if (
    (x[1]==x[17] and x[2]==x[18]) and ((x[3]!=x[19]) or (x[4]!=x[20]) or (x[5]!=x[21]) 
                                       or (str(x[6])!=str(x[22])) or (str(x[7])!=str(x[23])) 
                                       or (str(x[8])!=str(x[24])) or (x[9]!=x[25]) 
                                       or (x[10]!=x[26]) or (x[11]!=x[27]) or (x[12]!=x[28]) 
                                       or (x[13]!=x[29]) or (x[14]!=x[30])
                                       )                                     
    ) else 'N', axis=1)

ins_upd_rec=stream_join.loc[stream_join['ins_upd_flag']=='UI']
ins_upd_rec=ins_upd_rec[['rw_id_src', 'hedge_id_src', 'projet_id_src', 
                         'projet_src', 'technologie_src', 'type_hedge_src', 
                         'date_debut_src', 'date_fin_src', 'puissance_installée_src', 
                         'date_dementelement_src', 'profil_src', 'pct_couverture_src', 
                         'contrepartie_src', 'pays_contrepartie_src', 'en_planif_src']]

#Flag record to insert and update with 1 as recent version
ins_upd_rec['version']=1
ins_upd_rec.columns = ins_upd_rec.columns.map(lambda x: x.removesuffix('_src')) 

#update the version of records in which data change is detected
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('hedge', 
                           metadata,
                          Column('version', Integer),
                          autoload=True
                           )
session=sessionmaker(bind=mssql_engine())
session=session()
#flag the updated records with 0 as previous version
for ind, row in ins_upd_rec.iterrows():
    upd=sqlalchemy.sql.update(datatable).values({'version':0}) \
        .where(sqlalchemy.and_(datatable.c.projet_id==row.projet_id and datatable.c.hedge_id==row.hedge_id))   
    session.execute(upd)
session.flush()
session.commit()

#Insert data in DB in hedge table
table_name='hedge'
ins_upd_rec.to_sql(table_name, 
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
                       'type_hedge':String(100),
                       'date_debut':Date,
                       'date_fin':Date, 
                       'date_dementelement':Date,
                       'puissance_installée':DECIMAL(10, 3),
                       'profil':String(100), 
                       'pct_couverture':DECIMAL(5, 2), 
                       'contrepartie':String(100), 
                       'pays_contrepartie':String(100),
                       'en_planif':String(50),
                       'version':Integer
                       })


