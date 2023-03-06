# -*- coding: utf-8 -*-
"""
Created on Wed Sep 14 11:22:21 2022

@author: hermann.ngayap
"""

#==============This script is to update the hedge table in the DB

import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import os

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#==============================================================================
#====       update template hedge with date from template hedge      ==========
#=======================        SCD type 2       ==============================
#==============================================================================
#Open SQL connection to fetch monthly prices data derrived from price curve
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, event
from server_credentials import server_credentials
from sqlalchemy.orm import sessionmaker
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

#To load from the source stream
stream_src=pd.read_excel(path_dir_in+'template_hedge.xlsx')

#This is to transform only data of asset in planification.
#because the data of asset in productions are already in the DB
#============================================================
#===============     Hedge Planif     =======================
#============================================================
df_hedge_planif=pd.read_excel(path_dir_temp+"hedge_planif.xlsx")
df_hedge_planif["type_hedge"] = "CR"
df_hedge_planif["profil"] = np.nan
df_hedge_planif["pct_couverture"] = np.nan
df_hedge_planif["contrepartie"] = np.nan
df_hedge_planif["pays_contrepartie"] = np.nan

df_hedge_planif = df_hedge_planif[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                                   "cod", "date_merchant", "date_dementelement", "puissance_installée", 
                                   "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

df_hedge_planif.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)

ppa_planif = ["SE19", "SE07"]
df_hedge_planif.loc[df_hedge_planif.projet_id.isin(ppa_planif) == True, "type_hedge"] = "PPA"
df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "CR", "pct_couverture"] = 1
df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "PPA", "pct_couverture"] = 1

df_hedge_planif.to_excel(path_dir_temp+"template_hedge_planif.xlsx", index=False, float_format="%.3f")

#To merge both data frame
frames = [df_hedge_planif]
hedge_template = pd.concat(frames)
hedge_template.reset_index(inplace=True, drop=True)

hedge_template.drop("rw_id", axis=1, inplace=True)
hedge_template=hedge_template.assign(rw_id=[1 + i for i in xrange(len(hedge_template))])[['rw_id'] + hedge_template.columns.tolist()]
hedge_template=hedge_template[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                     "date_debut", "date_fin", "date_dementelement", "puissance_installée", 
                     "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

#Export temporary hedge template as excel file
hedge_template.to_excel(path_dir_temp+"hedge_planif.xlsx", index=False, float_format="%.3f")



#Fix data type
stream_src['date_debut']=pd.to_datetime(stream_src.date_debut)
stream_src['date_debut']=stream_src['date_debut'].dt.date

stream_src['date_fin']=pd.to_datetime(stream_src.date_fin)
stream_src['date_fin']=stream_src['date_fin'].dt.date

stream_src['date_dementelement']=pd.to_datetime(stream_src.date_dementelement)
stream_src['date_dementelement']=stream_src['date_dementelement'].dt.date

#rename columns
stream_src.rename(columns={'rw_id':'rw_id_src', 'hedge_id':'hedge_id_src', 'projet_id':'projet_id_src', 
                          'projet':'projet_src', 'technologie':'technologie_src', 'type_hedge':'type_hedge_src', 
                          'date_debut':"date_debut_src", 'date_fin':'date_fin_src', 'puissance_installée':'puissance_installée_src', 
                          'date_dementelement':'date_dementelement_src', 'profil':'profil_src', 'pct_couverture':'pct_couverture_src', 
                          'contrepartie':'contrepartie_src', 'pays_contrepartie':'pays_contrepartie_src', 'en_planif':'en_planif_src'},  inplace=True)




#Load data from destination stream hedge (hedge table)
cnx=open_database()
cursor = cnx.cursor()
stream_tbl=pd.read_sql_query('''
                            SELECT *
                            FROM hedge_scd2;
                            ''', cnx)

#Fix data type
stream_tbl['date_debut']=pd.to_datetime(stream_tbl.date_debut)
stream_tbl['date_debut']=stream_tbl['date_debut'].dt.date

stream_tbl['date_fin']=pd.to_datetime(stream_tbl.date_fin)
stream_tbl['date_fin']=stream_tbl['date_fin'].dt.date

stream_tbl['date_dementelement']=pd.to_datetime(stream_tbl.date_dementelement)
stream_tbl['date_dementelement']=stream_tbl['date_dementelement'].dt.date

stream_tbl.rename(columns={'surr_id':'surr_tgt','rw_id':'rw_id_tgt', 'hedge_id':'hedge_id_tgt', 'projet_id':'projet_id_tgt', 
                          'projet':'projet_tgt', 'technologie':'technologie_tgt', 'type_hedge':'type_hedge_tgt', 
                          'date_debut':"date_debut_tgt", 'date_fin':'date_fin_tgt', 'puissance_installée':'puissance_installée_tgt', 
                          'date_dementelement':'date_dementelement_tgt', 'profil':'profil_tgt', 'pct_couverture':'pct_couverture_tgt', 
                          'contrepartie':'contrepartie_tgt', 'pays_contrepartie':'pays_contrepartie_tgt', 'en_planif':'en_planif_tgt'},  inplace=True)

#Left join source and target df
stream_join=pd.merge(stream_src, stream_tbl, left_on=['projet_id_src', 'hedge_id_src'], 
                   right_on=['projet_id_tgt', 'hedge_id_tgt'], how='left')

#Identify new records and create a separate df
stream_join['ins_flag']=stream_join.apply(lambda x:'I' if (pd.isnull(x[17]) and pd.isnull(x[18])) else 'N', axis=1)

#
ins_rec=stream_join.loc[stream_join['ins_flag']=='I']
ins_rec=ins_rec[['rw_id_src', 'hedge_id_src', 'projet_id_src', 
                 'projet_src', 'technologie_src', 'type_hedge_src', 
                 'date_debut_src', 'date_fin_src', 'puissance_installée_src', 
                 'date_dementelement_src', 'profil_src', 'pct_couverture_src', 
                 'contrepartie_src', 'pays_contrepartie_src', 'en_planif_src']]

ins_rec.rename(columns={'rw_id_src':'rw_id', 'hedge_id_src':'hedge_id', 'projet_id_src':'projet_id', 
                        'projet_src':'projet', 'technologie_src':'technologie', 'type_hedge_src':'type_hedge', 
                        'date_debut_src':'date_debut', 'date_fin_src':'date_fin', 'puissance_installée_src':'puissance_installée', 
                        'date_dementelement_src':'date_dementelement', 'profil_src':'profil', 'pct_couverture_src':'pct_couverture', 
                        'contrepartie_src':'contrepartie', 'pays_contrepartie_src':'pays_contrepartie', 'en_planif_src':'en_planif'}, inplace=True)

ins_df=ins_rec[['rw_id', 'hedge_id', 'projet_id', 
                 'projet', 'technologie', 'type_hedge', 
                 'date_debut', 'date_fin', 'puissance_installée', 
                 'date_dementelement', 'profil', 'pct_couverture', 
                 'contrepartie', 'pays_contrepartie', 'en_planif']]
#flag=1 for 1st version
ins_df['version']=1

#Insert data in DB in hedge table
table_name='hedge_scd2'
ins_df.to_sql(table_name, con=mssql_engine(), index=False, if_exists='append')

#Identify recors where there is a change in data, update their previous version and 
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
ins_upd_rec.rename(columns={'rw_id_src':'rw_id', 'hedge_id_src':'hedge_id', 'projet_id_src':'projet_id', 
                            'projet_src':'projet', 'technologie_src':'technologie', 'type_hedge_src':'type_hedge', 
                            'date_debut_src':'date_debut', 'date_fin_src':'date_fin', 'puissance_installée_src':'puissance_installée', 
                            'date_dementelement_src':'date_dementelement', 'profil_src':'profil', 'pct_couverture_src':'pct_couverture', 
                            'contrepartie_src':'contrepartie', 'pays_contrepartie_src':'pays_contrepartie', 'en_planif_src':'en_planif'}, inplace=True)


#update the version of records in which data change is detected
metadata=sqlalchemy.MetaData(bind=mssql_engine())
datatable=sqlalchemy.Table('hedge', metadata, autoload=True)
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
ins_upd_rec.to_sql(table_name, con=mssql_engine(), index=False, if_exists='append')