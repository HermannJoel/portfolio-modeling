# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:50:15 2022

@author: hermann.ngayap
"""

# =============================================================================
#- This notebook is to select and combine hedge data columns `projet_id`, `cod`, 
#- `date_merchant`, `date_dementelement` and market prices
# - Pull the monthly market prices scrapped from eex and save in the table `market_prices_fr_eex`.
# - Derrived the prices curves accross our time frame 2022-2028.
# - Merge the monthly market prices with hedge_id
# - Load market prices accross 2022-2028 into the table market_prices
# - Compute mtm historical values and insert in mark_to_market table
# =============================================================================
import pandas as pd
import numpy as np
import os
xrange = range
import warnings
warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 200)
cwd=os.getcwd()
ncwd='D:/blx_mdp/heroku_blx/'
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

#============================= To change cotation date  =======================
#============== Format=yyyy-mm-dd: Change the date to extract==================
#==============          Market prices accordingly            =================
cotationdate='2022-09-16'
cotationdate=pd.to_datetime(cotationdate).strftime('%Y-%m-%d')
#==============================================================================
#Open SQL connection to fetch monthly prices data derrived from price curve
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, Column
from server_credentials import server_credentials
from sqlalchemy.types import Integer, String, Date, DECIMAL, Numeric
from sqlalchemy.orm import sessionmaker


hostname= 'localhost'
database= 'blxmdpdwdev'  
portid= 5432
username='postgres'
pwd='24Fe1988'

def open_db():
    print('Connecting to Postgres')
    connection_string = f"postgresql://{username}:{pwd}@{hostname}:{portid}/{database}"
    cnxn = psycopg2.connect(connection_string)
    cursor = cnxn.cursor()
    print( "Connected!\n")
    return cnxn

   
def postgressql_engine(): 
    engine = create_engine('postgresql+psycopg2://postgres:24Fe1988@localhost:5432/blxmdpdwdev') 
    return engine

#Extract market prices data from the DB
sql_to_df=pd.read_sql_query('''
                            SELECT
                                deliveryperiod,
                                settlementprice,
                                cotationdate,
                                DATE_PART('YEAR', deliveryperiod) AS year,
                                DATE_PART('QUARTER', deliveryperiod) AS quarter,
                                DATE_PART('MONTH', deliveryperiod) AS month
                                FROM dash.pricecurvecotation 
                            WHERE cotationdate='%s'
                                '''%cotationdate, open_db())

market_prices=sql_to_df[['deliveryperiod', 'settlementprice', 'year', 
                            'quarter', 'month']]

#==========To combine hedge hedge data with market prices============= 
#To import hedge data
hedge_d=pd.read_excel(path_dir_in+"template_hedge.xlsx")
#Extract only projet_id, cod, date_merchant, date_dementelement, date_debut, date_fin
hedge_d_=hedge_d[['hedge_id', 'projet_id', 'date_debut', 'date_fin']]

#To multiply hedge df by the len of prices df
n=len(market_prices)
df_hedge = pd.DataFrame(
                np.repeat(hedge_d_.values, n, axis=0),
                columns=hedge_d_.columns,
            )

#To multiply prices df by the len of hedge df
n=len(hedge_d_)
market_prices_=pd.concat([market_prices]*n, ignore_index=True)


#To merge the 2 data frame
frame=[df_hedge, market_prices_]
hedge_market_prices=pd.concat(frame, axis=1, ignore_index=False)

hedge_market_prices=hedge_market_prices[['hedge_id', 'projet_id', 'deliveryperiod', 
                                         'year', 'quarter', 'month', 'settlementprice']]

hedge_market_prices['hedge_id']=hedge_market_prices['hedge_id'].apply(np.int64)
hedge_market_prices['year']=hedge_market_prices['year'].apply(np.int64)
hedge_market_prices['quarter']=hedge_market_prices['quarter'].apply(np.int64)
hedge_market_prices['month']=hedge_market_prices['month'].apply(np.int64)
hedge_market_prices['deliveryperiod']=pd.to_datetime(hedge_market_prices.deliveryperiod)
hedge_market_prices['deliveryperiod']=hedge_market_prices['deliveryperiod'].dt.date

#==============================================================
#== INSERT market_prices into DB in the market_prices table ===
#==============================================================
connection_string = f"postgresql://{username}:{pwd}@{hostname}:{portid}/{database}"
engine=create_engine(connection_string)

cnxn = psycopg2.connect(connection_string)
cursor=cnxn.cursor()   

trunc=("TRUNCATE TABLE dash.marketprice")
cursor.execute(trunc)
cnxn.commit()

for index, row in hedge_market_prices.iterrows():
    ins=("INSERT INTO dash.marketprice (hedgeid, projetid, deliveryperiod, year, quarter, month, settlementprice) values(%s, %s, %s, %s, %s, %s, %s)")
    insvalue=(row.hedge_id, row.projet_id, row.deliveryperiod, row.year, 
              row.quarter, row.month, row.settlementprice)
    cursor.execute(ins, insvalue)
    cnxn.commit()
    print('\n')
    print(f"{row.hedge_id, row.projet_id} inserted in the DB!")

#=====================================================
#==== Retrieve prod, market prices and contract prices
#====      data to compute mtm history       =========
#=====================================================
sql_to_df2=pd.read_sql_query('''
                                SELECT
                                --h.year, 
                                ---CAST(ROUND(SUM(h.p50), 2) AS DECIMAL(20, 3)) AS prod
                                --,CAST(ROUND(p.prix, 2) AS DECIMAL(10, 2)) AS strike_price
                                ---,CAST(ROUND(pu.settlementprice,2) AS DECIMAL(10, 2)) AS market_price 
                                CAST(ROUND(SUM(-h.p50*(cp.contractprice-mp.settlementprice))/1000000, 2) AS DECIMAL(20, 3)) AS mtm
                                FROM dash.p50p90hedge AS h
                                INNER JOIN  dash.contractprice AS cp
                                ON h.projetid=cp.projetid AND h.hedgeid=cp.hedgeid AND h.year=cp.year AND CAST(substr(h.quarter, 2, 1) AS INTEGER)=cp.quarter AND h.month=cp.month
                                INNER JOIN dash.marketprice AS mp
                                ON h.projetid=mp.projetid AND h.hedgeid=mp.hedgeid AND h.year=mp.year AND CAST(substr(h.quarter, 2, 1) AS INTEGER)=mp.quarter AND h.month=mp.month
                                WHERE h.hedgetype = 'PPA' OR h.hedgetype IS NULL 
                                --GROUP BY h.year  
                                --ORDER BY h.year;
                                ''', open_db())

#To create a colum containing the cotaion date. 
sql_to_df2['cotationdate']=pd.to_datetime(sql_to_df['cotationdate'][0])
mtm=sql_to_df2[['cotationdate', 'mtm']]
#===========================================================
#==To Insert mtm history in mark_to_market table in th DB ==
#===========================================================

for index, row in mtm.iterrows():
    ins=("INSERT INTO dash.mtm (cotationdate, mtm) values(%s, %s)")
    insvalue=(row.cotationdate, row.mtm)
    cursor.execute(ins, insvalue)
    cnxn.commit()
    print('\n')
    print(f"{row.mtm} was inserted in the db!")
    
# =============================================================================
# #To insert template hedge into the destination table hedge
# metadata=sqlalchemy.MetaData(bind=open_db(), schema='dash')
# datatable=sqlalchemy.Table('mtm', 
#                            metadata,
#                            Column('cotationdate', Date),
#                            Column('mtm', DECIMAL(10, 5))
#                            )
# session=sessionmaker(bind=open_db())
# session=session()
# #Loop over the target df and update to update records
# cursor = cnxn.cursor()
# for ind, row in mtm.iterrows():
#      ins=sqlalchemy.sql.Insert(datatable).values({'cotationdate':row.cotationdate, 'mtm':row.mtm                            
#                                                   })
#      session.execute(ins)
# session.flush()
# session.commit()
# print('\n')
# print("mtm historical inserted in the DB!")
# cnxn.close()
# =============================================================================

# =============================================================================
# try:
#     cnxn = psycopg2.connect(connection_string)
#     cursor=cnxn.cursor()
#     #ins=('INSERT INTO dash.mtm (cotationdate, mtm) values(, )", row.cotationdate, row.mtm')
#     cursor.execute()
# except Exception as error:
#     print(error)
# finally:
#     if cursor is not None:
#         cursor.close()
#     if cnxn is not None:
#         cnxn.close()
# =============================================================================

# =============================================================================
# 
# table_name='marketprice'
# hedge_market_prices.to_sql(table_name, 
#                    con=cnxn, 
#                    index=False, 
#                    if_exists='replace',
#                    schema='dash',
#                    chunksize=1000,
#                    dtype={
#                        'hedge_id':Integer(),
#                        'projet_id':String(50),
#                        'deliveryperiod':Date(),
#                        'year':Integer(),
#                        'quarter':Integer(),
#                        'month':Integer(),
#                        'settlementprice':Numeric(7, 3)
#                        }
#                    )
# =============================================================================
