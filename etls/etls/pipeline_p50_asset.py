# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:07:51 2022

@author: hermann.ngayap
"""
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
import os
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.types import Integer, String, Date, Float, DECIMAL, Numeric
pd.options.display.float_format = '{:.3f}'.format
from functions.etl_functions import (
                                     RemoveP50P90TypeHedge, CreateDataFrame, 
                                     MergeDataFrame, AdjustedByPct, ChooseCwd, 
                                     ReadExcelFile, SelectColumns,CreateMiniDataFrame, 
                                     postgressql_engine, RemoveP50P90
                                     ) 

ChooseCwd(cwd='D:/blx_mdp/BlxHerokuDash/')
path_dir_in='D:/blx_mdp/cwd/in/'
path_dir_temp='D:/blx_mdp/cwd/temp/'

class ETLAsset():
    
    def ExtractData(**kwargs):
        prod=ReadExcelFile(path_dir_in+"template_prod.xlsx", sheet_name="prod")
        prod_pct=ReadExcelFile(path_dir_in+"template_prod.xlsx",sheet_name="prod_perc")
        mean_pct=ReadExcelFile(path_dir_in+"template_prod.xlsx",sheet_name="mean_perc")
        #Subset of asset data 
        sub_data_asset=ReadExcelFile(path_dir_in + "template_asset.xlsx", 
                                     usecols=['asset_id', 'projet_id', 'technologie', 
                                              'cod', 'puissance_installée', 'date_merchant', 
                                              'date_dementelement', 'en_planif'])
        #Full data asset
        data_asset=ReadExcelFile(path_dir_in + "template_asset.xlsx")
        
        return prod, prod_pct, mean_pct, sub_data_asset, data_asset 
   
    def TransformAssetInProd(self, data_prod, **kwargs):
        """
        To create compute P50 & p90 of asset in production    
    args:
        data_prod (DataFrame) : Productibles, annual P50, P90 assets in production
        
        **kwargs : keyworded arguments
            data (DataFrame) : Sub-set of data of asset in  
            a (int) : Takes the value 0
            b (int) : Takes the value of the length of our horizon (12*7)
            profile_pct (dictionaries) : Production profile prod_pct
            n_prod (int) : The arg takes the value length of data 
            date (str) : The arg takes the value of date colum label 'date'
        """
        print('\n')
        print('compute Asset starts!:\n')
        print('here we go:\n')
        data=kwargs['data']
        data=data.loc[data["en_planif"]=="Non"]
        data=data.merge(data_prod, on='projet_id')
        data.reset_index(drop=True, inplace=True)
        n_prod=len(data) 
        prod_profile=kwargs['profile'].rename(columns=data_prod.set_index('projet_id')['projet'])
        prod_profile_dict=prod_profile.to_dict()
        print('create asset in production df:\n')
        #This code is to compute monthly p50 and p90.  
        d=CreateDataFrame(data, '01-01-2022' , a=0, b=12*7, n=n_prod, date='date', profile=prod_profile_dict)    
        d["cod"]=pd.to_datetime(d["cod"])
        d["date"]=pd.to_datetime(d["date"])
        d["date_dementelement"]=pd.to_datetime(d["date_dementelement"])
        d["date_merchant"]=pd.to_datetime(d["date_merchant"])
        d['année']=d['date'].dt.year
        d['trim']=d['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d['mois']=d['date'].dt.month
        #To remove p50, p90 based on cod and date_dementelement 
        results=RemoveP50P90(d, cod='cod', dd='date_dementelement', p50='p50_adj', 
                             p90='p90_adj', date='date', projetid='projet_id')
        
        asset_vmr= SelectColumns(results, 'asset_id', 'projet_id', 'projet', 
                                 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        asset_vmr.to_csv(path_dir_temp + 'p50_p90_asset_vmr.txt', index=False, sep=";")
        return asset_vmr
        
    def TransformAssetPlanif(self, data_prod, data_mean_profile, **kwargs):
        """
        To create compute P50 & p90 of assets in planification    
    args:
        data_prod (DataFrame) : Takes the value of productibles P50, P90 assets in production
        
        **kwargs : keyworded arguments
            data_mean_profile (DataFrame) : 
        """
        data=kwargs['data']
        data["date_merchant"].fillna(data["cod"] + pd.DateOffset(years=20), inplace=True) 
        #To select only data with 2023 cod 
        filter = data['cod'] > dt.datetime.today().strftime('%Y-%m-%d') 
        data = data.loc[filter]       
        data.loc[data['technologie']=='éolien', 'p50']=data["puissance_installée"]*8760*0.25
        data.loc[data['technologie']=='éolien', 'p90']=data["puissance_installée"]*8760*0.20
        data.loc[data['technologie']=='solaire', 'p50']=data["puissance_installée"]*8760*0.15
        data.loc[data['technologie']=='solaire', 'p90']=data["puissance_installée"]*8760*0.13
        #Select columns
        data=SelectColumns(data, "asset_id", "projet_id", "projet", "technologie", "cod", 
                        'date_dementelement','p50', 'p90')
        data_solar = data.loc[data['technologie'] == "solaire"]
        data_wp = data.loc[data['technologie'] == "éolien"]
        data_solar.reset_index(drop = True, inplace=True)
        data_wp.reset_index(drop = True, inplace=True)
        n_sol = len(data_solar) 
        n_wp = len(data_wp)
        mean_profile=data_mean_profile
        mean_profile_sol = mean_profile.iloc[:,[0, 1]]
        mean_profile_wp = mean_profile.iloc[:,[0,-1]]
        print('create solar df:\n')
        #create a df solar
        d1=CreateMiniDataFrame(data_solar, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1.reset_index(drop=True, inplace=True)
        mean_profile_sol=mean_profile_sol.assign(mth=[1 + i for i in xrange(len(mean_profile_sol))])[['mth'] + mean_profile_sol.columns.tolist()]
        #To calculate adjusted p50 and p90 solar adusted by the mean profile 
        s=mean_profile_sol.set_index('mth')['m_pct_solaire']
        pct = pd.to_datetime(d1['date']).dt.month.map(s)
        d1['p50_adj'] = -d1['p50'] * pct
        d1['p90_adj'] = -d1['p90'] * pct
            
        d1['cod'] = pd.to_datetime(d1['cod'])
        d1['date_dementelement'] = pd.to_datetime(d1['date_dementelement'])
        d1['date'] = pd.to_datetime(d1['date'])
        #To create new columns année, trimestre and mois
        d1['année'] = d1['date'].dt.year
        d1['trim'] = d1['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d1['mois'] = d1['date'].dt.month
        #To remove p50, p90, based on cod and date_dementelement solar
        results=RemoveP50P90(d1, cod='cod', dd='date_dementelement', p50='p50_adj', 
                        p90='p90_adj', date='date', projetid='projet_id')
        asset_solar=SelectColumns(results, 'asset_id', 'projet_id', 'projet', 'date', 
                                'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        print('create wind power df:\n')
        #create a df wind power
        d2=CreateMiniDataFrame(data_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
        d2.reset_index(drop=True, inplace=True)
        #create a mth column containing number of month
        mean_profile_wp = mean_profile_wp.assign(mth=[1 + i for i in xrange(len(mean_profile_wp))])[['mth'] + mean_profile_wp.columns.tolist()]
        #To calculate adjusted p50 and p90 wp (adjusted with p50)
        s2 = mean_profile_wp.set_index('mth')['m_pct_eolien']
        pct = pd.to_datetime(d2['date']).dt.month.map(s2)
        d2['p50_adj'] = -d2['p50'] * pct
        d2['p90_adj'] = -d2['p90'] * pct
        d2["cod"] = pd.to_datetime(d2["cod"])
        d2['date_dementelement'] = pd.to_datetime(d2['date_dementelement'])
        d2["date"] = pd.to_datetime(d2["date"])
        #To create new columns année, trimestre and mois
        d2['année'] = d2['date'].dt.year
        d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2['mois'] = d2['date'].dt.month
        #To remove p50, p90, based on cod and date_dementelement wind power
        res=RemoveP50P90(d2, cod='cod', dd='date_dementelement', p50='p50_adj', 
                        p90='p90_adj', date='date', projetid='projet_id')
        
        asset_wp=SelectColumns(res, 'asset_id', 'projet_id', 'projet', 'date', 
                            'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        #To merge asset in prod and asset in planification
        asset_vmr_planif=MergeDataFrame(kwargs['asset_vmr'], asset_solar, asset_wp)
        #Export hedge data as excel file
        asset_vmr_planif.to_csv(path_dir_in+"p50_p90_asset_vmr_planif.txt", index=False, sep=';')
        print('compute asset ends!:\n')        
        return asset_vmr_planif
    
        
class_obj=ETLAsset()
src_asset=class_obj.TransformAssetPlanif(prod, mean_pct, data=data_asset, asset_vmr=class_obj.TransformAssetProd(prod, data=sub_data_asset, profile=prod_pct))
      
#----------     Load prod asset into p50_p90_asset table  ----------#

def postgressql_engine(): 
    engine = create_engine('postgresql+psycopg2://postgres:24Fe1988@localhost:5432/blxmdpdwdev') 
    return engine

cnxn=postgressql_engine()
cursor=cnxn.cursor()
#Fix data type
src_asset['date']=pd.to_datetime(src_asset.date)
src_asset['date']=src_asset['date'].dt.date
#Rename p50 and p90 columns
src_asset.rename(columns={'p50_adj':'p50', 'p90_adj':'p90', 
                         'trim':'trimestre'},  inplace=True)
#Insert data in DB in asset table
table_name='p50_p90_asset'
src_asset.to_sql(table_name, 
                   con=mssql_engine(), 
                   index=False, 
                   if_exists='replace',
                   schema='staging',
                   chunksize=1000,
                   dtype={
                       'asset_id':Integer,
                       'projet_id':String(50),
                       'projet':String(250),
                       'date':Date,
                       'année':Integer,
                       'trimestre':String(50),
                       'mois':Integer, 
                       'p50':Float(10, 3),
                       'p90':Float(10, 3)
                       })

#To update new records into the destination table
metadata=sqlalchemy.MetaData(bind=cnxn)
datatable=sqlalchemy.Table('p50_p90_asset', 
                           metadata,
                           Column('assetid', Integer , nullable=False),
                           Column('projetid', String(100), nullable=False),
                           Column('project', String(250)),
                           Column('date', Date),
                           Column('year', Integer),
                           Column('quarter', String(50)),
                           Column('month', Integer),
                           Column('p50', Float(10, 3)),
                           Column('p90', Float(10, 3))
                           )
session=sessionmaker(bind=cnxn)
session=session()
#Loop over the target df and update to update records

for ind, row in src_asset.iterrows():
    ins=sqlalchemy.sql.Insert(datatable).values({'assetid':row.asset_id, 
                                                 'projetid':row.projet_id, 'project':row.projet, 'date':row.date,
                                                 'year':row.année, 'quarter':row.trimestre, 'month':row.mois, 
                                                 'p50':row.p50, 'p90':row.p90
                                                 })
    session.execute(ins)
session.flush()
session.commit()
cnxn.close()


