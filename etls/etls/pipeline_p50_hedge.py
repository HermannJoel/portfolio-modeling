# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:08:19 2022

@author: hermann.ngayap
"""
import pandas as pd
import numpy as np
xrange = range
import os
#sql alchemy import
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, String, Date, Float, Numeric, DECIMAL
from sqlalchemy import create_engine
from functions.etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                                     MergeDataFrame, AdjustedByPct, ChooseCwd, 
                                     ReadExcelFile, SelectColumns,CreateMiniDataFrame
                                     ) 

ChooseCwd(cwd='D:/blx_mdp/BlxHerokuDash/')
path_dir_in='D:/blx_mdp/cwd/in/'
path_dir_temp='D:/blx_mdp/cwd/temp/'

#path_dir_in + "template_prod.xlsx", sheet_name="prod"
#path_dir_in + "template_prod.xlsx", sheet_name="prod_perc"
#path_dir_in + "template_prod.xlsx", sheet_name="mean_perc"
#path_dir_in + "template_hedge.xlsx"
class ETLHedge:
    
    def __init__(self):
        self.ExtractData = self.ExtractData()
    
    class ExtractData: 
          
        def __init__(self, *args): 
            self.prod = ReadExcelFile(args[0])
            self.prod_pct = ReadExcelFile(args[1])
            self.mean_pct = ReadExcelFile(args[2])
            self.data_hedge = ReadExcelFile(args[3])
            self.data_hedge_vmr = self.data_hedge.loc[self.data_hedge["en_planif"]=="Non"]
            self.data_hedge_planif = self.data_hedge.loc[self.data_hedge["en_planif"]=="Oui"] 
              
        def ExtractExcel(self):
            
            self.df_OA = self.data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                              "date_fin", "date_dementelement", "pct_couverture"]]
            self.df_OA = self.df_OA.loc[self.df_OA["type_hedge"] == "OA"]
            
            self.df_CR = self.data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                              "date_fin", "date_dementelement", "pct_couverture"]]
            self.df_CR = self.df_CR.loc[(self.df_CR["type_hedge"] != "OA") & (self.df_CR["type_hedge"]!= "PPA")]
            
            self.df_PPA = self.data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                               "date_fin", "date_dementelement", "pct_couverture"]]
            self.df_PPA = self.df_PPA.loc[self.df_PPA["type_hedge"] == "PPA"]
            
ETLHedge=ETLHedge() 
ExtractData=ETLHedge.ExtractData()
df_prod=ExtractData.ExtractExcel(ReadExcelFile(path_dir_in + "template_prod.xlsx"))     
            
           
    def ExtractData(self, *args):
        """
        Extract input data. 
        
        Parameters
        ----------
        *args
            These parameters will be passed to the ExtractData() function.
        
        prod : DataFrame
                template prod      args[0]
        prod_pct : DataFrame
                prod profile       args[1]
        mean_pct : DataFrame  
                mean prod profile  args[2]
        data_hedge : DataFrame  
                template hedge     args[3]      
        """
        prod=ReadExcelFile(args[0], sheet_name="prod")
        prod_pct=ReadExcelFile(args[1])
        mean_pct=ReadExcelFile(args[2])
        data_hedge=ReadExcelFile(args[3])
        data_hedge_vmr=data_hedge.loc[data_hedge["en_planif"]=="Non"]
        data_hedge_planif=data_hedge.loc[data_hedge["en_planif"]=="Oui"]
        
        df_OA=data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                "date_fin", "date_dementelement", "pct_couverture"]]
        df_OA=df_OA.loc[df_OA["type_hedge"] == "OA"]
        df_CR=data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                "date_fin", "date_dementelement", "pct_couverture"]]
        df_CR=df_CR.loc[(df_CR["type_hedge"] != "OA") & (df_CR["type_hedge"] != "PPA")]
        df_PPA=data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                               "date_fin", "date_dementelement" ,"pct_couverture"]]
        df_PPA=df_PPA.loc[df_PPA["type_hedge"] == "PPA"]
                  
        return prod, prod_pct, mean_pct, df_OA, df_CR, df_PPA, data_hedge_planif 
    
    #This method Transform hedge data of assets in production   
    def TransformHedgeProd(self, data_prod, **kwargs):
        """
        To compute P50 & p90 hedge vmr     
    args:
        data_prod (DataFrame) : 
        *args: non-keyworded arguments
            sd (str) : Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
        **kwargs : keyworded arguments
            oa (DataFrame) : 
            cr (DataFrame) : 
            ppa (DataFrame) : 
            profile (DataFrame) : 
            a (int) : Takes the value 0
            b (int) : Takes the value of the length of our horizon (12*7)
            profile (dictionaries) : The arg takes the value of the production profile
            n (int) : The arg takes the value length of data 
            date (str) : The arg takes the value of date colum label 'date'
        """
        print('\n')
        print('compute Hedge starts!:\n')
        print('here we go:\n')
        #To merge cod data and prod data
        df_oa=kwargs['oa'].merge(data_prod, on='projet_id')
        #To merge cod data and prod data
        df_cr=kwargs['cr'].merge(data_prod, on='projet_id')
        #To merge cod data and prod data
        df_ppa=kwargs['ppa'].merge(data_prod, on='projet_id')
        #To determine the number of asset under OA
        n_oa=len(df_oa)
        #To determine the number of asset under CR
        n_cr= len(df_cr)
        #To determine the number of asset under OA
        n_ppa= len(df_ppa)
        #rename prod label with projet_id
        prod_profile=kwargs['profile'].rename(columns=data_prod.set_index('projet')['projet_id'])
        #To define a dict containing prod percentage  
        prod_profile_dict=prod_profile.to_dict()
        #----------  To create OA, CR, PPA hedge dfs ----------#
        print('create oa df:\n')
        d=CreateDataFrame(df_oa, '01-01-2022', a=0, b=12*7, n=n_oa, date='date', 
                        profile=prod_profile_dict)
        d.reset_index(inplace=True, drop=True)
        print('create cr df:\n')
        d2=CreateDataFrame(df_cr, '01-01-2022', a=0, b=12*7, n=n_cr, date='date', 
                        profile=prod_profile_dict)
        d2.reset_index(inplace=True, drop=True)
        print('create ppa df:\n')
        d3=CreateDataFrame(df_ppa, '01-01-2022', a=0, b=12*7, n=n_ppa, date='date', 
                        profile=prod_profile_dict)
        d3.reset_index(inplace=True, drop=True)
        
        #---------- OA  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d["p50_adj"]=AdjustedByPct(d, col1='p50_adj', col2='pct_couverture')
        d["p90_adj"]=AdjustedByPct(d, col1='p90_adj', col2='pct_couverture')     
        #To convert date
        d["date_debut"] = pd.to_datetime(d["date_debut"])
        d["date"] = pd.to_datetime(d["date"])
        d["date_fin"] = pd.to_datetime(d["date_fin"])
        d["date_dementelement"] = pd.to_datetime(d["date_dementelement"])
        #Extract year/month/day
        d['année'] = d['date'].dt.year
        d['trim'] = d['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d['mois'] = d['date'].dt.month
        
        #---------- CR  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d2["p50_adj"]=AdjustedByPct(d2, col1='p50_adj', col2='pct_couverture')
        d2["p90_adj"]=AdjustedByPct(d2, col1='p90_adj', col2='pct_couverture')    
        #To create new columns
        d2["date_debut"] = pd.to_datetime(d2["date_debut"])
        d2["date"] = pd.to_datetime(d2["date"])
        d2["date_fin"] = pd.to_datetime(d2["date_fin"])
        d2["date_dementelement"] = pd.to_datetime(d2["date_dementelement"])
        #Extract year/month/day values from date
        d2['année'] = d2['date'].dt.year
        d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2['mois'] = d2['date'].dt.month
        
        #---------- PPA  ----------#
        #Multiply P50 with pct_couverture to obtain adjusted values by hedge percentage
        d3["p50_adj"]=AdjustedByPct(d3, col1='p50_adj', col2='pct_couverture')
        d3["p90_adj"]=AdjustedByPct(d3, col1='p90_adj', col2='pct_couverture') 
        #To convert columns to datetime
        d3["date_debut"] = pd.to_datetime(d3["date_debut"])
        d3["date"] = pd.to_datetime(d3["date"])
        d3["date_fin"] = pd.to_datetime(d3["date_fin"])
        d3["date_dementelement"] = pd.to_datetime(d3["date_dementelement"])
        #Extract year/month/day from date
        d3['année'] = d3['date'].dt.year
        d3['trim'] = d3['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d3['mois'] = d3['date'].dt.month
        
        #To remove p50, p90, type_hedge based on start_date and end_date
        #---------- OA  ----------#
        res=RemoveP50P90TypeHedge(d, sd='date_debut',ed='date_fin', p50='p50_adj', 
                                    p90='p90_adj', th='type_hedge', date='date', 
                                    projetid='projet_id', hedgeid='hedge_id')
        res=SelectColumns(res, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                        'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        res.to_csv(path_dir_temp + 'hedge_oa.txt', index=False, sep=';') 
        #---------- CR  ----------#
        res2=RemoveP50P90TypeHedge(d2, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                        p90='p90_adj', th='type_hedge', date='date', 
                                        projetid='projet_id', hedgeid='hedge_id')

        res2=SelectColumns(res2, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                            'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        res2.to_csv(path_dir_temp + 'hedge_cr.txt', index=False, sep=';')
        #---------- PPA  ----------#
        res3=RemoveP50P90TypeHedge(d3, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')

        res3=SelectColumns(res3, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                        'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To export results as a data frame
        res3.to_csv(path_dir_temp + 'hedge_ppa.txt', index=False, sep=';')
        
        #To merge hedge OA, CR, and PPA dfs
        hedge_vmr=MergeDataFrame(res, res2, res3)
        
        #Export p50_p90_hedge_vmr as p50_p90_hedge_vmr.xlsx
        hedge_vmr = hedge_vmr.assign(id=[1 + i for i in xrange(len(hedge_vmr))])[['id'] + hedge_vmr.columns.tolist()]
        hedge_vmr = hedge_vmr[['id', 'hedge_id', 'projet_id', 'projet', 'type_hedge', 
                            'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]
        hedge_vmr.to_csv(path_dir_temp+"p50_p90_hedge_vmr.txt", index=False, sep=';')
        
        return hedge_vmr
        

    #This method Hedge Planif 
    def TransformHedgePlanif(self, data, mean_pct, **kwargs):
        """
        To create compute P50 & p90 hedge planif     
    args:
        data (DataFrame) : hedge data of asset in planif
        mean_pct (DataFrame) : mean production profile for solar & wind power
        hedge_vmr (DataFrame) : result df of ComputeHedgePlanif 
        *args: non-keyworded arguments
            sd (str) : Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
        **kwargs : keyworded arguments
            oa (DataFrame) : 
            cr (DataFrame) : 
            ppa (DataFrame) : 
            profile (DataFrame) : 
            a (int) : Takes the value 0
            b (int) : Takes the value of the length of our horizon (12*7)
            dict (dictionaries) : The arg takes the value of the column label 'p90_adj'
            n (int) : The arg takes the value length of data 
            date (str) : The arg takes the value of date colum label 'date'
        """
        #8760=24*365(operating hours).To calculate p50/p90 in mw/h of assets in planification.mw*8760*charging factor 
        #wind power
        data.loc[(data['technologie']=='éolien') & (data['en_planif']=='Oui'), 'p50']=data["puissance_installée"]*8760*0.25
        data.loc[(data['technologie']=='éolien') & (data['en_planif']=='Oui'), 'p90']=data["puissance_installée"]*8760*0.20
        #solar
        data.loc[(data['technologie']=='solaire') & (data['en_planif']=='Oui'), 'p50']=data["puissance_installée"]*8760*0.15
        data.loc[(data['technologie']=='solaire') & (data['en_planif']=='Oui'), 'p90']=data["puissance_installée"]*8760*0.13

        #To calculate p50 p90 adjusted by the pct_couverture
        data["p50"]=data["p50"]*data["pct_couverture"]
        data["p90"]=data["p90"]*data["pct_couverture"]

        prod_planif_solar=data.loc[(data['technologie'] == "solaire") & (data['en_planif'] == 'Oui')]
        prod_planif_solar.reset_index(drop = True, inplace=True)
        prod_planif_wp=data.loc[(data['technologie'] == "éolien") & (data['en_planif'] == 'Oui')]
        prod_planif_wp.reset_index(drop = True, inplace=True)

        #To determine the number of solar and eolien
        n_sol=len(prod_planif_solar)
        n_wp=len(prod_planif_wp)
        mean_pct_sol=mean_pct.iloc[:,[0, 1]]
        mean_pct_wp=mean_pct.iloc[:,[0,-1]]
        
        print('create solar & wind power dfs:\n')
        #create a df solar
        d1=CreateMiniDataFrame(prod_planif_solar, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1.reset_index(drop=True, inplace=True)
        #create a df wind power
        d2=CreateMiniDataFrame(prod_planif_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
        d2.reset_index(drop=True, inplace=True)
        #Solar
        mean_pct_sol=mean_pct_sol.assign(mth=[1 + i for i in xrange(len(mean_pct_sol))])[['mth'] + mean_pct_sol.columns.tolist()]
        #To calculate adjusted p50 and p90 solar adusted by the mean profile 
        s=mean_pct_sol.set_index('mth')['m_pct_solaire']
        pct = pd.to_datetime(d1['date']).dt.month.map(s)
        d1['p50_adj'] = -d1['p50'] * pct
        d1['p90_adj'] = -d1['p90'] * pct
        #To create new columns année et mois
        d1["date_debut"] = pd.to_datetime(d1["date_debut"])
        d1["date_fin"] = pd.to_datetime(d1["date_fin"])
        d1["date_dementelement"] = pd.to_datetime(d1["date_dementelement"])
        d1['année'] = d1['date'].dt.year
        d1['trim'] = d1['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d1['mois'] = d1['date'].dt.month
        d1 = d1[['hedge_id','projet_id', 'projet', 'type_hedge', 'date_debut', "date_fin", 
                'date_dementelement', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]
        
        #wind power
        #create a mth column containing number of month
        mean_pct_wp = mean_pct_wp.assign(mth=[1 + i for i in xrange(len(mean_pct_wp))])[['mth'] + mean_pct_wp.columns.tolist()]
        #To calculate adjusted p50 and p90 wp (adjusted with p50)
        s2 = mean_pct_wp.set_index('mth')['m_pct_eolien']
        pct = pd.to_datetime(d2['date']).dt.month.map(s2)
        d2['p50_adj'] = -d2['p50'] * pct
        d2['p90_adj'] = -d2['p90'] * pct
        #To create new columns
        d2["date_debut"] = pd.to_datetime(d2["date_debut"])
        d2["date_fin"] = pd.to_datetime(d2["date_fin"])
        d2['année'] = d2['date'].dt.year
        d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2['mois'] = d2['date'].dt.month
        d2 = d2[['hedge_id','projet_id', 'projet', 'type_hedge', 'date_debut', 'date_fin', 
                'date_dementelement', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

        #To remove p50 p90 based on date_debut
        res=RemoveP50P90TypeHedge(data=d1, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')
        hedge_solar=SelectColumns(res, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                                'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        #To remove p50 p90 based on date_debut
        res2=RemoveP50P90TypeHedge(data=d2, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')
        hedge_wp=SelectColumns(res2, 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                            'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        #To merge hedge_vmr and hedge_planif
        hedge_vmr_planif=MergeDataFrame(kwargs['hedge_vmr'], hedge_solar, hedge_wp)
        #Export hedge data as excel file
        hedge_vmr_planif.to_csv(path_dir_in+"p50_p90_hedge_vmr_planif.txt", index=False, sep=';')
        print('compute hedge ends!:\n')
        return hedge_vmr_planif

ETLHedge=ETLHedge()
hedge_df=ETLHedge.TransformHedgePlanif(data_hedge, mean_pct, hedge_vmr=ETLHedge.TransformHedgeProd(prod, oa=df_OA, cr=df_CR, ppa=df_PPA, profile=prod_pct))


#----------      Load hedge volume into p50_p90_hedge table  -----------#
def postgressql_engine(): 
    engine = create_engine('postgresql+psycopg2://postgres:24Fe1988@localhost:5432/blxmdpdwdev') 
    return engine

#Fix data type
src_df=hedge_df.copy()
src_df['date']=src_df['date'].dt.date
src_df['date']=pd.to_datetime(src_df.date)
#Rename p50 and p90 columns
src_df.rename(columns={'p50_adj':'p50', 'p90_adj':'p90', 
                         'trim':'trimestre'},  inplace=True)
#Insert data in DB in asset table
cnxn=postgressql_engine()
cursor=cnxn.cursor()

table_name='p50_p90_hedge'
hedge_df.to_sql(table_name, 
                   con=cnx, 
                   index=False, 
                   if_exists='replace',
                   schema='staging',
                   chunksize=1000,
                   dtype={
                       'hedgeid':Integer,
                       'projetid':String(50),
                       'project':String(250),
                       'hedgetype':String(50),
                       'date':Date,
                       'year':Integer,
                       'quarter':String(50),
                       'month':Integer, 
                       'p50':Float(10, 3),
                       'p90':Float(10, 3)
                       })

#To load new records into the destination table p50_p90_
metadata=sqlalchemy.MetaData(bind=postgressql_engine())
datatable=sqlalchemy.Table('p50_p90_hedge', metadata, 
                           Column('hedgeid', Integer , nullable=False),
                           Column('projetid', String(50), nullable=False),
                           Column('project', String(250)),
                           Column('hedgetype', String(50)),
                           Column('date', Date),
                           Column('year', Integer),
                           Column('quarter', String(50)),
                           Column('month', Integer),
                           Column('p50', Numeric(10, 3), DEFAULT=0),
                           Column('p90', Numeric(10, 3), DEFAULT=0)
                           )
session=sessionmaker(bind=postgressql_engine())
session=session()
#Loop over the target df and update to update records
cnx=postgressql_engine()
cursor = cnx.cursor()
for ind, row in hedge_df.iterrows():
    ins=sqlalchemy.sql.Insert(datatable).values({'hedgeid':row.hedge_id, 
                                                 'projetid':row.projet_id, 'project':row.projet, 
                                                 'hedgetype':row.type_hedge,'date':row.date,
                                                 'year':row.année, 'quarter':row.trimestre, 
                                                 'month':row.mois, 'p50':row.p50, 'p90':row.p90 
                                                 })
    session.execute(ins)
session.flush()
session.commit()
cnx.close()