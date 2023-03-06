import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
import os
pd.options.display.float_format = '{:.3f}'.format
from functions.etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                                     MergeDataFrame, AdjustedByPct, ChooseCwd, 
                                     ReadExcelFile, SelectColumns, ReadExcelFiles, CreateMiniDataFrame) 

ChooseCwd(cwd='D:/blx_mdp/BlxHerokuDash/')
path_dir_in='D:/blx_mdp/cwd/in/'
path_dir_temp='D:/blx_mdp/cwd/temp/'

prod=pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod")
prod_pct=pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="prod_perc")
mean_pct=pd.read_excel(path_dir_in + "template_prod.xlsx", sheet_name="mean_perc")


df_hedge=pd.read_excel(path_dir_in + "template_hedge.xlsx")
df_hedge_vmr=df_hedge.loc[df_hedge["en_planif"]=="Non"]
df_hedge_planif=df_hedge.loc[df_hedge["en_planif"]=="Oui"]

df_OA = df_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", "date_fin", "date_dementelement", "pct_couverture"]]
df_OA = df_OA.loc[df_OA["type_hedge"] == "OA"]

df_CR = df_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", "date_fin", "date_dementelement", "pct_couverture"]]
df_CR=df_CR.loc[(df_CR["type_hedge"] != "OA") & (df_CR["type_hedge"] != "PPA")]

df_PPA=df_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", "date_fin", "date_dementelement" ,"pct_couverture"]]
df_PPA=df_PPA.loc[df_PPA["type_hedge"] == "PPA"]

#To merge cod data and prod data
df_OA=df_OA.merge(prod, on='projet_id')
#To merge cod data and prod data
df_CR = df_CR.merge(prod, on='projet_id')
#To merge cod data and prod data
df_PPA = df_PPA.merge(prod, on='projet_id')
#To determine the number of asset under OA
n_OA=len(df_OA.loc[df_OA["type_hedge"] == "OA"])
#To determine the number of asset under CR
n_CR = len(df_CR.loc[(df_CR["type_hedge"] != "OA") & (df_CR["type_hedge"] != "PPA")])
#To determine the number of asset under OA
n_PPA = len(df_PPA.loc[df_PPA["type_hedge"] == "PPA"])
#rename prod label with projet_id
profile_df=prod_pct.rename(columns=prod.set_index('projet')['projet_id'])
#To define a dict containing prod percentage  
profile_df=profile_df.to_dict()

#######  HEDGE OA ###########
d=CreateDataFrame(df_OA, '01-01-2022', a=0, b=12*7, n=n_OA, date='date', profile=profile_df)
d.reset_index(inplace=True, drop=True)

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
#Select columns
d=d[['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 'date', 
    'date_fin', "date_dementelement", 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]

#To remove p50, p90, type_hedge based on date_fin and date_fin
results=RemoveP50P90TypeHedge(d,sd='date_debut',ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')

results=SelectColumns(results, 'id', 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                      'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
#To export results as a data frame
results.to_excel(path_dir_temp + 'hedge_OA_test.xlsx', index=False, float_format="%.3f")

###############  HEDGE CR   ##################
d2=CreateDataFrame(df_CR, '01-01-2022', a=0, b=12*7, n=n_CR, date='date', 
                        dict=dict)
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

#To remove p50, p90, type_hedge based on date_fin and date_fin
results=RemoveP50P90TypeHedge(data=d2, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')

results=SelectColumns(results, 'id', 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                      'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
#To export results as a data frame
results.to_excel(path_dir_temp + 'hedge_CR.xlsx', index=False, float_format="%.3f")

#--------------------   Hedge PPA  --------------------#
d3=CreateDataFrame(df_PPA, '01-01-2022', a=0, b=12*7, n=n_PPA, date='date', 
                        dict=dict)

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

#To remove p50, p90, type_hedge based on date_fin and date_fin
results=RemoveP50P90TypeHedge(d3, sd='date_debut', ed='date_fin', p50='p50_adj', 
                                p90='p90_adj', th='type_hedge', date='date', 
                                projetid='projet_id', hedgeid='hedge_id')

results=SelectColumns(results, 'id', 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 
                      'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
#To export results as a data frame
results.to_excel(path_dir_temp + 'hedge_PPA.xlsx', index=False, float_format="%.3f")

#Import excel exports
df_1=pd.read_excel(path_dir_temp+'hedge_OA.xlsx', usecols=['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj'])
df_2=pd.read_excel(path_dir_temp+'hedge_CR.xlsx', usecols=['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj'])
df_3=pd.read_excel(path_dir_temp+'hedge_PPA.xlsx', usecols=['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 'année', 'trim','mois', 'p50_adj', 'p90_adj'])

#Merge hedge OA, CR, and PPA data frames
p50_p90_hedge_vmr=MergeDataFrame(df_1, df_2, df_3)

#Export p50_p90_hedge_vmr as p50_p90_hedge_vmr.xlsx
p50_p90_hedge_vmr = p50_p90_hedge_vmr.assign(id=[1 + i for i in xrange(len(p50_p90_hedge_vmr))])[['id'] + p50_p90_hedge_vmr.columns.tolist()]
p50_p90_hedge_vmr = p50_p90_hedge_vmr[['id', 'hedge_id', 'projet_id', 'projet', 'type_hedge', 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj']]
p50_p90_hedge_vmr.to_excel(path_dir_temp+"p50_p90_hedge_vmr.xlsx", index=False, float_format="%.3f")

