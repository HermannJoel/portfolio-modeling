# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 15:34:29 2022

@author: hermann.ngayap
"""
#==============================================================================
#========This script is to create a df containing contract prices==============
#=====                     of assets under PPA contract              ========== 
#==============================================================================
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
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

#====================================================
#=========    To change time horizon  ===============
#====================================================
nb_months=12
nb_years=(2028-2022)+1     #2028:is the end year while 2022 represents the starting year.
horizon=nb_months*nb_years #It represents the nber of months between the start date and the end date. 
date_obj="01-01-2022"      #To change the starting date of our horizon ex:To "01-01-2023" if we are in 2023


#To import ppa 
ppa_= pd.read_excel(path_dir_in + 'ppa.xlsx')
ppa_=ppa_.iloc[:,np.r_[0, 1, 2, 4, 5, 6, -1]]

#Import date cod & date_dementelement from asset
asset_ = pd.read_excel(path_dir_in + "template_asset.xlsx")
asset_=asset_[['asset_id', 'projet_id', 'cod', 'date_dementelement', 'date_merchant']]
asset_.reset_index(drop=True, inplace=True)

#To merge ppa prices data and template asset 
ppa=pd.merge(ppa_, asset_, how='left', on=['projet_id'])


#To create a df containing all the dates within the time horizon  
df = ppa.copy()
nbr = len(df)     
start_date = pd.to_datetime([date_obj] * nbr)
d = pd.DataFrame()
for i in range(0, horizon):
    df_buffer= df 
    df_buffer["date"] = start_date
    d = pd.concat([d, df_buffer], axis=0)
    start_date= start_date + pd.DateOffset(months=1)
    
#reset index    
d.reset_index(drop=True, inplace=True)
#To create quarter and month columns
d['année'] = d['date'].dt.year
d['trimestre'] = d['date'].dt.quarter
d['mois'] = d['date'].dt.month

#To remove price based on date_debut
#Condition:date column is less (in total seconds) than first date for each projet_id's first date_cod value
cond=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_debut'].transform('first')).dt.total_seconds())<0
d['price'] = np.where(cond,'', d['price'])
#To remove price based on date_fin
cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_fin'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_2, '', d['price'])
#To remove price based on date_dementelemnt
cond_2=((d['date'] - d.groupby(['hedge_id', 'projet_id'])['date_dementelement'].transform('first')).dt.total_seconds())>0
d['price'] = np.where(cond_2, '', d['price'])

prices_ppa=d[['hedge_id', 'projet_id', 'projet', 'type_hedge', 'date_debut', 
     'date_fin', 'date', 'année', 'trimestre', 'mois', 'price']]

#To export contract prices data of assets under ppa as excel file
prices_ppa.to_excel(path_dir_temp+'contracts_prices_ppa.xlsx', index=False, float_format="%.3f")
