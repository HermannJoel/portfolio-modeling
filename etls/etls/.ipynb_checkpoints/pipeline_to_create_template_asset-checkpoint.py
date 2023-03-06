# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 13:23:00 2022

@author: hermann.ngayap
"""

#====================This script is to create the template asset called "asset_vmr_planif".

import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
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

#This part of the code of asset in production is set as comment because 
#they are are already loaded in the DB.consequently, don't need to be preprocessed again. 
#In case of need, remove the comments and run again.
#====================================================================
#=====   data preprocessing of assets in production       ===========
#====================================================================
df=pd.read_excel(path_dir_in+ "Volumes marchés_ Repowering.xlsx", sheet_name="vmr", header=0)
#To create a list containing parcs that are out of service
out_projets = ["Bougainville", "Cham Longe 1", "Evits et Josaphats", "Remise Reclainville", 
                "Evits et Josaphats", "Remise Reclainville", "Maurienne / Gourgançon", "La Bouleste", 
                "Cham Longe 1 - off", "Remise Reclainville - off", "Evits et Josaphats - off", "Bougainville - off", 
                "Maurienne / Gourgançon - off", "Saint-André - off"]
 
df.rename(columns = {"Alias":"projet", "Technologie":"technologie", "COD":"cod", "MW 100%":"mw", "Taux succès":"taux_succès", 
                      "MW pondérés":"puissance_installée", "EOH":"eoh","Mécanisme":"type_hedge", "Début FiT ajusté":"date_debut", 
                      "Date Merchant":"date_merchant"}, inplace = True)
 
#Drop rows that contain any value in the list and reset index
df = df[df['Parc '].isin(out_projets) == False]
df.reset_index(inplace=True, drop=True)
 
#To select rows where projet name is NaN
df=df[df['projet'].notna()]
df.reset_index(inplace=True, drop=True)

#To correct eolien & solar orthograph
df["technologie"] = df["technologie"].str.replace("Eolien", "éolien")
df["technologie"] = df["technologie"].str.replace("PV", "solaire")
#To set "taux_succès" of all parcs in exploitation equal to 100%
df["taux_succès"] = 1
#To compute pnderated "puissance_installée"
df["puissance_installée"] = df["mw"] * df["taux_succès"]
#To set "date_dementelement" 6 months before "date_msi"
df["date_dementelement"] = df["date_msi"] - pd.DateOffset(months=6)
#To create "en_planif" column Bolean: Non=for parc already in exploitation/Oui=projet in planification
df["en_planif"] = "Non"

df = df.assign(asset_id=[1 + i for i in xrange(len(df))])[['asset_id'] + df.columns.tolist()]
df = df.assign(rw_id=[1 + i for i in xrange(len(df))])[['rw_id'] + df.columns.tolist()]

df_asset = df[["rw_id","asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", "puissance_installée", 
               "type_hedge", "date_debut", "eoh", "date_merchant", "date_dementelement", "repowering", 
                "date_msi", "en_planif"]]
 
#To create a df containing projects with a cod>= 2023 
vmr_to_planif = df_asset[df_asset['cod'] > (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]
vmr_to_planif = vmr_to_planif[["rw_id", "asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", 
                                "puissance_installée", "eoh", "date_merchant", "date_dementelement", 
                                "repowering", "date_msi", "en_planif"]]
 
#To create a df containing projets already in exploitation
df_asset=df_asset[df_asset['cod'] <= (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]
 
#To select specific rows to create a hedge df
hedge_vmr=df_asset[["rw_id", "projet_id", "projet", "technologie", "type_hedge", "cod", 
                       "date_merchant", "date_dementelement", "puissance_installée", "en_planif"]]
#To create a column containing hedge_id
hedge_vmr = hedge_vmr.assign(hedge_id=[1 + i for i in xrange(len(hedge_vmr))])[['hedge_id'] + hedge_vmr.columns.tolist()]
#To select specific columns 
hedge_vmr = hedge_vmr[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", "cod", 
                        "date_merchant", "date_dementelement", "puissance_installée", "en_planif"]]
#Select specific columns to create asset template    
df_asset=df_asset[["rw_id", "asset_id", "projet_id", "projet", "technologie", "cod", "mw", "taux_succès", "puissance_installée", 
                      "eoh", "date_merchant", "date_dementelement", "repowering", "date_msi", "en_planif"]]
 
#To make export as excel files
vmr_to_planif.to_excel(path_dir_temp+"asset_vmr_to_planif.xlsx", index=False, float_format="%.3f")#This file contains data of assets still in planification but were in assets in planification. 
df_asset.to_excel(path_dir_temp + "projet_names.xlsx", index=False, columns=["asset_id", "projet_id", "projet"])
hedge_vmr.to_excel(path_dir_temp + "hedge_vmr.xlsx", index=False, float_format="%.3f")#This file contains data tha will be used to create hedge template of assets in production.
#To create a partial template asset that contains only data of assets in production.
df_asset.to_excel(path_dir_temp + "template_asset_vmr.xlsx", index=False, float_format="%.3f") 


#This part of the code is to preprocess data of assets in planification
#==============================================================================
#=============== Data preprocessing  of Asset in palnification  ===============
#==============================================================================
#To import data frame containing projects in planification
df = pd.read_excel(path_dir_in+"Outils planification agrege 2022-2024.xlsm", sheet_name="Planification", header=20, 
                    usecols=['#', 'Nom', 'Technologie', 'Puissance totale (pour les  repowering)', 
                             'date MSI depl', "date d'entrée dans statut S", 'Taux de réussite'])

#To import hedge 
hedge_vmr=pd.read_excel(path_dir_temp+"hedge_vmr.xlsx")
#To import asset 
asset_vmr=pd.read_excel(path_dir_temp+"template_asset_vmr.xlsx") 

#To drop all projects with "Nom" as optimisation 
rows_to_drop = sqldf("select * from df where Nom like 'optimisation%';", locals())
rows_to_drop = list(rows_to_drop['Nom'])
#To drop all projects with "Nom" as Poste
rows_to_drop2 = sqldf("select * from df where Nom like 'Poste%';", locals())
rows_to_drop2 = list(rows_to_drop2['Nom'])
#To drop all projects with "Nom" as Stockage 
rows_to_drop3 = sqldf("select * from df where Nom like 'Stockage%';", locals())
rows_to_drop3 = list(rows_to_drop3['Nom'])
#To drop all projects with "Nom" as Regul 
rows_to_drop4 = sqldf("select * from df where Nom like 'Régul%';", locals())
rows_to_drop4 = list(rows_to_drop4['Nom'])

#To rename columns
df.rename(columns = {'#':'projet_id', 'Nom':'projet', 'Technologie':'technologie', 
                      'Puissance totale (pour les  repowering)':'mw','date MSI depl':'date_msi', 
                      'Taux de réussite':'taux_succès'}, inplace=True)

#drop optimisation
#df5.loc[df5['Nom'].isin([rows_to_drop])]
df = df[df.projet.isin(rows_to_drop) == False]
#drop projects poste de...
df = df[df.projet.isin(rows_to_drop2) == False]
#drop projects Stockage de...
df = df[df.projet.isin(rows_to_drop3) == False]
#drop projects Regul de...
df = df[df.projet.isin(rows_to_drop4) == False]
#To select all projets where technologie is not autre 
df = df.loc[df['technologie'] != 'autre']


df['date_msi']=pd.to_datetime(df["date_msi"])

#To fill n/a of date_msi column with with date today + 50 years
df["date_msi"].fillna((dt.datetime.today() + pd.DateOffset(years=50)).strftime('%Y-%m-%d'), inplace=True)

#To select projects in planif with a cod date less than 2023. These projects should be moved to projects in planif  
df_to_asset_vmr = df[df['date_msi'] < (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d')]


#To select only data with cod superior to year's end date
filt = df['date_msi'] > (dt.datetime.today() + pd.offsets.YearEnd()).strftime('%Y-%m-%d') 
df = df.loc[filt]

#To select rows where Nom is NaN
df = df[df['projet'].notna()]
df_to_asset_vmr.reset_index(inplace=True, drop=True)
df.reset_index(inplace=True, drop=True)

#To set date cod equals to date msi
df['cod'] = df['date_msi']
df_to_asset_vmr['cod'] = df_to_asset_vmr['date_msi']

#To fill n/a in taux_succès column with default value
df["taux_succès"].fillna(0.599, inplace=True)
df_to_asset_vmr['taux_succès'].fillna(1, inplace=True)

#To calculate mw100%
df['puissance_installée']=df['mw'] * df["taux_succès"]
df_to_asset_vmr['puissance_installée'] = df_to_asset_vmr["mw"] * df_to_asset_vmr["taux_succès"]
#To create a column called eoh
df['eoh'] = np.nan
df_to_asset_vmr['eoh'] = np.nan

#To set a date merchant as date cod + 20 years 
df['date_merchant'] = df["cod"] + pd.DateOffset(years=20) 
df_to_asset_vmr['date_merchant'] = df_to_asset_vmr["cod"] + pd.DateOffset(years=20) 
#Create a column date dementelemnt and set default value as nan
df['date_dementelement'] = np.nan
df_to_asset_vmr['date_dementelement'] = np.nan

#To create a repowering column
df['repowering'] = np.nan
df_to_asset_vmr['repowering'] = np.nan
#To create a column en_planif
df['en_planif'] = 'Oui'
df_to_asset_vmr['en_planif'] = 'Non'

#correct correct "eolien" spelling
df["technologie"] = df["technologie"].str.replace("éolien ", "éolien")
df_to_asset_vmr["technologie"] = df_to_asset_vmr["technologie"].str.replace("éolien ", "éolien")

#To create a column rw_id of respective data frame
df = df.assign(rw_id=[1 + i for i in xrange(len(df))])[['rw_id'] + df.columns.tolist()]
df = df.assign(asset_id=[(len(asset_vmr)+1) + i for i in xrange(len(df))])[['asset_id'] + df.columns.tolist()]
df_to_asset_vmr = df_to_asset_vmr.assign(rw_id=[1 + i for i in xrange(len(df_to_asset_vmr))])[['rw_id'] + df_to_asset_vmr.columns.tolist()]
df_to_asset_vmr = df_to_asset_vmr.assign(asset_id=[1 + i for i in xrange(len(df_to_asset_vmr))])[['asset_id'] + df_to_asset_vmr.columns.tolist()]

#To select only specific rows
df = df[['rw_id', "asset_id", 'projet_id', 'projet', 'technologie', 'cod', 'mw', 'taux_succès', 
         'puissance_installée', 'eoh', 'date_merchant', 'date_dementelement', 
         'repowering', 'date_msi', 'en_planif']]

#To select only specific rows(df containing hedge template data of projects in planification)
hedge_planif = df[["rw_id", "projet_id", "projet", "technologie", "cod", "date_merchant", "date_dementelement", 
                   "puissance_installée", "en_planif"]]
hedge_planif = hedge_planif.assign(hedge_id=[(len(hedge_vmr)+1) + i for i in xrange(len(hedge_planif))])[['hedge_id'] + hedge_planif.columns.tolist()] 
hedge_planif = hedge_planif[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "cod", "date_merchant", "date_dementelement", 
                             "puissance_installée", "en_planif"]]

#Select only specific columns 
df_to_asset_vmr = df_to_asset_vmr[['rw_id', 'projet_id', 'projet', 'technologie', 'cod', 'mw', 'taux_succès', 
                                 'puissance_installée', 'eoh', 'date_merchant', 'date_dementelement', 
                                 'repowering', 'date_msi', 'en_planif']]
#To export data as excel files
df_to_asset_vmr.to_excel(path_dir_temp+'planif_to_asset_vmr.xlsx', index=False, float_format="%.3f")#This file contains data of assets already in production but were in planification
hedge_planif.to_excel(path_dir_temp+"hedge_planif.xlsx", index=False, float_format="%.3f")#This file contains data tha will be used to create hedge template of assets that are in planification
#To export as excel file a partial template that contains only data of assets in planification
df.to_excel(path_dir_temp+'template_asset_planif.xlsx', index=False, float_format="%.3f")

#==============================================================================
#===================== Joining Asset VMR and Asset Planif =====================
#==============================================================================

#To import the partial templates 
df_asset_vmr = pd.read_excel(path_dir_temp+"template_asset_vmr.xlsx")
df_asset_planif = pd.read_excel(path_dir_temp+"template_asset_planif.xlsx")

#To join the 2 partial template
frames = [df_asset_vmr, df_asset_planif]
asset_vmr_planif = pd.concat(frames)
asset_vmr_planif.reset_index(inplace=True, drop=True)

#To create a column containing row number
asset_vmr_planif.drop("rw_id", axis=1, inplace=True)
asset_vmr_planif=asset_vmr_planif.assign(rw_id=[1 + i for i in xrange(len(asset_vmr_planif))])[['rw_id'] + asset_vmr_planif.columns.tolist()]

#To export asset vmr planif as excel file as tamporary template asset.
#This file combine data from asset already in production with asset in planification
asset_vmr_planif.to_excel(path_dir_temp+"asset_vmr_planif.xlsx", index=False, float_format="%.3f")

