# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 13:40:00 2022

@author: hermann.ngayap
"""
#==============This script is to create a temporary template hedge (hedge_vmr_planif)

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

#This part of the code can be set as comment because it transforms 
#data of asset already in production. The data are already loaded in the DB 

#To import hedge_vmr data. this was generated when creating the template_asset. 
df_hedge_vmr=pd.read_excel(path_dir_temp+"hedge_vmr.xlsx")
#To create hedge df with vmr data
df_hedge_vmr["profil"]=np.nan
df_hedge_vmr["pct_couverture"]=np.nan
df_hedge_vmr["contrepartie"]=np.nan
df_hedge_vmr["pays_contrepartie"]=np.nan
 
 
df_hedge_vmr = df_hedge_vmr[["rw_id", "hedge_id", "projet_id","projet", "technologie", "type_hedge", 
                              "cod", "date_merchant", "date_dementelement", "puissance_installée", "profil", 
                              "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]
 
df_hedge_vmr.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)
 
ppa_vmr = ["NIBA" , "CHEP", "ALBE", "ALME", "ALMO", "ALVE", "PLOU"]
 
df_hedge_vmr["type_hedge"] = df_hedge_vmr["type_hedge"].str.replace("FiT", "OA")
df_hedge_vmr.loc[df_hedge_vmr.projet_id.isin(ppa_vmr) == True, "type_hedge"] = "PPA" 

df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "OA", "pct_couverture"] = 1
df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] != "OA", "pct_couverture"] = 1
df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "PPA", "pct_couverture"] = 1

df_hedge_vmr.to_excel(path_dir_temp+"template_hedge_vmr.xlsx", index=False, float_format="%.3f")

#This is to transform only data of asset in planification.
#============================================================
#===============     Hedge Planif     =======================
#============================================================
#To import hedge_planif data. this was generated when creating the template_asset. 
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
frames = [df_hedge_vmr, df_hedge_planif]
hedge_template = pd.concat(frames)
hedge_template.reset_index(inplace=True, drop=True)

hedge_template.drop("rw_id", axis=1, inplace=True)
hedge_template=hedge_template.assign(rw_id=[1 + i for i in xrange(len(hedge_template))])[['rw_id'] + hedge_template.columns.tolist()]
hedge_template=hedge_template[["rw_id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                     "date_debut", "date_fin", "date_dementelement", "puissance_installée", 
                     "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

#Export temporary hedge template as excel file
hedge_template.to_excel(path_dir_temp+"hedge_vmr_planif.xlsx", index=False, float_format="%.3f")





