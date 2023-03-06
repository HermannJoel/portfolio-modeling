# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 22:13:33 2022

@author: hermann.ngayap
"""
#==============================================================================
#This script is to create a tamplate_prices containing contract prices(OA, CR)
#==============================================================================

import pandas as pd
import numpy as np
import os
from datetime import datetime
import datetime as dt
import pickle
xrange = range

pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
pd.set_option('display.max_rows', 200)

cwd=os.getcwd()
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


price=pd.read_excel(path_dir_in+'Production et stats de tous les parcs B22.xlsx', 
                      sheet_name='1-EO_Calcul Reporting', header=10)
#To choose only columns rows and columns with price
price=price.iloc[:106, 80:93]
#To rename columns
price.rename(columns={'Site.4': 'site', 'JAN [€/MWh].3': 'jan', 'FEB [€/MWh].3':'feb', 'MAR [€/MWh].3':'mar', 'APR [€/MWh].3':'apr', 
                      'MAY [€/MWh].3':'may', 'JUNE [€/MWh].3':'june', 'JULY [€/MWh].3':'july', 'AUG [€/MWh].3':'aug', 'SEPT [€/MWh].3':'sep',
                      'OCT [€/MWh].3':'oct', 'NOV [€/MWh].3':'nov', 'DEC [€/MWh].3':'dec'}, inplace=True)
#To create a list containing projets out of service
out_projets = ['Blendecques Elec', 'Bougainville', 'Cham Longe Bel Air', 'CDB Doux le vent' ,'Cham Longe Le Courbil (Eole Cevennes)',
              'Evits et Josaphats', 'La Bouleste', 'Renardières mont de Bezard', 'Remise Reclainville', "Stockage de l'Arce", ]

#To change PBF Blanches Fosses into Blanches Fosses PBF
price.loc[price['site']=='PBF Blanches Fosses', 'site']='Blanches Fosses PBF'
#To drop rows that contain any value in the list and reset index
price = price[price['site'].isin(out_projets) == False]
price.sort_values(by=['site'], inplace=True, ignore_index=True)
price.reset_index(inplace=True, drop=True)

#To import projet_id from template asset
projet_names_id = pd.read_excel(path_dir_in+"template_asset.xlsx", usecols = ["projet_id", "projet", "en_planif"])
projet_names_id = projet_names_id.loc[projet_names_id["en_planif"] == "Non"]
projet_names_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
projet_names_id.drop("en_planif", axis=1, inplace=True)
projet_names_id.reset_index(drop=True, inplace=True)
#rename projet_id as code
projet_names_id.rename(columns={"projet_id":"code"}, inplace=True)

#To join projet_id to template_price
frame=[projet_names_id, price]
price_id = pd.concat(frame, axis=1, ignore_index=False)

#To create a new column with projet_id
#Compare the 1st 5 character of projet names and site. set projet_id=code when the values match.
n = 5
price_id.loc[price_id['site'].str[:n] == price_id['projet'].str[:n], 'projet_id'] = price_id["code"]
price_id=price_id[["projet_id", "site", "jan", "feb", "mar", "apr", "may", "june","july", "aug", "sep", "oct", "nov", "dec", ]]

#To export template prices as excel file
price_id.to_excel(path_dir_in+'template_prices.xlsx', index=False)


