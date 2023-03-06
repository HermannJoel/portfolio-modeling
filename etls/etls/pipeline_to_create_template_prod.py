# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:42:07 2022

@author: hermann.ngayap
"""

import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
import datetime as dt
xrange = range
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())
import xlsxwriter
pd.options.display.float_format = '{:.3f}'.format
pd.set_option('display.max_columns', 200)
import os

print("The working directory was: {0}".format(os.getcwd()))
os.chdir("C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/")
print("The current working directory is: {0}".format(os.getcwd()))

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#==================================================================
#====================   pipeline productible ======================
#==================================================================

#To import prod data from as pd data frame  
df1 = pd.read_excel(path_dir_in+"Copie de Productibles - Budget 2022 - version 1 loadé du 21 09 2021.xlsx", sheet_name="Budget 2022", header=1)
df1 = df1[["Projet", "Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]
df1 = df1.iloc[0:105,:]#Eventualy this can change if new parcs are added. 0:105 because 105 is the last row containing prod data
#Divide prod by 1000 to convert in MWH 
df1[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]] = df1[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]/1000
df1.columns = ["projet", "p50", "p90"]
#To create a list containing projects that are not longer in production (dismantled or sold)
out_projets = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", "La Bouleste", "CDB Doux le vent",
        "Evits et Josaphats", "Remise Reclainville", "Bougainville", "Renardières mont de Bezard",
        "Blendecques Elec", "Stockage de l'Arce"]

#Drop rows that contain any value in the list and reset index
df1 = df1[df1.projet.isin(out_projets) == False]
df1.reset_index(inplace=True, drop=True)

#To import profil data as pd data frame
df2 = pd.read_excel(path_dir_in +  "Copie de Productibles - Budget 2022 - version 1 loadé du 21 09 2021.xlsx", sheet_name="BP2022 - Distribution mensuelle", header=1)
df2 = df2.iloc[0:12, 2:108]#This may change
df2.rename(columns = {'% du P50':'month'}, inplace=True)

#To create a list containing projects that are not longer in production (dismantled or sold)
out_projets2 = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", 
                "La Bouleste", "CDB Doux le vent", "Evits et Josaphats", 
                "Remise Reclainville", "Bougainville", "Renardières mont de Bezard", 
                "Blendecques Elec"]
#Drop project taht are in the list above
df2.drop(out_projets2, axis=1, inplace=True)

#To create a list containing solar parcs
solaire = ["Boralex Solaire Les Cigalettes SAS (Montfort)", 
           "Boralex Solaire Lauragais SAS",
           "Saint Christophe (Clé des champs)", 
           "Peyrolles"]
#To calculate typical solar profil as the mean of solar profil
df2["m_pct_solaire"] = df2.loc[:,solaire].mean(axis=1)
##To calculate typical wind power profil as the mean of wind power profil
df2["m_pct_eolien"] = df2.iloc[:,1:].drop(solaire, axis=1).mean(axis=1)

#To create a df containing   
mean_perc = df2.iloc[:,[0,-2,-1]]
prod_perc = df2.iloc[:, 0:-2]

#To rename (add parentheses) on projet names
prod_perc.rename(columns = {'Extension seuil de Bapaume XSB':'Extension seuil de Bapaume (XSB)'}, inplace=True)
prod_perc.rename(columns = {"Extension plaine d'Escrebieux XPE":"Extension plaine d'Escrebieux (XPE)"}, inplace=True)
 
#To export prod with no projet_id, profil with no projet_id, typical profil data as one excel file 
#Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(path_dir_temp + 'template_prod_no_id.xlsx', engine='xlsxwriter')
#Write each dataframe to a different worksheet.
df1.to_excel(writer, sheet_name="prod", float_format="%.3f", index=False)
prod_perc.to_excel(writer, sheet_name="prod_perc", float_format="%.3f", index=False)
mean_perc.to_excel(writer, sheet_name="mean_perc", float_format="%.3f", index=False)
#Close the Pandas Excel writer and output the Excel file.
writer.save()



