# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 15:18:04 2022

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

#===================================================================================
#========================== To add projet_id to prod data ==========================
#===================================================================================
#To import template prod with no projet_id df 
prod = pd.read_excel(path_dir_temp + "template_prod_no_id.xlsx")
prod.sort_values(by=['projet'], inplace=True, ignore_index=True)
prod_perc = pd.read_excel(path_dir_temp + "template_prod_no_id.xlsx", sheet_name="prod_perc")
prod_perc = prod_perc.iloc[:,1:]
mean_perc = pd.read_excel(path_dir_temp + "template_prod_no_id.xlsx", sheet_name="mean_perc")

#To create a df containing all projet_id, projet names from template asset 
projet_names_id = pd.read_excel(path_dir_temp + "asset_vmr_planif.xlsx", usecols = ["projet_id", "projet", "en_planif"])
projet_names_id = projet_names_id.loc[projet_names_id["en_planif"] == "Non"]
projet_names_id.sort_values(by=['projet'], inplace=True, ignore_index=True)
projet_names_id.drop("en_planif", axis=1, inplace=True)
projet_names_id.rename(columns={"projet_id":"code", "projet":"projet_names"}, inplace=True)

#To join projet_id, projet and prod data frame
frames = [projet_names_id, prod]
df=pd.concat(frames, axis=1, ignore_index=False)

#To create a new column containing projet_id in the prod data frame
n = 4#n=3 to choose the first 3 string
df.loc[df['projet'].str[:n] == df['projet_names'].str[:n], 'projet_id'] = df["code"]
df=df[["projet_id", "projet", "p50", "p90"]]

#To replace pejet name by projet_id in the prod_perc data frame
n = 5#n=5 to choose the first 5 string

#Compare the 5 first character of column label of prod_perc data frame with the first 5 character of each "projet_names" of projet_names_id data frame 
#If the first 5 character match, replace the column label by the corresponding projet_id.      
s = (df.assign(names=df['projet'].str[:n])
    .drop_duplicates('names')
    .set_index('names')['projet_id']
)

prod_perc_id = (pd
   .concat([prod_perc.columns.to_frame().T, prod_perc])
   .rename(columns=lambda x: s.loc[x[:n]])

)

prod_perc_id.reset_index(inplace=True, drop=True)
prod_perc_id = prod_perc_id.iloc[1:,:]

#To export prod with projet_id, profil with projet_id, profil without projet_id, typical profil as pd df.
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter(path_dir_in + 'template_prod.xlsx', engine='xlsxwriter')
#Write each dataframe to a different worksheet.
df.to_excel(writer, sheet_name="prod", float_format="%.3f", index=False)
prod_perc_id.to_excel(writer, sheet_name="prod_perc_id", float_format="%.3f", index=False)
prod_perc.to_excel(writer, sheet_name="prod_perc", float_format="%.3f", index=False)
mean_perc.to_excel(writer, sheet_name="mean_perc", float_format="%.3f", index=False)
#Close the Pandas Excel writer and output the Excel file.
writer.save()


