import pandas as pd
import numpy as np
xrange = range
import os
import configparser
from datetime import datetime
import datetime as dt
import sys
import xlsxwriter
# adding etls/functions to the system path
sys.path.insert(0, 'D:/git-local-cwd/portfolio-modeling/etls/functions')
from etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                           MergeDataFrame, AdjustedByPct, ChooseCwd,
                           RemoveP50P90, ReadExcelFile, SelectColumns,CreateMiniDataFrame)

ChooseCwd(cwd='D:\git-local-cwd\portfolio-modeling')
#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'config/config.ini') 
config=configparser.ConfigParser()
config.read(config_file)

# Initialize Variables
eng_conn=config['develop']['conn_str']
dest_dir=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
temp_dir=os.path.join(os.path.dirname("__file__"),config['develop']['temp_dir'])
src_dir=os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
vmr=os.path.join(os.path.dirname("__file__"),config['develop']['vmr'])
planif=os.path.join(os.path.dirname("__file__"),config['develop']['planif'])

dest_dir="//DESKTOP-JDQLDT1/SharedFolder/d-eng/out/"
temp_dir="//DESKTOP-JDQLDT1/SharedFolder/d-eng/temp/"
productible="//DESKTOP-JDQLDT1/SharedFolder/d-eng/in/Copie de Productibles - Budget 2022 - version 1 loadé du 21 09 2021.xlsx"

def Extract(productible_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    productible_path: str
        path excel file containing data hedge in prod   
    Returns
    =======
    template_prod: DataFrame
        hedge vmr dataframe
    '''
    try:
        df_productible=ReadExcelFile(productible_path, sheet_name="Budget 2022", header=1)
        df_profile=ReadExcelFile(productible_path, sheet_name="BP2022 - Distribution mensuelle", header=1)
        return df_productible, df_profile
    except Exception as e:
        print("Data Extraction error!: "+str(e))

df_productible, df_profile=Extract(productible_path=productible)

def transform(data_productible, data_profile):
    try:
        #To import prod data from as pd data frame  
        df = data_productible
        df = df[["Projet", "Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]
        df = df.iloc[0:105,:]#Eventualy this can change if new parcs are added. 0:105 because 105 is the last row containing prod data
        #Divide prod by 1000 to convert in MWH 
        df[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]] = df[["Budget 2022 (KWh) - P50", "Budget 2022 (KWh) - P90 "]]/1000
        df.columns = ["projet", "p50", "p90"]
        #To create a list containing projects that are not longer in production (dismantled or sold)
        out_projets = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", 
                       "La Bouleste", "CDB Doux le vent","Evits et Josaphats", "Remise Reclainville", 
                       "Bougainville", "Renardières mont de Bezard","Blendecques Elec", "Stockage de l'Arce"]
        
        #Drop rows that contain any value in the list and reset index
        df_productible = df[df.projet.isin(out_projets) == False]
        df_productible.reset_index(inplace=True, drop=True)

        df_ =data_profile
        df_ = df_.iloc[0:12, 2:108]#This may change
        df_.rename(columns = {'% du P50':'month'}, inplace=True)

        #To create a list containing projects that are not longer in production (dismantled or sold)
        out_projets_ = ["Cham Longe Le Courbil (Eole Cevennes)", "Cham Longe Bel Air", 
                        "La Bouleste", "CDB Doux le vent", "Evits et Josaphats", 
                        "Remise Reclainville", "Bougainville", "Renardières mont de Bezard", 
                        "Blendecques Elec"]
        #Drop project taht are in the list above
        df_.drop(out_projets_, axis=1, inplace=True)

        #To create a list containing solar parcs
        solaire = ["Boralex Solaire Les Cigalettes SAS (Montfort)", 
                   "Boralex Solaire Lauragais SAS",
                   "Saint Christophe (Clé des champs)", 
                   "Peyrolles"]
        #To calculate typical solar profil as the mean of solar profil
        df_["m_pct_solaire"] = df_.loc[:,solaire].mean(axis=1)
        ##To calculate typical wind power profil as the mean of wind power profil
        df_["m_pct_eolien"] = df_.iloc[:,1:].drop(solaire, axis=1).mean(axis=1)

        #To create a df containing   
        mean_profile = df_.iloc[:,[0,-2,-1]]
        profile = df_.iloc[:, 0:-2]

        #To rename (add parentheses) on projet names
        profile.rename(columns = {'Extension seuil de Bapaume XSB':'Extension seuil de Bapaume (XSB)'}, inplace=True)
        profile.rename(columns = {"Extension plaine d'Escrebieux XPE":"Extension plaine d'Escrebieux (XPE)"}, inplace=True)
        
        return df_productible, profile, mean_profile
    
    except Exception as e:
        print("Template hedge transformation error!: "+str(e))

df_prod, df_profile, df_mean_profile=transform(data_productible=df_productible, data_profile=df_profile)        
        
def Load(dest_dir, src_productible, src_profile, src_mean_profile, file_name):
    try:
        #To export prod with no projet_id, profil with no projet_id, typical profil data as one excel file 
        #Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(dest_dir+file_name+'.xlsx', engine='xlsxwriter')
        #Write each dataframe to a different worksheet.
        src_productible.to_excel(writer, sheet_name="productible", float_format="%.4f", index=False)
        src_profile.to_excel(writer, sheet_name="profile", float_format="%.4f", index=False)
        src_mean_profile.to_excel(writer, sheet_name="mean_profile", float_format="%.4f", index=False)
        #Close the Pandas Excel writer and output the Excel file.
        writer.save()
        print("Data loaded succesfully!")
    except Exception as e:
        print("Data load error!: "+str(e))
        
Load(dest_dir=dest_dir, src_productible=df_prod, src_profile=df_profile, src_mean_profile=df_mean_profile, file_name="template_prod")