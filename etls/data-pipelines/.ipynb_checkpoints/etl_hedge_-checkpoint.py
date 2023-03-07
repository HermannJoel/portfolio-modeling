import pandas as pd
import numpy as np
xrange = range
import os
import configparser
from pandasql import sqldf
pysqldf=lambda q: sqldf(q, globals())
from datetime import datetime
import datetime as dt
import sys

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
vmr="//DESKTOP-JDQLDT1/SharedFolder/d-eng/in/Volumes_Market_Repowering.xlsx"
planif="//DESKTOP-JDQLDT1/SharedFolder/d-eng/in/Outils planification agrege 2022-2024.xlsm"
hedge_vmr="//DESKTOP-JDQLDT1/SharedFolder/d-eng/temp/hedge_vmr.xlsx"
hedge_planif="//DESKTOP-JDQLDT1/SharedFolder/d-eng/temp/hedge_planif.xlsx"

def Extract(hedge_vmr_path, hedge_planif_path):
    ''' Function to extract excel files.
    Parameters
    ==========
    hedge_vmr_path: str
        path excel file containing data hedge in prod
    hedge_planif_path: str
        path excel file containing data hedge in planif    
    Returns
    =======
    df_hedge_vmr: DataFrame
        hedge vmr dataframe
    df_hedge_planif: DataFrame
        hedge planif dataframe
    '''
    try:
        df_hedge_vmr=ReadExcelFile(hedge_vmr_path)
        df_hedge_planif=ReadExcelFile(hedge_planif_path)
        return df_hedge_vmr, df_hedge_planif 
    except Exception as e:
        print("Data Extraction error!: "+str(e))

df_hedge_vmr, df_hedge_planif=Extract(hedge_vmr_path=vmr, hedge_planif_path=planif)

def transform():
    try:
        #===============     Hedge vmr     =======================
        #To create hedge df with vmr data
        df_hedge_vmr["profil"]=np.nan
        df_hedge_vmr["pct_couverture"]=np.nan
        df_hedge_vmr["contrepartie"]=np.nan
        df_hedge_vmr["pays_contrepartie"]=np.nan
 
        df_hedge_vmr = df_hedge_vmr[["id", "hedge_id", "projet_id","projet", "technologie", "type_hedge", 
                              "cod", "date_merchant", "date_dementelement", "puissance_installée", "profil", 
                              "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]
 
        df_hedge_vmr.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)

        ppa_vmr = ["NIBA" , "CHEP", "ALBE", "ALME", "ALMO", "ALVE", "PLOU"]

        df_hedge_vmr["type_hedge"] = df_hedge_vmr["type_hedge"].str.replace("FiT", "OA")
        df_hedge_vmr.loc[df_hedge_vmr.projet_id.isin(ppa_vmr) == True, "type_hedge"] = "PPA" 

        df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "OA", "pct_couverture"] = 1
        df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] != "OA", "pct_couverture"] = 1
        df_hedge_vmr.loc[df_hedge_vmr['type_hedge'] == "PPA", "pct_couverture"] = 1

        #===============     Hedge Planif     =======================
        #To import hedge_planif data. this was generated when creating the template_asset. 
        df_hedge_planif=pd.read_excel(path_dir_temp+"hedge_planif.xlsx")
        df_hedge_planif["type_hedge"] = "CR"
        df_hedge_planif["profil"] = np.nan
        df_hedge_planif["pct_couverture"] = np.nan
        df_hedge_planif["contrepartie"] = np.nan
        df_hedge_planif["pays_contrepartie"] = np.nan

        df_hedge_planif = df_hedge_planif[["id", "hedge_id", "projet_id", "projet", "technologie", "type_hedge", 
                                           "cod", "date_merchant", "date_dementelement", "puissance_installée", 
                                           "profil", "pct_couverture", "contrepartie", "pays_contrepartie", "en_planif"]]

        df_hedge_planif.rename(columns={"cod":"date_debut", "date_merchant":"date_fin"}, inplace = True)

        ppa_planif = ["SE19", "SE07"]
        df_hedge_planif.loc[df_hedge_planif.projet_id.isin(ppa_planif) == True, "type_hedge"] = "PPA"
        df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "CR", "pct_couverture"] = 1
        df_hedge_planif.loc[df_hedge_planif['type_hedge'] == "PPA", "pct_couverture"] = 1

        #To merge both data frame
        frames = [df_hedge_vmr, df_hedge_planif]
        hedge_template = pd.concat(frames)
        hedge_template.reset_index(inplace=True, drop=True)

        hedge_template.drop("id", axis=1, inplace=True)
        hedge_template=hedge_template.assign(id=[1 + i for i in xrange(len(hedge_template))])[['id'] + hedge_template.columns.tolist()]
  
    except Exception as e:
        print("Template hedge transformation error!: "+str(e))

template_hedge=transform()