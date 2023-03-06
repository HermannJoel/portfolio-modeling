import pandas as pd
import pathlib
import os
import configparser
from functions.etl_functions import*
import sqlalchemy
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.types import Integer, String, Date, Float, DECIMAL, Numeric
import pandas as pd
import numpy as np
import datetime as dt
xrange = range
import psycopg2

ChooseCwd(cwd='D:/blx_mdp/BlxHerokuDash/')
#Load Config
config_file=os.path.join(os.path.dirname("__file__"), 'config/config.ini')
config=configparser.ConfigParser()
config.read(config_file)

# Initialize Variables
eng_conn=config['develop']['conn_str']
dest_path=os.path.join(os.path.dirname("__file__"),config['develop']['dest_dir'])
src_path=os.path.join(os.path.dirname("__file__"),config['develop']['src_dir'])
prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])


class Extract:
      
    def __init__(self, prod, prod_pct, mean_pct, asset, hedge):
        self.prod=prod
        self.prod_pct=prod_pct
        self.mean_pct=mean_pct
        self.hedge=hedge
        self.asset=asset
    
    def ExtractFiles(self, **kwargs):
        prod=ReadExcelFile(self.prod, sheet_name='prod')
        prod_pct=ReadExcelFile(self.prod, sheet_name='prod_perc')
        mean_pct=ReadExcelFile(self.prod, sheet_name='mean_perc')
        asset=ReadExcelFile(self.asset)
        hedge=ReadExcelFile(self.hedge)
        #Subset of asset data 
        sub_asset=ReadExcelFile(self.asset, 
                                     usecols=['asset_id', 'projet_id', 'technologie', 
                                              'cod', 'puissance_installée', 'date_merchant', 
                                              'date_dementelement', 'en_planif'])
        #Full data asset
        df_asset=ReadExcelFile(self.asset)
        
        return prod, prod_pct, mean_pct, hedge, sub_asset, df_asset 


Extract=Extract()
prod=Extract.ExtractFiles(prod)
    
    
