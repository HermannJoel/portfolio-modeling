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
df_prod=os.path.join(os.path.dirname("__file__"),config['develop']['prod'])
asset=os.path.join(os.path.dirname("__file__"),config['develop']['asset'])
hedge=os.path.join(os.path.dirname("__file__"),config['develop']['hedge'])

class ETLAsset():
    """
    Class to Extract Transform and Load asset data 
    
    parameters:
    ----------
    
    prod : DataFrame
           Prod data
    
    
    """
    def __init__(self, prod, prod_pct, mean_pct, asset, hedge):
        self.prod=prod
        self.prod_pct=prod_pct
        self.mean_pct=mean_pct
        self.asset=asset
        self.hedge=hedge
    
    def ExtractData(self, **kwargs):
        self.prod=ReadExcelFile(df_prod, sheet_name='prod')
        self.prod_pct=ReadExcelFile(df_prod, sheet_name='prod_perc')
        self.mean_pct=ReadExcelFile(df_prod, sheet_name='mean_perc')
        #Subset of asset data 
        sub_data_asset=ReadExcelFile(asset, 
                                     usecols=['asset_id', 'projet_id', 'technologie', 
                                              'cod', 'puissance_installée', 'date_merchant', 
                                              'date_dementelement', 'en_planif'])
        #Full data asset
        data_asset=ReadExcelFile(asset)
        data_hedge=ReadExcelFile(hedge)
        
        return self.prod, self.prod_pct, self.mean_pct, sub_data_asset, data_asset, data_hedge 
   
    def TransformAssetInProd(self, **kwargs):
        """
        To create compute P50 & p90 of asset in production
            
        parameters:
        ----------
        
        data_prod : DataFrame 
                    Productibles, annual P50, P90 assets in production
        
        **kwargs : 
            data : DataFrame 
                   Sub-set of asset data  
            a : int 
                Takes the value 0
            b : int
                Takes the value of the length of our horizon (12*7)
            profile_pct : dictionaries
                          Production profile prod_pct
            n_prod : int  
                     The arg takes the value length of data 
            date : str
                   The arg takes the value of date colum label 'date'
        """
        print('\n')
        print('compute Asset starts!:\n')
        print('here we go:\n')
        data=kwargs['data']
        data=data.loc[data["en_planif"]=="Non"]
        data=data.merge(self.prod, on='projet_id')
        data.reset_index(drop=True, inplace=True)
        n_prod=len(data) 
        prod_profile=kwargs['profile'].rename(columns=self.prod.set_index('projet_id')['projet'])
        prod_profile_dict=prod_profile.to_dict()
        print('create asset in production df:\n')
        #This code is to compute monthly p50 and p90.  
        d=CreateDataFrame(data, '01-01-2022' , a=0, b=12*7, n=n_prod, date='date', profile=prod_profile_dict)    
        d["cod"]=pd.to_datetime(d["cod"])
        d["date"]=pd.to_datetime(d["date"])
        d["date_dementelement"]=pd.to_datetime(d["date_dementelement"])
        d["date_merchant"]=pd.to_datetime(d["date_merchant"])
        d['année']=d['date'].dt.year
        d['trim']=d['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d['mois']=d['date'].dt.month
        #To remove p50, p90 based on cod and date_dementelement 
        results=RemoveP50P90(d, cod='cod', dd='date_dementelement', p50='p50_adj', 
                             p90='p90_adj', date='date', projetid='projet_id')
        
        asset_vmr=SelectColumns(results, 'asset_id', 'projet_id', 'projet', 
                                 'date', 'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        return asset_vmr
        
    def TransformAssetPlanif(self, **kwargs):
        """
        To create compute P50 & p90 of assets in planification    
        Parameters :
        ----------
        data_prod (DataFrame) : Takes the value of productibles P50, P90 assets in production
        
        **kwargs : keyworded arguments
            data_mean_profile (DataFrame) : 
        """
        data=kwargs['data']
        data["date_merchant"].fillna(data["cod"] + pd.DateOffset(years=20), inplace=True) 
        #To select only data with 2023 cod 
        filter = data['cod'] > dt.datetime.today().strftime('%Y-%m-%d') 
        data = data.loc[filter]       
        data.loc[data['technologie']=='éolien', 'p50']=data["puissance_installée"]*8760*0.25
        data.loc[data['technologie']=='éolien', 'p90']=data["puissance_installée"]*8760*0.20
        data.loc[data['technologie']=='solaire', 'p50']=data["puissance_installée"]*8760*0.15
        data.loc[data['technologie']=='solaire', 'p90']=data["puissance_installée"]*8760*0.13
        #Select columns
        data=SelectColumns(data, "asset_id", "projet_id", "projet", "technologie", "cod", 
                        'date_dementelement','p50', 'p90')
        data_solar=data.loc[data['technologie'] == "solaire"]
        data_wp=data.loc[data['technologie'] == "éolien"]
        data_solar.reset_index(drop = True, inplace=True)
        data_wp.reset_index(drop = True, inplace=True)
        n_sol = len(data_solar) 
        n_wp = len(data_wp)
        mean_profile=self.mean_pct
        mean_profile_sol = mean_profile.iloc[:,[0, 1]]
        mean_profile_wp = mean_profile.iloc[:,[0,-1]]
        print('create solar df:\n')
        #create a df solar
        d1=CreateMiniDataFrame(data_solar, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1.reset_index(drop=True, inplace=True)
        mean_profile_sol=mean_profile_sol.assign(mth=[1 + i for i in xrange(len(mean_profile_sol))])[['mth'] + mean_profile_sol.columns.tolist()]
        #To calculate adjusted p50 and p90 solar adusted by the mean profile 
        s=mean_profile_sol.set_index('mth')['m_pct_solaire']
        pct = pd.to_datetime(d1['date']).dt.month.map(s)
        d1['p50_adj'] = -d1['p50'] * pct
        d1['p90_adj'] = -d1['p90'] * pct
            
        d1['cod'] = pd.to_datetime(d1['cod'])
        d1['date_dementelement'] = pd.to_datetime(d1['date_dementelement'])
        d1['date'] = pd.to_datetime(d1['date'])
        #To create new columns année, trimestre and mois
        d1['année'] = d1['date'].dt.year
        d1['trim'] = d1['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d1['mois'] = d1['date'].dt.month
        #To remove p50, p90, based on cod and date_dementelement solar
        results=RemoveP50P90(d1, cod='cod', dd='date_dementelement', p50='p50_adj', 
                        p90='p90_adj', date='date', projetid='projet_id')
        asset_solar=SelectColumns(results, 'asset_id', 'projet_id', 'projet', 'date', 
                                'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        print('create wind power df:\n')
        #create a df wind power
        d2=CreateMiniDataFrame(data_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
        d2.reset_index(drop=True, inplace=True)
        #create a mth column containing number of month
        mean_profile_wp = mean_profile_wp.assign(mth=[1 + i for i in xrange(len(mean_profile_wp))])[['mth'] + mean_profile_wp.columns.tolist()]
        #To calculate adjusted p50 and p90 wp (adjusted with p50)
        s2 = mean_profile_wp.set_index('mth')['m_pct_eolien']
        pct = pd.to_datetime(d2['date']).dt.month.map(s2)
        d2['p50_adj'] = -d2['p50'] * pct
        d2['p90_adj'] = -d2['p90'] * pct
        d2["cod"] = pd.to_datetime(d2["cod"])
        d2['date_dementelement'] = pd.to_datetime(d2['date_dementelement'])
        d2["date"] = pd.to_datetime(d2["date"])
        #To create new columns année, trimestre and mois
        d2['année'] = d2['date'].dt.year
        d2['trim'] = d2['date'].dt.to_period('Q').dt.strftime("Q%q-%y")
        d2['mois'] = d2['date'].dt.month
        #To remove p50, p90, based on cod and date_dementelement wind power
        res=RemoveP50P90(d2, cod='cod', dd='date_dementelement', p50='p50_adj', 
                        p90='p90_adj', date='date', projetid='projet_id')
        
        asset_wp=SelectColumns(res, 'asset_id', 'projet_id', 'projet', 'date', 
                            'année', 'trim', 'mois', 'p50_adj', 'p90_adj')
        
        #To merge asset in prod and asset in planification
        asset_vmr_planif=MergeDataFrame(kwargs['asset_vmr'], asset_solar, asset_wp)   
        print('compute asset ends!:\n')        
        return asset_vmr_planif
    
    def Load(self, target, data_to_load):
        #To export results as a data frame
        asset_vmr.to_csv(path_dir_temp + 'p50_p90_asset_vmr.txt', index=False, sep=";")
        #Export hedge data as excel file
        asset_vmr_planif.to_csv(path_dir_in+"p50_p90_asset_vmr_planif.txt", index=False, sep=';')
        
    
        
ETLAsset=ETLAsset()
src_asset=ETLAsset.TransformAssetPlanif(prod, mean_pct, data=data_asset, asset_vmr=ETLAsset.TransformAssetProd(prod, data=sub_data_asset, profile=prod_pct))








class ETLHedge:
    def __init__(self, prod, prod_pct, mean_pct, data_hedge):
        self.prod=prod
        self.prod_pct=prod_pct
        self.mean_pct=mean_pct
        self.data_hedge=data_hedge
            
    def ExtractData(self):    
        self.prod=ReadExcelFile("D:/blx_mdp/cwd/in/template_prod.xlsx", sheet_name="prod")
        self.prod_pct=ReadExcelFile("D:/blx_mdp/cwd/in/template_prod.xlsx", sheet_name="prod_perc")
        self.mean_pct=ReadExcelFile("D:/blx_mdp/cwd/in/template_prod.xlsx", sheet_name="mean_perc")
        self.data_hedge=ReadExcelFile("D:/blx_mdp/cwd/in/template_hedge.xlsx")
        self.data_hedge_vmr=self.data_hedge.loc[self.data_hedge["en_planif"]=="Non"]
        self.data_hedge_planif=self.data_hedge.loc[self.data_hedge["en_planif"]=="Oui"] 
        self.df_OA=self.data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                        "date_fin", "date_dementelement", "pct_couverture"]]
        self.df_OA=self.df_OA.loc[self.df_OA["type_hedge"] == "OA"]
        
        self.df_CR=self.data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                        "date_fin", "date_dementelement", "pct_couverture"]]
        self.df_CR=self.df_CR.loc[(self.df_CR["type_hedge"] != "OA") & (self.df_CR["type_hedge"]!= "PPA")]
            
        self.df_PPA=self.data_hedge_vmr[["hedge_id", "projet_id", "type_hedge", "date_debut", 
                                        "date_fin", "date_dementelement", "pct_couverture"]]
        self.df_PPA=self.df_PPA.loc[self.df_PPA["type_hedge"] == "PPA"]
 
if __name__ == "__main__":
    ETLHedge=ETLHedge()
    
    

    
