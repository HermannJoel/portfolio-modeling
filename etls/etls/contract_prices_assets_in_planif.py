# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 15:40:53 2022

@author: hermann.ngayap
"""
import pandas as pd
import numpy as np
xrange = range
import os
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine, Column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, String, Date, Float, Numeric, DECIMAL
from sqlalchemy import create_engine
from functions.etl_functions import (RemoveP50P90TypeHedge, CreateDataFrame, 
                                     MergeDataFrame, AdjustedByPct, ChooseCwd, 
                                     ReadExcelFile, SelectColumns, RemoveContractPrices,
                                     CreateMiniDataFrame) 

ChooseCwd(cwd='D:/blx_mdp/BlxHerokuDash/')
path_dir_in='D:/blx_mdp/cwd/in/'
path_dir_temp='D:/blx_mdp/cwd/temp/'


class ETLContractPrices:
    """
    A class to Extract Transform and Load contract prices data.
    ...
    
    Attributes
    ----------
    name : str
        first name of the person
    surname : str
        family name of the person
    age : int
        age of the person

    Methods
    -------
    ExtractData(self, *args):
        Extract input data
    TransformPricesPlanif():
        Transform .
    """
    
    
    def ExtractData(self, *args):
        """
        A method extract hedge data from hedge template 
        
        *args:
        
        data_hedge : DataFrame 
                template Hedge
        data_prices : (DataFrame)  
                template prices     
        """
        data_hedge=ReadExcelFile(args[0])
        data_prices=ReadExcelFile(args[1])
        
        return data_hedge, data_prices
    
    #This method transform prices of assets in planification
    def TransformPricesPlanif(self, data_hedge, **kwargs):
        data_hedge=data_hedge.loc[data_hedge['en_planif']=='Oui']
        data_hedge.reset_index(drop=True, inplace=True)
        #create a list containing assets under ppa contracts
        ppa=['Ally Bessadous', 'Ally Mercoeur', 'Ally Monteil', 
             'Ally Verseilles', 'Chépy', 'La citadelle', 'Nibas', 
             'Plouguin', 'Mazagran', 'Pézènes-les-Mines']
        #To create subset of solar and wind power
        data_hedge_wp=data_hedge.loc[(data_hedge['technologie']=='éolien')]
        data_hedge_sol=data_hedge.loc[(data_hedge['technologie']=='solaire')]
        #To remove ppa from solar and wind power
        data_hedge_sol=data_hedge_sol[data_hedge_sol['projet'].isin(ppa) == False]
        data_hedge_wp=data_hedge_wp[data_hedge_wp['projet'].isin(ppa) == False]

        data_hedge_sol=data_hedge_sol.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
        n_sol=len(data_hedge_sol)
        data_hedge_wp=data_hedge_wp.iloc[:,np.r_[1, 2, 3, 5, 6, 7]]
        n_wp=len(data_hedge_wp)
        print('create solar & wind power dfs:\n')
        #create a df solar
        d1=CreateMiniDataFrame(data_hedge_sol, '01-01-2022', n=n_sol, a=0, b=12*7, date='date')   
        d1.reset_index(drop=True, inplace=True)
        #create a df wind power
        d2=CreateMiniDataFrame(data_hedge_wp, '01-01-2022', n=n_wp, a=0, b=12*7, date='date')   
        d2.reset_index(drop=True, inplace=True)
        #To create quarter and month columns
        d1['année'] = d1['date'].dt.year
        d1['trimestre'] = d1['date'].dt.quarter
        d1['mois'] = d1['date'].dt.month
        d2['année'] = d2['date'].dt.year 
        d2['trimestre'] = d2['date'].dt.quarter
        d2['mois'] = d2['date'].dt.month
        #Create price column
        d1.loc[d1['type_hedge']=='CR', 'price'] = 60
        d2.loc[d2['type_hedge']=='CR', 'price'] = 70
        #To merge hedge_vmr and hedge_planif
        d=MergeDataFrame(d1, d2)
        #To remove price based on date_debut and date_fin
        prices_planif=RemoveContractPrices(data=d, sd='date_debut', ed='date_fin', price='price',
                                      th='type_hedge', date='date', projetid='projet_id', 
                                      hedgeid='hedge_id')
        
        prices_planif=SelectColumns(prices_planif, ) 
        
        return prices_planif
        
    def Load(self, target_pth, src_data):
        src_data.to_csv(target_pth, ignore_index=True)
        
    loaded_data=Load(target_pth='D:/blx_mdp/cwd/in/prices_planif.txt', src_data=TransformPricesPlanif())
    
    #This method transform prices of assets in planification
    def TransformPricesProd(self, data, **kwargs):
        
        return prices_prod
        


class_obj=ETLContractPrices()
data_hedge, data_price=ExtractData('D:/blx_mdp/cwd/in/'+'template_hedge.xlsx', 
                                       'D:/blx_mdp/cwd/in/'+'template_prices.xlsx')
