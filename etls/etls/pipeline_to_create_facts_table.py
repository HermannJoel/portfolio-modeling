# -*- coding: utf-8 -*-
"""
Created on Sun Sep 18 15:57:19 2022

@author: hermann.ngayap
"""
import pandas as pd
import os
pd.options.display.float_format = '{:.0f}'.format
pd.set_option('display.max_columns', 200)

ncwd='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
try:
    os.chdir(ncwd)
    print('the working directory has been changed!')
    print('wd: %s ' % os.getcwd())
except NotADirectoryError:
    print('you have not chosen directory!')
except FileNotFoundError:
    print('the folder was not found. the path is incorect!')
except PermissionError:
    print('you do not have access to this folder/file!')

path_dir_in='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/in/'
path_dir_temp='C:/Users/hermann.ngayap/Boralex/Marchés Energie - FR - Equipe Marchés - Gestion de portefeuille/temp/'

#To import p50_P90_asset, p50_P90_hedge and contract prices(strike prices OA, CR, PPA)   
#To rename columns to merge the 3 data frames
prod_asset=pd.read_excel(path_dir_in+'p50_P90_asset_vmr_planif.xlsx')
prod_hedge=pd.read_excel(path_dir_in+'p50_P90_hedge_vmr_planif.xlsx')
cont_prices=pd.read_excel(path_dir_in+'contracts_prices_oa_cr_ppa.xlsx')
mark_prices=pd.read_excel(path_dir_in+'hedge_id_settlement_prices.xlsx')

prod_asset.columns=[str(col) + '_a' for col in prod_asset.columns]
prod_hedge.columns=[str(col) + '_h' for col in prod_hedge.columns]
cont_prices.columns=[str(col) + '_cp' for col in cont_prices.columns]
mark_prices.columns=[str(col) + '_mp' for col in mark_prices.columns]

print(prod_asset.shape)
print(prod_hedge.shape)
print(cont_prices.shape)
print(mark_prices.shape)

#To merge p_50, P_90 asset and p_50, P90_hedge
a_h=pd.merge(prod_hedge, prod_asset, 
                 left_on=['hedge_id_h', 'projet_id_h', 'date_h','année_h', 'trim_h'], 
                 right_on=['asset_id_a', 'projet_id_a', 'date_a','année_a', 'trim_a'], 
                 how='outer')
print(a_h.shape)

#To merge p_50, p_50, P90_hedge and contract prices(OA, CR, PPA)
a_h_cont_p=pd.merge(a_h, cont_prices,
                      left_on=['hedge_id_h', 'projet_id_h', 'date_h','année_h'], 
                      right_on=['hedge_id_cp', 'projet_id_cp', 'date_cp','année_cp'],
                       how='outer'
                      )
print(a_h_cont_p.shape)

#To merge p_50, p_50, P90_hedge and contract prices(OA, CR, PPA) and market prices
a_h_cont_mark_p=pd.merge(a_h_cont_p, mark_prices,
                      left_on=['hedge_id_h', 'projet_id_h', 'date_h','année_h'], 
                      right_on=['hedge_id_mp', 'projet_id_mp', 'delivery_period_mp','years_mp'],
                       how='outer'
                      )

#To select specific columns
a_h_cont_mark_p_df=a_h_cont_mark_p[['rw_id_h', 'hedge_id_h', 'asset_id_a','projet_id_h', 'projet_h', 
                                 'type_hedge_h', 'date_h', 'année_h', 'trimestre_cp', 'mois_h',
                                 'p50_adj_h', 'p90_adj_h','delivery_period_mp', 'p50_adj_a',
                                 'p90_adj_a', 'settlement_price_mp', 'price_cp'
                                ]]
#To rename columns
a_h_cont_mark_p_df.rename(columns={'rw_id_h':'id', 'hedge_id_h':'hedge_id','asset_id_a':'aset_id',
                                'projet_id_h':'projet_id', 'projet_h':'projet','date_h':'date',
                                'année_h':'année', 'trimestre_cp':'trimestre', 'mois_h':'mois',
                                'p50_adj_a':'p50_asset', 'p90_adj_a':'p90_asset','p50_adj_h':'p50_hedge',
                                'p90_adj_h':'p90_hedge','delivery_period_mp':'delivery_period',
                                'price_cp':'contract_price','settlement_price_mp':'settlement_price',
                                'type_hedge_h':'type_hedge'   
                               }, inplace=True)

#To fix data type
a_h_cont_mark_p_df['date']=pd.to_datetime(a_h_cont_mark_p_df.date)
a_h_cont_mark_p_df['date']=a_h_cont_mark_p_df['date'].dt.date

a_h_cont_mark_p_df['delivery_period']=pd.to_datetime(a_h_cont_mark_p_df.delivery_period)
a_h_cont_mark_p_df['delivery_period']=a_h_cont_mark_p_df['delivery_period'].dt.date

a_h_cont_mark_p_df['aset_id']=pd.to_numeric(a_h_cont_mark_p_df['aset_id'])
a_h_cont_mark_p_df['settlement_price']=pd.to_numeric(a_h_cont_mark_p_df['settlement_price'])

#To export as excel file containing p_50, P_90 asset, p_50, P90_hedge and contract prices(OA, CR, PPA)
a_h_cont_mark_p_df.to_excel(path_dir_in+'table_faits.xlsx', index=False, float_format="%.3f")

