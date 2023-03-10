import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
xrange = range
import warnings
import os
import pathlib

def RemoveP50P90TypeHedge(data, *args, **kwargs):
    """
    To remove p50 p90 values based on date_debut and date_fin
    condition:The date value is less than date_debut and higher than date_fin    
*Args:
    data (DataFrame) :
    sd (str) : The arg takes the value 'date_debut' 
    ed (str) : The arg takes the value 'date_fin'
    p50 (str) : The arg takes the value 'p50_adj'
    p90 (str) : The arg takes the value of the column label 'p90_adj'
    th (str) : The arg takes the value 'type_hedge'
    date (str) : The arg takes the value 'date'
    projetid (str) : The arg takes the value 'projet_id'
    hedgeid (str) :  The arg takes the value 'hedge_id'
    
Parameters:
    cond : (condition 1) 'date' column is less (in total seconds) than the given projet_id's first 'date_debut' value 
    cond_2 : (condition 2) 'date' column is higher (in total seconds) than the given projet_id's first 'date_fin' value
    """
    cond=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['sd']].transform('first')).dt.total_seconds())<0
    data[kwargs['p50']] = np.where(cond,'', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond,'', data[kwargs['p90']])
    #To remove type of hedge based on date cod
    data[kwargs['th']]=np.where(cond,'', data[kwargs['th']])
    #To remove p50 p90 based on date_fin
    cond_2=((data[kwargs['date']] - data.groupby([kwargs['projetid'], kwargs['hedgeid']])[kwargs['ed']].transform('first')).dt.total_seconds())>0
    data[kwargs['p50']] = np.where(cond_2, '', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond_2, '', data[kwargs['p90']])
    #To remove type_hedge based on date_fin
    data[kwargs['th']]=np.where(cond_2,'', data[kwargs['th']])
    #To reset index
    data.reset_index(inplace=True, drop=True)
    data=data.assign(id=[1 + i for i in xrange(len(data))])[['id'] + data.columns.tolist()]
    return data

def RemoveP50P90(data, *args, **kwargs):
    """
    To remove p50 p90 values based on date_debut and date_fin
    condition:The date value is prior to date_debut and post to date_fin    
*Args:
    data (DataFrame) :
    cod (str) : The arg takes the value 'cod' 
    dd (str) : The arg takes the value 'date_dementelement'
    p50 (str) : The arg takes the value 'p50_adj'
    p90 (str) : The arg takes the value of the column label 'p90_adj'
    date (str) : The arg takes the value 'date'
    projetid (str) : The arg takes the value 'projet_id'
    assetid (str) :  The arg takes the value 'asset_id'
    
Parameters:
    cond : (condition 1) 'date' column is less (in total seconds) than a given projet_id's first 'date_debut' value 
    cond_2 : (condition 2) 'date' column is higher (in total seconds) than a given projet_id's first 'date_fin' value
    """
    cond=((data[kwargs['date']] - data.groupby(kwargs['projetid'])[kwargs['cod']].transform('first')).dt.total_seconds())<0
    data[kwargs['p50']] = np.where(cond,'', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond,'', data[kwargs['p90']])
    #To remove p50 p90 based on date_fin
    cond_2=((data[kwargs['date']] - data.groupby(kwargs['projetid'])[kwargs['dd']].transform('first')).dt.total_seconds())>0
    data[kwargs['p50']] = np.where(cond_2, '', data[kwargs['p50']])
    data[kwargs['p90']] = np.where(cond_2, '', data[kwargs['p90']])
    #To reset index
    data.reset_index(inplace=True, drop=True)
    data=data.assign(id=[1 + i for i in xrange(len(data))])[['id'] + data.columns.tolist()]
    return data

def CreateDataFrame(data, *args, **kwargs):
    """
    To create a DataFrame containing p50 and P90 across our time horizon     
    args:
    data (DataFrame) :
    
    *args: non-keyworded arguments
        sd (str) : Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
    **kwargs : keyworded arguments
        a (int) : Takes the value 0
        b (int) : Takes the value of the length of our horizon (12*7)
        profile (dictionaries) : The arg takes the value of the production profile
        n (int) : The arg takes the value length of data 
        date (str) : The arg takes the value of date colum label 'date'
    """
    pd.options.display.float_format = '{:.5f}'.format
    start_date=pd.to_datetime(args*kwargs['n'])
    d=pd.DataFrame()
    for i in range(kwargs['a'], kwargs['b']):
        data.loc[:, kwargs['date']]=start_date
        list_p50=[]
        list_p90=[]
        for elm in data.projet_id:
            try: 
                list_p50.append(-kwargs['profile'][elm][start_date.month[0]-1]*float(data[data.projet_id==elm]["p50"].values[0]))
            except:
                list_p50.append("NA")
            try:
                list_p90.append(-kwargs['profile'][elm][start_date.month[0]-1]*float(data[data.projet_id==elm]["p90"].values[0]))
            except:
                list_p90.append("NA")
        data["p50_adj"]=list_p50
        data["p90_adj"]=list_p90
        d=pd.concat([d, data],axis=0)
        start_date=start_date + pd.DateOffset(months=1) 
    return d 


def AdjustedByPct(data, **kwargs):
    """
    To compute adjusted p50 & p90 by hedge percentage (pct_couverture)    
Args:
    data (DataFrame) :
    col1 (str) : Takes the value p50_adj column label
    col2 (str) : Takes the value of pct_couverture column label
    """   
    return round(data[kwargs['col1']], 4) * round(data[kwargs['col2']], 4)

def MergeDataFrame(*args):
    """
    To merge df    
Args:
    **kwargs : 
    """
    frames=args
    merged_df=pd.concat(frames)
    merged_df.reset_index(drop=True, inplace=True)
    return  merged_df


def ChooseCwd(**kwargs):
    try:
        os.chdir(kwargs['cwd'])
        print('the working directory has been changed!') 
        print('cwd: %s ' % os.getcwd())
    except NotADirectoryError():
        print('you have not chosen directory!')
    except FileNotFoundError():
        print('the folder was not found. the path is incorect!')
    except PermissionError():
        print('you do not have access to this folder/file!')
        

def ReadExcelFile(path, **kwargs):
    ext=pathlib.Path(path).suffix
    if ext in ['.xlsx', '.xls', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
        return pd.read_excel(path, **kwargs)
    else: 
        return pd.read_csv(path, **kwargs)
    
   
def format_float(df, column, decimals=2):
    df[column] = df[column].apply(lambda x: f"{x:,.{decimals}f}")
    return df
 
def SelectColumns(data, *args):
    columns=args
    list=[]
    for i in columns:
        list.append(i)
    selection=data[list]
    return selection

def CreateMiniDataFrame(data, *args, **kwargs):
    """
    To create a DataFrame containing p50 and P90 across our time horizon     
    args:
    data (DataFrame) : 
    *args: non-keyworded arguments
        sd (str) : Takes the value of the start of the horizon  dd-mm-yyyy  '01-01-2022'
    **kwargs : keyworded arguments
        a (int) : Takes the value 0
        b (int) : Takes the value of the length of our horizon (12*7)
        n (int) : The arg takes the value length of data 
        date (str) : The arg takes the value of date colum label 'date'
    """
    start_date=pd.to_datetime(args*kwargs['n'])
    d=pd.DataFrame()
    for i in range(kwargs['a'], kwargs['b']):
        data.loc[:, kwargs['date']]=start_date
        d=pd.concat([d, data],axis=0)
        start_date=start_date + pd.DateOffset(months=1)
    return d


def dis_warn():
    warnings.warn("deprecated", DeprecationWarning)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
    
def postgressql_engine(): 
    engine = create_engine('postgresql+psycopg2://postgres:24Fe1988@localhost:5432/blxmdpdwdev') 
    return engine

def RemoveContractPrices(data, *args, **kwargs):
    """
    To remove Contract prices values based on date_debut and date_fin
    condition:The date value is prior to date_debut and post to date_fin    
Args:
    data (DataFrame) :
    date_debut (str) : The arg takes the value 'date_debut' 
    date_fin (str) : The arg takes the value 'date_fin'
    price (str) : The arg takes the value 'contract_price'
    date (str) : The arg takes the value 'date'
    projetid (str) : The arg takes the value 'projet_id'
    hedgeid (str) :  The arg takes the value 'hedge_id'
    th  (str) : Type hedge   
Parameters:
    cond : (condition 1) 'date' column is less (in total seconds) than a given projet_id's first 'date_debut' value 
    cond_2 : (condition 2) 'date' column is higher (in total seconds) than a given projet_id's first 'date_fin' value
    """
    cond=((data[kwargs['date']] - data.groupby(kwargs['projetid'], kwargs['hedgeid'])[kwargs['date_debut']].transform('first')).dt.total_seconds())<0
    data[kwargs['price']] = np.where(cond,'', data[kwargs['price']])
    #To remove type of hedge based on date cod
    data[kwargs['th']]=np.where(cond,'', data[kwargs['th']])
    #To remove p50 p90 based on date_fin
    cond_2=((data[kwargs['date']] - data.groupby(kwargs['projetid'], kwargs['hedgeid'])[kwargs['date_fin']].transform('first')).dt.total_seconds())>0
    data[kwargs['price']] = np.where(cond_2, '', data[kwargs['price']])
    #To remove type of hedge based on date cod
    data[kwargs['th']]=np.where(cond,'', data[kwargs['th']])
    #To reset index
    data.reset_index(inplace=True, drop=True)
    data=data.assign(id=[1 + i for i in xrange(len(data))])[['id'] + data.columns.tolist()]
    return data


def Extract():
    try:
        directory=dir
        for filename in os.listdir(dir):
            files_wo_ext=os.path.splitext(filename)[0]
            if filename.endswith(".xlsx"):
                f=os.path.join(directory, filename)
                if os.path.isfile(f):
                    df=pd.read_excel(f)
    except Exception as e:
        eml.SendEmail(to, "File upload, data extract error!: ", f"Data Extract Error: File location {dir}" + str(e))
        print("Data Extract error!: "+str(e))
        
"""
from email.mine.text import MIMEText
from email.mine.application import MIMEApplication
from email.mine.multipart import MIMEMultipart
from email.mine.base import MIMEBase
from email.message import EmailMessage
from email import encoders
import smtplib

sender=hermannjoel.ngayap@yahoo.fr
to
"""
        
        
def SendEmail(to, subject, content):
    message=MIMEMultipart()
    message["Subject"]=subject
    message["From"]=sender
    message["To"]=to
    
    body_content=content
    message.attach(MiMEText(body_content, "html"))
    msg_body=message.as_string()
    
    server=smtplib.SMTP("localhost")
    server.login(email, password)
    server.sendmail(sender, to, msg_body)
    
    server.quit()