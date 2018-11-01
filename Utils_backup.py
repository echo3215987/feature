import pandas as pd
import numpy as np
import pyodbc
import re
import sqlite3
import time
import datetime
import numpy as npweb_query
from IPython.display import display, HTML
from IPython.display import clear_output
from dask import dataframe as dd
from threading import Thread,Lock
import os
import math
#from bokeh.io import push_notebook, show, output_notebook
from bokeh.layouts import row
from bokeh.plotting import figure
from bokeh.models import LabelSet
import bokeh
from bokeh.layouts import row
#output_notebook(bokeh.resources.INLINE)
#import matplotlib
#import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import urllib #import for writing data to RDB
from sqlalchemy import create_engine #import for writing data to RDB
#plt.style.use('ggplot')
#get_ipython().magic('matplotlib inline')

from main import main

# In[3]:
def write_Log(Log_File, Log_Str):
    Log = open(Log_File, 'a')
    Log.write(Log_Str)
    Log.close()


# In[5]:
# Define Function
def series_str_cleaner(inpurt_series):
    inpurt_series = inpurt_series.fillna('')
    inpurt_series = inpurt_series.map(lambda x: x if (isinstance(x, float) or x is None) else x.replace(' ', ''))
    inpurt_series = inpurt_series.map(lambda x: '' if pd.isnull(x) else re.sub('[^0-9a-zA-Z]+', '', x).upper())
    inpurt_series = inpurt_series.astype(str)
    return inpurt_series


def time_cleaner(Input_Series):
    temp = pd.DataFrame()  # 由
    temp[u'DT'] = Input_Series.map(lambda x: x if (isinstance(x, float) or x is None) else x.replace(' ', ''))
    temp[u'DT_fix'] = pd.to_datetime(temp[u'DT'], format='%Y%m%d', errors='coerce')
    return temp[u'DT_fix']


# def for 30Features_construction_FOR_FIRST_TABLE
# 對相關日期 做跨車輛的資訊計算 max min count range
def LRDT_calculator_dask(df_input, groupby_column, cal_column_name):
    data = dd.from_pandas(df_input, npartitions=8)
    agg_methods = ['min', 'max', 'count']
    result = data.groupby(groupby_column)[cal_column_name].agg(agg_methods)
    result['range'] = result['max'] - result['min']
    result['range'] = result['range'].map(lambda x: None if pd.isnull(x) else x.days)
    result['min'] = (pd.Timestamp(main.END_DATE) - result['min']).map(lambda x: None if pd.isnull(x) else x.days)
    result['max'] = (pd.Timestamp(main.END_DATE) - result['max']).map(lambda x: None if pd.isnull(x) else x.days)
    df_result = result.compute()  # backto PANDAS
    df_result.columns = [cal_column_name + '_min', cal_column_name + '_max', cal_column_name + '_count',
                         cal_column_name + '_range']
    return df_result


def year3_LRDT_calculator_dask(df_input, groupby_column, cal_column_name):
    data = dd.from_pandas(df_input, npartitions=8)
    agg_methods = ['count']
    result = data.groupby(groupby_column)[cal_column_name].agg(agg_methods)
    df_result = result.compute()  # backto PANDAS
    df_result.columns = [cal_column_name + '_count_3year']
    return df_result


# date max min count range
def IS_calculator_dask(df_input, groupby_column, cal_column_name):
    data = dd.from_pandas(df_input, npartitions=8)
    agg_methods = ['min', 'max', 'count']
    result = data.groupby(groupby_column)[cal_column_name].agg(agg_methods)
    result['range'] = result['max'] - result['min']
    result['range'] = result['range'].map(lambda x: None if pd.isnull(x) else x.days)
    result['min'] = (pd.Timestamp(main.END_DATE) - result['min']).map(lambda x: None if pd.isnull(x) else x.days)
    result['max'] = (pd.Timestamp(main.END_DATE) - result['max']).map(lambda x: None if pd.isnull(x) else x.days)
    df_result = result.compute()  # backto PANDAS
    df_result.columns = [cal_column_name + '_min', cal_column_name + '_max', cal_column_name + '_count',
                         cal_column_name + '_range']
    return df_result


# '''自動判斷連續型離散型等基本特徵屬性'''
def dataframe_quick_look(df_input):
    df_feature_desc = pd.DataFrame()
    df_feature_desc[u'columns'] = df_input.columns
    df_feature_desc[u'dtypes'] = df_input.dtypes.reset_index(drop=True)
    df_feature_desc[u'value_counts'] = df_feature_desc[u'columns'].map(lambda x: df_input[x].value_counts().shape[0])
    df_feature_desc[u'inNA'] = df_input.isnull().any().reset_index(drop=True)
    df_feature_desc[u'IsCont'] = df_feature_desc[u'value_counts'].map(lambda x: True if x > 20 else False)
    df_feature_desc[u'NA_Rate'] = df_feature_desc[u'columns'].map(
        lambda x: float(df_input[df_input[x].isnull()].shape[0]) / float(df_input.shape[0]))
    df_feature_desc[u'cate>20'] = df_feature_desc[u'value_counts'].map(lambda x: True if x > 20 else False)
    return df_feature_desc


# In[6]:
# define class， for mutithread run SQL select
class DatabaseWorker(Thread):
    __lock = Lock()

    def __init__(self, query, result_queue):
        Thread.__init__(self)
        self.query = query
        self.result_queue = result_queue
        self.return_index = -1

    def run(self):
        result = None
        try:
            conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
            result = pd.read_sql(self.query, conn)
            conn.close()
        except:
            print("Error..")
        self.result_queue.append(result)
        self.return_index = len(main.result_queue) - 1


#         print(len(result_queue)-1)
#         print (self.query)
#         return(len(result_queue)-1)

# In[7]:
# SQL code
def SQL_df_CRCAMF():
    df_CRCAMF = pd.DataFrame()
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRCAMF = pd.read_sql(u"""
        SELECT * FROM cdp.CRCAMF  WHERE (FRAN in ('L','T') or  
        upper(CARNM) like '%TOYOTA%' or  
        upper(CARNM) like '%CORONA%' or  
        upper(CARNM) like '%EXSIOR%' or  
        upper(CARNM) like '%PREMIO%' or  
        upper(CARNM) like '%國瑞%' or  
        upper(CARNM) like '%豐田%' or  
        upper(CARNM) like '%LS400%' or  
        upper(CARNM) like '%IS250%' or  
        upper(CARNM) like '%ZACE%' or  
        upper(CARNM) like '%CAMRY%' or  
        upper(CARNM) like '%VIOS%' or  
        upper(CARNM) like '%COROLLA%' or  
        upper(CARNM) like '%SIENNA%' or  
        upper(CARNM) like '%LEXUS%' or  
        upper(CARNM) like '%ALTIS%' or  
        upper(CARNM) like '%GS300%' or  
        upper(CARNM) like '%ES300%' or  
        upper(CARNM) like '%CROWN%' or  
        upper(CARNM) like '%TERCEL%' or  
        upper(CARNM) like '%WISH%' or  
        upper(CARNM) like '%CT200H%' or  
        upper(CARNM) like '%RX330%' or  
        upper(CARNM) like '%RX300%' or  
        upper(CARNM) like '%ES350%' or  
        upper(CARNM) like '%TERCEL%'  ) and upper(CARNM) not like '%DYNA%' and upper(CARNM) not like '%HINO%'""", conn)
    conn.close()
    return df_CRCAMF


def SQL_df_SSHSCHISTORY():
    df_SSHSCHISTORY = pd.DataFrame()
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SSHSCHISTORY = pd.read_sql(u"""
        SELECT * FROM cdp.SSHSCHISTORY WHERE
        ( UPPER(BRAND) like '%KUIZUO%' or 
          UPPER(BRAND) like '%KUO%' or 
          UPPER(BRAND) like '%KOUZUI%'or
          UPPER(BRAND) like '%TOROTA%'  or
          UPPER(BRAND) like '%KUOZUI%' or 
          UPPER(BRAND) like '%TOYOTA%' or 
          UPPER(BRAND) like '%7TOYOTA%' or 
          UPPER(BRAND) like '%AMCTOYOTA%' or 
          UPPER(BRAND) like '%AMCTPYPTA%' or 
          UPPER(BRAND) like '%CAMRY%' or 
          UPPER(BRAND) like '%CAMRYLE%' or 
          UPPER(BRAND) like '%CORLLA%' or 
          UPPER(BRAND) like '%COROLLA%' or 
          UPPER(BRAND) like '%CORONA%' or 
          UPPER(BRAND) like '%LESUS%' or 
          UPPER(BRAND) like '%LEXUS%' or 
          UPPER(BRAND) like '%OYOTA%' or
          UPPER(BRAND) like '%ROYOTA%' or 
          UPPER(BRAND) like '%TOOYTA%' or 
          UPPER(BRAND) like '%TOROTA%' or 
          UPPER(BRAND) like '%TOTOTA%' or 
          UPPER(BRAND) like '%TOTYOTA%' or 
          UPPER(BRAND) like '%TOTYTA%' or 
          UPPER(BRAND) like '%TOUOTA%' or 
          UPPER(BRAND) like '%TOY0TA%' or 
          UPPER(BRAND) like '%TOYATA%' or 
          UPPER(BRAND) like '%TOYATO%' or 
          UPPER(BRAND) like '%TOYOA%' or 
          UPPER(BRAND) like '%TOYOPTA%' or 
          UPPER(BRAND) like '%TOYOT%' or 
          UPPER(BRAND) like '%TOYOTA%' or 
          UPPER(BRAND) like '%TOYOTO%' or 
          UPPER(BRAND) like '%TOYOTYA%' or 
          UPPER(BRAND) like '%TOYOYA%' or 
          UPPER(BRAND) like '%TOYPTA%' or 
          UPPER(BRAND) like '%TOYTA%' or 
          UPPER(BRAND) like '%TOYTOA%' or 
          UPPER(BRAND) like '%TOYUOTA%' or 
          UPPER(BRAND) like '%TPYPTA%' or 
          UPPER(BRAND) like '%TTOYOTA%' or 
          UPPER(BRAND) like '%TYOOTA%' or 
          UPPER(BRAND) like '%TYOYTA%' or 
          UPPER(BRAND) like '%TYYOTA%' or 
          UPPER(BRAND) like '%YOTOTA%' or 
          UPPER(BRAND) like '%YOYOTA%' or 
          UPPER(BRAND) like '%amcTOYOTA%' or 
          UPPER(BRAND) like '%toyota%' or 
          UPPER(BRAND) like '%國%' or 
          UPPER(BRAND) like '%豐%' or 
          UPPER(BRAND) like '%瑞%' or 
          UPPER(GRPNM) like '%COROLLA%' or
          UPPER(GRPNM) like '%CAMRY%' or
          UPPER(GRPNM) like '%4RUNNER%' or
          UPPER(GRPNM) like '%GS450%' or
          UPPER(GRPNM) like '%IS300%' or
          UPPER(GRPNM) like '%LX470%' or
          UPPER(GRPNM) like '%RX400%' or
          UPPER(GRPNM) like '%LANDCRUISER%' or
          UPPER(GRPNM) like '%PRIUS%' or
          UPPER(GRPNM) like '%CT200%' or
          UPPER(GRPNM) like '%LS460%' or
          UPPER(GRPNM) like '%SIENNA%' or
          UPPER(GRPNM) like '%GS430%' or
          UPPER(GRPNM) like '%GS350%' or
          UPPER(GRPNM) like '%SC430%' or
          UPPER(GRPNM) like '%RX450%' or
          UPPER(GRPNM) like '%TACOMA%' or
          UPPER(GRPNM) like '%SUPRA%' or
          UPPER(GRPNM) like '%ES330%' or
          UPPER(GRPNM) like '%ES350%' or
          UPPER(GRPNM) like '%RX350%' or
          UPPER(GRPNM) like '%ES300%' or
          UPPER(GRPNM) like '%CRESSIDA%' or
          UPPER(GRPNM) like '%CROWN%' or
          UPPER(GRPNM) like '%PREVIA%' or
          UPPER(GRPNM) like '%IS250%' or
          UPPER(GRPNM) like '%CARINA%' or
          UPPER(GRPNM) like '%IS200%' or
          UPPER(GRPNM) like '%CELICA%' or
          UPPER(GRPNM) like '%RX300%' or
          UPPER(GRPNM) like '%LS430%' or
          UPPER(GRPNM) like '%RX330%' or
          UPPER(GRPNM) like '%STARLET%' or
          UPPER(GRPNM) like '%INNOVA%' or
          UPPER(GRPNM) like '%GS300%' or
          UPPER(GRPNM) like '%LS400%' or
          UPPER(GRPNM) like '%RAV4%' or
          UPPER(GRPNM) like '%HILUX%' or
          UPPER(GRPNM) like '%YARIS%' or
          UPPER(GRPNM) like '%AVALON%' or
          UPPER(GRPNM) like '%HIACE%' or
          UPPER(GRPNM) like '%WISH%' or
          UPPER(GRPNM) like '%VIOS%' or
          UPPER(GRPNM) like '%TERCEL%' or
          UPPER(GRPNM) like '%CAMRY%' or
          UPPER(GRPNM) like '%ZACE%' or
          UPPER(GRPNM) like '%COROLLA%' or
          UPPER(GRPNM) like '%CORONA%' or
          UPPER(MODELM) Like '%4RUNNER%' or
          UPPER(MODELM) Like '%ALTIS%' or
          UPPER(MODELM) Like '%AT2EMD%' or
          UPPER(MODELM) Like '%AT2EMN%' or
          UPPER(MODELM) Like '%AT2EPD%' or
          UPPER(MODELM) Like '%AT2EPN%' or
          UPPER(MODELM) Like '%AT2LMN%' or
          UPPER(MODELM) Like '%AVALON%' or
          UPPER(MODELM) Like '%CAMRY%' or
          UPPER(MODELM) Like '%CARINA%' or
          UPPER(MODELM) Like '%CELICA%' or
          UPPER(MODELM) Like '%COROLLA%' or
          UPPER(MODELM) Like '%CORONA%' or
          UPPER(MODELM) Like '%CRESSIDA%' or
          UPPER(MODELM) Like '%CROWN%' or
          UPPER(MODELM) Like '%CT200%' or
          UPPER(MODELM) Like '%EL1EHD%' or
          UPPER(MODELM) Like '%ES300%' or
          UPPER(MODELM) Like '%ES350%' or
          UPPER(MODELM) Like '%GS300%' or
          UPPER(MODELM) Like '%GS350%' or
          UPPER(MODELM) Like '%GS430%' or
          UPPER(MODELM) Like '%GS450%' or
          UPPER(MODELM) Like '%HIACE%' or
          UPPER(MODELM) Like '%HILUX%' or
          UPPER(MODELM) Like '%INNOVA%' or
          UPPER(MODELM) Like '%IS200%' or
          UPPER(MODELM) Like '%IS250%' or
          UPPER(MODELM) Like '%IS300%' or
          UPPER(MODELM) Like '%LANDCRUISER%' or
          UPPER(MODELM) Like '%LS400%' or
          UPPER(MODELM) Like '%LS430%' or
          UPPER(MODELM) Like '%LS460%' or
          UPPER(MODELM) Like '%LX470%' or
          UPPER(MODELM) Like '%MARK2%' or
          UPPER(MODELM) Like '%MR2%' or
          UPPER(MODELM) Like '%PREMIO%' or
          UPPER(MODELM) Like '%PREVIA%' or
          UPPER(MODELM) Like '%PRIUS%' or
          UPPER(MODELM) Like '%RAV4%' or
          UPPER(MODELM) Like '%RX270%' or
          UPPER(MODELM) Like '%RX300%' or
          UPPER(MODELM) Like '%RX330%' or
          UPPER(MODELM) Like '%RX350%' or
          UPPER(MODELM) Like '%RX400%' or
          UPPER(MODELM) Like '%RX450%' or
          UPPER(MODELM) Like '%SC430%' or
          UPPER(MODELM) Like '%SIENNA%' or
          UPPER(MODELM) Like '%ST2EMN%' or
          UPPER(MODELM) Like '%ST2EPM%' or
          UPPER(MODELM) Like '%ST2EPN%' or
          UPPER(MODELM) Like '%ST2LMN%' or
          UPPER(MODELM) Like '%ST2LPM%' or
          UPPER(MODELM) Like '%STARLET%' or
          UPPER(MODELM) Like '%SUPRA%' or
          UPPER(MODELM) Like '%TACOMA%' or
          UPPER(MODELM) Like '%TERCEL%' or
          UPPER(MODELM) Like '%TL1EMN%' or
          UPPER(MODELM) Like '%TL1EPN%' or
          UPPER(MODELM) Like '%VIOS%' or
          UPPER(MODELM) Like '%WISH%' or
          UPPER(MODELM) Like '%WISH%' or
          UPPER(MODELM) Like '%YARIS%' or
          UPPER(MODELM) Like '%ZACE%') and GRPNM in ('RX200T',
          'ES250','NX300','IS250C','GS460','IS200T','ALPHARD','NX200T',
          'ES240','LS600','GS250','COASTER','MARK2','R19','86','PRIUS C',
          'RX270','4RUNNER','GS450','CHARADE','IS300','LX470','RX400',
          'REXTON','LANDCRUISER','PRIUS','MR2','CT200','LS460','SIENNA',
          'GS430','GS350','SC430','RX450','TACOMA','SUPRA','ES330','ES350',
          'RX350','ES300','CRESSIDA','PREVIA','CROWN','IS250','CARINA',
          'IS200','CELICA','RX300','LS430','RX330','STARLET','INNOVA',
          'GS300','LS400','RAV4','HILUX','YARIS','AVALON','HIACE','WISH',
          'VIOS','TERCEL','CAMRY','ZACE','COROLLA','CORONA') and
          GRPNM not in ('HINO','ZZZ-C','DYNA','ZZZ-P','LT-1','LT-0')""", conn)
    conn.close()
    return df_SSHSCHISTORY


def SQL_df_SSHUCHISTORY():
    df_SSHUCHISTORY = pd.DataFrame()
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SSHUCHISTORY = pd.read_sql(u"""
        SELECT BRAND, GRPNM, MODELM, BDNOM, BODYNO, EGNOM, ENGINENO, ISSUE  FROM cdp.SSHUCHISTORY WHERE 
        ( UPPER(BRAND) like '%KUIZUO%' or 
        UPPER(BRAND) like '%KUO%' or 
        UPPER(BRAND) like '%KOUZUI%'or
        UPPER(BRAND) like '%TOROTA%'  or
        UPPER(BRAND) like '%KUOZUI%' or 
        UPPER(BRAND) like '%TOYOTA%' or 
        UPPER(BRAND) like '%7TOYOTA%' or 
        UPPER(BRAND) like '%AMCTOYOTA%' or 
        UPPER(BRAND) like '%AMCTPYPTA%' or 
        UPPER(BRAND) like '%CAMRY%' or 
        UPPER(BRAND) like '%CAMRYLE%' or 
        UPPER(BRAND) like '%CORLLA%' or 
        UPPER(BRAND) like '%COROLLA%' or 
        UPPER(BRAND) like '%CORONA%' or 
        UPPER(BRAND) like '%LESUS%' or 
        UPPER(BRAND) like '%LEXUS%' or 
        UPPER(BRAND) like '%OYOTA%' or
        UPPER(BRAND) like '%ROYOTA%' or 
        UPPER(BRAND) like '%TOOYTA%' or 
        UPPER(BRAND) like '%TOROTA%' or 
        UPPER(BRAND) like '%TOTOTA%' or 
        UPPER(BRAND) like '%TOTYOTA%' or 
        UPPER(BRAND) like '%TOTYTA%' or 
        UPPER(BRAND) like '%TOUOTA%' or 
        UPPER(BRAND) like '%TOY0TA%' or 
        UPPER(BRAND) like '%TOYATA%' or 
        UPPER(BRAND) like '%TOYATO%' or 
        UPPER(BRAND) like '%TOYOA%' or 
        UPPER(BRAND) like '%TOYOPTA%' or 
        UPPER(BRAND) like '%TOYOT%' or 
        UPPER(BRAND) like '%TOYOTA%' or 
        UPPER(BRAND) like '%TOYOTO%' or 
        UPPER(BRAND) like '%TOYOTYA%' or 
        UPPER(BRAND) like '%TOYOYA%' or 
        UPPER(BRAND) like '%TOYPTA%' or 
        UPPER(BRAND) like '%TOYTA%' or 
        UPPER(BRAND) like '%TOYTOA%' or 
        UPPER(BRAND) like '%TOYUOTA%' or 
        UPPER(BRAND) like '%TPYPTA%' or 
        UPPER(BRAND) like '%TTOYOTA%' or 
        UPPER(BRAND) like '%TYOOTA%' or 
        UPPER(BRAND) like '%TYOYTA%' or 
        UPPER(BRAND) like '%TYYOTA%' or 
        UPPER(BRAND) like '%YOTOTA%' or 
        UPPER(BRAND) like '%YOYOTA%' or 
        UPPER(BRAND) like '%amcTOYOTA%' or 
        UPPER(BRAND) like '%toyota%' or 
        UPPER(BRAND) like '%國%' or 
        UPPER(BRAND) like '%豐%' or 
        UPPER(BRAND) like '%瑞%' or 
        UPPER(GRPNM) like '%COROLLA%' or
        UPPER(GRPNM) like '%CAMRY%' or
        UPPER(GRPNM) like '%4RUNNER%' or
        UPPER(GRPNM) like '%GS450%' or
        UPPER(GRPNM) like '%IS300%' or
        UPPER(GRPNM) like '%LX470%' or
        UPPER(GRPNM) like '%RX400%' or
        UPPER(GRPNM) like '%LANDCRUISER%' or
        UPPER(GRPNM) like '%PRIUS%' or
        UPPER(GRPNM) like '%CT200%' or
        UPPER(GRPNM) like '%LS460%' or
        UPPER(GRPNM) like '%SIENNA%' or
        UPPER(GRPNM) like '%GS430%' or
        UPPER(GRPNM) like '%GS350%' or
        UPPER(GRPNM) like '%SC430%' or
        UPPER(GRPNM) like '%RX450%' or
        UPPER(GRPNM) like '%TACOMA%' or
        UPPER(GRPNM) like '%SUPRA%' or
        UPPER(GRPNM) like '%ES330%' or
        UPPER(GRPNM) like '%ES350%' or
        UPPER(GRPNM) like '%RX350%' or
        UPPER(GRPNM) like '%ES300%' or
        UPPER(GRPNM) like '%CRESSIDA%' or
        UPPER(GRPNM) like '%CROWN%' or
        UPPER(GRPNM) like '%PREVIA%' or
        UPPER(GRPNM) like '%IS250%' or
        UPPER(GRPNM) like '%CARINA%' or
        UPPER(GRPNM) like '%IS200%' or
        UPPER(GRPNM) like '%CELICA%' or
        UPPER(GRPNM) like '%RX300%' or
        UPPER(GRPNM) like '%LS430%' or
        UPPER(GRPNM) like '%RX330%' or
        UPPER(GRPNM) like '%STARLET%' or
        UPPER(GRPNM) like '%INNOVA%' or
        UPPER(GRPNM) like '%GS300%' or
        UPPER(GRPNM) like '%LS400%' or
        UPPER(GRPNM) like '%RAV4%' or
        UPPER(GRPNM) like '%HILUX%' or
        UPPER(GRPNM) like '%YARIS%' or
        UPPER(GRPNM) like '%AVALON%' or
        UPPER(GRPNM) like '%HIACE%' or
        UPPER(GRPNM) like '%WISH%' or
        UPPER(GRPNM) like '%VIOS%' or
        UPPER(GRPNM) like '%TERCEL%' or
        UPPER(GRPNM) like '%CAMRY%' or
        UPPER(GRPNM) like '%ZACE%' or
        UPPER(GRPNM) like '%COROLLA%' or
        UPPER(GRPNM) like '%CORONA%' or
        UPPER(MODELM) Like '%4RUNNER%' or
        UPPER(MODELM) Like '%ALTIS%' or
        UPPER(MODELM) Like '%AT2EMD%' or
        UPPER(MODELM) Like '%AT2EMN%' or
        UPPER(MODELM) Like '%AT2EPD%' or
        UPPER(MODELM) Like '%AT2EPN%' or
        UPPER(MODELM) Like '%AT2LMN%' or
        UPPER(MODELM) Like '%AVALON%' or
        UPPER(MODELM) Like '%CAMRY%' or
        UPPER(MODELM) Like '%CARINA%' or
        UPPER(MODELM) Like '%CELICA%' or
        UPPER(MODELM) Like '%COROLLA%' or
        UPPER(MODELM) Like '%CORONA%' or
        UPPER(MODELM) Like '%CRESSIDA%' or
        UPPER(MODELM) Like '%CROWN%' or
        UPPER(MODELM) Like '%CT200%' or
        UPPER(MODELM) Like '%EL1EHD%' or
        UPPER(MODELM) Like '%ES300%' or
        UPPER(MODELM) Like '%ES350%' or
        UPPER(MODELM) Like '%GS300%' or
        UPPER(MODELM) Like '%GS350%' or
        UPPER(MODELM) Like '%GS430%' or
        UPPER(MODELM) Like '%GS450%' or
        UPPER(MODELM) Like '%HIACE%' or
        UPPER(MODELM) Like '%HILUX%' or
        UPPER(MODELM) Like '%INNOVA%' or
        UPPER(MODELM) Like '%IS200%' or
        UPPER(MODELM) Like '%IS250%' or
        UPPER(MODELM) Like '%IS300%' or
        UPPER(MODELM) Like '%LANDCRUISER%' or
        UPPER(MODELM) Like '%LS400%' or
        UPPER(MODELM) Like '%LS430%' or
        UPPER(MODELM) Like '%LS460%' or
        UPPER(MODELM) Like '%LX470%' or
        UPPER(MODELM) Like '%MARK2%' or
        UPPER(MODELM) Like '%MR2%' or
        UPPER(MODELM) Like '%PREMIO%' or
        UPPER(MODELM) Like '%PREVIA%' or
        UPPER(MODELM) Like '%PRIUS%' or
        UPPER(MODELM) Like '%RAV4%' or
        UPPER(MODELM) Like '%RX270%' or
        UPPER(MODELM) Like '%RX300%' or
        UPPER(MODELM) Like '%RX330%' or
        UPPER(MODELM) Like '%RX350%' or
        UPPER(MODELM) Like '%RX400%' or
        UPPER(MODELM) Like '%RX450%' or
        UPPER(MODELM) Like '%SC430%' or
        UPPER(MODELM) Like '%SIENNA%' or
        UPPER(MODELM) Like '%ST2EMN%' or
        UPPER(MODELM) Like '%ST2EPM%' or
        UPPER(MODELM) Like '%ST2EPN%' or
        UPPER(MODELM) Like '%ST2LMN%' or
        UPPER(MODELM) Like '%ST2LPM%' or
        UPPER(MODELM) Like '%STARLET%' or
        UPPER(MODELM) Like '%SUPRA%' or
        UPPER(MODELM) Like '%TACOMA%' or
        UPPER(MODELM) Like '%TERCEL%' or
        UPPER(MODELM) Like '%TL1EMN%' or
        UPPER(MODELM) Like '%TL1EPN%' or
        UPPER(MODELM) Like '%VIOS%' or
        UPPER(MODELM) Like '%WISH%' or
        UPPER(MODELM) Like '%WISH%' or
        UPPER(MODELM) Like '%YARIS%' or
        UPPER(MODELM) Like '%ZACE%') and 
        GRPNM in ('RX200T','ES250','NX300','IS250C','GS460','IS200T',
        'ALPHARD','NX200T','ES240','LS600','GS250','COASTER','MARK2',
        'R19','86','PRIUS C','RX270','4RUNNER','GS450','CHARADE','IS300',
        'LX470','RX400','REXTON','LANDCRUISER','PRIUS','MR2','CT200','LS460',
        'SIENNA','GS430','GS350','SC430','RX450','TACOMA','SUPRA','ES330',
        'ES350','RX350','ES300','CRESSIDA','PREVIA','CROWN','IS250','CARINA',
        'IS200','CELICA','RX300','LS430','RX330','STARLET','INNOVA','GS300',
        'LS400','RAV4','HILUX','YARIS','AVALON','HIACE','WISH','VIOS',
        'TERCEL','CAMRY','ZACE','COROLLA','CORONA') and
        GRPNM not in ('HINO','ZZZ-C','DYNA','ZZZ-P','LT-1','LT-0')""", conn)
    conn.close()
    return df_SSHUCHISTORY


# SRMINVO 發票金額分析   BY VIN   因為歷史資料或15年以上車輛 在發票上沒有登記 車號
# def getSRMINVO_query_data(query_colNM):
def getSRMINVO_query_data():
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    # SRMIVSLP發票工單對照檔  SRMSLPH工單主檔 SRMINVO發票主檔
    df_temp1 = pd.read_sql(u"""select DLRCD, BRNHCD, WORKNO, INVONO from cdp.SRMIVSLP""", conn)
    df_temp2 = pd.read_sql(
        u"""select VIN, DLRCD, BRNHCD, WORKNO from cdp.SRMSLPH where VIN!='' and VIN IS NOT NULL and CMPTDT IS NOT NULL""",
        conn)
    df_temp3 = pd.read_sql(
        u"""select INVSTS, DETRMK, INVONO, INVODT, DLRCD, BRNHCD, INVTXCD, TOTAMT, INSURCD, IRNAMT, WSHAMT from cdp.SRMINVO""",
        conn)
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'B')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'C')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'D')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'E')]
    df_temp3 = df_temp3[~(df_temp3['DETRMK'] == '*')]
    df_SRMINVO = df_temp1.merge(df_temp2, how='left', on=['DLRCD', 'BRNHCD', 'WORKNO'])
    df_SRMINVO = df_SRMINVO.merge(df_temp3, how='left', on=['DLRCD', 'BRNHCD', 'INVONO'])
    df_SRMINVO = pd.DataFrame(df_SRMINVO,
                              columns=['VIN', 'INVODT', 'DLRCD', 'BRNHCD', 'INVONO', 'INVTXCD', 'TOTAMT', 'INSURCD',
                                       'IRNAMT', 'WSHAMT'])

    df_temp1 = pd.read_sql(u"""select DLRCD, BRNHCD, WORKNO, INVONO from cdp.SRHIVSLP""", conn)
    df_temp2 = pd.read_sql(
        u"""select VIN, DLRCD, BRNHCD, WORKNO from cdp.SRHSLPH where VIN!='' and VIN IS NOT NULL and CMPTDT IS NOT NULL""",
        conn)
    df_temp3 = pd.read_sql(
        u"""select INVSTS, DETRMK, INVONO, INVODT, DLRCD, BRNHCD, INVTXCD, TOTAMT, INSURCD, IRNAMT, WSHAMT from cdp.SRHINVO""",
        conn)

    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'B')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'C')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'D')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'E')]
    df_temp3 = df_temp3[~(df_temp3['DETRMK'] == '*')]
    df_SRHINVO = df_temp1.merge(df_temp2, how='left', on=['DLRCD', 'BRNHCD', 'WORKNO'])
    df_SRHINVO = df_SRHINVO.merge(df_temp3, how='left', on=['DLRCD', 'BRNHCD', 'INVONO'])

    df_temp1 = pd.read_sql(u"""select DLRCD, BRNHCD, WORKNO, INVONO from cdp.SRHIVSLP15""", conn)
    df_temp2 = pd.read_sql(
        u"""select VIN, DLRCD, BRNHCD, WORKNO from cdp.SRHSLPH15 where VIN!='' and VIN IS NOT NULL and CMPTDT IS NOT NULL""",
        conn)
    df_temp3 = pd.read_sql(
        u"""select INVSTS, DETRMK, INVONO, INVODT, DLRCD, BRNHCD, INVTXCD, TOTAMT, INSURCD, IRNAMT, WSHAMT from cdp.SRHINVO15""",
        conn)
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'B')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'C')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'D')]
    df_temp3 = df_temp3[~(df_temp3['INVSTS'] == 'E')]
    df_temp3 = df_temp3[~(df_temp3['DETRMK'] == '*')]
    df_SRHINVO15 = df_temp1.merge(df_temp2, how='left', on=['DLRCD', 'BRNHCD', 'WORKNO'])
    df_SRHINVO15 = df_SRHINVO15.merge(df_temp3, how='left', on=['DLRCD', 'BRNHCD', 'INVONO'])

    df_allSRMINVO = pd.DataFrame()
    df_allSRMINVO = df_allSRMINVO.append(df_SRMINVO, ignore_index=False)
    df_allSRMINVO = df_allSRMINVO.append(df_SRHINVO, ignore_index=False)
    df_allSRMINVO = df_allSRMINVO.append(df_SRHINVO15, ignore_index=False)
    conn.close()
    return df_allSRMINVO
#TODO
'''
def DTtime_cleaner(Input_Series, data_type=0):
    temp = pd.DataFrame()
    if data_type == 1:
        temp[u'DT_fix'] = pd.to_datetime(Input_Series, errors='coerce')
        temp[u'DT_fix'].fillna(pd.Timestamp('1900-01-01'), inplace=True)
        temp[u'DT_fix'] = (pd.Timestamp(main.END_DATE) - temp[u'DT_fix']).map(lambda x: x.days)
        temp.loc[(temp[u'DT_fix'] > 15000) | (temp[u'DT_fix'] < 0), u'DT_fix'] = df_LICSNO0717_Features.loc[
                                                                                     (temp[u'DT_fix'] > 15000) | (temp[
                                                                                                                      u'DT_fix'] < 0), u'CARAGE'] * 365
    elif data_type == 2:
        temp[u'DT_fix'] = pd.to_datetime(Input_Series, errors='coerce')
        temp[u'DT_fix'].fillna(pd.Timestamp('1900-01-01'), inplace=True)
        temp[u'DT_fix'] = (pd.Timestamp(main.END_DATE) - temp[u'DT_fix']).map(lambda x: x.days)
        temp.loc[(temp[u'DT_fix'] > 15000), u'DT_fix'] = df_LICSNO0717_Features.loc[
                                                             (temp[u'DT_fix'] > 15000), u'CARAGE'] * 365
    else:
        temp[u'DT'] = Input_Series.map(lambda x: x if (isinstance(x, float) or x is None) else x.replace(' ', ''))
        temp[u'DT_fix'] = pd.to_datetime(temp[u'DT'], format='%Y%m%d', errors='coerce')
        temp[u'DT_fix'].fillna(pd.Timestamp('1900-01-01'), inplace=True)
        temp[u'DT_fix'] = (pd.Timestamp(main.END_DATE) - temp[u'DT_fix']).map(lambda x: x.days)
        temp.loc[(temp[u'DT_fix'] > 15000) | (temp[u'DT_fix'] < 0), u'DT_fix'] = df_LICSNO0717_Features.loc[
                                                                                     (temp[u'DT_fix'] > 15000) | (temp[
                                                                                                                      u'DT_fix'] < 0), u'CARAGE'] * 365
    return temp[u'DT_fix']

'''