# coding: utf-8

# In[1]:
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
from threading import Thread, Lock
import os
import math
# from bokeh.io import push_notebook, show, output_notebook
from bokeh.layouts import row
from bokeh.plotting import figure
from bokeh.models import LabelSet
import bokeh
from bokeh.layouts import row
# output_notebook(bokeh.resources.INLINE)
# import matplotlib
# import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import urllib  # import for writing data to RDB
from sqlalchemy import create_engine  # import for writing data to RDB
#pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType, StringType, FloatType
from pyspark.sql.window import Window

# plt.style.use('ggplot')
# get_ipython().magic('matplotlib inline')

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
    result['min'] = (pd.Timestamp(END_DATE) - result['min']).map(lambda x: None if pd.isnull(x) else x.days)
    result['max'] = (pd.Timestamp(END_DATE) - result['max']).map(lambda x: None if pd.isnull(x) else x.days)
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
    result['min'] = (pd.Timestamp(END_DATE) - result['min']).map(lambda x: None if pd.isnull(x) else x.days)
    result['max'] = (pd.Timestamp(END_DATE) - result['max']).map(lambda x: None if pd.isnull(x) else x.days)
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
        self.return_index = len(result_queue) - 1


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


def DTtime_cleaner(Input_Series, data_type=0):
    temp = pd.DataFrame()
    if data_type == 1:
        temp[u'DT_fix'] = pd.to_datetime(Input_Series, errors='coerce')
        temp[u'DT_fix'].fillna(pd.Timestamp('1900-01-01'), inplace=True)
        temp[u'DT_fix'] = (pd.Timestamp(END_DATE) - temp[u'DT_fix']).map(lambda x: x.days)
        temp.loc[(temp[u'DT_fix'] > 15000) | (temp[u'DT_fix'] < 0), u'DT_fix'] = df_LICSNO0717_Features.loc[
                                                                                     (temp[u'DT_fix'] > 15000) | (temp[
                                                                                                                      u'DT_fix'] < 0), u'CARAGE'] * 365
    elif data_type == 2:
        temp[u'DT_fix'] = pd.to_datetime(Input_Series, errors='coerce')
        temp[u'DT_fix'].fillna(pd.Timestamp('1900-01-01'), inplace=True)
        temp[u'DT_fix'] = (pd.Timestamp(END_DATE) - temp[u'DT_fix']).map(lambda x: x.days)
        temp.loc[(temp[u'DT_fix'] > 15000), u'DT_fix'] = df_LICSNO0717_Features.loc[
                                                             (temp[u'DT_fix'] > 15000), u'CARAGE'] * 365
    else:
        temp[u'DT'] = Input_Series.map(lambda x: x if (isinstance(x, float) or x is None) else x.replace(' ', ''))
        temp[u'DT_fix'] = pd.to_datetime(temp[u'DT'], format='%Y%m%d', errors='coerce')
        temp[u'DT_fix'].fillna(pd.Timestamp('1900-01-01'), inplace=True)
        temp[u'DT_fix'] = (pd.Timestamp(END_DATE) - temp[u'DT_fix']).map(lambda x: x.days)
        temp.loc[(temp[u'DT_fix'] > 15000) | (temp[u'DT_fix'] < 0), u'DT_fix'] = df_LICSNO0717_Features.loc[
                                                                                     (temp[u'DT_fix'] > 15000) | (temp[
                                                                                                                      u'DT_fix'] < 0), u'CARAGE'] * 365
    return temp[u'DT_fix']


if __name__ == '__main__':
    # In[2]:
    today = datetime.datetime.now()
    # Path_year = today.strftime("%Y")
    # Path_month = today.strftime("%m")
    Path_by_day = today \
        .strftime("%Y%m")
    Result_Path = './' + Path_by_day + '/'  # refactory by HD
    '''
    if not os.path.isdir(Result_Path):
        os.makedirs(Result_Path)
    else:
        command = "rm -rf %s"
        command = command % Result_Path
        os.system(command)  # shutil.rmtree(Result_Path) by HD
        os.makedirs(Result_Path)
    Temp_Path = './Temp_Data/'
    if not os.path.isdir(Temp_Path):
        os.makedirs(Temp_Path)
    else:
        command = "rm -rf %s"
        command = command % Temp_Path
        os.system(command)  # 全部檔案連同目錄都砍掉	#shutil.rmtree(Temp_Path) by HD
        os.makedirs(Temp_Path)  # 重新建目錄給他
    '''
    # HT_Result_Path = './Result/' #改寫到RDB，所以這段程式碼不用了
    # if not os.path.isdir(HT_Result_Path):
    #     os.makedirs(HT_Result_Path)
    # else:
    #     command = "rm -rf %s"
    #     command = command % HT_Result_Path
    #     os.system(command) #全部檔案連同目錄都砍掉
    #     os.makedirs(HT_Result_Path)#重新建目錄給他
    Log_Path = './Log/'
    '''
    if not os.path.isdir(Log_Path):
        os.makedirs(Log_Path)
    '''
    Log_File = Log_Path + 'Log_' + Path_by_day + '.txt'  # refactory by HD
    Import_Data_Path = './Import_Data/'

    # In[4]:

    # Set paramenter
    login_info = r'DRIVER={Simba Spark ODBC Driver}; SERVER=10.201.2.130; DATABASE=cdp;'  # 連Carsandra DBC

    # 自動設定 END_DATE，因原本程式 END_DATE使用string 格式，所以在這裡要做轉換
    # today = datetime.datetime.now() #mark by HD
    yy = str(today.year)
    mm = str(today.month)
    END_DATE = yy + '-' + mm + '-01'  # END_DATE 設成每月的 1 日

    Candidate_Car_age = 15  # 用來挑選候選車子的車齡參數
    delay = 1  # for mutithread

    y_3y = str(today.year - 3)
    m_3y = str(today.month)
    DATE_3y = y_3y + '-' + m_3y + '-01'  # END_DATE 設成每月的 1 日
    '''
    write_Log(Log_File, "\n01. Initial folder process......OK\n")
    write_Log(Log_File, "    Start time is %s\n" % str(datetime.datetime.now()))
    write_Log(Log_File, "    END_DATE of data is %s\n" % END_DATE)
    # print (END_DATE)
    # print(DATE_3y)

    # for writing result data to RDB
    login_info = r'DRIVER={ODBC Driver 13 for SQL Server}; SERVER=10.201.2.4; DATABASE=CDP_WEB; UID=sysbigdata; PWD=50$/2*aZ17@yR;'
    params = urllib.parse.quote(login_info)
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

    # In[ ]:

    # Main Code
    # Code 1: 02CRCAMF_vs_SSHSCHISTORY_after20150708
    ## CRCAMF 篩選出所有和泰車 

    write_Log(Log_File, "02. %s | Select data from cdp.CRCAMF......" % str(datetime.datetime.now()))
    Original_df_CRCAMF = SQL_df_CRCAMF()
    df_CRCAMF = Original_df_CRCAMF.copy()  # SQL 執行一次之後，儲存於 Original_df_CRCAMF，要用時直接copy出來，節省執行時間
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "03. %s | Clean the df_CRCAMF......" % str(datetime.datetime.now()))
    df_CRCAMF[u'LSKIDT_fix'] = time_cleaner(df_CRCAMF[u'LSKIDT'])
    df_CRCAMF[u'SEDLDT_fix'] = time_cleaner(df_CRCAMF[u'SEDLDT'])
    df_CRCAMF[u'REDLDT_fix'] = time_cleaner(df_CRCAMF[u'REDLDT'])
    df_CRCAMF[u'LSKIDT_stamp'] = df_CRCAMF[[u'LSKIDT_fix', u'SEDLDT_fix', u'REDLDT_fix']].min(axis=1)

    # 修正 NaT 為 isnull()
    df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'LSKIDT_stamp'] = df_CRCAMF.loc[
        df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'STRDT']

    # 車輛篩選
    # u'PRODDT', u'LOADT', u'ISADT', u'SALEDT', u'LSKIDT', u'REDLDT'
    df_CRCAMF[u'LSKIDT_fix'] = time_cleaner(df_CRCAMF[u'LSKIDT'])
    df_CRCAMF[u'SEDLDT_fix'] = time_cleaner(df_CRCAMF[u'SEDLDT'])
    df_CRCAMF[u'REDLDT_fix'] = time_cleaner(df_CRCAMF[u'REDLDT'])
    df_CRCAMF[u'LSKIDT_stamp'] = df_CRCAMF[[u'LSKIDT_fix', u'SEDLDT_fix', u'REDLDT_fix']].min(axis=1)

    df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'LSKIDT_stamp'] = df_CRCAMF.loc[
        df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'STRDT']

    # print ('暫用15年以上車輛') today.year - Candidate_Car_age
    df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'LSKIDT_stamp'] <= datetime.datetime(today.year - Candidate_Car_age, today.month,
                                                                           1, 0, 0, 0, 0)) & (
                                      df_CRCAMF[u'LSKIDT_stamp'] >= datetime.datetime(1988, 1, 1, 0, 0, 0, 0))]

    # print ('去除買不到 1 年的中古車') # 法規規定中古車須購買滿 1 年始可申請貨物稅補助
    df_CRCAMF = df_CRCAMF[
        (df_CRCAMF[u'UCDELIVIDT'] <= datetime.datetime(today.year - 1, today.month, 1, 0, 0, 0, 0)) | df_CRCAMF[
            u'UCDELIVIDT'].isnull()]

    # print ('去除舊車牌已更換新車牌者')
    # df_remove_LICSNO = pd.read_csv(r"./Data/remove_LICSNO.csv", sep=',', encoding = 'utf-8' )
    df_remove_LICSNO = pd.read_csv(Import_Data_Path + "remove_LICSNO.csv", sep=',', encoding='utf-8')

    df_remove_LICSNO[u'LICSNO'] = df_remove_LICSNO[u'LICSNO'].map(lambda x: x.strip().upper())
    remove_LICSNO_list = df_remove_LICSNO['LICSNO'].tolist()
    df_CRCAMF[u'LICSNO'] = df_CRCAMF[u'LICSNO'].map(lambda x: x.strip())
    df_CRCAMF = df_CRCAMF[~df_CRCAMF[u'LICSNO'].str.upper().isin(remove_LICSNO_list)]
    df_remove_LICSNO = None
    remove_LICSNO_list = None

    # print ('指定報廢區間為 20150708~ENDDATE現在 ') #沒有報廢日期者 不會被標示 因此三年內 回廠 有可能 已經報廢
    df_CRCAMF[u'LICSNO_upper'] = df_CRCAMF[u'LICSNO'].map(lambda x: x.strip().upper())
    # df_web_query = pd.read_csv(r"./Data/df_web_query_fix.csv", sep=',', encoding = 'utf-8' )
    df_web_query = pd.read_csv(Import_Data_Path + "df_web_query_fix.csv", sep=',', encoding='utf-8')
    df_web_query[u'LICSNO_upper'] = df_web_query[u'LICSNO'].map(lambda x: x.strip().upper())
    df_web_query[u'CARDATE_fix'] = pd.to_datetime(df_web_query[u'CARDATE_fix'], format='%Y/%m/%d', errors='coerce')
    df_web_query = df_web_query[(df_web_query[u'CARDATE_fix'] >= datetime.datetime(2015, 7, 8, 0, 0, 0, 0)) & (
                df_web_query[u'CARDATE_fix'] <= pd.Timestamp(END_DATE))]

    df_CRCAMF = df_CRCAMF.merge(df_web_query[['LICSNO_upper', 'STATUS']], how='left', on='LICSNO_upper')
    df_CRCAMF = df_CRCAMF.drop('LICSNO_upper', 1)
    df_CRCAMF_web_query20150708 = df_CRCAMF[df_CRCAMF['STATUS'] == u'已回收']

    # print ('撈出', df_CRCAMF_web_query20150708.shape[0])
    df_CRCAMF_web_query20150708 = df_CRCAMF_web_query20150708[
        [u'LICSNO', u'CARNM', u'CARMDL', u'BDNO', u'EGNO', u'VIN']]
    # print (df_CRCAMF_web_query20150708.shape)

    df_CRCAMF = df_CRCAMF[df_CRCAMF['STATUS'] != u'已回收']  # 這裡是扣除已回收的車籍資料
    # print ('剩下', df_CRCAMF.shape)

    # print u'5. 去CARNM出現次數過少者 這邊不去除過少 由報廢去篩選出來'
    df_CRCAMF[u'CARNM'] = df_CRCAMF[u'CARNM'].map(lambda x: x.strip())
    dict_replacce_CARNM = {'EXSIOR': 'CORONA', 'PREMIO': 'CORONA', 'ALTIS': 'COROLLA'}
    df_CRCAMF[u'CARNM_M'] = df_CRCAMF[u'CARNM'].replace(dict_replacce_CARNM)
    df_CRCAMF[u'BDNO'] = series_str_cleaner(df_CRCAMF[u'BDNO'])
    df_CRCAMF[u'EGNO'] = series_str_cleaner(df_CRCAMF[u'EGNO'])
    df_CRCAMF[u'VIN'] = series_str_cleaner(df_CRCAMF[u'VIN'])
    df_CRCAMF['BDNO'] = df_CRCAMF['BDNO'].str[-10:]
    df_CRCAMF['EGNO'] = df_CRCAMF['EGNO'].str[-10:]
    df_CRCAMF['VIN'] = df_CRCAMF['VIN'].str[-10:]
    df_CRCAMF = df_CRCAMF.reset_index(drop=True)

    write_Log(Log_File, "ok\n")

    # 下載所有報廢車輛並且進行篩選
    write_Log(Log_File, "04. %s | Select data from cdp.SSHSCHISTORY......" % str(datetime.datetime.now()))
    Original_df_SSHSCHISTORY = SQL_df_SSHSCHISTORY()
    df_SSHSCHISTORY = Original_df_SSHSCHISTORY.copy()  ##SQL 執行一次之後，儲存於 Original_df_XXX，要用時直接copy出來，節省執行時間
    write_Log(Log_File, "ok\n")
    # print (df_SSHSCHISTORY.shape)

    write_Log(Log_File, "05. %s | Clean the df_SSHSCHISTORY......" % str(datetime.datetime.now()))
    # print ('留下15年以上的車輛')# 10612改成留下15年的車子，ISSUE:發照日期
    df_SSHSCHISTORY[u'ISSUE_fix'] = time_cleaner(df_SSHSCHISTORY[u'ISSUE'])
    df_SSHSCHISTORY = df_SSHSCHISTORY[(df_SSHSCHISTORY[u'ISSUE_fix'] <= datetime.datetime(
        today.year - Candidate_Car_age, today.month, 1, 0, 0, 0, 0)) & (
                                                  df_SSHSCHISTORY[u'ISSUE_fix'] >= datetime.datetime(1988, 1, 1, 0, 0,
                                                                                                     0, 0))]
    # print (df_SSHSCHISTORY.shape)

    # print ("2015 0708之後報廢的車輛 需要找回")
    df_SSHSCHISTORY[u'MODDT_fix'] = time_cleaner(df_SSHSCHISTORY[u'MODDT'])  # 這次找2015 0708之後報廢的車輛  七月的從MODDT 8/3開始
    df_SSHSCHISTORY = df_SSHSCHISTORY[df_SSHSCHISTORY['MODDT_fix'] >= datetime.datetime(2015, 7, 8, 0, 0, 0, 0)]
    # print (df_SSHSCHISTORY.shape)

    df_SSHSCHISTORY[u'ENGINENO'] = series_str_cleaner(df_SSHSCHISTORY[u'ENGINENO'])
    df_SSHSCHISTORY[u'EGNOM'] = series_str_cleaner(df_SSHSCHISTORY[u'EGNOM'])
    df_SSHSCHISTORY[u'BODYNO'] = series_str_cleaner(df_SSHSCHISTORY[u'BODYNO'])
    df_SSHSCHISTORY[u'BDNOM'] = series_str_cleaner(df_SSHSCHISTORY[u'BDNOM'])
    df_SSHSCHISTORY[u'GRPNM'] = df_SSHSCHISTORY[u'GRPNM'].map(lambda x: x.strip())
    df_SSHSCHISTORY[u'EGNOM'] = df_SSHSCHISTORY.apply(lambda x: x['EGNOM'] if x['EGNOM'] not in x['ENGINENO'] else '',
                                                      axis=1)
    df_SSHSCHISTORY[u'BDNOM'] = df_SSHSCHISTORY.apply(lambda x: x['BDNOM'] if x['BDNOM'] not in x['BODYNO'] else '',
                                                      axis=1)

    # print (u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除')
    df_web_query[u'EGNO'] = series_str_cleaner(df_web_query[u'EGNO']).str[-9:]
    # 9碼以下不清 後九碼相同者視為已報廢
    remove_LICSNO_list = df_web_query[u'EGNO'][df_web_query[u'EGNO'].map(lambda x: len(x)) > 8].tolist()
    df_SSHSCHISTORY['ENGINENO_temp'] = df_SSHSCHISTORY['ENGINENO'].str[-9:]
    #   有五萬多 ENGINENO_temp 是空白因此找不到 
    # print (u'找到', df_SSHSCHISTORY[df_SSHSCHISTORY['ENGINENO_temp'].isin(remove_LICSNO_list)].shape)
    df_SSHSCHISTORY = df_SSHSCHISTORY[~df_SSHSCHISTORY['ENGINENO_temp'].isin(remove_LICSNO_list)]
    # print (u'剩下',df_SSHSCHISTORY.shape)

    df_SSHSCHISTORY.loc[:, 'is_scrapped'] = '1'

    # 用車身號碼 引擎號碼 後面10碼來比對 CRCAMF
    df_SSHSCHISTORY['ENGINENO'] = df_SSHSCHISTORY['ENGINENO'].str[-10:]
    df_SSHSCHISTORY['BODYNO'] = df_SSHSCHISTORY['BODYNO'].str[-10:]
    df_SSHSCHISTORY['EGNOM'] = df_SSHSCHISTORY['EGNOM'].str[-10:]
    df_SSHSCHISTORY['BDNOM'] = df_SSHSCHISTORY['BDNOM'].str[-10:]

    # 報廢資料 限定9碼以上
    df_SSHSCHISTORY[u'ENGINENO'] = df_SSHSCHISTORY[u'ENGINENO'].map(lambda x: x if len(x) > 9 else '')
    df_SSHSCHISTORY[u'BODYNO'] = df_SSHSCHISTORY[u'BODYNO'].map(lambda x: x if len(x) > 9 else '')
    df_SSHSCHISTORY[u'EGNOM'] = df_SSHSCHISTORY[u'EGNOM'].map(lambda x: x if len(x) > 9 else '')
    df_SSHSCHISTORY[u'BDNOM'] = df_SSHSCHISTORY[u'BDNOM'].map(lambda x: x if len(x) > 9 else '')
    write_Log(Log_File, "ok\n")

    # list_HIST = ['ENGINENO','BODYNO','EGNOM','BDNOM']
    # list_CRCAMF = ['BDNO', 'EGNO', 'VIN']

    list_HIST = ['BODYNO', 'BDNOM']
    list_CRCAMF = ['BDNO']

    # 交叉比對 也比對車名
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            #         print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF[['BDNO', 'EGNO', 'VIN', 'CARNM_M']].merge(
                df_SSHSCHISTORY[df_SSHSCHISTORY[indexHIST] != ''][[indexHIST, 'GRPNM', 'is_scrapped']].drop_duplicates(
                    indexHIST), how='left', left_on=[indexCRCAMF, 'CARNM_M'], right_on=[indexHIST, 'GRPNM'])
            merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
            df_CRCAMF[indexCRCAMF + '_' + indexHIST] = merged['is_scrapped'].reset_index(drop=True)

    # 加回已知在20150708報廢的車輛 網站上查詢到 在crcamf比對到的 from  0001_web_query_minipulation.ipynb
    df_result_csv = df_CRCAMF[(df_CRCAMF['BDNO_BODYNO'] == '1') | (df_CRCAMF['BDNO_BDNOM'] == '1')]
    df_result_csv = df_result_csv.append(df_CRCAMF_web_query20150708, ignore_index=True)
    df_result_csv.reset_index(drop=True, inplace=True)
    df_result_csv = df_result_csv[[u'LICSNO', u'CARNM', u'CARMDL', u'BDNO', u'EGNO', u'VIN']]
    df_result_csv.to_csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv", sep=',', encoding='utf-8')
    del df_result_csv

    # Code 2: 05CRCAMF_car_selection
    ## CRCAMF 篩選出所有和泰車 
    write_Log(Log_File, "06. %s | HT's cars selection......" % str(datetime.datetime.now()))
    df_CRCAMF = Original_df_CRCAMF.copy()  # SQL 執行一次之後，儲存於 Original_df_CRCAMF，要用時直接copy出來，節省執行時間
    df_CRCAMF[u'LSKIDT_fix'] = time_cleaner(df_CRCAMF[u'LSKIDT'])
    df_CRCAMF[u'SEDLDT_fix'] = time_cleaner(df_CRCAMF[u'SEDLDT'])
    df_CRCAMF[u'REDLDT_fix'] = time_cleaner(df_CRCAMF[u'REDLDT'])
    df_CRCAMF[u'LSKIDT_stamp'] = df_CRCAMF[[u'LSKIDT_fix', u'SEDLDT_fix', u'REDLDT_fix']].min(axis=1)

    # 修正 NaT 為 isnull()
    df_CRCAMF.loc[df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'LSKIDT_stamp'] = df_CRCAMF.loc[
        df_CRCAMF[u'LSKIDT_stamp'].isnull(), u'STRDT']

    # print u'1. 暫用15年以上車輛' 
    df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'LSKIDT_stamp'] <= datetime.datetime(today.year - Candidate_Car_age, today.month,
                                                                           1, 0, 0, 0, 0)) & (
                                      df_CRCAMF[u'LSKIDT_stamp'] >= datetime.datetime(1988, 1, 1, 0, 0, 0, 0))]

    # print u'2. 去除買不到一年的中古車' 
    df_CRCAMF = df_CRCAMF[
        (df_CRCAMF[u'UCDELIVIDT'] <= datetime.datetime(today.year - 1, today.month, 1, 0, 0, 0, 0)) | df_CRCAMF[
            u'UCDELIVIDT'].isnull()]

    # print u'上列兩步驟 會刪除部分汰舊換新車輛'

    # print u'3. 去除車輛狀態不是為 更改為 不得為 2過戶3失竊5報廢'
    df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'STSCD'] != '2')]
    df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'STSCD'] != '3')]
    df_CRCAMF = df_CRCAMF[(df_CRCAMF[u'STSCD'] != '5')]

    # print u'4. 去除舊車牌已更換新車牌者'
    df_remove_LICSNO = pd.read_csv(Import_Data_Path + "remove_LICSNO.csv", sep=',', encoding='utf-8')
    df_remove_LICSNO['LICSNO'] = df_remove_LICSNO['LICSNO'].map(lambda x: x.strip().upper())
    remove_LICSNO_list = df_remove_LICSNO['LICSNO'].tolist()
    df_CRCAMF[u'LICSNO'] = df_CRCAMF[u'LICSNO'].map(lambda x: x.strip())
    df_CRCAMF = df_CRCAMF[~df_CRCAMF[u'LICSNO'].str.upper().isin(remove_LICSNO_list)]  # 去除車牌已轉換的資料
    df_remove_LICSNO = None
    remove_LICSNO_list = None

    # print u'5. 去除CARNM出現次數過少者'
    df_CRCAMF[u'CARNM'] = df_CRCAMF[u'CARNM'].map(lambda x: x.strip())
    brand_list = df_CRCAMF.CARNM.value_counts().reset_index()
    brand_list = brand_list[brand_list['CARNM'] > 123]['index'].tolist()
    df_CRCAMF = df_CRCAMF[df_CRCAMF.CARNM.isin(brand_list)]
    # 為符合報廢車輛名稱 進行名稱轉換
    dict_replacce_CARNM = {'EXSIOR': 'CORONA', 'PREMIO': 'CORONA', 'ALTIS': 'COROLLA'}
    df_CRCAMF[u'CARNM_M'] = df_CRCAMF[u'CARNM'].replace(dict_replacce_CARNM)
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "07. %s | Remove scrapped car from web query......" % str(datetime.datetime.now()))
    # print u'6. 扣除車牌(指定時間之前的才清除 避免網站查詢不同步)已在網站上查詢到, 後續會加回最近報廢的'
    df_web_query = pd.read_csv(Import_Data_Path + "df_web_query_fix.csv", sep=',', encoding='utf-8')
    df_web_query[u'LICSNO'] = df_web_query[u'LICSNO'].map(lambda x: x.strip().upper())
    df_web_query[u'CARDATE_fix'] = pd.to_datetime(df_web_query[u'CARDATE_fix'], format='%Y/%m/%d', errors='coerce')
    df_web_query = df_web_query[df_web_query[u'CARDATE_fix'] < pd.Timestamp(END_DATE)]  # 指定時間之前的才清除 避免網站查詢不同步
    remove_LICSNO_list = df_web_query[u'LICSNO'].tolist()
    df_CRCAMF = df_CRCAMF[~df_CRCAMF['LICSNO'].str.upper().isin(remove_LICSNO_list)]
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "08. %s | Remove stolen cars......" % str(datetime.datetime.now()))
    # print u'7. 扣除車牌"所有"已在網站上查詢到, 已經車牌失竊或車輛失竊'
    df_stolen = pd.read_csv(Import_Data_Path + "Stolen.csv", sep=',', encoding='utf-8')
    df_stolen.columns = ['type', 'LICSNO', 'result', 'date']
    df_stolen['LICSNO'] = df_stolen['LICSNO'].map(lambda x: x.strip().upper())
    df_stolen = df_stolen[df_stolen['result'] != u'查無資料']
    df_CRCAMF = df_CRCAMF[~df_CRCAMF['LICSNO'].str.upper().isin(df_stolen['LICSNO'].tolist())]
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "09. %s | Compare with scrapped data from web query and df_SSHSCHISTORY......" % str(
        datetime.datetime.now()))
    df_SSHSCHISTORY = Original_df_SSHSCHISTORY.copy()  ##SQL 執行一次之後，儲存於 Original_df_XXX，要用時直接copy出來，節省執行時間
    del Original_df_SSHSCHISTORY

    # print '留下15年以上的車輛' 
    df_SSHSCHISTORY[u'ISSUE_fix'] = time_cleaner(df_SSHSCHISTORY[u'ISSUE'])
    df_SSHSCHISTORY = df_SSHSCHISTORY[(df_SSHSCHISTORY[u'ISSUE_fix'] <= datetime.datetime(
        today.year - Candidate_Car_age, today.month, 1, 0, 0, 0, 0)) & (
                                                  df_SSHSCHISTORY[u'ISSUE_fix'] >= datetime.datetime(1988, 1, 1, 0, 0,
                                                                                                     0, 0))]

    # print "df_SSHSCHISTORY 2015-07-08 之後報廢的需要移除 因為後續會加回  只需清除之前報廢的"
    df_SSHSCHISTORY[u'MODDT_fix'] = time_cleaner(df_SSHSCHISTORY[u'MODDT'])
    df_SSHSCHISTORY = df_SSHSCHISTORY[
        df_SSHSCHISTORY['MODDT_fix'] < datetime.datetime(2015, 7, 8, 0, 0, 0, 0)]  # 監理站資料，移除2015/07/08之後報廢的資料
    # print df_SSHSCHISTORY.shape # 監理站報廢車檔，剩下車齡14（或15），且在2015/07/08以前報廢的資料

    df_SSHSCHISTORY[u'ENGINENO'] = series_str_cleaner(df_SSHSCHISTORY[u'ENGINENO'])
    df_SSHSCHISTORY[u'EGNOM'] = series_str_cleaner(df_SSHSCHISTORY[u'EGNOM'])
    df_SSHSCHISTORY[u'BODYNO'] = series_str_cleaner(df_SSHSCHISTORY[u'BODYNO'])
    df_SSHSCHISTORY[u'BDNOM'] = series_str_cleaner(df_SSHSCHISTORY[u'BDNOM'])

    # print u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除'
    df_web_query = pd.read_csv(Import_Data_Path + "df_web_query_fix.csv", sep=',', encoding='utf-8')
    df_web_query[u'CARDATE_fix'] = pd.to_datetime(df_web_query[u'CARDATE_fix'], format='%Y/%m/%d', errors='coerce')
    df_web_query = df_web_query[df_web_query[u'CARDATE_fix'] < datetime.datetime(2015, 7, 8, 0, 0, 0, 0)]
    df_web_query[u'EGNO'] = series_str_cleaner(df_web_query[u'EGNO'])  # 環保署2015/07/08之前的報廢資料
    # 五碼以下不清
    remove_LICSNO_list = df_web_query[u'EGNO'][df_web_query[u'EGNO'].map(lambda x: len(x)) > 5].tolist()
    # 環保署2015/07/08之前的報廢資料
    df_SSHSCHISTORY = df_SSHSCHISTORY[~df_SSHSCHISTORY['ENGINENO'].isin(remove_LICSNO_list)]  # 
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "10. %s | Clean df_SSHSCHISTORY ......." % str(datetime.datetime.now()))
    df_SSHSCHISTORY[u'GRPNM'] = df_SSHSCHISTORY[u'GRPNM'].map(lambda x: x.strip())

    # 沒有出現在ENGINENO OR BODYNO 才會被留著
    df_SSHSCHISTORY[u'EGNOM'] = df_SSHSCHISTORY.apply(lambda x: x['EGNOM'] if x['EGNOM'] not in x['ENGINENO'] else '',
                                                      axis=1)
    df_SSHSCHISTORY[u'BDNOM'] = df_SSHSCHISTORY.apply(lambda x: x['BDNOM'] if x['BDNOM'] not in x['BODYNO'] else '',
                                                      axis=1)

    df_SSHSCHISTORY.loc[:, 'is_scrapped'] = '1'

    # 清整比對報廢車輛 並 標示於CRCAMF
    df_SSHSCHISTORY['ENGINENO'] = df_SSHSCHISTORY['ENGINENO'].str[-9:]
    df_SSHSCHISTORY['BODYNO'] = df_SSHSCHISTORY['BODYNO'].str[-9:]
    df_SSHSCHISTORY['EGNOM'] = df_SSHSCHISTORY['EGNOM'].str[-9:]
    df_SSHSCHISTORY['BDNOM'] = df_SSHSCHISTORY['BDNOM'].str[-9:]

    # 報廢資料 限定8碼以上 六碼者 後面另行比對
    df_SSHSCHISTORY[u'ENGINENO'] = df_SSHSCHISTORY[u'ENGINENO'].map(lambda x: x if len(x) > 8 else '')
    df_SSHSCHISTORY[u'BODYNO'] = df_SSHSCHISTORY[u'BODYNO'].map(lambda x: x if len(x) > 8 else '')
    df_SSHSCHISTORY[u'EGNOM'] = df_SSHSCHISTORY[u'EGNOM'].map(lambda x: x if len(x) > 8 else '')
    df_SSHSCHISTORY[u'BDNOM'] = df_SSHSCHISTORY[u'BDNOM'].map(lambda x: x if len(x) > 8 else '')

    # 5碼以下的不使用
    df_SSHSCHISTORY['ENGINENO_6'] = df_SSHSCHISTORY['ENGINENO'].map(lambda x: '' if len(x) < 6 else x)
    df_SSHSCHISTORY['BODYNO_6'] = df_SSHSCHISTORY['BODYNO'].map(lambda x: '' if len(x) < 6 else x)
    df_SSHSCHISTORY['ENGINENO_6'] = df_SSHSCHISTORY['ENGINENO_6'].str[-6:]
    df_SSHSCHISTORY['BODYNO_6'] = df_SSHSCHISTORY['BODYNO_6'].str[-6:]

    write_Log(Log_File, "ok\n")

    # data cleaning
    write_Log(Log_File, "11. %s | Clearn df_CRCAMF......" % str(datetime.datetime.now()))
    df_CRCAMF[u'BDNO'] = series_str_cleaner(df_CRCAMF[u'BDNO'])
    df_CRCAMF[u'EGNO'] = series_str_cleaner(df_CRCAMF[u'EGNO'])
    df_CRCAMF[u'VIN'] = series_str_cleaner(df_CRCAMF[u'VIN'])

    df_CRCAMF['BDNO'] = df_CRCAMF['BDNO'].str[-9:]
    df_CRCAMF['EGNO'] = df_CRCAMF['EGNO'].str[-9:]
    df_CRCAMF['VIN'] = df_CRCAMF['VIN'].str[-9:]

    df_CRCAMF = df_CRCAMF.reset_index(drop=True)

    list_HIST = ['ENGINENO', 'BODYNO', 'EGNOM', 'BDNOM']
    list_CRCAMF = ['BDNO', 'EGNO', 'VIN']

    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            #         print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF[['BDNO', 'EGNO', 'VIN', 'CARNM_M']].merge(
                df_SSHSCHISTORY[df_SSHSCHISTORY[indexHIST] != ''][[indexHIST, 'GRPNM', 'is_scrapped']].drop_duplicates(
                    indexHIST), how='left', left_on=[indexCRCAMF, 'CARNM_M'], right_on=[indexHIST, 'GRPNM'])
            merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
            df_CRCAMF[indexCRCAMF + '_' + indexHIST] = merged['is_scrapped'].reset_index(drop=True)
    # 這邊需要確認 去留數量???

    # 九碼以上的移除 因為已在先前比對過
    df_CRCAMF['BDNO_6'] = df_CRCAMF['BDNO'].map(lambda x: '' if len(x) >= 9 else x)
    df_CRCAMF['EGNO_6'] = df_CRCAMF['EGNO'].map(lambda x: '' if len(x) >= 9 else x)
    # df_CRCAMF['VIN_6']  = df_CRCAMF['VIN'].map(lambda x: '' if len(x)>=9 else x)
    # 
    df_CRCAMF['BDNO_6'] = df_CRCAMF['BDNO_6'].str[-6:]
    df_CRCAMF['EGNO_6'] = df_CRCAMF['EGNO_6'].str[-6:]
    # df_CRCAMF['VIN_6']  = df_CRCAMF['VIN_6'].str[-6:] #skip VIN

    # 只選六碼時 限定 BD VS BD  , EG VS EG 避免 因為碼數較小誤判

    merged = None
    merged = df_CRCAMF[['EGNO_6', 'CARNM_M']].merge(
        df_SSHSCHISTORY[['ENGINENO_6', 'GRPNM', 'is_scrapped']][df_SSHSCHISTORY['ENGINENO_6'] != ''].drop_duplicates(
            ['ENGINENO_6', 'GRPNM']), how='left', left_on=['EGNO_6', 'CARNM_M'], right_on=['ENGINENO_6', 'GRPNM'])
    merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
    df_CRCAMF['EGNO_6_ENGINENO_6'] = merged['is_scrapped'].reset_index(drop=True)

    merged = None
    merged = df_CRCAMF[['BDNO_6', 'CARNM_M']].merge(
        df_SSHSCHISTORY[['BODYNO_6', 'GRPNM', 'is_scrapped']][df_SSHSCHISTORY['BODYNO_6'] != ''].drop_duplicates(
            ['BODYNO_6', 'GRPNM']), how='left', left_on=['BDNO_6', 'CARNM_M'], right_on=['BODYNO_6', 'GRPNM'])
    merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
    df_CRCAMF['BDNO_6_BODYNO_6'] = merged['is_scrapped'].reset_index(drop=True)

    ##重複者一併刪除 數量約在3000以下
    # 任一條件皆刪除
    df_CRCAMF = df_CRCAMF[(df_CRCAMF['BDNO_ENGINENO'] == '0') & (df_CRCAMF['EGNO_ENGINENO'] == '0') & (
                df_CRCAMF['VIN_ENGINENO'] == '0') & (df_CRCAMF['BDNO_BODYNO'] == '0') & (
                                      df_CRCAMF['EGNO_BODYNO'] == '0') & (df_CRCAMF['VIN_BODYNO'] == '0') & (
                                      df_CRCAMF['BDNO_EGNOM'] == '0') & (df_CRCAMF['EGNO_EGNOM'] == '0') & (
                                      df_CRCAMF['VIN_EGNOM'] == '0') & (df_CRCAMF['BDNO_BDNOM'] == '0') & (
                                      df_CRCAMF['EGNO_BDNOM'] == '0') & (df_CRCAMF['VIN_BDNOM'] == '0') & (
                                      df_CRCAMF['EGNO_6_ENGINENO_6'] == '0') & (df_CRCAMF['BDNO_6_BODYNO_6'] == '0')][
        ['LICSNO', u'CARNM', 'CARNM_M', u'CARMDL', 'BDNO', 'EGNO', 'VIN', 'BDNO_6', 'EGNO_6', 'UCDELIVIDT']]
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF_used = df_CRCAMF[df_CRCAMF['UCDELIVIDT'].notnull()]
    df_CRCAMF_used = df_CRCAMF_used[[u'LICSNO', u'CARNM', u'CARMDL', u'BDNO', u'EGNO', u'VIN', u'UCDELIVIDT']]
    df_CRCAMF_NOTused = df_CRCAMF[df_CRCAMF['UCDELIVIDT'].isnull()]

    # 如果中古車的 VIN、EGNO、BDNO 在非中古車中出現，就將該非中古車移除（以 xxx_used 為主）
    df_CRCAMF_NOTused = df_CRCAMF_NOTused[
        ~df_CRCAMF_NOTused['VIN'].isin(df_CRCAMF_used['VIN'][df_CRCAMF_used['VIN'].map(lambda x: len(x)) > 5].tolist())]
    df_CRCAMF_NOTused = df_CRCAMF_NOTused[~df_CRCAMF_NOTused['EGNO'].isin(
        df_CRCAMF_used['EGNO'][df_CRCAMF_used['EGNO'].map(lambda x: len(x)) > 5].tolist())]
    df_CRCAMF_NOTused = df_CRCAMF_NOTused[~df_CRCAMF_NOTused['BDNO'].isin(
        df_CRCAMF_used['BDNO'][df_CRCAMF_used['BDNO'].map(lambda x: len(x)) > 5].tolist())]

    write_Log(Log_File, "ok\n")
    # 特別注意，SSHUCHISTORY是過戶歷史檔（SSHSCHISTORY是報廢歷史檔）
    # 過戶 車身號碼 引擎號碼 清理
    df_SSHSCHISTORY = None
    df_SSHUCHISTORY = SQL_df_SSHUCHISTORY()  # df_SSHUCHISTORY 只執行 1 次，所以不用Original_ 跟 .copy()
    # print "有過戶的就移除 因為不確定新的車主資料"

    write_Log(Log_File, "12. %s | Clearn df_SSHUCHISTORY......" % str(datetime.datetime.now()))
    # print ('留下15年以上的車輛') 
    df_SSHUCHISTORY[u'ISSUE_fix'] = time_cleaner(df_SSHUCHISTORY[u'ISSUE'])
    df_SSHUCHISTORY = df_SSHUCHISTORY[(df_SSHUCHISTORY[u'ISSUE_fix'] <= datetime.datetime(
        today.year - Candidate_Car_age, today.month, 1, 0, 0, 0, 0)) & (
                                                  df_SSHUCHISTORY[u'ISSUE_fix'] >= datetime.datetime(1988, 1, 1, 0, 0,
                                                                                                     0, 0))]
    df_SSHUCHISTORY[u'ENGINENO'] = series_str_cleaner(df_SSHUCHISTORY[u'ENGINENO'])
    df_SSHUCHISTORY[u'EGNOM'] = series_str_cleaner(df_SSHUCHISTORY[u'EGNOM'])
    df_SSHUCHISTORY[u'BODYNO'] = series_str_cleaner(df_SSHUCHISTORY[u'BODYNO'])
    df_SSHUCHISTORY[u'BDNOM'] = series_str_cleaner(df_SSHUCHISTORY[u'BDNOM'])
    df_SSHUCHISTORY[u'EGNOM'] = df_SSHUCHISTORY.apply(lambda x: x['EGNOM'] if x['EGNOM'] not in x['ENGINENO'] else '',
                                                      axis=1)
    df_SSHUCHISTORY[u'BDNOM'] = df_SSHUCHISTORY.apply(lambda x: x['BDNOM'] if x['BDNOM'] not in x['BODYNO'] else '',
                                                      axis=1)
    df_SSHUCHISTORY[u'GRPNM'] = df_SSHUCHISTORY[u'GRPNM'].map(lambda x: x.strip())
    df_SSHUCHISTORY.loc[:, 'is_scrapped'] = '1'

    # 清整比對過戶車輛 並 標示於CRCAMF
    # 因為 CRCAMF BDNO -9重複較多
    df_SSHUCHISTORY['ENGINENO'] = df_SSHUCHISTORY['ENGINENO'].str[-9:]
    df_SSHUCHISTORY['BODYNO'] = df_SSHUCHISTORY['BODYNO'].str[-9:]
    df_SSHUCHISTORY['EGNOM'] = df_SSHUCHISTORY['EGNOM'].str[-9:]
    df_SSHUCHISTORY['BDNOM'] = df_SSHUCHISTORY['BDNOM'].str[-9:]

    # 過戶資料 限定8碼以上
    df_SSHUCHISTORY[u'ENGINENO'] = df_SSHUCHISTORY[u'ENGINENO'].map(lambda x: x if len(x) > 8 else '')
    df_SSHUCHISTORY[u'BODYNO'] = df_SSHUCHISTORY[u'BODYNO'].map(lambda x: x if len(x) > 8 else '')
    df_SSHUCHISTORY[u'EGNOM'] = df_SSHUCHISTORY[u'EGNOM'].map(lambda x: x if len(x) > 8 else '')
    df_SSHUCHISTORY[u'BDNOM'] = df_SSHUCHISTORY[u'BDNOM'].map(lambda x: x if len(x) > 8 else '')

    df_CRCAMF_NOTused = df_CRCAMF_NOTused.reset_index(drop=True)

    list_HIST = ['ENGINENO', 'BODYNO', 'EGNOM', 'BDNOM']
    list_CRCAMF = ['BDNO', 'EGNO', 'VIN']
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            #         print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF_NOTused[['BDNO', 'EGNO', 'VIN', 'CARNM_M']].merge(
                df_SSHUCHISTORY[df_SSHUCHISTORY[indexHIST] != ''][[indexHIST, 'GRPNM', 'is_scrapped']].drop_duplicates(
                    indexHIST), how='left', left_on=[indexCRCAMF, 'CARNM_M'], right_on=[indexHIST, 'GRPNM'])
            merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
            df_CRCAMF_NOTused[indexCRCAMF + '_' + indexHIST] = merged['is_scrapped'].reset_index(drop=True)

    # 五碼以下不使用
    df_SSHUCHISTORY['ENGINENO_6'] = df_SSHUCHISTORY['ENGINENO'].map(lambda x: '' if len(x) < 6 else x)
    df_SSHUCHISTORY['BODYNO_6'] = df_SSHUCHISTORY['BODYNO'].map(lambda x: '' if len(x) < 6 else x)

    df_SSHUCHISTORY['ENGINENO_6'] = df_SSHUCHISTORY['ENGINENO_6'].str[-6:]
    df_SSHUCHISTORY['BODYNO_6'] = df_SSHUCHISTORY['BODYNO_6'].str[-6:]
    write_Log(Log_File, "ok\n")
    # 只選六碼時 限定 BD VS BD  , EG VS EG 避免 因為碼數較小誤判
    merged = None
    merged = df_CRCAMF_NOTused[['EGNO_6', 'CARNM_M']].merge(
        df_SSHUCHISTORY[['ENGINENO_6', 'GRPNM', 'is_scrapped']][df_SSHUCHISTORY['ENGINENO_6'] != ''].drop_duplicates(
            ['ENGINENO_6', 'GRPNM']), how='left', left_on=['EGNO_6', 'CARNM_M'], right_on=['ENGINENO_6', 'GRPNM'])
    merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
    df_CRCAMF_NOTused['EGNO_6_ENGINENO_6'] = merged['is_scrapped'].reset_index(drop=True)

    merged = None
    merged = df_CRCAMF_NOTused[['BDNO_6', 'CARNM_M']].merge(
        df_SSHUCHISTORY[['BODYNO_6', 'GRPNM', 'is_scrapped']][df_SSHUCHISTORY['BODYNO_6'] != ''].drop_duplicates(
            ['BODYNO_6', 'GRPNM']), how='left', left_on=['BDNO_6', 'CARNM_M'], right_on=['BODYNO_6', 'GRPNM'])
    merged['is_scrapped'] = merged['is_scrapped'].fillna('0')
    df_CRCAMF_NOTused['BDNO_6_BODYNO_6'] = merged['is_scrapped'].reset_index(drop=True)
    del df_SSHUCHISTORY
    df_CRCAMF_NOTused = df_CRCAMF_NOTused[
        (df_CRCAMF_NOTused['BDNO_ENGINENO'] == '0') & (df_CRCAMF_NOTused['EGNO_ENGINENO'] == '0') & (
                    df_CRCAMF_NOTused['VIN_ENGINENO'] == '0') & (df_CRCAMF_NOTused['BDNO_BODYNO'] == '0') & (
                    df_CRCAMF_NOTused['EGNO_BODYNO'] == '0') & (df_CRCAMF_NOTused['VIN_BODYNO'] == '0') & (
                    df_CRCAMF_NOTused['BDNO_EGNOM'] == '0') & (df_CRCAMF_NOTused['EGNO_EGNOM'] == '0') & (
                    df_CRCAMF_NOTused['VIN_EGNOM'] == '0') & (df_CRCAMF_NOTused['BDNO_BDNOM'] == '0') & (
                    df_CRCAMF_NOTused['EGNO_BDNOM'] == '0') & (df_CRCAMF_NOTused['VIN_BDNOM'] == '0') & (
                    df_CRCAMF_NOTused['EGNO_6_ENGINENO_6'] == '0') & (df_CRCAMF_NOTused['BDNO_6_BODYNO_6'] == '0')][
        ['LICSNO', u'CARNM', u'CARMDL', 'BDNO', 'EGNO', 'VIN', 'UCDELIVIDT']]
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF = df_CRCAMF_NOTused.append(df_CRCAMF_used, ignore_index=True)
    # 找出 最近三年內有回廠的15年以上車
    write_Log(Log_File, "13. %s | Look out the repaired cars over 15 years old in the past 3 years......" % str(
        datetime.datetime.now()))
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRCAMF_3year = pd.read_sql(u"""select a.*, b.LRDT from cdp.CRCAMF a, cdp.SRWHMF b
      WHERE  (a.FRAN in ('L','T') or  
      upper(a.CARNM) like '%TOYOTA%' or  
      upper(a.CARNM) like '%CORONA%' or  
      upper(a.CARNM) like '%EXSIOR%' or  
      upper(a.CARNM) like '%PREMIO%' or  
      upper(a.CARNM) like '%國瑞%' or  
      upper(a.CARNM) like '%豐田%' or  
      upper(a.CARNM) like '%LS400%' or  
      upper(a.CARNM) like '%IS250%' or  
      upper(a.CARNM) like '%ZACE%' or  
      upper(a.CARNM) like '%CAMRY%' or  
      upper(a.CARNM) like '%VIOS%' or  
      upper(a.CARNM) like '%COROLLA%' or  
      upper(a.CARNM) like '%SIENNA%' or  
      upper(a.CARNM) like '%LEXUS%' or  
      upper(a.CARNM) like '%ALTIS%' or  
      upper(a.CARNM) like '%GS300%' or  
      upper(a.CARNM) like '%ES300%' or  
      upper(a.CARNM) like '%CROWN%' or  
      upper(a.CARNM) like '%TERCEL%' or  
      upper(a.CARNM) like '%WISH%' or  
      upper(a.CARNM) like '%CT200H%' or  
      upper(a.CARNM) like '%RX330%' or  
      upper(a.CARNM) like '%RX300%' or  
      upper(a.CARNM) like '%ES350%' or  
      upper(a.CARNM) like '%TERCEL%') 
      and upper(a.CARNM) not like '%DYNA%' and upper(a.CARNM) not like '%HINO%'
      and a.LICSNO = b.LICSNO and b.LRDT > '2014-11-01'
      """, conn)
    conn.close()
    df_CRCAMF_3year[u'LSKIDT_fix'] = time_cleaner(df_CRCAMF_3year[u'LSKIDT'])
    df_CRCAMF_3year[u'SEDLDT_fix'] = time_cleaner(df_CRCAMF_3year[u'SEDLDT'])
    df_CRCAMF_3year[u'REDLDT_fix'] = time_cleaner(df_CRCAMF_3year[u'REDLDT'])
    df_CRCAMF_3year[u'LSKIDT_stamp'] = df_CRCAMF_3year[[u'LSKIDT_fix', u'SEDLDT_fix', u'REDLDT_fix']].min(axis=1)
    # 修正 NaT 為 isnull()
    df_CRCAMF_3year.loc[df_CRCAMF_3year[u'LSKIDT_stamp'].isnull(), u'LSKIDT_stamp'] = df_CRCAMF_3year.loc[
        df_CRCAMF_3year[u'LSKIDT_stamp'].isnull(), u'STRDT']

    # print u'1. 暫用15年以上車輛' 
    df_CRCAMF_3year = df_CRCAMF_3year[(df_CRCAMF_3year[u'LSKIDT_stamp'] <= datetime.datetime(
        today.year - Candidate_Car_age, today.month, 1, 0, 0, 0, 0)) & (
                                                  df_CRCAMF_3year[u'LSKIDT_stamp'] >= datetime.datetime(1988, 1, 1, 0,
                                                                                                        0, 0, 0))]
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "14. %s | Remove unused data......" % str(datetime.datetime.now()))
    # print u'2. 去除買不到一年的中古車' #自2018/01/01 起算 回推一年
    df_CRCAMF_3year = df_CRCAMF_3year[
        (df_CRCAMF_3year[u'UCDELIVIDT'] <= datetime.datetime(today.year - 1, today.month, 1, 0, 0, 0, 0)) |
        df_CRCAMF_3year[u'UCDELIVIDT'].isnull()]

    # print u'3. 去除車輛狀態不是為 更改為 不得為 2過戶3失竊5報廢'
    df_CRCAMF_3year = df_CRCAMF_3year[(df_CRCAMF_3year[u'STSCD'] != '2')]
    df_CRCAMF_3year = df_CRCAMF_3year[(df_CRCAMF_3year[u'STSCD'] != '3')]
    df_CRCAMF_3year = df_CRCAMF_3year[(df_CRCAMF_3year[u'STSCD'] != '5')]

    # print u'4. 去除舊車牌已更換新車牌者'
    df_remove_LICSNO = pd.read_csv(Import_Data_Path + "remove_LICSNO.csv", sep=',', encoding='utf-8')
    df_remove_LICSNO[u'LICSNO'] = df_remove_LICSNO[u'LICSNO'].map(lambda x: x.strip().upper())
    remove_LICSNO_list = df_remove_LICSNO['LICSNO'].tolist()
    df_CRCAMF_3year[u'LICSNO'] = df_CRCAMF_3year[u'LICSNO'].map(lambda x: x.strip())
    df_CRCAMF_3year = df_CRCAMF_3year[~df_CRCAMF_3year[u'LICSNO'].str.upper().isin(remove_LICSNO_list)]
    df_remove_LICSNO = None
    remove_LICSNO_list = None

    # print u'5. 扣除車牌"所有"已在網站上查詢到, 後續會加回最近報廢的'
    df_web_query = pd.read_csv(Import_Data_Path + "df_web_query_fix.csv", sep=',', encoding='utf-8')
    df_web_query[u'LICSNO'] = df_web_query[u'LICSNO'].map(lambda x: x.strip().upper())
    df_web_query[u'CARDATE_fix'] = pd.to_datetime(df_web_query[u'CARDATE_fix'], format='%Y/%m/%d', errors='coerce')
    df_web_query = df_web_query[df_web_query[u'CARDATE_fix'] < pd.Timestamp(END_DATE)]  # 指定時間之前的才清除 避免網站查詢不同步
    # 沒有報廢日期的 在這邊有可能被加入 後續篩選時 要濾掉

    remove_LICSNO_list = df_web_query[u'LICSNO'].tolist()
    df_CRCAMF_3year = df_CRCAMF_3year[~df_CRCAMF_3year['LICSNO'].str.upper().isin(remove_LICSNO_list)]

    # print u'6. 扣除車牌"所有"已在網站上查詢到, 已經車牌失竊或車輛失竊'
    df_stolen = pd.read_csv(Import_Data_Path + "Stolen.csv", sep=',', encoding='utf-8')
    df_stolen.columns = ['type', 'LICSNO', 'result', 'date']
    df_stolen['LICSNO'] = df_stolen['LICSNO'].map(lambda x: x.strip().upper())
    df_stolen = df_stolen[df_stolen['result'] != u'查無資料']
    df_CRCAMF_3year = df_CRCAMF_3year[~df_CRCAMF_3year['LICSNO'].str.upper().isin(df_stolen['LICSNO'].tolist())]

    df_CRCAMF_3year = df_CRCAMF_3year[[u'LICSNO', u'CARNM', u'CARMDL', u'BDNO', u'EGNO', u'VIN']].reset_index(drop=True)

    # print '清除報廢後 再加上在20150708 之後報廢的車輛 記得要改路徑'
    df_scrapped_20150708 = pd.read_csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv", sep=',', encoding='utf-8')
    df_CRCAMF = df_CRCAMF.append(df_scrapped_20150708, ignore_index=True)

    # print ' 再加上三年內有回廠 車齡15年以上的車輛 '
    df_CRCAMF = df_CRCAMF.append(df_CRCAMF_3year, ignore_index=True)

    df_CRCAMF['LICSNO'] = df_CRCAMF['LICSNO'].map(lambda x: x.strip())

    df_CRCAMF = df_CRCAMF.drop_duplicates('LICSNO', keep='first')
    df_CRCAMF.reset_index(drop=True, inplace=True)
    df_CRCAMF.to_csv(Temp_Path + "df_CRCAMF.csv", sep=',', encoding='utf-8')

    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF""", conn)
    conn.close()

    # 資料清整
    df_CRCAMF.loc[:, 'qualified'] = '1'  # df.loc[rows, col] = 'value'

    # 清除前後空白
    df_CRCAMF['LICSNO'] = df_CRCAMF['LICSNO'].map(lambda x: x.strip())
    df_CRCAMF = df_CRCAMF.drop_duplicates('LICSNO', keep='first')
    df_CRAURF['LICSNO'] = df_CRAURF['LICSNO'].map(lambda x: x.strip())
    # df_CRAURF 五種人 所以本來就會重複

    df_CRAURF = df_CRAURF[df_CRAURF['LICSNO'] != '']

    # matching for LICSNO with qualified car for all people
    # 找出符合條件的車輛下的所有人
    # df_CRCAMF 這邊LICSNO 尚未清整 因此 MERGE後 會變多
    # 已經DROP DUPLICATES
    df_CRAURF = df_CRAURF.merge(df_CRCAMF[['LICSNO', 'qualified']], how='left', on=u'LICSNO')

    # print '留下qualified')
    df_CRAURF = df_CRAURF[[u'CUSTID', u'qualified']][df_CRAURF['qualified'] == '1']

    # print '清掉前後空白'
    df_CRAURF = df_CRAURF[~df_CRAURF['CUSTID'].isnull()]
    df_CRAURF['CUSTID'] = df_CRAURF['CUSTID'].map(lambda x: x.strip())

    # print '身分證空的清掉'
    df_CRAURF = df_CRAURF[df_CRAURF['CUSTID'] != '']

    # print '身分證一樣的清掉'
    df_CRAURF = df_CRAURF.drop_duplicates(u'CUSTID', keep='first')

    # RE-DOWNLOAD df_CRAURF FOR MATCHING CUSTID
    # 再由找到的所有人 去找出所有車輛

    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF_all = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF""", conn)
    conn.close()

    # 清掉前後空白
    df_CRAURF_all = df_CRAURF_all[~df_CRAURF_all['CUSTID'].isnull()]
    df_CRAURF = df_CRAURF[~df_CRAURF['CUSTID'].isnull()]
    df_CRAURF_all['CUSTID'] = df_CRAURF_all['CUSTID'].map(lambda x: x.strip())
    df_CRAURF['CUSTID'] = df_CRAURF['CUSTID'].map(lambda x: x.strip())
    df_CRAURF_all = df_CRAURF_all[df_CRAURF_all['CUSTID'] != '']
    df_CRAURF = df_CRAURF[df_CRAURF['CUSTID'] != '']
    df_CRAURF_all = df_CRAURF_all.merge(df_CRAURF, how='left', on=u'CUSTID')
    df_CRAURF_all['LICSNO'] = df_CRAURF_all['LICSNO'].map(lambda x: x.strip())  # 車牌清掉空白後 會有一筆重複車牌 
    df_CRAURF_all[df_CRAURF_all.qualified == '1'].drop_duplicates('LICSNO', keep='first').to_csv(
        Temp_Path + "df_CRAUR.csv", index=False, sep=',', encoding='utf-8')
    del df_CRAURF_all
    write_Log(Log_File, "ok\n")
    # Code 3: 20FIRST_table_combining
    # 符合條件的車
    #  df_CRCAMF704969_0810  把候選名單加入
    write_Log(Log_File, "15. %s | Combine data......" % str(datetime.datetime.now()))
    df_CRCAMF = pd.read_csv(Temp_Path + "df_CRCAMF.csv", sep=',', encoding='utf-8', low_memory=False)  # 讀取候選車輛名單

    # isinstance(x, float) 用來判斷 x是否為float type， 衍伸範例：isinstance(x, (int, float, str, list))
    df_CRCAMF['LICSNO'] = df_CRCAMF['LICSNO'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_CRCAMF = df_CRCAMF.drop('Unnamed: 0.1', 1)

    # 所有被選出的車牌數量 from 人車
    # 把候選名單 中的 其他擁有車輛加入
    # df_CRAUR_1220.csv儲存從CRCAMF取出候選車輛名單，再用候選車輛名單到CRAUR找出所有對應的人員名單，再用人員名單串出所有車輛，然後刪除重複車號的結果
    df_CRAURF_select_LICSNO = pd.read_csv(Temp_Path + "df_CRAUR.csv", sep=',', encoding='utf-8')
    df_CRAURF_select_LICSNO['LICSNO'] = df_CRAURF_select_LICSNO['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())

    # 清除空白車牌
    df_CRAURF_select_LICSNO = df_CRAURF_select_LICSNO[df_CRAURF_select_LICSNO['LICSNO'] != '']  # 刪除 LICSNO=''的資料
    df_CRAURF_select_LICSNO = df_CRAURF_select_LICSNO[df_CRAURF_select_LICSNO['LICSNO'].notnull()]  # 刪除LICSNO= NULL的資料

    # 將符合條件者 與 其名下所有車輛 合併
    # CRCAMF：車輛基本資料檔，以車為主，紀錄車子的相關資訊，包含保險、是否為二手車等，不含車主資訊
    # CRAURF：車輛顧客關聯檔，以車為主，紀錄與車子有關的人，包含領照人、使用人、聯絡人、發票人、服務聯絡人，一車對多人
    df_CRCAMF.loc[:, 'qualified'] = '1'
    df_CRAURF_select_LICSNO.loc[:, 'qualified'] = '0'

    # CRCAMF 裡的 qualified全為1，CRAURF 裡面的 qualified全為 0，下面把 2 個 df 合併起來，資料筆數等於兩個 df 相加
    df_combined = pd.DataFrame()
    df_combined = df_CRCAMF[['LICSNO', 'qualified']].append(df_CRAURF_select_LICSNO[['LICSNO', 'qualified']],
                                                            ignore_index=True)

    df_combined = df_combined.drop_duplicates('LICSNO', keep='first')  # 這邊清除 重複車牌
    df_combined = df_combined.reset_index(drop=True)

    # 申請汰舊換新相關資料整理 前處理
    # 用以標示 那些車輛有進行汰舊換新
    # 這邊目前沒有另外篩選出 原本身分證是否已在客戶主檔 或 車牌與身分證 需有關連
    # 採用 原先crcamf過濾 汰舊與過戶篩選後 (讓它自然流過篩選) 再來標示   而不是 直接加汰舊換新者加入
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_PSLTAXORDMF = pd.read_sql(
        u"SELECT * FROM cdp.PSLTAXORDMF WHERE OBRAND like '%TOYOTA%' or OBRAND like '%LEXUS%' or OBRAND like '%AMCTOYOTA%' or OBRAND like '%TOYOYA%' or OBRAND like '%豐田%' or OBRAND like '%YOYOTA%' or OBRAND like '%國%' or OBRAND like '%T0YOTA%' or OBRAND like '%TOYATA%'",
        conn)
    conn.close()

    # DELDT不能有日期 表示沒有被退件 資料內容要是1900-01-01
    df_PSLTAXORDMF = df_PSLTAXORDMF[df_PSLTAXORDMF[u'DELDT'] < datetime.datetime(2010, 1, 1, 0, 0, 0, 0)]
    df_PSLTAXORDMF['OLICSNO'] = df_PSLTAXORDMF['OLICSNO'].map(lambda x: x.strip())
    df_PSLTAXORDMF = df_PSLTAXORDMF[df_PSLTAXORDMF['OLICSNO'] != '']
    df_PSLTAXORDMF.loc[:, 'PSLTAX'] = '1'

    df_PSLTAXORDMF['OLICSNO'].replace(
        {'HU3767': 'HU-3767',
         'LE4622': 'LE-4622',
         'RB9528': 'RB-9528',
         'GV5751': 'GV-5751',
         'H31026': 'H3-1026',
         'CU8227': 'CU-8227',
         'FE2052': 'FE-2052',
         'KD8468': 'KD-8468'}, inplace=True)

    # 在這之前的df_combined包含df_CRCAMF（qualified=1）跟df_CRAURF(qualified=0)的所有資料
    # df_PSLTAXORDMF包含所有和泰車已經報廢並申請貨物稅補助的資料（PSLTAXORDMF是貨物稅退稅案件檔）
    # 下面合併完之後，可以知道那些車輛已經申請貨物稅補助（PSLTAX=1的資料）
    df_combined = df_combined.merge(df_PSLTAXORDMF[['OLICSNO', 'PSLTAX']], how='left', left_on='LICSNO',
                                    right_on='OLICSNO')
    df_combined = df_combined.drop('OLICSNO', 1)

    # PSLTAX =1 表示有汰舊換新，空值的部分補 0
    df_combined['PSLTAX'] = df_combined['PSLTAX'].fillna('0')

    # 沒有qualified=0而汰舊換新，    qualified=='0'是沒報廢，df_combined.PSLTAX=='1'是有申請貨物稅補助
    temp_df_combined = df_combined[(df_combined.qualified == '0') & (df_combined.PSLTAX == '1')]
    df_combined = df_combined[~df_combined['LICSNO'].isin(temp_df_combined['LICSNO'])]

    # print '清除報廢後 再標示在20150708 之後報廢的車輛 ' 
    df_scrapped_20150708 = pd.read_csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv", sep=',', encoding='utf-8')
    df_scrapped_20150708['LICSNO'] = df_scrapped_20150708['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_scrapped_20150708['scrapped_150708'] = '1'
    df_combined = df_combined.merge(df_scrapped_20150708[['LICSNO', 'scrapped_150708']], how='left', left_on='LICSNO',
                                    right_on='LICSNO')
    df_combined['scrapped_150708'] = df_combined['scrapped_150708'].fillna('0')

    df_combined.loc[df_combined.PSLTAX == '1', 'scrapped_150708'] = '1'

    # reload df_CRCAMF for merge all related data
    # 下載CRCAMF將車輛資料串出
    #
    df_CRCAMF = None
    df_CRCAMF = Original_df_CRCAMF.copy()  # SQL 執行一次之後，儲存於 Original_df_CRCAMF，要用時直接copy出來，節省執行時間
    del Original_df_CRCAMF  # 後面用不到了，把記憶體釋放

    df_CRCAMF = df_CRCAMF[~df_CRCAMF['LICSNO'].isnull()]
    df_CRCAMF['LICSNO'] = df_CRCAMF['LICSNO'].map(lambda x: x.strip())

    # print '清除重複車牌'
    df_CRCAMF = df_CRCAMF.drop_duplicates('LICSNO', keep='first')

    df_combined = df_combined.merge(df_CRCAMF, how='left', on='LICSNO')
    df_CRCAMF = None  # df_CRCAMF 在這段code 已經用不到了

    df_combined = df_combined[
        ~((df_combined[u'EGNO'].isnull()) & (df_combined[u'BDNO'].isnull()) & (df_combined[u'FRAN'].isnull()))]
    df_combined = df_combined.reset_index(drop=True)

    # 後續 是將 五種人都串出來

    # TARGET='1'
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF_all = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF WHERE TARGET='1'""", conn)
    conn.close()
    df_CRAURF_all = df_CRAURF_all[~df_CRAURF_all['LICSNO'].isnull()]
    df_CRAURF_all['LICSNO'] = df_CRAURF_all['LICSNO'].map(lambda x: x.strip())
    # 沒有一個車牌多個人
    df_CRAURF_all.columns = ['LICSNO', 'TARGET1', 'CUSTID1', 'FORCEID1']
    df_combined = df_combined.merge(df_CRAURF_all, how='left', on='LICSNO')

    # TARGET='2'
    df_CRAURF_all = None
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF_all = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF WHERE TARGET='2'""", conn)
    conn.close()
    df_CRAURF_all = df_CRAURF_all[~df_CRAURF_all['LICSNO'].isnull()]
    df_CRAURF_all['LICSNO'] = df_CRAURF_all['LICSNO'].map(lambda x: x.strip())
    df_CRAURF_all.columns = ['LICSNO', 'TARGET2', 'CUSTID2', 'FORCEID2']
    df_combined = df_combined.merge(df_CRAURF_all, how='left', on='LICSNO')

    # TARGET='3'
    df_CRAURF_all = None
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF_all = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF WHERE TARGET='3'""", conn)
    conn.close()
    df_CRAURF_all = df_CRAURF_all[~df_CRAURF_all['LICSNO'].isnull()]
    df_CRAURF_all['LICSNO'] = df_CRAURF_all['LICSNO'].map(lambda x: x.strip())
    df_CRAURF_all.columns = ['LICSNO', 'TARGET3', 'CUSTID3', 'FORCEID3']
    df_combined = df_combined.merge(df_CRAURF_all, how='left', on='LICSNO')

    # TARGET='4'
    df_CRAURF_all = None
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF_all = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF WHERE TARGET='4'""", conn)
    conn.close()
    df_CRAURF_all = df_CRAURF_all[~df_CRAURF_all['LICSNO'].isnull()]
    df_CRAURF_all['LICSNO'] = df_CRAURF_all['LICSNO'].map(lambda x: x.strip())
    df_CRAURF_all.columns = ['LICSNO', 'TARGET4', 'CUSTID4', 'FORCEID4']
    df_combined = df_combined.merge(df_CRAURF_all, how='left', on='LICSNO')

    # TARGET='5'
    df_CRAURF_all = None
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRAURF_all = pd.read_sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF WHERE TARGET='5'""", conn)
    conn.close()
    df_CRAURF_all = df_CRAURF_all[~df_CRAURF_all['LICSNO'].isnull()]
    df_CRAURF_all['LICSNO'] = df_CRAURF_all['LICSNO'].map(lambda x: x.strip())
    df_CRAURF_all.columns = ['LICSNO', 'TARGET5', 'CUSTID5', 'FORCEID5']
    df_combined = df_combined.merge(df_CRAURF_all, how='left', on='LICSNO')

    # 濾掉 qualified =1 但 使用人為id碼數不為10碼者  #限定為自然人
    # print '新增條件 濾掉 qualified =1 但 使用人為id碼數不為10碼者'
    df_temp = pd.DataFrame()
    df_combined['CUSTID1'] = df_combined['CUSTID1'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_combined['CUSTID2'] = df_combined['CUSTID2'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())

    df_combined['FORCEID1'] = df_combined['FORCEID1'].map(lambda x: 0 if pd.isnull(x) else x)
    df_combined['FORCEID2'] = df_combined['FORCEID2'].map(lambda x: 0 if pd.isnull(x) else x)
    df_combined['FORCEID3'] = df_combined['FORCEID3'].map(lambda x: 0 if pd.isnull(x) else x)
    df_combined['FORCEID4'] = df_combined['FORCEID4'].map(lambda x: 0 if pd.isnull(x) else x)
    df_combined['FORCEID5'] = df_combined['FORCEID5'].map(lambda x: 0 if pd.isnull(x) else x)

    df_combined = df_combined[~df_combined['CUSTID2'].isnull()]
    df_temp['lenCUSTID2'] = df_combined.CUSTID2.map(lambda x: len(str(x)) if (isinstance(x, float)) else len(x))

    df_combined = df_combined[
        (df_combined['qualified'] == '0') | ((df_temp['lenCUSTID2'] == 10) & (df_combined['qualified'] == '1'))]

    # print '去除重複車號 '

    df_combined = df_combined.drop_duplicates('LICSNO', keep='first')
    df_combined = df_combined.reset_index(drop=True)

    # 將資料前後空白去掉
    obj_list = df_combined.columns[df_combined.dtypes == 'object'].tolist()
    for index in obj_list:
        df_combined[index] = df_combined[index].map(
            lambda x: x if (isinstance(x, float) or x is None or x is not datetime.datetime) else x.strip())

    df_combined = df_combined[
        ~((df_combined.qualified == '0') & (df_combined.scrapped_150708 == '1') & (df_combined.PSLTAX == '1'))]
    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "16. %s | Save comfined data......" % str(datetime.datetime.now()))
    df_combined.to_csv(Temp_Path + "df_selected_LICSNO.csv", index=False, sep=',', encoding='utf-8')
    del df_combined
    write_Log(Log_File, "ok\n")

    # Code 4: 30Features_construction_FOR_FIRST_TABLE
    # LRDT from SR data
    # 工單相關日期 特徵抽取
    # 新增結帳需要有日期 (NOT null)
    # ----Spark SQL select function with mutithread----start----------------
    # SQL1 -> df_SRMSLPH
    # SQL2 -> df_SRHSLPH
    # SQL3 -> df_SRHSLPH15
    # DatabaseWorker 是用來執行 mutithread 的time class
    # 20180223，經詢問，Spark SQL目前有8顆core來處理 SQL 的 request，所以暫時以8個request為上限

    write_Log(Log_File,
              "17. %s | Features construction from cdp.SRMSLPH and cdp.ISMAIN......" % str(datetime.datetime.now()))
    result_queue = []
    return_index = [None] * 6

    SQL1 = (
        u"""SELECT VIN, WSLPDT, RTPTDT, STRWKDT, FINDT, AGVCARDT, CMPTDT FROM cdp.SRMSLPH WHERE CMPTDT IS NOT NULL""")
    SQL2 = (
        u"""SELECT  VIN, WSLPDT, RTPTDT, STRWKDT,FINDT,AGVCARDT, CMPTDT  FROM cdp.SRHSLPH WHERE CMPTDT IS NOT NULL""")
    SQL3 = (
        u"""SELECT  VIN, WSLPDT, RTPTDT, STRWKDT,FINDT,AGVCARDT, CMPTDT  FROM cdp.SRHSLPH15 WHERE CMPTDT IS NOT NULL""")
    SQL4 = (u"""SELECT LICSNO, EGNO, UPCOD, FENDAT, UENDAT FROM cdp.ISMAIN where UPCOD IS NULL""")
    SQL5 = (u"""SELECT LICSNO, EGNO, UPCOD, FENDAT, UENDAT FROM cdp.ISMAIN where UPCOD ='1'""")
    SQL6 = (u"""SELECT LICSNO, EGNO, UPCOD, FENDAT, UENDAT FROM cdp.ISMAIN where UPCOD ='2'""")

    work1 = DatabaseWorker(SQL1, result_queue)
    work2 = DatabaseWorker(SQL2, result_queue)
    work3 = DatabaseWorker(SQL3, result_queue)
    work4 = DatabaseWorker(SQL4, result_queue)
    work5 = DatabaseWorker(SQL5, result_queue)
    work6 = DatabaseWorker(SQL6, result_queue)

    work1.start()
    work2.start()
    work3.start()
    work4.start()
    work5.start()
    work6.start()

    while len(result_queue) < 6:
        time.sleep(delay)
    job_done = True

    df_SRMSLPH = result_queue[work1.return_index]
    df_SRHSLPH = result_queue[work2.return_index]
    df_SRHSLPH15 = result_queue[work3.return_index]
    df_ISMAIN = result_queue[work4.return_index]
    df_ISMAIN1 = result_queue[work5.return_index]
    df_ISMAIN2 = result_queue[work6.return_index]

    # ----Spark SQL select function with mutithread----end----------------

    df_allSR = pd.DataFrame()
    df_allSR = df_allSR.append(df_SRMSLPH, ignore_index=False)
    df_allSR = df_allSR.append(df_SRHSLPH, ignore_index=False)
    df_allSR = df_allSR.append(df_SRHSLPH15, ignore_index=False)

    df_allSR.AGVCARDT = pd.to_datetime(df_allSR.AGVCARDT, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df_allSR = df_allSR[~df_allSR['VIN'].isnull()]
    df_allSR['VIN'] = df_allSR['VIN'].map(lambda x: x.strip())

    # 設定去除異常資料
    df_allSR.loc[df_allSR.WSLPDT < pd.Timestamp('1988-01-01'), 'WSLPDT'] = np.nan
    df_allSR.loc[df_allSR.RTPTDT < pd.Timestamp('1988-01-01'), 'RTPTDT'] = np.nan
    df_allSR.loc[df_allSR.STRWKDT < pd.Timestamp('1988-01-01'), 'STRWKDT'] = np.nan
    df_allSR.loc[df_allSR.FINDT < pd.Timestamp('1988-01-01'), 'FINDT'] = np.nan
    df_allSR.loc[df_allSR.AGVCARDT < pd.Timestamp('1988-01-01'), 'AGVCARDT'] = np.nan

    df_allSR.loc[df_allSR.WSLPDT > pd.Timestamp(END_DATE), 'WSLPDT'] = np.nan
    df_allSR.loc[df_allSR.RTPTDT > pd.Timestamp(END_DATE), 'RTPTDT'] = np.nan
    df_allSR.loc[df_allSR.STRWKDT > pd.Timestamp(END_DATE), 'STRWKDT'] = np.nan
    df_allSR.loc[df_allSR.FINDT > pd.Timestamp(END_DATE), 'FINDT'] = np.nan
    df_allSR.loc[df_allSR.AGVCARDT > pd.Timestamp(END_DATE), 'AGVCARDT'] = np.nan

    # WSLPDT, RTPTDT, STRWKDT,FINDT,AGVCARDT
    df_result_WSLPDT = LRDT_calculator_dask(df_allSR, 'VIN', 'WSLPDT')
    df_result_RTPTDT = LRDT_calculator_dask(df_allSR, 'VIN', 'RTPTDT')
    df_result_STRWKDT = LRDT_calculator_dask(df_allSR, 'VIN', 'STRWKDT')
    df_result_FINDT = LRDT_calculator_dask(df_allSR, 'VIN', 'FINDT')
    df_result_AGVCARDT = LRDT_calculator_dask(df_allSR, 'VIN', 'AGVCARDT')

    result = pd.DataFrame()
    result = pd.concat([df_result_WSLPDT, df_result_RTPTDT], axis=1)
    result = pd.concat([result, df_result_STRWKDT], axis=1)
    result = pd.concat([result, df_result_FINDT], axis=1)
    result = pd.concat([result, df_result_AGVCARDT], axis=1)

    # 另外加入新的特徵 2014年前的移除 只找三年內的

    df_allSR.loc[df_allSR.WSLPDT < pd.Timestamp(DATE_3y), 'WSLPDT'] = np.nan
    df_result_WSLPDT_3year = year3_LRDT_calculator_dask(df_allSR, 'VIN', 'WSLPDT')
    result = pd.concat([result, df_result_WSLPDT_3year], axis=1)
    result = result.reset_index()
    result.to_csv(Temp_Path + "Feature_LRDT.csv", index=False, sep=',', encoding='utf-8')
    del result
    df_SRMSLPH = None
    df_SRHSLPH = None
    df_SRHSLPH15 = None
    df_allSR = None

    # ## ISMAIN 相關資訊
    df_ISMAIN1.loc[:, 'FENDAT'] = np.nan
    df_ISMAIN2.loc[:, 'UENDAT'] = np.nan

    df_ISMAIN_all = pd.DataFrame()
    df_ISMAIN_all = df_ISMAIN_all.append(df_ISMAIN, ignore_index=True)
    df_ISMAIN_all = df_ISMAIN_all.append(df_ISMAIN1, ignore_index=True)
    df_ISMAIN_all = df_ISMAIN_all.append(df_ISMAIN2, ignore_index=True)

    df_ISMAIN_all.FENDAT = pd.to_datetime(df_ISMAIN_all.FENDAT, format='%Y%m%d', errors='coerce')
    df_ISMAIN_all.UENDAT = pd.to_datetime(df_ISMAIN_all.UENDAT, format='%Y%m%d', errors='coerce')
    df_ISMAIN_all = df_ISMAIN_all[~df_ISMAIN_all['LICSNO'].isnull()]
    df_ISMAIN_all['LICSNO'] = df_ISMAIN_all['LICSNO'].map(lambda x: x.strip())

    # REMOVE abnormal date
    df_ISMAIN_all.loc[df_ISMAIN_all.FENDAT < pd.Timestamp('1988-01-01'), 'FENDAT'] = np.nan
    df_ISMAIN_all.loc[df_ISMAIN_all.UENDAT < pd.Timestamp('1988-01-01'), 'UENDAT'] = np.nan

    df_ISMAIN_all.loc[df_ISMAIN_all.FENDAT > pd.Timestamp(END_DATE), 'FENDAT'] = np.nan
    df_ISMAIN_all.loc[df_ISMAIN_all.UENDAT > pd.Timestamp(END_DATE), 'UENDAT'] = np.nan

    df_result_FENDAT_byLISCNO = IS_calculator_dask(df_ISMAIN_all, 'LICSNO', 'FENDAT')
    df_result_UENDAT_byLISCNO = IS_calculator_dask(df_ISMAIN_all, 'LICSNO', 'UENDAT')

    result_byLISCNO = pd.concat([df_result_FENDAT_byLISCNO, df_result_UENDAT_byLISCNO], axis=1)

    result_byLISCNO = result_byLISCNO.reset_index()
    result_byLISCNO.to_csv(Temp_Path + "Feature_ISrelated.csv", index=False, sep=',', encoding='utf-8')
    del result_byLISCNO
    df_ISMAIN = None
    df_ISMAIN1 = None
    df_ISMAIN2 = None
    df_ISMAIN_all = None
    write_Log(Log_File, "ok\n")
    write_Log(Log_File,
              "18. %s | Features construction from cdp.CRAURF and cdp.CRCAMF......" % str(datetime.datetime.now()))

    ## 車輛擁有數CARCNT 計算 尚未說明
    # 從 CRAURF、CRCAMF把資料撈出來，利用CRAURF裡面的Target欄位（紀錄CUSTID是屬於領照人（=1）或是使用人（=2）)
    # 計算領照人名下有幾台車，且Target是領照人，u'carCNT11
    # 計算領照人名下有幾台車，且Target是使用人，u'carCNT12
    # 計算使用人名下有幾台車，且Target是領照人，u'carCNT21
    # 計算使用人名下有幾台車，且Target是使用人，u'carCNT22
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)

    # 1 領照人 2 使用人 
    # 找出 車輛領照人或使用人
    # df_CRAURF1 = pd.read_sql(u"""SELECT LICSNO, CUSTID as CUSTID1, TARGET FROM CRAURF where (TARGET = 1 or TARGET = 3)  order by TARGET """, conn)
    df_CRAURF1 = pd.read_sql(u"""
  SELECT a.LICSNO, a.CUSTID as CUSTID1, a.TARGET, b.LSKIDT , b.SEDLDT, b.STRDT
    FROM cdp.CRAURF a
    LEFT JOIN cdp.CRCAMF b on a.LICSNO=b.LICSNO where (a.TARGET = 1 or a.TARGET = 3)  order by a.TARGET 
  """, conn)

    # 找出車輛使用人
    # df_CRAURF2 = pd.read_sql(u"""SELECT LICSNO, CUSTID as CUSTID2, TARGET FROM CRAURF where TARGET =2""", conn)
    df_CRAURF2 = pd.read_sql(u"""
  SELECT a.LICSNO, a.CUSTID as CUSTID2, a.TARGET, b.LSKIDT , b.SEDLDT, b.STRDT
    FROM cdp.CRAURF a
    LEFT JOIN cdp.CRCAMF b on a.LICSNO=b.LICSNO where a.TARGET = 2""", conn)

    # 濾掉 身分證錯誤者  FORECEID =1 備註: 身分證號碼錯誤 仍然可以存在系統中 當key 只是格式不符 #有些 沒有領照人 但有 使CR用 發票 聯絡
    conn.close()
    # df_CRAURF1: Target = '1:領照人' or '3:發票人'，表示CUSTID是領照人 or 發票人
    # df_CRAURF2: Target = '2:使用人'，表示CUSTID是使用人

    df_CRAURF1.drop_duplicates('LICSNO', keep='first', inplace=True)

    # u'PRODDT', u'LOADT', u'ISADT',   u'SALEDT', u'LSKIDT', u'REDLDT'
    # LSKIDT: 登錄日期，from CRCAMF
    # SEDLDT: 原始交車日，from CRCAMF    
    df_CRAURF1[u'LSKIDT_fix'] = time_cleaner(df_CRAURF1[u'LSKIDT'])
    df_CRAURF1[u'SEDLDT_fix'] = time_cleaner(df_CRAURF1[u'SEDLDT'])
    df_CRAURF2[u'LSKIDT_fix'] = time_cleaner(df_CRAURF2[u'LSKIDT'])
    df_CRAURF2[u'SEDLDT_fix'] = time_cleaner(df_CRAURF2[u'SEDLDT'])

    # LSKIDT_fix:登錄日期、SEDLDT_fix:原始交車日兩個取小的存在 df_CRAURF1[u'LSKIDT_stamp'] 
    df_CRAURF1[u'LSKIDT_stamp'] = df_CRAURF1[[u'LSKIDT_fix', u'SEDLDT_fix']].min(axis=1)
    df_CRAURF1.loc[df_CRAURF1[u'LSKIDT_stamp'] == 'NaT', u'LSKIDT_stamp'] = df_CRAURF1.loc[
        df_CRAURF1[u'LSKIDT_stamp'] == 'NaT', u'STRDT']
    # STRDT: 建檔日期，from CRCAMF

    df_CRAURF2[u'LSKIDT_stamp'] = df_CRAURF2[[u'LSKIDT_fix', u'SEDLDT_fix']].min(axis=1)
    df_CRAURF2.loc[df_CRAURF2[u'LSKIDT_stamp'] == 'NaT', u'LSKIDT_stamp'] = df_CRAURF2.loc[
        df_CRAURF2[u'LSKIDT_stamp'] == 'NaT', u'STRDT']
    # 最近新買的車移除
    df_CRAURF1 = df_CRAURF1[df_CRAURF1[u'LSKIDT_stamp'] <= '2015-07-08'].reset_index(drop=True)
    df_CRAURF2 = df_CRAURF2[df_CRAURF2[u'LSKIDT_stamp'] <= '2015-07-08'].reset_index(drop=True)

    # space remove
    df_CRAURF1['CUSTID1'] = df_CRAURF1['CUSTID1'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_CRAURF2['CUSTID2'] = df_CRAURF2['CUSTID2'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_CRAURF1['LICSNO'] = df_CRAURF1['LICSNO'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_CRAURF2['LICSNO'] = df_CRAURF2['LICSNO'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())

    # CUSTID1 領照的車牌數量統計 車牌號碼不重複的數量
    result_CARCNT_CUSTID1_nunique = df_CRAURF1[['LICSNO', 'CUSTID1']].groupby('CUSTID1').agg(lambda x: x.nunique())
    result_CARCNT_CUSTID1_nunique.reset_index(drop=False, inplace=True)
    # CUSTID2 使用的車牌數量統計
    result_CARCNT_CUSTID2_nunique = df_CRAURF2[['LICSNO', 'CUSTID2']].groupby('CUSTID2').agg(lambda x: x.nunique())
    result_CARCNT_CUSTID2_nunique.reset_index(drop=False, inplace=True)

    df_CRAURF_join12 = df_CRAURF1[['LICSNO', 'CUSTID1']].merge(df_CRAURF2[['LICSNO', 'CUSTID2']], how='left',
                                                               on='LICSNO')
    # 確認df_CRAURF1 df_CRAURF2都沒有重複車牌

    # 計算 領照人名下領照人車輛數 領照人名下使用人車輛數 使用人名下領照人車輛數  使用人名下使用人車輛數 
    df_CRAURF_join12 = df_CRAURF_join12.merge(result_CARCNT_CUSTID1_nunique, how='left', left_on='CUSTID1',
                                              right_on='CUSTID1', suffixes=('', '_y'))
    df_CRAURF_join12 = df_CRAURF_join12.rename(index=str, columns={u'LICSNO_y': u'carCNT11'})

    df_CRAURF_join12 = df_CRAURF_join12.merge(result_CARCNT_CUSTID1_nunique, how='left', left_on='CUSTID2',
                                              right_on='CUSTID1', suffixes=('', '_y'))
    df_CRAURF_join12.drop('CUSTID1_y', axis=1, inplace=True)
    df_CRAURF_join12 = df_CRAURF_join12.rename(index=str, columns={u'LICSNO_y': u'carCNT21'})

    df_CRAURF_join12 = df_CRAURF_join12.merge(result_CARCNT_CUSTID2_nunique, how='left', left_on='CUSTID1',
                                              right_on='CUSTID2', suffixes=('', '_y'))
    df_CRAURF_join12 = df_CRAURF_join12.rename(index=str, columns={u'LICSNO_y': u'carCNT12'})

    df_CRAURF_join12 = df_CRAURF_join12.merge(result_CARCNT_CUSTID2_nunique, how='left', left_on='CUSTID2',
                                              right_on='CUSTID2', suffixes=('', '_y'))
    df_CRAURF_join12.drop('CUSTID2_y', axis=1, inplace=True)
    df_CRAURF_join12 = df_CRAURF_join12.rename(index=str, columns={u'LICSNO_y': u'carCNT22'})

    # 0804 只計算20150701之後的車輛 
    df_CRAURF_join12.to_csv(Temp_Path + "Feature_CARCNT_new.csv", index=False, sep=',', encoding='utf-8')
    df_CRAURF1 = None
    df_CRAURF2 = None
    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "19. %s | Features construction from cdp.SRMSLPH, cdp.SRHSLPH, and SRHSLPH15......" % str(
        datetime.datetime.now()))

    # sr定保 維修 次數計算
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SRMSLPH = pd.read_sql(
        u"""SELECT VIN, WORKNO, WSLPDT, WKCTCD, PAYCD, INJ, CMPTDT FROM cdp.SRMSLPH WHERE CMPTDT IS NOT NULL""", conn)
    df_SRHSLPH = pd.read_sql(
        u"""SELECT VIN, WORKNO, WSLPDT, WKCTCD, PAYCD, INJ, CMPTDT FROM cdp.SRHSLPH WHERE CMPTDT IS NOT NULL""", conn)
    df_SRHSLPH15 = pd.read_sql(
        u"""SELECT  VIN, WORKNO, WSLPDT, WKCTCD, PAYCD, INJ, CMPTDT FROM cdp.SRHSLPH15 WHERE CMPTDT IS NOT NULL""",
        conn)
    conn.close()

    df_allSR = pd.DataFrame()
    df_allSR = df_allSR.append(df_SRMSLPH, ignore_index=False)
    df_allSR = df_allSR.append(df_SRHSLPH, ignore_index=False)
    df_allSR = df_allSR.append(df_SRHSLPH15, ignore_index=False)
    df_allSR['VIN'] = df_allSR['VIN'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    # WKCTCD = *(star):零件外販、 1:千公里定保、2:萬公里定保、3:一般修理、B:板金、P:噴漆、T:TPS LINE
    result_SR_WKCTCD_star = df_allSR[df_allSR['WKCTCD'] == '*'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_1 = df_allSR[df_allSR['WKCTCD'] == '1'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_2 = df_allSR[df_allSR['WKCTCD'] == '2'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_3 = df_allSR[df_allSR['WKCTCD'] == '3'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_B = df_allSR[df_allSR['WKCTCD'] == 'B'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_P = df_allSR[df_allSR['WKCTCD'] == 'P'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_T = df_allSR[df_allSR['WKCTCD'] == 'T'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    # PAYCD = "":未指定、A:甲式、 B:乙式、 C:丙式、 D:財損、 E:自費
    result_SR_PAYCD_ = df_allSR[df_allSR['PAYCD'] == ''][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_PAYCD_A = df_allSR[df_allSR['PAYCD'] == 'A'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_PAYCD_B = df_allSR[df_allSR['PAYCD'] == 'B'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_PAYCD_C = df_allSR[df_allSR['PAYCD'] == 'C'][['VIN', 'WORKNO']].groupby('VIN').agg('count')

    # INJ = A:A級損傷、 B:B級損傷、 C:C級損傷、 D:D級損傷
    result_SR_INJ_A = df_allSR[df_allSR['INJ'] == 'A'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_INJ_B = df_allSR[df_allSR['INJ'] == 'B'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_INJ_C = df_allSR[df_allSR['INJ'] == 'C'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_INJ_D = df_allSR[df_allSR['INJ'] == 'D'][['VIN', 'WORKNO']].groupby('VIN').agg('count')

    result_SR_WKCTCD = pd.DataFrame()
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD_star, result_SR_WKCTCD_1], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_2], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_3], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_B], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_P], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_T], axis=1)

    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_A], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_B], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_C], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_A], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_B], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_C], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_D], axis=1)

    # 最近三年前

    df_allSR = df_allSR[df_allSR.WSLPDT > pd.Timestamp(DATE_3y)].reset_index(drop=True)

    result_SR_WKCTCD_star_3year = df_allSR[df_allSR['WKCTCD'] == '*'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_1_3year = df_allSR[df_allSR['WKCTCD'] == '1'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_2_3year = df_allSR[df_allSR['WKCTCD'] == '2'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_3_3year = df_allSR[df_allSR['WKCTCD'] == '3'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_B_3year = df_allSR[df_allSR['WKCTCD'] == 'B'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_P_3year = df_allSR[df_allSR['WKCTCD'] == 'P'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_WKCTCD_T_3year = df_allSR[df_allSR['WKCTCD'] == 'T'][['VIN', 'WORKNO']].groupby('VIN').agg('count')

    result_SR_PAYCD_3year = df_allSR[df_allSR['PAYCD'] == ''][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_PAYCD_A_3year = df_allSR[df_allSR['PAYCD'] == 'A'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_PAYCD_B_3year = df_allSR[df_allSR['PAYCD'] == 'B'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_PAYCD_C_3year = df_allSR[df_allSR['PAYCD'] == 'C'][['VIN', 'WORKNO']].groupby('VIN').agg('count')

    result_SR_INJ_A_3year = df_allSR[df_allSR['INJ'] == 'A'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_INJ_B_3year = df_allSR[df_allSR['INJ'] == 'B'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_INJ_C_3year = df_allSR[df_allSR['INJ'] == 'C'][['VIN', 'WORKNO']].groupby('VIN').agg('count')
    result_SR_INJ_D_3year = df_allSR[df_allSR['INJ'] == 'D'][['VIN', 'WORKNO']].groupby('VIN').agg('count')

    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_star_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_1_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_2_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_3_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_B_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_P_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_WKCTCD_T_3year], axis=1)

    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_A_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_B_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_PAYCD_C_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_A_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_B_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_C_3year], axis=1)
    result_SR_WKCTCD = pd.concat([result_SR_WKCTCD, result_SR_INJ_D_3year], axis=1)

    result_SR_WKCTCD.columns = ['WKCTCD_star_counts', 'WKCTCD_1_counts', 'WKCTCD_2_counts', 'WKCTCD_3_counts',
                                'WKCTCD_B_counts', 'WKCTCD_P_counts', 'WKCTCD_T_counts', 'PAYCD_counts',
                                'PAYCD_A_counts', 'PAYCD_B_counts', 'PAYCD_C_counts', 'INJ_A_counts', 'INJ_B_counts',
                                'INJ_C_counts', 'INJ_D_counts', 'WKCTCD_star_counts_3year', 'WKCTCD_1_counts_3year',
                                'WKCTCD_2_counts_3year', 'WKCTCD_3_counts_3year', 'WKCTCD_B_counts_3year',
                                'WKCTCD_P_counts_3year', 'WKCTCD_T_counts_3year', 'PAYCD_counts_3year',
                                'PAYCD_A_counts_3year', 'PAYCD_B_counts_3year', 'PAYCD_C_counts_3year',
                                'INJ_A_counts_3year', 'INJ_B_counts_3year', 'INJ_C_counts_3year', 'INJ_D_counts_3year']

    result_SR_WKCTCD = result_SR_WKCTCD.fillna(0)

    result_SR_WKCTCD = result_SR_WKCTCD.reset_index()
    result_SR_WKCTCD.to_csv(Temp_Path + "Feature_SR_WKCTCD.csv", index=False, sep=',', encoding='utf-8')

    del result_SR_WKCTCD  # 釋放 result_SR_WKCTCD 的記憶體空間
    df_allSR = None
    df_SRMSLPH = None
    df_SRHSLPH = None
    df_SRHSLPH15 = None

    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "20. %s | Features construction from cdp.SRLFOWM......" % str(datetime.datetime.now()))

    # 計算2014/01/01之後，車子進場維修的各項統計，包含維修名稱、付款方式、損傷級數，結果存放在 Feature_SR_WKCTCD_1220.csv
    # SRLFOWM feature construction to csv file
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SRLFOWM = pd.read_sql(u"""SELECT * FROM cdp.SRLFOWM""", conn)
    conn.close()


    def dataframe_object_strip(df_input):
        for index in range(0, len(df_input.columns)):
            if (df_input[df_input.columns[index]].dtypes == 'object'):
                #             print (index)
                df_input[df_input.columns[index]] = df_input[df_input.columns[index]].map(
                    lambda x: str(x).strip() if x != None else None)
                #     print ('done')
        return df_input


    df_SRLFOWM = dataframe_object_strip(df_SRLFOWM)

    # DMFWDT:勸誘日期，把小1988/01/01之前跟END_DATE之後的，資料改成nan
    df_SRLFOWM.loc[df_SRLFOWM.DMFWDT < pd.Timestamp('1988-01-01'), 'DMFWDT'] = np.nan
    df_SRLFOWM.loc[df_SRLFOWM.DMFWDT > pd.Timestamp(END_DATE), 'DMFWDT'] = np.nan

    ##勸誘時間max min range     SRLFOWM_DMFWDT_max SRLFOWM_DMFWDT_min SRLFOWM_DMFWDT_range
    result_DMFWDT_max = df_SRLFOWM[['DMFWDT', 'VIN']].groupby('VIN').agg('max')
    result_DMFWDT_min = df_SRLFOWM[['DMFWDT', 'VIN']].groupby('VIN').agg('min')
    result_DMFWDT_range = result_DMFWDT_max - result_DMFWDT_min

    # # DLRCD_BR 個數  SRLFOWM_DLRCD_BR_nunique SRLFOWM_DLRCD_BR_maxoccu
    # DLRCD:經銷商代碼、BRNHCD:據點代碼，DLRCD_BR:兩個合併
    df_SRLFOWM['DLRCD_BR'] = df_SRLFOWM['DLRCD'] + df_SRLFOWM['BRNHCD']
    result_DLRCD_BR_nunique = df_SRLFOWM[['DLRCD_BR', 'VIN']].groupby('VIN').agg(lambda x: x.nunique())
    result_DLRCD_BR_maxoccu = df_SRLFOWM[['DLRCD_BR', 'VIN']].groupby('VIN').agg(
        lambda x: x.value_counts(dropna=False).index[0])

    # 專員 個數  SRLFOWM_SRVNO_nunique SRLFOWM_SRVNO_maxoccu
    result_SRVNO_nunique = df_SRLFOWM[['SRVNO', 'VIN']].groupby('VIN').agg(lambda x: x.nunique())
    result_SRVNO_maxoccu = df_SRLFOWM[['SRVNO', 'VIN']].groupby('VIN').agg(
        lambda x: x.value_counts(dropna=False).index[0])

    # 定保勸誘里程數max   SRLFOWM_FXKM_max
    result_FXKM_max = df_SRLFOWM[['FXKM', 'VIN']].groupby('VIN').agg('max')

    # EXCSTAF A S
    result_EXCSTAF_Acount = df_SRLFOWM[df_SRLFOWM['EXCSTAF'] == 'A'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_EXCSTAF_Scount = df_SRLFOWM[df_SRLFOWM['EXCSTAF'] == 'S'][['DLRCD', 'VIN']].groupby('VIN').agg('count')

    # APPFLG 
    result_APPFLG_Ncount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'N'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_Ycount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'Y'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_Scount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'S'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_NAcount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == ''][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_Bcount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'B'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_Acount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'A'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_Dcount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'D'][['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_APPFLG_Ccount = df_SRLFOWM[df_SRLFOWM['APPFLG'] == 'C'][['DLRCD', 'VIN']].groupby('VIN').agg('count')

    result = pd.DataFrame()
    result = pd.concat([result_DMFWDT_max, result_DMFWDT_min], axis=1)
    result = pd.concat([result, result_DMFWDT_range], axis=1)
    result = pd.concat([result, result_DLRCD_BR_nunique], axis=1)
    result = pd.concat([result, result_DLRCD_BR_maxoccu], axis=1)
    result = pd.concat([result, result_SRVNO_nunique], axis=1)
    result = pd.concat([result, result_SRVNO_maxoccu], axis=1)
    result = pd.concat([result, result_FXKM_max], axis=1)
    result = pd.concat([result, result_EXCSTAF_Acount], axis=1)
    result = pd.concat([result, result_EXCSTAF_Scount], axis=1)
    result = pd.concat([result, result_APPFLG_Ncount], axis=1)
    result = pd.concat([result, result_APPFLG_Ycount], axis=1)
    result = pd.concat([result, result_APPFLG_Scount], axis=1)
    result = pd.concat([result, result_APPFLG_NAcount], axis=1)
    result = pd.concat([result, result_APPFLG_Bcount], axis=1)
    result = pd.concat([result, result_APPFLG_Acount], axis=1)
    result = pd.concat([result, result_APPFLG_Dcount], axis=1)
    result = pd.concat([result, result_APPFLG_Ccount], axis=1)

    result.columns = ['SRLFOWM_DMFWDT_max',
                      'SRLFOWM_DMFWDT_min',
                      'SRLFOWM_DMFWDT_range',
                      'SRLFOWM_DLRCD_BR_nunique',
                      'SRLFOWM_DLRCD_BR_maxoccu',
                      'SRLFOWM_SRVNO_nunique',
                      'SRLFOWM_SRVNO_maxoccu',
                      'SRLFOWM_FXKM_max',
                      'SRLFOWM_EXCSTAF_Acount',
                      'SRLFOWM_EXCSTAF_Scount',
                      'SRLFOWM_APPFLG_Ncount',
                      'SRLFOWM_APPFLG_Ycount',
                      'SRLFOWM_APPFLG_Scount',
                      'SRLFOWM_APPFLG_NAcount',
                      'SRLFOWM_APPFLG_Bcount',
                      'SRLFOWM_APPFLG_Acount',
                      'SRLFOWM_APPFLG_Dcount',
                      'SRLFOWM_APPFLG_Ccount']
    result.head()
    ##save csv
    result.reset_index(inplace=True)
    result = result.rename(columns={'index': 'VIN'})
    result.to_csv(Temp_Path + "Feature_SRLFOWM.csv", index=False, sep=',', encoding='utf-8')
    write_Log(Log_File, "ok\n")
    END_DATE
    del result
    df_SRLFOWM = None
    '''
    write_Log(Log_File
              , "21. %s | Features construction from cdp.SRMINVO......" % str(datetime.datetime.now()))

    spark = SparkSession.builder.appName("21.Features-construction-from-cdp.SRMINVO") \
        .master("local") \
        .config("", sparkConf) \
        .config("spark.cassandra.connection.host", "10.201.2.130,10.201.2.131,10.201.2.132") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    FIX_DATE = datetime.datetime.strptime('1988-01-01', "%Y-%m-%d")
    END_DATE_TIME = datetime.datetime.strptime(END_DATE, "%Y-%m-%d")
    DATE_3y_TIME = datetime.datetime.strptime(DATE_3y, "%Y-%m-%d")

    df_allSRMINVO = spark.read.parquet("/spark-prejoin-model/srh.parquet")

    # INVODT
    df_allSRMINVO = df_allSRMINVO.withColumn("VIN_STRIP", trim(col("VIN")))
    # Drop VIN field at first, then rename column VIN_STRIP to VIN
    df_allSRMINVO = df_allSRMINVO.drop("VIN")
    df_allSRMINVO = df_allSRMINVO.withColumnRenamed("VIN_STRIP", "VIN")
    # 新增欄位INVODT_TRANS 將INVODT小於1988-01-01 或是 大於今日的日期 轉換成NA
    transDate_UDF = udf(lambda date: None if (date is None or
                                              date < FIX_DATE or date > END_DATE_TIME) else date,
                        TimestampType())

    df_allSRMINVO = df_allSRMINVO.withColumn("INVODT_TRANS", transDate_UDF("INVODT"))
    # Drop INVODT field at first, then rename column INVODT_TRANS to INVODT
    df_allSRMINVO = df_allSRMINVO.drop("INVODT")
    df_allSRMINVO = df_allSRMINVO.withColumnRenamed("INVODT_TRANS", "INVODT")

    # INVODT min max range days
    print("INVODT min max range days")
    result_INVODT = df_allSRMINVO.groupby('VIN').agg(max('INVODT').alias("result_INVODT_max"),
                                                     min('INVODT').alias("result_INVODT_min"),
                                                     count('INVODT').alias("result_INVODT_count"))
    result_INVODT = result_INVODT.withColumn("result_INVODT_range",
                                             datediff(col("result_INVODT_max"), col("result_INVODT_min")).alias(
                                                 "result_INVODT_range"))
    result_INVODT = result_INVODT.withColumn("result_INVODT_END_DATE", lit(END_DATE))
    result_INVODT = result_INVODT.withColumn("result_INVODT_max_days",
                                             datediff("result_INVODT_END_DATE", "result_INVODT_max"))
    result_INVODT = result_INVODT.withColumn("result_INVODT_min_days",
                                             datediff("result_INVODT_END_DATE", "result_INVODT_min"))
    # DLRCD 經銷商營業所, 新增DLRCD+BRNHCD欄位
    df_allSRMINVO = df_allSRMINVO.withColumn('DLRCD_BRNHCD', concat(col('DLRCD'), col('BRNHCD')))
    # Nunique,  maxoccu
    print("Nunique,  maxoccu")
    result_SRMINVO_DLRCD_Nunique = df_allSRMINVO.groupBy('VIN').agg(
        countDistinct('DLRCD_BRNHCD').alias("result_SRMINVO_DLRCD_Nunique"))
    window_result_SRMINVO_DLRCD_maxoccu = Window.partitionBy(df_allSRMINVO['VIN']).orderBy(
        df_allSRMINVO['DLRCD_BRNHCD'].desc())
    df_allSRMINVO = df_allSRMINVO.withColumn("result_SRMINVO_DLRCD_maxoccu",
                                             row_number().over(window_result_SRMINVO_DLRCD_maxoccu))
    result_SRMINVO_DLRCD_maxoccu = df_allSRMINVO.where("result_SRMINVO_DLRCD_maxoccu=1").select("VIN", "DLRCD_BRNHCD")

    # INVONO  *免費  #出保固 次數
    print("INVONO  *免費  #出保固 次數")
    # 將INVONO第0位字元等於 * (免費)挑出來
    result_SRMINVO_INVONO_freecount = df_allSRMINVO.filter(df_allSRMINVO.INVONO.substr(0, 1) == "*").groupby('VIN').agg(
        count("INVONO").alias("result_SRMINVO_INVONO_freecount"))
    # 將INVONO第0位字元等於 # (出保固)挑出來
    result_SRMINVO_INVONO_quarcount = df_allSRMINVO.filter(df_allSRMINVO.INVONO.substr(0, 1) == "#").groupby('VIN').agg(
        count("INVONO").alias("result_SRMINVO_INVONO_quarcount"))

    # INVTXCD 主要發票聯式
    print("INVTXCD 主要發票聯式")
    window_result_SRMINVO_INVTXCD_maxoccu = Window.partitionBy(df_allSRMINVO['VIN']).orderBy(
        df_allSRMINVO['INVTXCD'].desc())
    df_allSRMINVO = df_allSRMINVO.withColumn("result_SRMINVO_INVTXCD_maxoccu",
                                             row_number().over(window_result_SRMINVO_INVTXCD_maxoccu))
    result_SRMINVO_INVTXCD_maxoccu = df_allSRMINVO.where("result_SRMINVO_INVTXCD_maxoccu=1").select("VIN", "INVTXCD")

    # TOTAMT 總金額 MAX MIN MEAN SUM
    print("TOTAMT 總金額 MAX MIN MEAN SUM")
    result_SRMINVO_TOTAMT = df_allSRMINVO.groupBy('VIN').agg(max('TOTAMT').alias("result_SRMINVO_TOTAMT_max"),
                                                             min('TOTAMT').alias("result_SRMINVO_TOTAMT_min"),
                                                             avg('TOTAMT').alias("result_SRMINVO_TOTAMT_mean"),
                                                             sum('TOTAMT').alias("result_SRMINVO_TOTAMT_sum"))

    # INSURCD 保險公司出險次數
    print("INSURCD 保險公司出險次數")
    result_SRMINVO_INSURCD_count = df_allSRMINVO.filter(df_allSRMINVO.INSURCD != '').groupBy('VIN').agg(
        count("INSURCD").alias("result_SRMINVO_INSURCD_count"))

    # IRNAMT 板金次數
    print("IRNAMT 板金次數")
    result_SRMINVO_IRNAMT_count = df_allSRMINVO.filter(df_allSRMINVO.IRNAMT > 0).groupBy('VIN').agg(
        count("IRNAMT").alias("result_SRMINVO_IRNAMT_count"))

    # WSHAMT 噴漆次數
    print("WSHAMT 噴漆次數")
    result_SRMINVO_WSHAMT_count = df_allSRMINVO.filter(df_allSRMINVO.WSHAMT > 0).groupBy('VIN').agg(
        count("WSHAMT").alias("result_SRMINVO_WSHAMT_count"))

    # 3year內
    print("3year內")
    df_allSRMINVO_3year = df_allSRMINVO.filter(df_allSRMINVO.INVODT > DATE_3y_TIME).select("VIN", "TOTAMT")

    # 三年內總金額
    print("三年內總金額")
    result_SRMINVO_TOTAMT_sum_3year = df_allSRMINVO_3year.groupBy('VIN').agg(
        sum('TOTAMT').alias("result_SRMINVO_TOTAMT_sum_3year"))

    result_SRMINVO = result_INVODT.join(result_SRMINVO_DLRCD_Nunique, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_DLRCD_maxoccu, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_INVONO_freecount, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_INVONO_quarcount, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_INVTXCD_maxoccu, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_TOTAMT, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_INSURCD_count, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_IRNAMT_count, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_WSHAMT_count, "VIN", "left")
    result_SRMINVO = result_SRMINVO.join(result_SRMINVO_TOTAMT_sum_3year, "VIN", "left")

    # udf fillna
    fillna_INT_UDF = udf(lambda value: 0 if value is None else value, IntegerType())
    fillna_String_UDF = udf(lambda value: 'NA' if value is None else value, StringType())
    fillna_Float_UDF = udf(lambda value: 0.0 if value is None else value, FloatType())

    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_INVODT_count',
                                               fillna_INT_UDF(result_SRMINVO.result_INVODT_count))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_INVONO_freecount',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_INVONO_freecount))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_INVONO_quarcount',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_INVONO_quarcount))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_INVTXCD_maxoccu', fillna_String_UDF(result_SRMINVO.INVTXCD))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_TOTAMT_max',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_TOTAMT_max))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_TOTAMT_min',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_TOTAMT_min))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_TOTAMT_mean',
                                               fillna_Float_UDF(result_SRMINVO.result_SRMINVO_TOTAMT_mean))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_TOTAMT_sum',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_TOTAMT_sum))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_IRNAMT_count',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_IRNAMT_count))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_WSHAMT_count',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_WSHAMT_count))
    result_SRMINVO = result_SRMINVO.withColumn('SRMINVO_TOTAMT_sum_3year',
                                               fillna_INT_UDF(result_SRMINVO.result_SRMINVO_TOTAMT_sum_3year))

    result_SRMINVO = result_SRMINVO.selectExpr("VIN",
                                               "result_INVODT_max_days as SRMINVO_INVODT_max",
                                               "result_INVODT_min_days as SRMINVO_INVODT_min",
                                               "SRMINVO_INVODT_count",
                                               "result_INVODT_range as SRMINVO_INVODT_range",
                                               "result_SRMINVO_DLRCD_Nunique as SRMINVO_DLRCD_Nunique",
                                               "DLRCD_BRNHCD as SRMINVO_DLRCD_maxoccu",
                                               "SRMINVO_INVONO_freecount",
                                               "SRMINVO_INVONO_quarcount",
                                               "SRMINVO_INVTXCD_maxoccu",
                                               "SRMINVO_TOTAMT_max",
                                               "SRMINVO_TOTAMT_min",
                                               "SRMINVO_TOTAMT_mean",
                                               "SRMINVO_TOTAMT_sum",
                                               "result_SRMINVO_INSURCD_count as SRMINVO_INSURCD_count",
                                               "SRMINVO_IRNAMT_count",
                                               "SRMINVO_WSHAMT_count",
                                               "SRMINVO_TOTAMT_sum_3year")

    ##save csv
    result_SRMINVO.coalesce(1).write.option('header', 'true').csv(Temp_Path + "Feature_SRMINVO.csv")

    df_allSRMINVO = None
    write_Log(Log_File, "ok\n")

    spark.stop()

    '''
    write_Log(Log_File
              , "21. %s | Features construction from cdp.SRMINVO......" % str(datetime.datetime.now()))
    # INVODT
    # df_allSRMINVO = getSRMINVO_query_data('c.INVODT, c.DLRCD, c.BRNHCD, c.INVONO, c.INVTXCD, c.TOTAMT, c.INSURCD, c.IRNAMT, c.WSHAMT')
    df_allSRMINVO = getSRMINVO_query_data()
    df_allSRMINVO['VIN'] = df_allSRMINVO['VIN'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_allSRMINVO.loc[df_allSRMINVO.INVODT < pd.Timestamp('1988-01-01'), 'INVODT'] = np.nan
    df_allSRMINVO.loc[df_allSRMINVO.INVODT > pd.Timestamp(END_DATE), 'INVODT'] = np.nan
    # min max range days
    result_INVODT_max = df_allSRMINVO[['VIN', 'INVODT']].groupby('VIN').agg('max')  #
    result_INVODT_min = df_allSRMINVO[['VIN', 'INVODT']].groupby('VIN').agg('min')  #
    result_INVODT_count = df_allSRMINVO[['VIN', 'INVODT']].groupby('VIN').agg('count')  #
    result_INVODT_range = result_INVODT_max - result_INVODT_min  #
    result_INVODT_max['INVODT'] = (pd.Timestamp(END_DATE) - result_INVODT_max['INVODT']).map(
        lambda x: x if pd.isnull(x) else x.days)
    result_INVODT_min['INVODT'] = (pd.Timestamp(END_DATE) - result_INVODT_min['INVODT']).map(
        lambda x: x if pd.isnull(x) else x.days)
    result_INVODT_range['INVODT'] = result_INVODT_range['INVODT'].map(lambda x: x if pd.isnull(x) else x.days)
    # DLRCD 經銷商營業所
    df_allSRMINVO['DLRCD_BRNHCD'] = df_allSRMINVO['DLRCD'] + df_allSRMINVO['BRNHCD']
    # Nunique,  maxoccu
    result_SRMINVO_DLRCD_Nunique = df_allSRMINVO[['DLRCD_BRNHCD', 'VIN']].groupby('VIN').agg(lambda x: x.nunique())
    result_SRMINVO_DLRCD_maxoccu = df_allSRMINVO[['DLRCD_BRNHCD', 'VIN']].groupby('VIN').agg(
        lambda x: x.value_counts(dropna=False).index[0])  #

    # INVONO  *免費  #出保固 次數
    result_SRMINVO_INVONO_freecount = df_allSRMINVO[df_allSRMINVO['INVONO'].str[0] == '*'][['INVONO', 'VIN']].groupby(
        'VIN').agg('count')  #
    result_SRMINVO_INVONO_quarcount = df_allSRMINVO[df_allSRMINVO['INVONO'].str[0] == '#'][['INVONO', 'VIN']].groupby(
        'VIN').agg('count')  #

    # INVTXCD 主要發票聯式
    result_SRMINVO_INVTXCD_maxoccu = df_allSRMINVO[['INVTXCD', 'VIN']].groupby('VIN').agg(
        lambda x: x.value_counts(dropna=False).index[0])  #

    # TOTAMT 總金額 MAX MIN MEAN SUM
    result_SRMINVO_TOTAMT_max = df_allSRMINVO[['TOTAMT', 'VIN']].groupby('VIN').agg('max')  #
    result_SRMINVO_TOTAMT_min = df_allSRMINVO[['TOTAMT', 'VIN']].groupby('VIN').agg('min')  #
    result_SRMINVO_TOTAMT_mean = df_allSRMINVO[['TOTAMT', 'VIN']].groupby('VIN').agg('mean')  #
    result_SRMINVO_TOTAMT_sum = df_allSRMINVO[['TOTAMT', 'VIN']].groupby('VIN').agg('sum')  #

    # INSURCD 保險公司出險次數
    result_SRMINVO_INSURCD_count = df_allSRMINVO[df_allSRMINVO['INSURCD'] != ''][['INSURCD', 'VIN']].groupby('VIN').agg(
        'count')  #

    # IRNAMT 板金次數
    result_SRMINVO_IRNAMT_count = df_allSRMINVO[df_allSRMINVO['IRNAMT'] > 0][['IRNAMT', 'VIN']].groupby('VIN').agg(
        'count')  #

    # WSHAMT 噴漆次數
    result_SRMINVO_WSHAMT_count = df_allSRMINVO[df_allSRMINVO['WSHAMT'] > 0][['WSHAMT', 'VIN']].groupby('VIN').agg(
        'count')  #
    # df_allSRMINVO=None

    # 3year
    # print ("DATE_3y=",DATE_3y)
    df_allSRMINVO = df_allSRMINVO[df_allSRMINVO.INVODT > pd.Timestamp(DATE_3y)]

    # 三年內總金額
    result_SRMINVO_TOTAMT_sum_3year = df_allSRMINVO[['TOTAMT', 'VIN']].groupby('VIN').agg('sum')  #

    result_SRMINVO = pd.DataFrame()
    result_SRMINVO = pd.concat([result_INVODT_max, result_INVODT_min], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_INVODT_count], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_INVODT_range], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_DLRCD_Nunique], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_DLRCD_maxoccu], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_INVONO_freecount], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_INVONO_quarcount], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_INVTXCD_maxoccu], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_TOTAMT_max], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_TOTAMT_min], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_TOTAMT_mean], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_TOTAMT_sum], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_INSURCD_count], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_IRNAMT_count], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_WSHAMT_count], axis=1)
    result_SRMINVO = pd.concat([result_SRMINVO, result_SRMINVO_TOTAMT_sum_3year], axis=1)

    result_SRMINVO.columns = ['SRMINVO_INVODT_max',
                              'SRMINVO_INVODT_min',
                              'SRMINVO_INVODT_count',
                              'SRMINVO_INVODT_range',
                              'SRMINVO_DLRCD_Nunique',
                              'SRMINVO_DLRCD_maxoccu',
                              'SRMINVO_INVONO_freecount',
                              'SRMINVO_INVONO_quarcount',
                              'SRMINVO_INVTXCD_maxoccu',
                              'SRMINVO_TOTAMT_max',
                              'SRMINVO_TOTAMT_min',
                              'SRMINVO_TOTAMT_mean',
                              'SRMINVO_TOTAMT_sum',
                              'SRMINVO_INSURCD_count',
                              'SRMINVO_IRNAMT_count',
                              'SRMINVO_WSHAMT_count',
                              'SRMINVO_TOTAMT_sum_3year']
    result_SRMINVO.head()

    result_SRMINVO.reset_index(inplace=True)
    result_SRMINVO = result_SRMINVO.rename(columns={'index': 'VIN'})
    result_SRMINVO.SRMINVO_INVODT_count.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_INVONO_freecount.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_INVONO_quarcount.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_INVTXCD_maxoccu.fillna('NA', inplace=True)
    result_SRMINVO.SRMINVO_TOTAMT_max.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_TOTAMT_min.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_TOTAMT_mean.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_TOTAMT_sum.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_TOTAMT_sum.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_TOTAMT_sum_3year.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_IRNAMT_count.fillna(0, inplace=True)
    result_SRMINVO.SRMINVO_WSHAMT_count.fillna(0, inplace=True)

    ##save csv
    result_SRMINVO.to_csv(Temp_Path + "Feature_SRMINVO.csv", index=False, sep=',', encoding='utf-8')
    df_allSRMINVO = None
    write_Log(Log_File, "ok\n")
    
    # 買預付的金額與次數
    write_Log(Log_File, "22. %s | Features construction from cdp.SRPPDPMF......" % str(datetime.datetime.now()))
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SRPPDPMF = pd.read_sql(u"""select * from cdp.SRPPDPMF""", conn)
    conn.close()
    df_SRPPDPMF = df_SRPPDPMF[df_SRPPDPMF.INVSTS == 'A']
    df_SRPPDPMF['VIN'] = df_SRPPDPMF['VIN'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_SRPPDPMF['DLRCD_BRNHCD'] = df_SRPPDPMF['DLRCD'] + df_SRPPDPMF['BRNHCD'].astype(int).astype(str)

    result_PPAID_diff_DLRCD_count = df_SRPPDPMF[['DLRCD', 'VIN']].groupby('VIN').agg(lambda x: x.nunique())
    result_PPAID_diff_DLRCD_BRNHCD_count = df_SRPPDPMF[['DLRCD_BRNHCD', 'VIN']].groupby('VIN').agg(
        lambda x: x.nunique())
    result_PPAID_buy_times = df_SRPPDPMF[['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_PPAID_totamt_sum = df_SRPPDPMF[['TOTAMT', 'VIN']].groupby('VIN').agg('sum')
    result_PPAID_is_vin_from = df_SRPPDPMF[['DLRCD', 'VIN']][df_SRPPDPMF.VIN_FROM != ''].groupby('VIN').agg('count')
    result_PPAID_is_vin_from['DLRCD'] = result_PPAID_is_vin_from['DLRCD'].map(lambda x: 0 if x < 1 else 1)

    result_SRPPDPMF = pd.DataFrame()
    result_SRPPDPMF = pd.concat([result_PPAID_diff_DLRCD_count, result_PPAID_diff_DLRCD_BRNHCD_count], axis=1)
    result_SRPPDPMF = pd.concat([result_SRPPDPMF, result_PPAID_buy_times], axis=1)
    result_SRPPDPMF = pd.concat([result_SRPPDPMF, result_PPAID_totamt_sum], axis=1)
    result_SRPPDPMF = pd.concat([result_SRPPDPMF, result_PPAID_is_vin_from], axis=1)
    result_SRPPDPMF.columns = ['PPAID_diff_DLRCD_count', 'PPAID_diff_DLRCD_BRNHCD_count', 'PPAID_buy_times',
                               'result_PPAID_totamt_sum', 'result_PPAID_is_vin_from']

    result_SRPPDPMF.reset_index(inplace=True)
    result_SRPPDPMF = result_SRPPDPMF.rename(columns={'index': 'VIN'})
    result_SRPPDPMF['PPAID_is_buy_prepaid'] = 1
    result_SRPPDPMF['result_PPAID_is_vin_from'].fillna(0.0, inplace=True)
    result_SRPPDPMF['result_PPAID_is_vin_from'] = result_SRPPDPMF['result_PPAID_is_vin_from'].astype(int)
    result_SRPPDPMF.to_csv(Temp_Path + "Feature_SRPPDPMF.csv", index=False, sep=',', encoding='utf-8')

    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "23. %s | Features construction from cdp.SRPPDCMF......" % str(datetime.datetime.now()))
    df_SRPPDPMF = None
    # 預付扣抵資料
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SRPPDCMF = pd.read_sql(u"""select * from cdp.SRPPDCMF""", conn)
    conn.close()
    df_SRPPDCMF = df_SRPPDCMF[df_SRPPDCMF.TRCANCD != '*']
    df_SRPPDCMF = df_SRPPDCMF[df_SRPPDCMF.INVSTS == 'A']
    df_SRPPDCMF.TRCANCD.value_counts(dropna=False)
    df_SRPPDCMF['VIN'] = df_SRPPDCMF['VIN'].map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_SRPPDCMF['DLRCD_BRNHCD'] = df_SRPPDCMF['DLRCD'] + df_SRPPDCMF['BRNHCD'].astype(int).astype(str)
    df_SRPPDCMF['SADLRCD_BRNHCD'] = df_SRPPDCMF['SADLR'] + df_SRPPDCMF['SABRNH'].astype(int).astype(str)
    result_PPAIDuse_diff_DLRCD_count = df_SRPPDCMF[['DLRCD', 'VIN']].groupby('VIN').agg(lambda x: x.nunique())
    result_PPAIDuse_diff_DLRCD_BRNHCD_count = df_SRPPDCMF[['DLRCD_BRNHCD', 'VIN']].groupby('VIN').agg(
        lambda x: x.nunique())
    result_PPAIDuse_times = df_SRPPDCMF[['DLRCD', 'VIN']].groupby('VIN').agg('count')
    result_PPAIDuse_TOTAMT_sum = df_SRPPDCMF[['TOTAMT', 'VIN']].groupby('VIN').agg('sum')
    # 買預付與使用不同地點的次數
    result_PPAID_diff_sa_use_times = df_SRPPDCMF[['DLRCD', 'VIN']][
        df_SRPPDCMF['DLRCD_BRNHCD'] != df_SRPPDCMF['SADLRCD_BRNHCD']].groupby('VIN').agg('count')

    result_SRPPDCMF = pd.DataFrame()
    result_SRPPDCMF = pd.concat([result_PPAIDuse_diff_DLRCD_count, result_PPAIDuse_diff_DLRCD_BRNHCD_count], axis=1)
    result_SRPPDCMF = pd.concat([result_SRPPDCMF, result_PPAIDuse_times], axis=1)
    result_SRPPDCMF = pd.concat([result_SRPPDCMF, result_PPAIDuse_TOTAMT_sum], axis=1)
    result_SRPPDCMF = pd.concat([result_SRPPDCMF, result_PPAID_diff_sa_use_times], axis=1)
    result_SRPPDCMF.columns = ['PPAIDuse_diff_DLRCD_count', 'PPAIDuse_diff_DLRCD_BRNHCD_count', 'PPAIDuse_times',
                               'PPAIDuse_TOTAMT_sum', 'PPAID_diff_sa_use_times']
    result_SRPPDCMF.reset_index(inplace=True)
    result_SRPPDCMF = result_SRPPDCMF.rename(columns={'index': 'VIN'})

    result_SRPPDCMF.to_csv(Temp_Path + "Feature_SRPPDCMF.csv", index=False, sep=',', encoding='utf-8')
    write_Log(Log_File, "ok\n")
    df_SRPPDCMF = None

    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRCUMF = pd.read_sql(u"""
      SELECT CUSTID,
      FORCEID,
      CUDT,
      VIDT,
      VIDLR,
      VIBRNH FROM cdp.CRCUMF
      where FORCEID = ''
      """, conn)
    conn.close()

    df_CRCUMF['CUSTID'] = df_CRCUMF['CUSTID'].map(lambda x: x.strip())
    df_CRCUMF['VIDLRBH'] = df_CRCUMF['VIDLR'] + df_CRCUMF['VIBRNH']
    df_CRCUMF = df_CRCUMF.drop('FORCEID', axis=1)
    df_CRCUMF = df_CRCUMF.drop('VIDLR', axis=1)
    df_CRCUMF = df_CRCUMF.drop('VIBRNH', axis=1)

    df_CRCUMF.VIDT = pd.to_datetime(df_CRCUMF.VIDT, format='%Y%m%d', errors='coerce')
    df_CRCUMF['VIDT'] = (pd.Timestamp(END_DATE) - df_CRCUMF['VIDT']).map(lambda x: None if pd.isnull(x) else x.days)
    df_CRCUMF['CUDT'] = (pd.Timestamp(END_DATE) - df_CRCUMF['CUDT']).map(lambda x: None if pd.isnull(x) else x.days)
    df_CRCUMF.to_csv(Temp_Path + "df_CRCUMF_for_feature.csv", index=False, sep=',', encoding='utf-8')

    # Code 5: 31Feature_CSMCFORM_byLICSNO_preprocessing

    # 客訴資料處理

    write_Log(Log_File, "24. %s | Features construction from cdp.CSMCFORM......" % str(datetime.datetime.now()))
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CSMCFORM = pd.read_sql(u"""SELECT * FROM cdp.CSMCFORM""", conn)
    conn.close()

    # 清除前後空白
    for index in range(0, len(df_CSMCFORM.columns)):
        if df_CSMCFORM[df_CSMCFORM.columns[index]].dtypes == 'object':
            clear_output(wait=True)
            print(index)
            df_CSMCFORM[df_CSMCFORM.columns[index]] = df_CSMCFORM[df_CSMCFORM.columns[index]].map(
                lambda x: x.strip() if x != None else None)
    print('done')

    # u'PRODDT', u'LOADT', u'ISADT', u'SALEDT', u'LSKIDT', u'REDLDT'
    df_CSMCFORM[u'SFORMDATE'] = time_cleaner(df_CSMCFORM[u'SFORMDATE'])
    df_CSMCFORM[u'CLOSEDATE'] = time_cleaner(df_CSMCFORM[u'CLOSEDATE'])

    ##客訴次數 CSMCFORM_CASEID_count
    result_CASEID_count = df_CSMCFORM[['CASEID', 'LICSNO']].groupby('LICSNO').agg('count')
    ##通常是誰客訴  CSMCFORM_CONTTYPE_maxoccu
    result_CONTTYPE_maxoccu = df_CSMCFORM[['CONTTYPE', 'LICSNO']].groupby('LICSNO').agg(
        lambda x: x.value_counts(dropna=False).index[0])
    ##出現幾種人  CSMCFORM_CONTTYPE_nunique
    result_CONTTYPE_nunique = df_CSMCFORM[['CONTTYPE', 'LICSNO']].groupby('LICSNO').agg(lambda x: x.nunique())
    ##出現幾支電話 CSMCFORM_CONTTEL_nunique
    result_CONTTEL_nunique = df_CSMCFORM[['CONTTEL', 'LICSNO']].groupby('LICSNO').agg(lambda x: x.nunique())
    ##通常客訴方式  CSMCFORM_SOURCETP_maxoccu
    result_SOURCETP_maxoccu = df_CSMCFORM[['SOURCETP', 'LICSNO']].groupby('LICSNO').agg(
        lambda x: x.value_counts(dropna=False).index[0])
    # CASETP R Q G C 次數   CSMCFORM_CASETP_Rcount  CASETP_CASETP_Qcount  CSMCFORM_CASETP_Gcount  CSMCFORM_CASETP_Ccount
    result_CASETP_Rcount = df_CSMCFORM[df_CSMCFORM['CASETP'] == 'R'][['CASEID', 'LICSNO']].groupby('LICSNO').agg(
        'count')
    result_CASETP_Qcount = df_CSMCFORM[df_CSMCFORM['CASETP'] == 'Q'][['CASEID', 'LICSNO']].groupby('LICSNO').agg(
        'count')
    result_CASETP_Gcount = df_CSMCFORM[df_CSMCFORM['CASETP'] == 'G'][['CASEID', 'LICSNO']].groupby('LICSNO').agg(
        'count')
    result_CASETP_Ccount = df_CSMCFORM[df_CSMCFORM['CASETP'] == 'C'][['CASEID', 'LICSNO']].groupby('LICSNO').agg(
        'count')
    ##抱怨過幾個點 CSMCFORM_CASETP_Rcount
    df_CSMCFORM['DLR_temp'] = df_CSMCFORM['DLR'] + df_CSMCFORM['BRNH']
    result_DLR_BRNH_count = df_CSMCFORM[['DLR_temp', 'LICSNO']].groupby('LICSNO').agg(lambda x: x.nunique())
    ##是否抱怨握營業所 或 保養廠 CSMCFORM_BRNHTP_if0  CSMCFORM_BRNHTP_if1 
    result_BRNHTP_if0 = df_CSMCFORM[df_CSMCFORM['BRNHTP'] == '0'][['CASEID', 'LICSNO']].groupby('LICSNO').agg(
        lambda x: 1.0 if x.nunique() > 0 else 0.0)
    result_BRNHTP_if1 = df_CSMCFORM[df_CSMCFORM['BRNHTP'] == '*'][['CASEID', 'LICSNO']].groupby('LICSNO').agg(
        lambda x: 1.0 if x.nunique() > 0 else 0.0)

    ##最大抱怨程度  CSMCFORM_COMPTP_max
    result_COMPTP_max = df_CSMCFORM[['COMPTP', 'LICSNO']].groupby('LICSNO').agg('max')

    ##發單 max range min    CSMCFORM_SFORMDATE_max CSMCFORM_SFORMDATE_min CSMCFORM_SFORMDATE_range
    result_SFORMDATE_max = df_CSMCFORM[['SFORMDATE', 'LICSNO']].groupby('LICSNO').agg('max')
    result_SFORMDATE_min = df_CSMCFORM[['SFORMDATE', 'LICSNO']].groupby('LICSNO').agg('min')
    result_SFORMDATE_range = result_SFORMDATE_max - result_SFORMDATE_min
    result_SFORMDATE_max = (pd.Timestamp(END_DATE) - result_SFORMDATE_max.SFORMDATE).map(lambda x: x.days)
    result_SFORMDATE_min = (pd.Timestamp(END_DATE) - result_SFORMDATE_min.SFORMDATE).map(lambda x: x.days)

    ##max回次數  CSMCFORM_MTIME_max 待修正
    result_MTIME_max = df_CSMCFORM[['MTIME', 'LICSNO']].groupby('LICSNO').agg('max')

    # #CLOSESTAT 最常出現類別  CSMCFORM_CLOSESTAT_maxoccu
    result_CLOSESTAT_maxoccu = df_CSMCFORM[['CLOSESTAT', 'LICSNO']].groupby('LICSNO').agg(
        lambda x: x.value_counts(dropna=False).index[0])

    df_CSMCFORM['DATE_DURATION'] = df_CSMCFORM['CLOSEDATE'] - df_CSMCFORM['SFORMDATE']
    result_DATE_DURATION_max = df_CSMCFORM[['DATE_DURATION', 'LICSNO']].groupby('LICSNO').agg('max')
    ##PROCSTAT 資料太多種 未取特徵
    ##很低比例有賠償 

    result = pd.DataFrame()
    result = pd.concat([result_CASEID_count, result_CONTTYPE_maxoccu], axis=1)
    result = pd.concat([result, result_CONTTYPE_nunique], axis=1)
    result = pd.concat([result, result_CONTTEL_nunique], axis=1)
    result = pd.concat([result, result_SOURCETP_maxoccu], axis=1)
    result = pd.concat([result, result_CASETP_Rcount], axis=1)
    result = pd.concat([result, result_CASETP_Qcount], axis=1)
    result = pd.concat([result, result_CASETP_Gcount], axis=1)
    result = pd.concat([result, result_CASETP_Ccount], axis=1)
    result = pd.concat([result, result_DLR_BRNH_count], axis=1)
    result = pd.concat([result, result_BRNHTP_if0], axis=1)
    result = pd.concat([result, result_BRNHTP_if1], axis=1)
    result = pd.concat([result, result_COMPTP_max], axis=1)
    result = pd.concat([result, result_SFORMDATE_max], axis=1)
    result = pd.concat([result, result_SFORMDATE_min], axis=1)
    result = pd.concat([result, result_SFORMDATE_range], axis=1)
    result = pd.concat([result, result_MTIME_max], axis=1)
    result = pd.concat([result, result_CLOSESTAT_maxoccu], axis=1)
    result = pd.concat([result, result_DATE_DURATION_max], axis=1)

    result.columns = [
        'CSMCFORM_CASEID_count',
        'CSMCFORM_CONTTYPE_maxoccu',
        'CSMCFORM_CONTTYPE_nunique',
        'CSMCFORM_CONTTEL_nunique',
        'CSMCFORM_SOURCETP_maxoccu',
        'CSMCFORM_CASETP_Rcount',
        'CSMCFORM_CASETP_Qcount',
        'CSMCFORM_CASETP_Gcount',
        'CSMCFORM_CASETP_Ccount',
        'CSMCFORM_DLR_BRNH_count',
        'CSMCFORM_BRNHTP_if0',
        'CSMCFORM_BRNHTP_if1',
        'CSMCFORM_COMPTP_max',
        'CSMCFORM_SFORMDATE_max',
        'CSMCFORM_SFORMDATE_min',
        'CSMCFORM_SFORMDATE_range',
        'CSMCFORM_MTIME_max',
        'CSMCFORM_CLOSESTAT_maxoccu',
        'CSMCFORM_DATE_DURATION_max']
    result.head()
    result = result.reset_index()
    result.to_csv(Temp_Path + "Feature_CSMCFORM_byLICSNO.csv", index=False, sep=',', encoding='utf-8')
    del result
    write_Log(Log_File, "ok\n")
    # Code 6: 40df_selected_LICSNO_FIRST_TABLE_FEATURE_COMBINE

    write_Log(Log_File, "25. %s | Combine all features......" % str(datetime.datetime.now()))
    # 讀取所有已經萃取出的特徵
    df_Feature_CARCNT = pd.read_csv(Temp_Path + "Feature_CARCNT_new.csv", sep=',', encoding='utf-8', low_memory=False)
    df_Feature_ISrelated = pd.read_csv(Temp_Path + "Feature_ISrelated.csv", sep=',', encoding='utf-8', low_memory=False)
    df_Feature_LRDT = pd.read_csv(Temp_Path + "Feature_LRDT.csv", sep=',', encoding='utf-8', low_memory=False)
    df_Feature_SR_WKCTCD = pd.read_csv(Temp_Path + "Feature_SR_WKCTCD.csv", sep=',', encoding='utf-8', low_memory=False)
    df_Feature_SRLFOWM_0717 = pd.read_csv(Temp_Path + "Feature_SRLFOWM.csv", sep=',', encoding='utf-8',
                                          low_memory=False)
    df_feature_SRMINVO = pd.read_csv(Temp_Path + "Feature_SRMINVO.csv", sep=',', encoding='utf-8', low_memory=False)
    df_feature_SRPPDPMF = pd.read_csv(Temp_Path + "Feature_SRPPDPMF.csv", sep=',', encoding='utf-8', low_memory=False)
    df_feature_SRPPDCMF = pd.read_csv(Temp_Path + "Feature_SRPPDCMF.csv", sep=',', encoding='utf-8', low_memory=False)
    df_Feature_CSMCFORM_byLICSNO = pd.read_csv(Temp_Path + "Feature_CSMCFORM_byLICSNO.csv", sep=',', encoding='utf-8')

    # 讀取20FIRST_table_combining.ipynb 選出的車輛清單 20 所做的csv檔案
    df_selected_LICSNO = pd.read_csv(Temp_Path + "df_selected_LICSNO.csv", sep=',', encoding='utf-8', low_memory=False)

    df_selected_LICSNO = df_selected_LICSNO[
        [u'LICSNO', u'qualified', 'scrapped_150708', u'PSLTAX', u'EGNO', u'BDNO', u'FRAN', u'CARNM', u'CARMDL',
         u'CARTYP', u'YYMDL', u'TRCANO', u'CARSFX', u'SADLR', u'SABRNH', u'SASALR', u'PRODDT', u'LOADT', u'ISADT',
         u'SALEDT', u'LSKIDT', u'REDLDT', u'STSCD', u'STS_DLRBRNH', u'STSDT', u'STR_DLRBRNH', u'STRDT', u'MT_DLRBRNH',
         u'MTDT', u'WMI', u'VIN', u'UCDELIVIDT', u'UCDLR', u'UCBRNH', u'UCENSURTYP', u'UCDELIMILES', u'UCGUABKNM',
         u'UCOCGUAEXP', u'UCUCGUAEXP', u'UCOCGUAMIL', u'UCUCGUAMIL', u'UCSALE', u'CARCD', u'EXCD', u'VDS', u'SEDLDT',
         u'FENDAT', u'UENDAT', u'CARENDAT', u'CUSTID1', u'FORCEID1', u'CUSTID2', u'FORCEID2']]

    df_selected_LICSNO['LICSNO'] = df_selected_LICSNO['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO['VIN'] = df_selected_LICSNO['VIN'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO['EGNO'] = df_selected_LICSNO['EGNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO['BDNO'] = df_selected_LICSNO['BDNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO[u'CUSTID2'] = df_selected_LICSNO['CUSTID2'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())

    # drop LICSNO *-0383
    df_selected_LICSNO = df_selected_LICSNO[df_selected_LICSNO['LICSNO'] != '*-0383'].reset_index(drop=True)

    orginal_row_num = df_selected_LICSNO.shape[0]
    df_selected_LICSNO.qualified = df_selected_LICSNO.qualified.astype(str)
    df_selected_LICSNO.PSLTAX = df_selected_LICSNO.PSLTAX.astype(str)

    # df_Feature_CARCNT

    # 改用新的carCNT計算方式
    df_selected_LICSNO['LICSNO'] = df_selected_LICSNO['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_Feature_CARCNT['LICSNO'] = df_Feature_CARCNT['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(
        df_Feature_CARCNT[[u'LICSNO', u'carCNT11', u'carCNT21', u'carCNT12', u'carCNT22']], how='left',
        left_on='LICSNO', right_on='LICSNO')
    del df_Feature_CARCNT  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('1.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    # df_Feature_SRLFOWM_0717
    df_Feature_SRLFOWM_0717['VIN'] = df_Feature_SRLFOWM_0717['VIN'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_Feature_SRLFOWM_0717, how='left', on='VIN')
    del df_Feature_SRLFOWM_0717  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('2.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    # OK#OK  df_Feature_ISrelated
    # print (df_Feature_ISrelated.columns) #LISCNO
    # print (df_Feature_ISrelated.shape) #LISCNO

    df_Feature_ISrelated['LICSNO'] = df_Feature_ISrelated['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO['LICSNO'] = df_selected_LICSNO['LICSNO'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())

    # df_Feature_ISrelated.LICSNO.value_counts(dropna=False)
    df_selected_LICSNO = df_selected_LICSNO.merge(df_Feature_ISrelated, how='left', on='LICSNO')
    del df_Feature_ISrelated  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('3.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    # OK df_Feature_LRDT
    df_Feature_LRDT.VIN = df_Feature_LRDT.VIN.map(lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO.EGNO = df_selected_LICSNO.EGNO.map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_Feature_LRDT, how='left', left_on='VIN',
                                                  right_on='VIN')  # 這邊left_on 改為vin
    del df_Feature_LRDT  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('4.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    # OK df_Feature_SR_WKCTCD
    df_Feature_SR_WKCTCD.index = df_Feature_SR_WKCTCD.index.map(
        lambda x: x if (isinstance(x, int) or x is None) else x.strip())
    df_selected_LICSNO.VIN = df_selected_LICSNO.VIN.map(lambda x: x if (isinstance(x, int) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_Feature_SR_WKCTCD, how='left', left_on='VIN',
                                                  right_on='index')  # 這邊left_on 改為vin
    del df_Feature_SR_WKCTCD  # merge 完後面用不到，就把記憶體釋放
    df_selected_LICSNO = df_selected_LICSNO.drop(u'index', 1)
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('5.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    df_Feature_CSMCFORM_byLICSNO.LICSNO = df_Feature_CSMCFORM_byLICSNO.LICSNO.map(
        lambda x: x if (isinstance(x, int) or x is None or isinstance(x, float)) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_Feature_CSMCFORM_byLICSNO, how='left', left_on='LICSNO',
                                                  right_on='LICSNO')
    del df_Feature_CSMCFORM_byLICSNO  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('6.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    # OK df_feature_SRMINVO
    df_feature_SRMINVO['VIN'] = df_feature_SRMINVO['VIN'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_feature_SRMINVO, how='left', left_on='VIN', right_on='VIN')
    del df_feature_SRMINVO  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('7.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    # OK df_feature_SRPPDPMF df_feature_SRPPDCMF
    df_feature_SRPPDPMF['VIN'] = df_feature_SRPPDPMF['VIN'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_feature_SRPPDPMF, how='left', left_on='VIN', right_on='VIN')
    del df_feature_SRPPDPMF  # merge 完後面用不到，就把記憶體釋放
    df_feature_SRPPDCMF['VIN'] = df_feature_SRPPDCMF['VIN'].map(
        lambda x: x if (isinstance(x, float) or x is None) else x.strip())
    df_selected_LICSNO = df_selected_LICSNO.merge(df_feature_SRPPDCMF, how='left', left_on='VIN', right_on='VIN')
    del df_feature_SRPPDCMF  # merge 完後面用不到，就把記憶體釋放
    # if orginal_row_num == df_selected_LICSNO.shape[0]:
    #     print ('8.OK! row number is same as orginal_row_num', orginal_row_num)
    # else:
    #     print ('something is wrong!!!!!!!!!! row number is not equal to ', orginal_row_num, 'is ', df_selected_LICSNO.shape[0])

    df_selected_LICSNO = df_selected_LICSNO.reset_index(drop=True)
    obj_list = df_selected_LICSNO.columns[df_selected_LICSNO.dtypes == 'object'].tolist()
    for index in obj_list:
        df_selected_LICSNO[index] = df_selected_LICSNO[index].map(
            lambda x: x if (isinstance(x, float) or x is None) else x.strip())

    df_selected_LICSNO = df_selected_LICSNO.reset_index()
    df_selected_LICSNO = df_selected_LICSNO.drop_duplicates('LICSNO')
    df_selected_LICSNO.to_csv(Temp_Path + "df_selected_LICSNO_Features.csv", index=False, sep=',', encoding='utf-8')
    del df_selected_LICSNO
    # write_Log(Log_File,"ok\n")    
    gc.collect()  # 實際釋放記憶體空間，如果沒做，接下來modeling 部分，記憶體會爆掉

    # # Modeling code
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SRWHMF = pd.read_sql(u"""
      select LICSNO, LRKM, FRKM, BPKM, LSFXKM from cdp.SRWHMF
      where LRKM is not null
      and FRKM is not null
      and BPKM is not null
      and LSFXKM is not null
      """, conn)
    conn.close()

    df_SRWHMF['LRKM_max'] = df_SRWHMF[['LRKM', 'FRKM', 'BPKM', 'LSFXKM']].max(axis=1)
    df_SRWHMF = pd.DataFrame(df_SRWHMF, columns=['LICSNO', 'LRKM_max'])
    df_SRWHMF.columns = ['LICSNO', 'LRKM']
    df_SRWHMF = df_SRWHMF[df_SRWHMF['LRKM'] > 0]
    df_SRWHMF.drop_duplicates('LICSNO')

    # 找出汰舊換新的 "新車"車號  用以將其排除 避免在跨車輛計算特徵時 被計算到 而導致倒果為因
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_PSLTAXORDMF_forNewcarRemove = pd.read_sql(u"""
      SELECT * FROM cdp.PSLTAXORDMF WHERE
      DEFUNCTDT < '""" + END_DATE + """' and
      (
      UPPER(OBRAND) like '%TOYOTA%' or 
      UPPER(OBRAND) like '%國瑞%' or 
      UPPER(OBRAND) like '%LEXUS%' or 
      UPPER(OBRAND) like '%AMCTOYOTA%' or 
      UPPER(OBRAND) like '%TOYOYA%' or 
      UPPER(OBRAND) like '%豐田%' or 
      UPPER(OBRAND) like '%YOYOTA%' or 
      UPPER(OBRAND) like '%國睿%' or 
      UPPER(OBRAND) like '%國叡%' or 
      UPPER(OBRAND) like '%T0YOTA%' or 
      UPPER(OBRAND) like '%TOYATA%' or
      UPPER(OBRAND) like '%KOUZUI%'
      )""", conn)
    conn.close()
    df_PSLTAXORDMF_forNewcarRemove = df_PSLTAXORDMF_forNewcarRemove[
        df_PSLTAXORDMF_forNewcarRemove[u'DELDT'] < datetime.datetime(2010, 1, 1, 0, 0, 0, 0)]
    df_PSLTAXORDMF_forNewcarRemove = df_PSLTAXORDMF_forNewcarRemove[
        ~(df_PSLTAXORDMF_forNewcarRemove['LICSNO'].isnull())]
    df_PSLTAXORDMF_forNewcarRemove['LICSNO'] = df_PSLTAXORDMF_forNewcarRemove['LICSNO'].map(lambda x: x.strip().upper())

    # 補LRKM
    # obtain LRKM from SRWHMF
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_SRWHMF = pd.read_sql(u"""
      select LICSNO, LRKM, FRKM, BPKM, LSFXKM from cdp.SRWHMF
      where LRKM is not null
      and FRKM is not null
      and BPKM is not null
      and LSFXKM is not null
      """, conn)
    conn.close()

    df_SRWHMF['LRKM_max'] = df_SRWHMF[['LRKM', 'FRKM', 'BPKM', 'LSFXKM']].max(axis=1)
    df_SRWHMF = pd.DataFrame(df_SRWHMF, columns=['LICSNO', 'LRKM_max'])
    df_SRWHMF.columns = ['LICSNO', 'LRKM']
    df_SRWHMF = df_SRWHMF[df_SRWHMF['LRKM'] > 0]
    df_SRWHMF.drop_duplicates('LICSNO')
    # print(df_SRWHMF.shape)
    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "26. %s | Create Big_Table......" % str(datetime.datetime.now()))
    df_LICSNO0717_Features = pd.read_csv(Temp_Path + "df_selected_LICSNO_Features.csv", sep=',', encoding='utf-8',
                                         low_memory=False)
    df_LICSNO0717_Features = df_LICSNO0717_Features.merge(df_SRWHMF, how='left', on='LICSNO')  # 將 LRKM 的資料寫入
    df_LICSNO0717_Features = df_LICSNO0717_Features[
        df_LICSNO0717_Features['LRKM'] > 5000]  # LRKM < 5000的濾除（假設5000以後沒回廠，視為非忠實客戶），濾除284筆(201701)
    del df_SRWHMF
    # 1070105新增的code，CARAGE=0的資料中，有很多['PSLTAX']==1，所以要補CARAGE的資料
    df_LICSNO0717_Features['CARAGE'] = pd.Timestamp(END_DATE).year - pd.to_numeric(df_LICSNO0717_Features['YYMDL'])
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_CRCAMF_for_CARAGE = pd.read_sql(u"""select LICSNO, YYMDL, CARNM from cdp.crcamf""", conn)
    df_CRCAMF_for_CARAGE = df_CRCAMF_for_CARAGE[~df_CRCAMF_for_CARAGE['LICSNO'].isnull()]
    df_CRCAMF_for_CARAGE = df_CRCAMF_for_CARAGE[~df_CRCAMF_for_CARAGE['YYMDL'].isnull()]
    conn.close()

    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_PSLTAXORDMF_for_CARAGE = pd.read_sql(
        u"""select * from cdp.psltaxordmf where defunctdt < '""" + END_DATE + """' and length(LSKCUSTID) = 10 and OLICSNO<>LICSNO""",
        conn)
    conn.close()
    df_PSLTAXORDMF_for_CARAGE = df_PSLTAXORDMF_for_CARAGE[~df_PSLTAXORDMF_for_CARAGE['OLICSNO'].isnull()]
    df_LICSNO0717_Features['LICSNO'] = df_LICSNO0717_Features['LICSNO'].map(lambda x: x.strip().upper())
    df_CRCAMF_for_CARAGE['LICSNO'] = df_CRCAMF_for_CARAGE['LICSNO'].map(lambda x: x.strip().upper())
    df_PSLTAXORDMF_for_CARAGE[u'OLICSNO'] = df_PSLTAXORDMF_for_CARAGE[u'OLICSNO'].map(lambda x: x.strip().upper())
    temp = df_CRCAMF_for_CARAGE[df_CRCAMF_for_CARAGE['LICSNO'].isin(df_PSLTAXORDMF_for_CARAGE['OLICSNO'])]
    temp_PSL = pd.DataFrame(df_PSLTAXORDMF_for_CARAGE, columns=['OLICSNO', 'DEFUNCTDT'])
    del df_PSLTAXORDMF_for_CARAGE
    temp_PSL = temp_PSL.drop_duplicates(['OLICSNO'], keep='first')
    temp = temp[temp['LICSNO'].isin(temp_PSL['OLICSNO'])]
    temp = temp.merge(temp_PSL, how='left', left_on='LICSNO', right_on='OLICSNO')
    temp['YEAR'] = pd.DatetimeIndex(temp['DEFUNCTDT']).year  # 抓取報廢年度資訊
    temp['CARAGE'] = temp['YEAR'].astype(int) - pd.to_numeric(temp['YYMDL'])  # 報廢年 - 年式
    temp = pd.DataFrame(temp, columns=['LICSNO', 'CARAGE'])
    temp.columns = ['LICSNO', 'CARAGE_1']  # 改column name
    df_LICSNO0717_Features = df_LICSNO0717_Features.merge(temp, how='left', on='LICSNO')
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features['LICSNO'].isin(temp['LICSNO']), 'CARAGE'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features['LICSNO'].isin(temp['LICSNO']), 'CARAGE_1']
    df_LICSNO0717_Features.drop(['CARAGE_1'], axis=1, inplace=True)
    df_LICSNO0717_Features['CARENDAT'] = pd.to_datetime(df_LICSNO0717_Features['CARENDAT'], errors='coerce')
    df_LICSNO0717_Features['FENDAT'] = pd.to_datetime(df_LICSNO0717_Features['FENDAT'], errors='coerce')
    # 抽取跨車輛的特徵 
    Agg_result_list = []
    df_LICSNO0717_Features['LICSNO'] = df_LICSNO0717_Features['LICSNO'].map(lambda x: x.strip().upper())
    # 刪除汰舊換新新車車輛
    df_LICSNO0717_Features = df_LICSNO0717_Features[
        ~df_LICSNO0717_Features['LICSNO'].isin(df_PSLTAXORDMF_forNewcarRemove['LICSNO'].tolist())]
    Agg_result_list.append(df_LICSNO0717_Features.groupby(['CUSTID1'])[
                               'WSLPDT_count', 'WSLPDT_count_3year', 'SRMINVO_TOTAMT_sum', 'SRMINVO_TOTAMT_sum_3year'].mean())
    Agg_result_list.append(df_LICSNO0717_Features.groupby(['CUSTID1'])[
                               'FENDAT_count', 'FINDT_count', 'SRMINVO_INVODT_count', 'CARAGE', 'SRMINVO_TOTAMT_mean', 'RTPTDT_count'].mean())
    Agg_result_list.append(df_LICSNO0717_Features.groupby(['CUSTID1'])[
                               'WSLPDT_count', 'WSLPDT_count_3year', 'CSMCFORM_CASETP_Ccount', 'CSMCFORM_CASETP_Gcount', 'CSMCFORM_CASETP_Qcount', 'CSMCFORM_CASETP_Rcount'].sum())
    result_agg = df_LICSNO0717_Features.groupby(['CUSTID1'])[
        'CARENDAT', 'FENDAT', 'FINDT_max', 'RTPTDT_max', 'SRMINVO_INVODT_max', 'WSLPDT_max'].agg(['max', 'min'])
    for index in ['CARENDAT', 'FENDAT', 'FINDT_max', 'RTPTDT_max', 'SRMINVO_INVODT_max', 'WSLPDT_max']:
        result_agg[index, 'range'] = result_agg[index]['max'] - result_agg[index]['min']
    Agg_result_list.append(result_agg)
    Agg_result_list[0].columns = ['WSLPDT_count_aggmean', 'WSLPDT_count_3year_aggmean', 'SRMINVO_TOTAMT_sum_aggmean',
                                  'SRMINVO_TOTAMT_sum_3year_aggmean']
    Agg_result_list[1].columns = [u'FENDAT_count_aggmean', u'FINDT_count_aggmean', u'SRMINVO_INVODT_count_aggmean',
                                  u'CARAGE_aggmean', u'SRMINVO_TOTAMT_mean_aggmean', u'RTPTDT_count_aggmean']
    Agg_result_list[2].columns = ['WSLPDT_count_aggsum', 'WSLPDT_count_3year_aggsum', 'CSMCFORM_CASETP_Ccount_aggsum',
                                  'CSMCFORM_CASETP_Gcount_aggsum', 'CSMCFORM_CASETP_Qcount_aggsum',
                                  'CSMCFORM_CASETP_Rcount_aggsum']
    Agg_result_list[3].columns = ['_'.join(col).strip() for col in Agg_result_list[3].columns.values]
    Agg_result_list[3].columns = [col + '_agg' for col in Agg_result_list[3].columns.values]
    for index in range(0, len(Agg_result_list)):  # reset index
        Agg_result_list[index].reset_index(inplace=True)
        df_LICSNO0717_Features = df_LICSNO0717_Features.merge(Agg_result_list[index], how='left', on='CUSTID1')
    Agg_result_list = None
    # print ('VIN 空的清掉')
    df_LICSNO0717_Features = df_LICSNO0717_Features[df_LICSNO0717_Features['VIN'].notnull()]
    # print ('qualified 需等於一')
    df_LICSNO0717_Features = df_LICSNO0717_Features[df_LICSNO0717_Features['qualified'] == 1]
    # print ('carage >=15 ')
    df_LICSNO0717_Features = df_LICSNO0717_Features[df_LICSNO0717_Features[u'CARAGE'] >= 15]  # 清掉 車齡過小
    df_LICSNO0717_Features = df_LICSNO0717_Features.reset_index(drop=True)

    # # ##NA清理 '濾掉na 超過一半者'  '補NA' 'type 轉換'
    # # print '補NA'
    df_LICSNO0717_Features[u'FENDAT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'UENDAT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WSLPDT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'RTPTDT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'STRWKDT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'FINDT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'AGVCARDT_count'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_star_counts'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_1_counts'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_2_counts'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_3_counts'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_B_counts'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_P_counts'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WKCTCD_T_counts'].fillna(0, inplace=True)

    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'WSLPDT_min'].isnull(), u'WSLPDT_min'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'WSLPDT_min'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'WSLPDT_max'].isnull(), u'WSLPDT_max'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'WSLPDT_max'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features[u'WSLPDT_range'] = df_LICSNO0717_Features[u'WSLPDT_max'] - df_LICSNO0717_Features[
        u'WSLPDT_min']

    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'RTPTDT_min'].isnull(), u'RTPTDT_min'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'RTPTDT_min'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'RTPTDT_max'].isnull(), u'RTPTDT_max'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'RTPTDT_max'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features[u'RTPTDT_range'] = df_LICSNO0717_Features[u'RTPTDT_max'] - df_LICSNO0717_Features[
        u'RTPTDT_min']

    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'STRWKDT_min'].isnull(), u'STRWKDT_min'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'STRWKDT_min'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'STRWKDT_max'].isnull(), u'STRWKDT_max'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'STRWKDT_max'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features[u'STRWKDT_range'] = df_LICSNO0717_Features[u'STRWKDT_max'] - df_LICSNO0717_Features[
        u'STRWKDT_min']

    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'FINDT_min'].isnull(), u'FINDT_min'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'FINDT_min'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'FINDT_max'].isnull(), u'FINDT_max'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'FINDT_max'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features[u'FINDT_range'] = df_LICSNO0717_Features[u'FINDT_max'] - df_LICSNO0717_Features[u'FINDT_min']

    # 先補零 後續再用修正的
    df_LICSNO0717_Features[u'LRKM'].fillna(0, inplace=True)
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'LRKM'] > 1000000, u'LRKM'] = 0
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'LRKM'] < 20000, u'LRKM'] = 0

    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'AGVCARDT_min'].isnull(), u'AGVCARDT_min'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'AGVCARDT_min'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'AGVCARDT_max'].isnull(), u'AGVCARDT_max'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'AGVCARDT_max'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features[u'AGVCARDT_range'] = df_LICSNO0717_Features[u'AGVCARDT_max'] - df_LICSNO0717_Features[
        u'AGVCARDT_min']

    df_LICSNO0717_Features[u'SASALR'].fillna('NaN', inplace=True)
    df_LICSNO0717_Features[u'CARCD'].fillna('NaN', inplace=True)

    df_LICSNO0717_Features[u'CARNM'].fillna('NaN', inplace=True)
    df_LICSNO0717_Features[u'STSCD'].fillna(1.0, inplace=True)

    df_LICSNO0717_Features[u'STR_DLRBRNH'].fillna('NaN', inplace=True)
    df_LICSNO0717_Features[u'MT_DLRBRNH'].fillna('NaN', inplace=True)

    df_LICSNO0717_Features[u'carCNT11'].fillna(0.0, inplace=True)
    df_LICSNO0717_Features[u'carCNT21'].fillna(0.0, inplace=True)
    df_LICSNO0717_Features[u'carCNT12'].fillna(0.0, inplace=True)
    df_LICSNO0717_Features[u'carCNT22'].fillna(0.0, inplace=True)

    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'SRMINVO_INVODT_min'].isnull(), u'SRMINVO_INVODT_min'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'SRMINVO_INVODT_min'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'SRMINVO_INVODT_max'].isnull(), u'SRMINVO_INVODT_max'] = \
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'SRMINVO_INVODT_max'].isnull(), u'CARAGE'] * 365
    df_LICSNO0717_Features[u'SRMINVO_INVODT_range'] = df_LICSNO0717_Features[u'SRMINVO_INVODT_max'] - \
                                                      df_LICSNO0717_Features[u'SRMINVO_INVODT_min']

    df_LICSNO0717_Features.FENDAT_range.fillna(0, inplace=True)
    df_LICSNO0717_Features.UENDAT_range.fillna(0, inplace=True)

    df_LICSNO0717_Features.SRMINVO_INVTXCD_maxoccu.fillna('A', inplace=True)
    df_LICSNO0717_Features.SRMINVO_DLRCD_maxoccu.fillna('NaN', inplace=True)

    df_LICSNO0717_Features.SRMINVO_DLRCD_Nunique.fillna(2, inplace=True)
    df_LICSNO0717_Features.SRMINVO_TOTAMT_max.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_TOTAMT_min.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_TOTAMT_mean.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_INVODT_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_INVONO_freecount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_INVONO_quarcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_INSURCD_count.fillna(0, inplace=True)

    df_LICSNO0717_Features.SRMINVO_IRNAMT_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_WSHAMT_count.fillna(0, inplace=True)

    df_LICSNO0717_Features.SRMINVO_WSHAMT_count.fillna(0, inplace=True)

    df_LICSNO0717_Features.CSMCFORM_CASEID_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_CASETP_Rcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_CASETP_Qcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_CASETP_Gcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_CASETP_Ccount.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_DLR_BRNH_count.fillna(0, inplace=True)

    df_LICSNO0717_Features.PPAID_diff_DLRCD_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAID_diff_DLRCD_BRNHCD_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAID_buy_times.fillna(0, inplace=True)
    df_LICSNO0717_Features.result_PPAID_totamt_sum.fillna(0, inplace=True)
    df_LICSNO0717_Features.result_PPAID_is_vin_from.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAID_is_buy_prepaid.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAIDuse_diff_DLRCD_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAIDuse_diff_DLRCD_BRNHCD_count.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAIDuse_times.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAIDuse_TOTAMT_sum.fillna(0, inplace=True)
    df_LICSNO0717_Features.PPAID_diff_sa_use_times.fillna(0, inplace=True)

    df_LICSNO0717_Features.SRLFOWM_EXCSTAF_Acount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_EXCSTAF_Scount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Ncount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Ycount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Scount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_NAcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Bcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Acount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Dcount.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRLFOWM_APPFLG_Ccount.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_BRNHTP_if0.fillna(0, inplace=True)
    df_LICSNO0717_Features.CSMCFORM_BRNHTP_if1.fillna(0, inplace=True)

    df_LICSNO0717_Features.SRMINVO_TOTAMT_sum_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.SRMINVO_TOTAMT_sum.fillna(0, inplace=True)
    df_LICSNO0717_Features.WSLPDT_count_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_A_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_B_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_C_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_A_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_B_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_C_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_D_counts.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_star_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_1_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_2_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_3_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_B_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_P_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.WKCTCD_T_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_A_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_B_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.PAYCD_C_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_A_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_B_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_C_counts_3year.fillna(0, inplace=True)
    df_LICSNO0717_Features.INJ_D_counts_3year.fillna(0, inplace=True)

    df_LICSNO0717_Features.CARMDL.fillna('NaN', inplace=True)
    df_LICSNO0717_Features.CARTYP.fillna('NaN', inplace=True)
    df_LICSNO0717_Features.TRCANO.fillna('NaN', inplace=True)
    df_LICSNO0717_Features.CARSFX.fillna('NaN', inplace=True)
    df_LICSNO0717_Features.VDS.fillna('NaN', inplace=True)
    temp = df_LICSNO0717_Features[u'YYMDL'].median()
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features.YYMDL > 2050, 'YYMDL'] = df_LICSNO0717_Features[u'YYMDL'].median()
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features.YYMDL < 1970, 'YYMDL'] = df_LICSNO0717_Features[u'YYMDL'].median()
    df_LICSNO0717_Features[u'YYMDL'].fillna(temp, inplace=True)
    df_LICSNO0717_Features[u'CARAGE'].fillna(df_LICSNO0717_Features[u'CARAGE'].median(), inplace=True)
    df_LICSNO0717_Features['SABRNH'].fillna('na', inplace=True)

    # 轉換為距離現在天數
    df_LICSNO0717_Features.PRODDT = DTtime_cleaner(df_LICSNO0717_Features.PRODDT)
    df_LICSNO0717_Features.LOADT = DTtime_cleaner(df_LICSNO0717_Features.LOADT)
    df_LICSNO0717_Features.ISADT = DTtime_cleaner(df_LICSNO0717_Features.ISADT)
    df_LICSNO0717_Features.SALEDT = DTtime_cleaner(df_LICSNO0717_Features.SALEDT)
    df_LICSNO0717_Features.LSKIDT = DTtime_cleaner(df_LICSNO0717_Features.LSKIDT)
    df_LICSNO0717_Features.REDLDT = DTtime_cleaner(df_LICSNO0717_Features.REDLDT)
    df_LICSNO0717_Features.SEDLDT = DTtime_cleaner(df_LICSNO0717_Features.SEDLDT)
    df_LICSNO0717_Features.MTDT = DTtime_cleaner(df_LICSNO0717_Features.MTDT, 1)
    df_LICSNO0717_Features.FENDAT = DTtime_cleaner(df_LICSNO0717_Features.FENDAT, 2)
    df_LICSNO0717_Features.UENDAT = DTtime_cleaner(df_LICSNO0717_Features.UENDAT, 2)
    df_LICSNO0717_Features.CARENDAT = DTtime_cleaner(df_LICSNO0717_Features.CARENDAT, 2)
    df_LICSNO0717_Features.CARENDAT_range_agg = DTtime_cleaner(df_LICSNO0717_Features.CARENDAT_range_agg, 1)
    df_LICSNO0717_Features.CARENDAT_max_agg = DTtime_cleaner(df_LICSNO0717_Features.CARENDAT_max_agg, 1)
    df_LICSNO0717_Features.CARENDAT_min_agg = DTtime_cleaner(df_LICSNO0717_Features.CARENDAT_min_agg, 1)
    df_LICSNO0717_Features.FENDAT_range_agg = DTtime_cleaner(df_LICSNO0717_Features.FENDAT_range_agg, 1)
    df_LICSNO0717_Features.FENDAT_max_agg = DTtime_cleaner(df_LICSNO0717_Features.FENDAT_max_agg, 1)
    df_LICSNO0717_Features.FENDAT_min_agg = DTtime_cleaner(df_LICSNO0717_Features.FENDAT_min_agg, 1)

    # fillna for agg data 
    df_LICSNO0717_Features[u'WSLPDT_count_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WSLPDT_count_3year_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'SRMINVO_TOTAMT_sum_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'SRMINVO_TOTAMT_sum_3year_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'FENDAT_count_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'FINDT_count_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'SRMINVO_INVODT_count_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'SRMINVO_TOTAMT_mean_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'RTPTDT_count_aggmean'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'CSMCFORM_CASETP_Ccount_aggsum'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'CSMCFORM_CASETP_Gcount_aggsum'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'CSMCFORM_CASETP_Qcount_aggsum'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'CSMCFORM_CASETP_Rcount_aggsum'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'FINDT_max_range_agg'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'RTPTDT_max_range_agg'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'SRMINVO_INVODT_max_range_agg'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WSLPDT_max_range_agg'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'FENDAT_range_agg'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WSLPDT_count_aggsum'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'WSLPDT_count_3year_aggsum'].fillna(0, inplace=True)
    df_LICSNO0717_Features[u'CARENDAT_range_agg'].fillna(0, inplace=True)

    for item in ['CARAGE_aggmean',
                 'FINDT_max_max_agg',
                 'FINDT_max_min_agg',
                 'RTPTDT_max_max_agg',
                 'RTPTDT_max_min_agg',
                 'SRMINVO_INVODT_max_max_agg',
                 'SRMINVO_INVODT_max_min_agg',
                 'WSLPDT_max_max_agg',
                 'WSLPDT_max_min_agg', 'FENDAT_max_agg', 'FENDAT_min_agg', 'CARENDAT_max_agg', 'CARENDAT_min_agg']:
        df_LICSNO0717_Features.loc[df_LICSNO0717_Features[item].isnull(), item] = df_LICSNO0717_Features.loc[
                                                                                      df_LICSNO0717_Features[
                                                                                          item].isnull(), u'CARAGE'] * 365

        # 是否為和泰認證中古車
    df_LICSNO0717_Features['Is_UCDCAR'] = 0
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features['UCGUABKNM'].notnull(), 'Is_UCDCAR'] = 1
    df_LICSNO0717_Features[u'FENDAT_count'] = df_LICSNO0717_Features[u'FENDAT_count'].astype(int)
    df_LICSNO0717_Features[u'UENDAT_count'] = df_LICSNO0717_Features[u'UENDAT_count'].astype(int)
    df_LICSNO0717_Features[u'WSLPDT_count'] = df_LICSNO0717_Features[u'WSLPDT_count'].astype(int)
    df_LICSNO0717_Features[u'RTPTDT_count'] = df_LICSNO0717_Features[u'RTPTDT_count'].astype(int)
    df_LICSNO0717_Features[u'STRWKDT_count'] = df_LICSNO0717_Features[u'STRWKDT_count'].astype(int)
    df_LICSNO0717_Features[u'FINDT_count'] = df_LICSNO0717_Features[u'FINDT_count'].astype(int)
    df_LICSNO0717_Features[u'AGVCARDT_count'] = df_LICSNO0717_Features[u'AGVCARDT_count'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_star_counts'] = df_LICSNO0717_Features[u'WKCTCD_star_counts'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_1_counts'] = df_LICSNO0717_Features[u'WKCTCD_1_counts'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_2_counts'] = df_LICSNO0717_Features[u'WKCTCD_2_counts'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_3_counts'] = df_LICSNO0717_Features[u'WKCTCD_3_counts'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_B_counts'] = df_LICSNO0717_Features[u'WKCTCD_B_counts'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_P_counts'] = df_LICSNO0717_Features[u'WKCTCD_P_counts'].astype(int)
    df_LICSNO0717_Features[u'WKCTCD_T_counts'] = df_LICSNO0717_Features[u'WKCTCD_T_counts'].astype(int)

    # LRKM 里程數需要跟回廠更新成 LRKM_modified
    df_LICSNO0717_Features[u'LRKM_modified'] = (df_LICSNO0717_Features[u'CARAGE'] * 365.0) * (
                df_LICSNO0717_Features[u'LRKM'] / (
                    (df_LICSNO0717_Features[u'CARAGE'] * 365.0) - df_LICSNO0717_Features[u'WSLPDT_max']))
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'LRKM_modified'] < 20000, 'LRKM_modified'] = None
    df_LICSNO0717_Features.loc[df_LICSNO0717_Features[u'LRKM_modified'] > 1000000, 'LRKM_modified'] = None
    df_LICSNO0717_Features[u'LRKM_modified'].fillna(df_LICSNO0717_Features[u'LRKM_modified'].median(), inplace=True)

    #  '濾掉不需要的特徵'
    # DROP LIST
    drop_columns_list = ['index',
                         'STS_DLRBRNH',  # 1070105加的
                         'CARAGE_aggmean',  # 1080418 加的，裡面有10筆資料值 > 100，不合理，且他的值跟CARAGE同質性太高
                         'EGNO', 'BDNO',
                         'qualified', 'VIN',
                         'SR_last_INVOCID',
                         'SR_last_LSKICID',
                         'CUSTID1',
                         'CUSTID2',
                         'virtual_SA',
                         'SADLR',
                         'STSDT'
                         ]

    for item in drop_columns_list:
        if item in df_LICSNO0717_Features.columns:
            df_LICSNO0717_Features.drop(item, axis=1, inplace=True)

    df_feature_desc = dataframe_quick_look(df_LICSNO0717_Features)
    # HTML(df_feature_desc.to_html())

    aa = 0
    for item in df_feature_desc[df_feature_desc.NA_Rate > 0.35]['columns'].tolist():
        if item in df_LICSNO0717_Features.columns:
            df_LICSNO0717_Features.drop(item, axis=1, inplace=True)
            #         print (item)
            aa += 1

    df_feature_desc = dataframe_quick_look(df_LICSNO0717_Features)
    HTML(df_feature_desc[df_feature_desc['NA_Rate'] != 0].to_html())
    df_LICSNO0717_Features = df_LICSNO0717_Features.drop_duplicates('LICSNO')
    # df_LICSNO0717_Features.to_csv(Result_Path+"Big_Table_Original.csv",  sep=',', index=False, encoding='utf-8')
    # df_LICSNO0717_Features.to_csv(HT_Result_Path+"Big_Table_Original.csv",  sep=',', index=False, encoding='utf-8')
    # df_LICSNO0717_Features.to_sql("md_Big_Table_Original_test", con = engine, if_exists='replace', index = False)
    write_Log(Log_File, "ok\n")

    ##這邊僅接受 類別300種以下的
    select_XY_list = []
    for index in range(0, df_feature_desc.shape[0]):
        if ((df_feature_desc['dtypes'].iloc[index] != 'object') | (df_feature_desc['value_counts'].iloc[index] < 300)):
            select_XY_list.append(df_feature_desc['columns'].iloc[index])

    # 過濾 相關係數過高者
    # select_XY_list[0]
    dropXY_list = []
    for index1 in range(0, len(select_XY_list)):
        for index2 in range(0, index1):
            if ((df_LICSNO0717_Features[select_XY_list[index1]].dtypes != 'object') & (
                    df_LICSNO0717_Features[select_XY_list[index2]].dtypes != 'object')):
                if math.fabs(df_LICSNO0717_Features[[select_XY_list[index1], select_XY_list[index2]]].corr().iloc[
                                 0, 1]) > 0.95:
                    dropXY_list.append(select_XY_list[index1])
    select_XY_list = [item for item in select_XY_list if item not in dropXY_list]
    del df_PSLTAXORDMF_forNewcarRemove
    gc.collect()

    # loading packages for models
    from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
    from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score, StratifiedKFold
    from sklearn.metrics import make_scorer
    from sklearn import metrics
    from patsy import dmatrices
    from sklearn.externals import joblib

    # remove 不要的特徵 
    # 並且建立dummy variables

    write_Log(Log_File, "27. %s | Create dummy variables from Big_Table......" % str(datetime.datetime.now()))
    X_feature_list = list(select_XY_list)
    X_feature_list.remove(u'PSLTAX')  # 先刪除 需要時 從df_LICSNO0717_Features取回
    X_feature_list.remove(u'scrapped_150708')  # 先刪除 需要時 從df_LICSNO0717_Features取回

    X_feature_list.remove(u'STSCD')

    for index in [u'STSDT', u'MTDT', u'latMT', u'lonMT', u'distanceMT']:
        try:
            X_feature_list.remove(index)
        #         print (index+' is removed')
        except ValueError:
            pass

    df_LICSNO0717_Features_D = pd.DataFrame()
    feature_number = 0
    if ((df_feature_desc[df_feature_desc['columns'] == X_feature_list[0]]['dtypes'].iloc[0] == 'int64') | (
            df_feature_desc[df_feature_desc['columns'] == X_feature_list[0]]['dtypes'].iloc[0] == 'float64')):
        df_LICSNO0717_Features_D = pd.concat([df_LICSNO0717_Features_D, df_LICSNO0717_Features[X_feature_list[0]]],
                                             axis=1)
    else:
        df_LICSNO0717_Features_D = pd.concat([df_LICSNO0717_Features_D,
                                              pd.get_dummies(df_LICSNO0717_Features[X_feature_list[0]],
                                                             prefix=X_feature_list[0])], axis=1)
    feature_number += 1

    for index in range(1, len(X_feature_list)):
        feature_number += 1
        if ((df_feature_desc[df_feature_desc['columns'] == X_feature_list[index]]['dtypes'].iloc[0] == 'int64') | (
                df_feature_desc[df_feature_desc['columns'] == X_feature_list[index]]['dtypes'].iloc[0] == 'float64')):
            df_LICSNO0717_Features_D = pd.concat(
                [df_LICSNO0717_Features_D, df_LICSNO0717_Features[X_feature_list[index]]], axis=1)
        else:
            df_LICSNO0717_Features_D = pd.concat([df_LICSNO0717_Features_D,
                                                  pd.get_dummies(df_LICSNO0717_Features[X_feature_list[index]],
                                                                 prefix=X_feature_list[index])], axis=1)
    write_Log(Log_File, "ok\n")

    for index in [u'CARNM_-99999', u'SRLFOWM_APPFLG_NAcount', u'TRCANO_NaN', u'SABRNH_na', u'CARCD_NaN',
                  'SRMINVO_DLRCD_maxoccu_NaN']:
        try:
            df_LICSNO0717_Features_D.drop(index, axis=1, inplace=True)
        except ValueError:
            pass

    # 因為資料中有Nan，所以程式執行出現error，因此加入FORCEID1跟FORCEID2的清理code
    # 在進入Cassasdra前，FORCEID如果是空白，都會被改成 0，所以基本上不應該出現Nan，
    df_LICSNO0717_Features_D['FORCEID1'] = df_LICSNO0717_Features_D['FORCEID1'].map(lambda x: 0 if pd.isnull(x) else x)
    df_LICSNO0717_Features_D['FORCEID2'] = df_LICSNO0717_Features_D['FORCEID2'].map(lambda x: 0 if pd.isnull(x) else x)
    # print("Done")

    aa = df_LICSNO0717_Features_D.copy()
    aa['LICSNO'] = df_LICSNO0717_Features['LICSNO']
    aa.to_csv(Result_Path + "Big_Table_Original.csv", getSRMINVO_query_datasep=',', index=False, encoding='utf-8')
    aa.to_sql("md_Big_Table_Original", con=engine, if_exists='replace', index=False)
    del aa
    gc.collect()

    # 建立y
    X = df_LICSNO0717_Features_D
    y = df_LICSNO0717_Features['PSLTAX']
    y = np.ravel(y)
    np.unique(y, return_counts=True)
    # print (len(X_feature_list))
    # print (X.shape)
    # print (np.unique(y, return_counts=True))
    # print (y.shape)
    del df_LICSNO0717_Features_D

    write_Log(Log_File, "28. %s | Compute feature importance values......" % str(datetime.datetime.now()))
    forest = RandomForestClassifier(min_samples_split=34, n_estimators=1500, criterion='gini', min_samples_leaf=16,
                                    n_jobs=-1, random_state=12345)
    forest.fit(X, y)
    importances = forest.feature_importances_
    std = np.std([tree.feature_importances_ for tree in forest.estimators_], axis=0)
    indices = np.argsort(importances)[::-1]

    importance_Filter = pd.read_csv(Import_Data_Path + "importance_Filter.csv", sep=',', encoding='utf-8')
    importance_Filter['Feature_Name'] = importance_Filter['Feature_Name'].map(lambda x: x.strip())
    importance_Feature_Name = importance_Filter['Feature_Name'].tolist()
    sum_value = 0
    f1 = open(Result_Path + 'importance.csv', 'w+')
    # f2 = open(HT_Result_Path+'importance.csv', 'w+')
    f1.write("Temp_Score,Feature_Name, Score\n")
    # f2.write("Temp_Score,Feature_Name, Score\n")
    count = 0
    for f in range(0, len(X.columns)):
        if X.columns[indices[f]] in importance_Feature_Name:
            sum_value = sum_value + importances[indices[f]]
            if (sum_value < 0.99999):
                score = 1
            else:
                score = 0
            f1.write("%d,%s,%f\n" % (score, X.columns[indices[f]], importances[indices[f]]))
    #         f2.write("%d,%s,%f\n" % (score, X.columns[indices[f]], importances[indices[f]]))
    f1.close()
    # f2.close()
    # write to importance.csv 的程式原本就寫好了，偷懶一下，df_importance從csv檔讀入再寫到RDB中
    df_importance = pd.read_csv(Result_Path + 'importance.csv', sep=',', encoding='utf-8', low_memory=False)
    df_importance.to_sql("md_importance", con=engine, if_exists='replace', index=False)
    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "29. %s | Random Forest parameters optimization......" % str(datetime.datetime.now()))

    Xtr, Xts, ytr, yts = train_test_split(X, y, test_size=0.3, stratify=y)
    # random forest parameter optimization
    # 用 GridSearchCV()，他可以從給定的param_grid 中，找出最佳的參數組合
    # 用 GridSearchCV()，將所有的參數資料輸入到param_grid中，就可以丟給電腦自己跑出一組最佳的參數組合
    # 下面的 rf 儲存固定參數，param 儲存所有要最佳化的參數
    rf = RandomForestClassifier(oob_score=False, random_state=12345, n_jobs=14, class_weight='balanced')
    param_grid = {"criterion": ['gini'],
                  "max_features": ['auto'],  # ,'sqrt',0.2
                  "min_samples_leaf": [2, 4, 8],  # 多次不同參數測試，值都等於 2 ，range(2,30,8),
                  "min_samples_split": [2, 46, 4],  # [2]
                  "n_estimators": [1000, 1500, 2000],
                  }
    gs = GridSearchCV(estimator=rf, param_grid=param_grid, scoring='f1', cv=StratifiedKFold(5).split(Xtr, ytr),
                      n_jobs=-1)
    gs = gs.fit(Xtr, ytr)  # PRECISION
    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "Best Parameter: %s\n" % str(gs.best_params_))

    write_Log(Log_File, "30. %s | Train model and predict results......" % str(datetime.datetime.now()))
    bp_list = []
    bp_list.append(gs.best_params_)
    for item in bp_list:
        print(item)
        bp = item
        rf = RandomForestClassifier(max_features='auto',
                                    criterion=bp['criterion'],
                                    n_estimators=bp['n_estimators'],
                                    min_samples_leaf=bp['min_samples_leaf'],
                                    min_samples_split=bp['min_samples_split'],
                                    oob_score=False,
                                    random_state=12345,
                                    n_jobs=-1, class_weight='balanced')  #
    rf.fit(Xtr, ytr)
    pred = rf.predict(Xts)
    rfprobs = rf.predict_proba(Xts)

    pred_alldata = rf.predict(X)
    rfprobs_alldata = rf.predict_proba(X)
    df_rfprobs_alldata = pd.DataFrame(data=rfprobs_alldata)
    df_rfprobs_alldata = df_rfprobs_alldata.sort_values(by=[1], ascending=False)
    df_rfprobs_alldata.columns = ['0', '1']
    df_rfprobs_alldata['y'] = pd.DataFrame(data=y)  # 實際的 y
    df_rfprobs_alldata['pred_y'] = pd.DataFrame(data=pred_alldata)
    df_rfprobs_alldata['LICSNO'] = df_LICSNO0717_Features['LICSNO'].map(lambda x: x.strip().upper())
    Final_List = pd.DataFrame(df_rfprobs_alldata, columns=['LICSNO', 'y', 'pred_y'])
    Final_List['probability'] = df_rfprobs_alldata['1']
    # Final_List.to_csv(Result_Path+"Final_List.csv",  sep=',', index=False, encoding='utf-8')
    # Final_List.to_csv(HT_Result_Path+"Final_List.csv",  sep=',', index=False, encoding='utf-8')
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "31. %s | Merge Mobile number and Customer ID......" % str(datetime.datetime.now()))
    # download mobile number for merging
    conn = pyodbc.connect(DSN="Simba Spark ODBC Driver", autocommit=True, unicode_results=True)
    df_LICSNO_CUSTID_MOBILE = pd.read_sql(u"""
      SELECT a.LICSNO, a.TARGET, a.CUSTID, a.FORCEID, b.MOBILE
      FROM cdp.CRAURF a
      left join cdp.CRCUMF b on a.CUSTID = b.CUSTID and a.FORCEID = b.FORCEID
      where a.TARGET = 2 and a.FORCEID not in ('1','2','3')
      order by a.LICSNO""", conn)
    conn.close()

    df_LICSNO_CUSTID_MOBILE['MOBILE'] = df_LICSNO_CUSTID_MOBILE['MOBILE'].astype(str)
    df_LICSNO_CUSTID_MOBILE['MOBILE'] = df_LICSNO_CUSTID_MOBILE['MOBILE'].map(
        lambda x: '' if pd.isnull(x) else re.sub('[^0-9]+', '', x))
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE[~(df_LICSNO_CUSTID_MOBILE['LICSNO'] == '')]
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE[~(df_LICSNO_CUSTID_MOBILE['MOBILE'] == '')]
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE[~(df_LICSNO_CUSTID_MOBILE['LICSNO'].isnull())]
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE[~(df_LICSNO_CUSTID_MOBILE['MOBILE'].isnull())]
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE.drop_duplicates('LICSNO')
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE.drop_duplicates('MOBILE')
    df_LICSNO_CUSTID_MOBILE = df_LICSNO_CUSTID_MOBILE[df_LICSNO_CUSTID_MOBILE['MOBILE'].map(lambda x: len(x)) == 10]

    Final_List_with_MOBILE = Final_List.merge(df_LICSNO_CUSTID_MOBILE, how='left', on='LICSNO')
    del Final_List  # 後面用不到這個 df 了

    Final_List_with_MOBILE.drop(['TARGET', 'FORCEID'], axis=1, inplace=True)
    Final_List_with_MOBILE = Final_List_with_MOBILE[~(Final_List_with_MOBILE['LICSNO'].isnull())]
    Final_List_with_MOBILE = Final_List_with_MOBILE[~(Final_List_with_MOBILE['MOBILE'].isnull())]
    Final_List_with_MOBILE = Final_List_with_MOBILE.drop_duplicates('LICSNO')
    Final_List_with_MOBILE.to_csv(Result_Path + "Final_List_drop_Mobile_duplicates.csv", sep=',', index=False,
                                  encoding='utf-8')
    # Final_List_with_MOBILE.to_csv(HT_Result_Path+"Final_List_drop_Mobile_duplicates.csv",  sep=',', index=False, encoding='utf-8')
    Final_List_with_MOBILE.to_sql("md_Final_List_drop_Mobile_duplicates", con=engine, if_exists='replace', index=False)

    FINAL_Result = Final_List_with_MOBILE[Final_List_with_MOBILE['y'] == 0]
    threshold = FINAL_Result['probability'].iloc[20000]
    del FINAL_Result
    # # FINAL_Result = FINAL_Result.head(10500)
    # FINAL_Result.to_csv(Result_Path+"FINAL_Result.csv",  sep=',', index=False, encoding='utf-8')
    # FINAL_Result.to_csv(HT_Result_Path+"FINAL_Result.csv",  sep=',', index=False, encoding='utf-8')

    # batch_config data write to RDB
    # yy =today.strftime("%Y")	#mark by HD
    # mm = today.strftime("%m")	#mark by HD
    File_DATE = yy + mm + '01'
    Model_Name = "MD_01_" + File_DATE
    batch_config_data = {'Model_Date': File_DATE, 'Model_Name': Model_Name, 'Threshold': threshold}
    df_batch_config = pd.DataFrame(data=batch_config_data, columns=['Model_Date', 'Model_Name', 'Threshold'],
                                   index=['1'])
    df_batch_config.to_sql("md_batch_config", con=engine, if_exists='replace', index=False)

    # batch_config data write to File
    batch_config = open(Result_Path + "batch_config.txt", 'w+')
    batch_config.write("%s\n" % File_DATE)
    batch_config.write("MD_01_%s\n" % File_DATE)
    batch_config.write(str(threshold))
    batch_config.close()

    write_Log(Log_File, "ok\n")
    write_Log(Log_File, "Complete\n\n")

    # In[ ]:

    # Final_List_drop_Mobile_duplicates.csv
    # importance.csv
    # Big_Table_Original.csv
    # batch_config.txt

    # In[ ]:

    # #cal mean std
    # def dataframe_quick_look_numerical(df_input):
    #     df_feature_desc = dataframe_quick_look(df_input)
    #     df_feature_desc_numerical = df_feature_desc[(df_feature_desc[u'dtypes']=='int64') | (df_feature_desc[u'dtypes']=='float64')].reset_index(drop=True)
    #     df_feature_desc_numerical['min']=df_feature_desc_numerical[u'columns'].map(lambda x: df_input[x].min())
    #     df_feature_desc_numerical['mean']=df_feature_desc_numerical[u'columns'].map(lambda x: df_input[x].mean())
    #     df_feature_desc_numerical['median']=df_feature_desc_numerical[u'columns'].map(lambda x: df_input[x].median())
    #     df_feature_desc_numerical['max']=df_feature_desc_numerical[u'columns'].map(lambda x: df_input[x].max())
    #     df_feature_desc_numerical['std']=df_feature_desc_numerical[u'columns'].map(lambda x: df_input[x].std())
    #     df_feature_desc_numerical['range']=df_feature_desc_numerical['max'] - df_feature_desc_numerical['min']
    #     return df_feature_desc_numerical
    # HTML(dataframe_quick_look_numerical(df_LICSNO0717_Features).sort_values('range', ascending=False).to_html(float_format=lambda x: '%10.2f'% x)) 
    # # print ("Done")
    '''