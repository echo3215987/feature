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
#TODO
#from dask import dataframe as dd
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
from Constant import *
from SparkUDF import *
from pyspark.sql.functions import *
from pyspark.storagelevel import *

# In[3]:
def write_Log(Log_File, Log_Str):
    Log = open(Log_File, 'a')
    Log.write(Log_Str)
    Log.close()

# In[5]:
# Define Function
'''
def series_str_cleaner(inpurt_series):
    inpurt_series = inpurt_series.fillna('')
    inpurt_series = inpurt_series.map(lambda x: x if (isinstance(x, float) or x is None) else x.replace(' ', ''))
    inpurt_series = inpurt_series.map(lambda x: '' if pd.isnull(x) else re.sub('[^0-9a-zA-Z]+', '', x).upper())
    inpurt_series = inpurt_series.astype(str)
    return inpurt_series
'''
def strip_string(df, columneName):
    df = df.withColumn(columneName+'_STRIP', trim(df[columneName]))
    df = df.drop(columneName).withColumnRenamed(columneName+"_STRIP", columneName)
    return df

def upper_string(df, columneName):
    df = df.withColumn(columneName+'_UPPER', upper(df[columneName]))
    df = df.drop(columneName).withColumnRenamed(columneName+"_UPPER", columneName)
    return df

def series_str_cleaner(inpurt_series, columnName):
    inpurt_series = inpurt_series.na.fill({columnName: ' '})
    inpurt_series = inpurt_series.withColumn(columnName + '_temp', regexp_replace(columnName, '[^0-9a-zA-Z]+', ''))
    inpurt_series = inpurt_series.drop(columnName).withColumnRenamed(columnName + '_temp', columnName)
    return inpurt_series
'''
def time_cleaner(Input_Series):
    temp = pd.DataFrame()  # 由
    temp[u'DT'] = Input_Series.map(lambda x: x if (isinstance(x, float) or x is None) else x.replace(' ', ''))
    temp[u'DT_fix'] = pd.to_datetime(temp[u'DT'], format='%Y%m%d', errors='coerce')
    return temp[u'DT_fix']
'''

# 取該欄位的倒數 x 碼
def substr_last_char(df, columnName, numberIndex):
    df = df.withColumn(columnName+'_SUBSTR', col(columnName).substr(-numberIndex, numberIndex))
    df = df.drop(columnName).withColumnRenamed(columnName+'_SUBSTR', columnName)
    return df

# 新增一個欄位, 取某欄位的倒數 x 碼
def substr_last_char_newColumn(df, columnName, numberIndex, newColumnName):
    df = df.withColumn(newColumnName, col(columnName).substr(-numberIndex, numberIndex))
    return df

# 該欄位長度小於x, 將該值改成''
def check_length_over_replacement(df, columnName, numberIndex):
    df = df.withColumn(columnName + '_REPLACEMENT', lengthReplacementOver_UDF(array(columnName, lit(numberIndex))))
    df = df.drop(columnName).withColumnRenamed(columnName + '_REPLACEMENT', columnName)
    return df

# 如果指定的欄位包含該欄位的值, 將值取代成 ''
def exist_value_replacement(df, columnName, fixedColumnName):
    df = df.withColumn(columnName + '_REPLACEMENT', existValueReplacement_UDF(array(columnName, fixedColumnName)))
    df = df.drop(columnName).withColumnRenamed(columnName + '_REPLACEMENT', columnName)
    return df

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
def SQL_df_CRCAMF(spark):

    df = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cdp").option("table", "crcamf").load()
    df.createOrReplaceTempView("CRCAMF")

    df_CRCAMF = spark.sql(u"""
        SELECT * FROM CRCAMF  WHERE (FRAN in ('L','T') or  
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
        upper(CARNM) like '%TERCEL%'  ) and upper(CARNM) not like '%DYNA%' and upper(CARNM) not like '%HINO%'""")
    return df_CRCAMF

def SQL_df_SSHSCHISTORY(spark):
    df_SSHSCHISTORY = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cdp").option("table", "sshschistory").load()
    df_SSHSCHISTORY.createOrReplaceTempView("SSHSCHISTORY")

    df_SSHSCHISTORY = spark.sql(u"""
        SELECT * FROM SSHSCHISTORY WHERE
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
          GRPNM not in ('HINO','ZZZ-C','DYNA','ZZZ-P','LT-1','LT-0')""")

    return df_SSHSCHISTORY

'''
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
'''

def SQL_df_SSHUCHISTORY(spark):
    df_SSHUCHISTORY = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cdp").option("table", "sshuchistory").load()
    df_SSHUCHISTORY.createOrReplaceTempView("SSHUCHISTORY")

    df_SSHUCHISTORY = spark.sql(u"""
        SELECT BRAND, GRPNM, MODELM, BDNOM, BODYNO, EGNOM, ENGINENO, ISSUE  FROM SSHUCHISTORY WHERE 
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
        GRPNM not in ('HINO','ZZZ-C','DYNA','ZZZ-P','LT-1','LT-0')""")
    return df_SSHUCHISTORY

'''
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
'''
def getCRCAMF_filter(spark, df_CRCAMF):
    # 將LSKIDT, SEDLDT, REDLDT 去空白之後轉換成 datetime 的格式
    df_CRCAMF = df_CRCAMF.withColumn('LSKIDT_fix', transDatetime_UDF(array('LSKIDT', lit(DATETIME_FORMAT1))))
    df_CRCAMF = df_CRCAMF.withColumn('SEDLDT_fix', transDatetime_UDF(array('SEDLDT', lit(DATETIME_FORMAT1))))
    df_CRCAMF = df_CRCAMF.withColumn('REDLDT_fix', transDatetime_UDF(array('REDLDT', lit(DATETIME_FORMAT1))))

    # 三個之中取一個最小的時間, 且如果 LSKIDT_stamp 為 NaT 時, 將STRDT的值取代原本NaT
    df_CRCAMF = df_CRCAMF.withColumn("LSKIDT_stamp",
                                     coalesce(least("LSKIDT_fix", "SEDLDT_fix", "REDLDT_fix"), df_CRCAMF['STRDT']))

    # 車輛篩選
    # print ('暫用15年以上車輛') today.year - Candidate_Car_age
    # LSKIDT_stamp 大於 1988/1/1號 且 LSKIDT_stamp 小於等於 15年前當月的1號
    df_CRCAMF = df_CRCAMF.where(
        (df_CRCAMF.LSKIDT_stamp <= datetime.datetime(today.year - Candidate_Car_age, today.month,
                                                     1, 0, 0, 0, 0)) & (
                    df_CRCAMF.LSKIDT_stamp >= datetime.datetime(1988, 1, 1, 0, 0, 0, 0)))

    # print ('去除買不到 1 年的中古車') # 法規規定中古車須購買滿 1 年始可申請貨物稅補助
    # UCDELIVIDT 小於等於 1年前當月1號 或 UCDELIVIDT是空值
    df_CRCAMF = df_CRCAMF.where(
        (df_CRCAMF['UCDELIVIDT'] <= datetime.datetime(today.year - 1, today.month, 1, 0, 0, 0, 0)) |
        col("UCDELIVIDT").isNull())

    # ('去除舊車牌已更換新車牌者')
    df_remove_LICSNO = spark.read.option('header', 'true').csv(Import_Data_Path + "remove_LICSNO.csv")

    # 將欄位Licsno去空白且轉大寫
    df_remove_LICSNO = strip_string(df_remove_LICSNO, 'LICSNO')
    df_remove_LICSNO = upper_string(df_remove_LICSNO, 'LICSNO')

    # 去除在remove_LICSNO.csv裡的所有車牌
    df_CRCAMF = strip_string(df_CRCAMF, 'LICSNO')
    df_CRCAMF = df_CRCAMF.withColumn('LICSNO_upper', upper(col("LICSNO")))
    df_CRCAMF = df_CRCAMF.repartition("LICSNO_upper").join(df_remove_LICSNO,
                                                           df_CRCAMF.LICSNO_upper == df_remove_LICSNO.LICSNO,
                                                           "leftanti").persist(StorageLevel.DISK_ONLY)
    df_remove_LICSNO = None

    return df_CRCAMF

def getCRCAMF_3year(spark):
    df_CRCAMF_3year = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cdp").option("table",
                                                                                                           "crcamf").load()
    df_CRCAMF_3year.createOrReplaceTempView("CRCAMF")
    df_CRCAMF_3year = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cdp").option("table",
                                                                                                           "srwhmf").load()
    df_CRCAMF_3year.createOrReplaceTempView("SRWHMF")

    df_CRCAMF_3year = spark.sql(u"""select a.*, b.LRDT from .CRCAMF a, SRWHMF b
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
          """)
    return df_CRCAMF_3year

def getWeb_query_filter(spark):
    df_web_query = spark.read.option('header', 'true').csv(Import_Data_Path + "df_web_query_fix.csv")
    df_web_query = strip_string(df_web_query, 'LICSNO')
    df_web_query = upper_string(df_web_query, 'LICSNO')
    df_web_query = df_web_query.withColumn('LICSNO_upper', upper(trim(col("LICSNO"))))
    df_web_query = df_web_query.withColumn('CARDATE_fix', transDatetime_UDF(array('CARDATE', lit(DATETIME_FORMAT2))))
    return df_web_query

# get CRAURF data
def getCRAURF(spark):
    df_CRAURF = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "cdp").option("table", "craurf").load()
    df_CRAURF.createOrReplaceTempView("CRAURF")
    df_CRAURF = spark.sql(u"""SELECT LICSNO, TARGET, CUSTID, FORCEID FROM cdp.CRAURF""")
    return df_CRAURF

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