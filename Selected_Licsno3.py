import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from Constant import *
from SparkUDF import *

def selected_licsno_code3(spark):
    # Code 3: 20FIRST_table_combining
    # 符合條件的車
    #  df_CRCAMF704969_0810  把候選名單加入
    write_Log(Log_File, "15. %s | Combine data......" % str(datetime.datetime.now()))

    df_CRCAMF = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF.csv") # 讀取候選車輛名單

    df_CRCAMF = strip_string_exclude_float(df_CRCAMF, 'LICSNO')
    # df_CRCAMF = df_CRCAMF.drop('Unnamed: 0.1', 1)

    # 所有被選出的車牌數量 from 人車
    # 把候選名單 中的 其他擁有車輛加入
    # df_CRAUR.csv儲存從CRCAMF取出候選車輛名單，再用候選車輛名單到CRAUR找出所有對應的人員名單，再用人員名單串出所有車輛，然後刪除重複車號的結果

    df_CRAURF_select_LICSNO = spark.read.option('header', 'true').csv(Temp_Path + "df_CRAUR.csv")
    df_CRAURF_select_LICSNO = strip_string_exclude_float(df_CRAURF_select_LICSNO, 'LICSNO')

    # 清除空白車牌
    df_CRAURF_select_LICSNO = df_CRAURF_select_LICSNO.filter(df_CRAURF_select_LICSNO['LICSNO'] != '')  # 刪除 LICSNO=''的資料
    df_CRAURF_select_LICSNO = df_CRAURF_select_LICSNO.filter(df_CRAURF_select_LICSNO['LICSNO'].notnull())  # 刪除LICSNO= NULL的資料

    '''
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
    '''