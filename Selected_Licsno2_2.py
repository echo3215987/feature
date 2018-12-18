import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from Constant import *
from SparkUDF import *

def selected_licsno_code2_2(spark):

    df_CRCAMF = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_union.csv")
    # 找出 最近三年內有回廠的15年以上車

    write_Log(Log_File, "13. %s | Look out the repaired cars over 15 years old in the past 3 years......" % str(
        datetime.datetime.now()))
    df_CRCAMF_3year = getCRCAMF_3year(spark)
    df_CRCAMF_3year = getCRCAMF_filter(spark, df_CRCAMF_3year)

    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "14. %s | Remove unused data......" % str(datetime.datetime.now()))

    # 去除車輛狀態不是為 更改為 不得為 2過戶3失竊5報廢'
    df_CRCAMF_3year = df_CRCAMF_3year.filter(
        ((df_CRCAMF_3year.STSCD != '2') & (df_CRCAMF_3year.STSCD != '3') & (df_CRCAMF_3year.STSCD != '5')) | (df_CRCAMF_3year.STSCD.isNull()))

    # 扣除車牌"所有"已在網站上查詢到, 後續會加回最近報廢的
    df_web_query = getWeb_query_filter(spark)
    # 指定時間之前的才清除 避免網站查詢不同步
    df_web_query = df_web_query.where(
        df_web_query['CARDATE_fix'] < datetime.datetime.strptime(END_DATE, DATETIME_FORMAT3))
    # 沒有報廢日期的 在這邊有可能被加入 後續篩選時 要濾掉
    df_CRCAMF_3year = df_CRCAMF_3year.join(df_web_query, df_CRCAMF_3year.LICSNO_upper == df_web_query.LICSNO,
                                           "leftanti")

    write_Log(Log_File, "ok\n")

    # print u'6. 扣除車牌"所有"已在網站上查詢到, 已經車牌失竊或車輛失竊'
    df_stolen = spark.read.option('header', 'true').csv(Import_Data_Path + "Stolen.csv")
    df_stolen = df_stolen.withColumnRenamed("車型", "type").withColumnRenamed("車牌", "LICSNO").withColumnRenamed("失車查詢結果","result") \
        .withColumnRenamed("查詢時間", "date")
    # 將欄位Licsno去空白且轉大寫
    df_stolen = strip_string(df_stolen, 'LICSNO')
    df_stolen = upper_string(df_stolen, 'LICSNO')
    df_stolen = df_stolen.filter((df_stolen['result'] != u'查無資料') | (df_stolen['result'].isNull()))
    df_CRCAMF_3year = df_CRCAMF_3year.repartition("LICSNO")\
        .join(df_stolen.repartition("LICSNO"), df_CRCAMF_3year.LICSNO_upper == df_stolen.LICSNO, "leftanti")\
        .persist(StorageLevel.DISK_ONLY)
    write_Log(Log_File, "ok\n")

    df_CRCAMF_3year = df_CRCAMF_3year.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN")
    # print '清除報廢後 再加上在20150708 之後報廢的車輛 記得要改路徑'

    df_scrapped_20150708 = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv")
    df_CRCAMF = df_CRCAMF.union(df_scrapped_20150708)
    # print ' 再加上三年內有回廠 車齡15年以上的車輛 '
    df_CRCAMF = df_CRCAMF.union(df_CRCAMF_3year)
    df_CRCAMF = strip_string(df_CRCAMF, 'LICSNO')
    df_CRCAMF = df_CRCAMF.dropDuplicates(['LICSNO'])
    df_CRCAMF.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF.csv")
    df_CRAURF = getCRAURF(spark)

    # 資料清整
    #df_CRCAMF = df_CRCAMF.withColumn("qualified", lit('1'))

    # 清除前後空白
    df_CRCAMF = strip_string(df_CRCAMF, 'LICSNO')
    df_CRCAMF = df_CRCAMF.dropDuplicates(['LICSNO'])
    df_CRAURF = strip_string(df_CRAURF, 'LICSNO')
    # df_CRAURF 五種人 所以本來就會重複

    df_CRAURF = df_CRAURF.filter((df_CRAURF['LICSNO'] != '') | (df_CRAURF['LICSNO'].isNull()))
    # matching for LICSNO with qualified car for all people
    # 找出符合條件的車輛下的所有人
    # df_CRCAMF 這邊LICSNO 尚未清整 因此 MERGE後 會變多
    # 已經DROP DUPLICATES
    df_CRAURF = df_CRAURF.repartition("LICSNO").join(df_CRCAMF.select("LICSNO").repartition("LICSNO"), "LICSNO", "inner").persist(StorageLevel.DISK_ONLY)
    # print '留下qualified')
    #df_CRAURF = df_CRAURF.filter(df_CRAURF['qualified']=='1').select("CUSTID", "qualified")

    # print '清掉前後空白'
    df_CRAURF = df_CRAURF.filter(df_CRAURF['CUSTID'].isNotNull())
    df_CRAURF = strip_string(df_CRAURF, 'CUSTID')

    # print '身分證空的清掉'
    df_CRAURF = df_CRAURF.filter((df_CRAURF['CUSTID'] != '') | (df_CRAURF['CUSTID'].isNull()))

    # print '身分證一樣的清掉'
    df_CRAURF = df_CRAURF.dropDuplicates(["CUSTID"])

    # RE-DOWNLOAD df_CRAURF FOR MATCHING CUSTID
    # 再由找到的所有人 去找出所有車輛
    df_CRAURF_all = getCRAURF(spark)

    # 清掉前後空白
    df_CRAURF_all = df_CRAURF_all.filter(df_CRAURF_all['CUSTID'].isNotNull())
    df_CRAURF = df_CRAURF[df_CRAURF['CUSTID'].isNotNull()]
    df_CRAURF_all = strip_string(df_CRAURF_all, 'CUSTID')
    df_CRAURF = strip_string(df_CRAURF, 'CUSTID')
    df_CRAURF_all = df_CRAURF_all.filter((df_CRAURF_all['CUSTID']!='') | (df_CRAURF_all['CUSTID'].isNull()))
    df_CRAURF = df_CRAURF.filter((df_CRAURF['CUSTID'] != '') | (df_CRAURF['CUSTID'].isNull()))
    df_CRAURF_all = df_CRAURF_all.join(df_CRAURF.select("CUSTID"), "CUSTID", "inner").persist(StorageLevel.DISK_ONLY)
    df_CRAURF_all = strip_string(df_CRAURF_all, 'LICSNO') #車牌清掉空白後 會有一筆重複車牌
    df_CRAURF_all.dropDuplicates(["LICSNO"]).write.option('header', 'true').csv(Temp_Path + "df_CRAUR.csv")
    del df_CRAURF_all
    write_Log(Log_File, "ok\n")
