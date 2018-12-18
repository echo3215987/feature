import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.storagelevel import *
from Constant import *
from SparkUDF import *
import sys

def selected_licsno_code1(spark):

    # Code 1: 02CRCAMF_vs_SSHSCHISTORY_after20150708(2-5)
    ## CRCAMF 篩選出所有和泰車

    write_Log(Log_File, "02. %s | Select data from cdp.CRCAMF......" % str(datetime.datetime.now()))
    # Original_df_CRCAMF = SQL_df_CRCAMF(spark)
    # df_CRCAMF = Original_df_CRCAMF.copy()  # SQL 執行一次之後，儲存於 Original_df_CRCAMF，要用時直接copy出來，節省執行時間
    df_CRCAMF = SQL_df_CRCAMF(spark)
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "03. %s | Clean the df_CRCAMF......" % str(datetime.datetime.now()))

    df_CRCAMF = getCRCAMF_filter(spark, df_CRCAMF)
    df_CRCAMF.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "STSCD", "LICSNO_upper", "UCDELIVIDT")\
        .write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_remove_LICSNO.csv")

    # print ('指定報廢區間為 20150708~ENDDATE現在 ') #沒有報廢日期者 不會被標示 因此三年內 回廠 有可能 已經報廢
    df_web_query = getWeb_query_filter(spark)

    # 篩選 CARDATE_fix大於等於2015年7月8號 且 CARDATE_fix小於等於END_DATE
    df_web_query = df_web_query.where(
        (df_web_query['CARDATE_fix'] >= datetime.datetime(2015, 7, 8, 0, 0, 0, 0)) &
        (df_web_query['CARDATE_fix'] <= datetime.datetime.strptime(END_DATE, "%Y-%m-%d"))).repartition("LICSNO_upper")

    df_CRCAMF = df_CRCAMF.join(df_web_query[['LICSNO_upper', 'STATUS']], "LICSNO_upper", 'left').persist(StorageLevel.DISK_ONLY)
    df_CRCAMF = df_CRCAMF.drop("LICSNO_upper")
    df_CRCAMF_web_query20150708 = df_CRCAMF.filter(df_CRCAMF.STATUS == '已回收')

    # print ('撈出', df_CRCAMF_web_query20150708.shape[0])
    df_CRCAMF_web_query20150708 = df_CRCAMF_web_query20150708.select('LICSNO', 'CARNM', 'CARMDL', 'BDNO', 'EGNO', 'VIN')
    # print (df_CRCAMF_web_query20150708.shape)

    df_CRCAMF = df_CRCAMF.where((df_CRCAMF['STATUS'] != '已回收') | (df_CRCAMF.STATUS.isNull())) # 這裡是扣除已回收的車籍資料
    # print ('剩下', df_CRCAMF.shape)
    # print u'5. 去CARNM出現次數過少者 這邊不去除過少 由報廢去篩選出來'
    df_CRCAMF = strip_string(df_CRCAMF, 'CARNM')

    # 將欄位值 EXSIOR, PREMIO取代成CORONA,  ALTIS 取代成COROLLA
    df_CRCAMF = df_CRCAMF.withColumn('CARNM_M', col("CARNM"))
    df_CRCAMF = df_CRCAMF.na.replace(['EXSIOR', 'PREMIO', 'ALTIS'], ['CORONA', 'CORONA', 'COROLLA'], 'CARNM_M')


    df_CRCAMF = series_str_cleaner(df_CRCAMF, 'BDNO')
    df_CRCAMF = series_str_cleaner(df_CRCAMF, 'EGNO')
    df_CRCAMF = series_str_cleaner(df_CRCAMF, 'VIN')

    #BDNO, EGNO, VIN取倒數十位
    df_CRCAMF = substr_last_char(df_CRCAMF, 'BDNO', 10)
    df_CRCAMF = substr_last_char(df_CRCAMF, 'EGNO', 10)
    df_CRCAMF = substr_last_char(df_CRCAMF, 'VIN', 10)

    write_Log(Log_File, "ok\n")

    # 下載所有報廢車輛並且進行篩選
    write_Log(Log_File, "04. %s | Select data from cdp.SSHSCHISTORY......" % str(datetime.datetime.now()))

    df_SSHSCHISTORY = SQL_df_SSHSCHISTORY(spark)
    #df_SSHSCHISTORY = Original_df_SSHSCHISTORY.copy()  ##SQL 執行一次之後，儲存於 Original_df_XXX，要用時直接copy出來，節省執行時間
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "05. %s | Clean the df_SSHSCHISTORY......" % str(datetime.datetime.now()))
    # print ('留下15年以上的車輛')# 10612改成留下15年的車留下15年以上的車輛子，ISSUE:發照日期
    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn('ISSUE_fix', transDatetime_UDF(array('ISSUE', lit(DATETIME_FORMAT1))))

    #ISSUE_fix 大於 1988/1/1號 且 ISSUE_fix 小於等於 15年前當月的1號
    df_SSHSCHISTORY = df_SSHSCHISTORY.where((df_SSHSCHISTORY.ISSUE_fix <= datetime.datetime(today.year - Candidate_Car_age, today.month,
        1, 0, 0, 0, 0)) & (df_SSHSCHISTORY.ISSUE_fix >= datetime.datetime(1988, 1, 1, 0, 0, 0, 0)))

    # print ("2015 0708之後報廢的車輛 需要找回")
    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn('MODDT_fix', transDatetime_UDF(array('MODDT', lit(DATETIME_FORMAT1))))

    # 這次找2015 0708之後報廢的車輛  七月的從MODDT 8/3開始
    df_SSHSCHISTORY = df_SSHSCHISTORY.where(df_SSHSCHISTORY.MODDT_fix >= datetime.datetime(2015, 7, 8, 0, 0, 0, 0))

    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'ENGINENO')
    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'EGNOM')
    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'BODYNO')
    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'BDNOM')
    df_SSHSCHISTORY = strip_string(df_SSHSCHISTORY, 'GRPNM')

    df_SSHSCHISTORY = exist_value_replacement(df_SSHSCHISTORY, 'EGNOM', 'ENGINENO')
    df_SSHSCHISTORY = exist_value_replacement(df_SSHSCHISTORY, 'BDNOM', 'BODYNO')

    # print (u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除')
    # EGNO取倒數9位
    df_web_query = series_str_cleaner(df_web_query, 'EGNO')
    df_web_query = substr_last_char(df_web_query, 'EGNO', 9)

    # 9碼以下不清 後九碼相同者視為已報廢
    remove_LICSNO_list = df_web_query.filter(expr("length(EGNO)>8")).repartition("EGNO")
    df_SSHSCHISTORY = substr_last_char_newColumn(df_SSHSCHISTORY, 'ENGINENO', 9, 'ENGINENO_temp')

    #   有五萬多 ENGINENO_temp 是空白因此找不到
    # 去除ENGINENO_temp有在remove_LICSNO_list清單裡的名單
    df_SSHSCHISTORY = df_SSHSCHISTORY.repartition("ENGINENO_temp").join(remove_LICSNO_list, df_SSHSCHISTORY.ENGINENO_temp == remove_LICSNO_list.EGNO,
                                           "leftanti").persist(StorageLevel.DISK_ONLY)

    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn("is_scrapped", lit(1))

    # 用車身號碼 引擎號碼 後面10碼來比對 CRCAMF
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'ENGINENO', 10)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'BODYNO', 10)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'EGNOM', 10)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'BDNOM', 10)

    # 報廢資料 限定超過9碼
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'ENGINENO', 9)
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'BODYNO', 9)
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'EGNOM', 9)
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'BDNOM', 9)
    write_Log(Log_File, "ok\n")

    list_HIST = ['BODYNO', 'BDNOM']
    list_CRCAMF = ['BDNO']

    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "CARNM_M")
    # 將BDNO_BODYNO與BDNO_BDNOM比對結果append
    df_result_csv = None
    # 交叉比對 也比對車名
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            # print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF.repartition(indexCRCAMF, "CARNM_M") \
                .join(df_SSHSCHISTORY.where((df_SSHSCHISTORY[indexHIST] != '') | (df_SSHSCHISTORY[indexHIST].isNull()))
                      .select(indexHIST, 'GRPNM', 'is_scrapped').dropDuplicates([indexHIST]).repartition(indexHIST,"GRPNM"),
                      (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                              df_CRCAMF[indexCRCAMF] == df_SSHSCHISTORY[indexHIST]), "left").persist(
                StorageLevel.DISK_ONLY)
            merged = merged.withColumn('is_scrapped_temp', fillna_INT_UDF(merged.is_scrapped))
            if df_result_csv is None:
                df_result_csv = merged
            else:
                df_result_csv = df_result_csv.union(merged)
    # 加回已知在20150708報廢的車輛 網站上查詢到 在crcamf比對到的 from  0001_web_query_minipulation.ipynb
    # 留下 BDNO_BODYNO 與 BDNO_BDNOM 是1的資料
    df_result_csv = df_result_csv.filter(df_result_csv.is_scrapped_temp == 1) \
        .select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN") \
        .union(df_CRCAMF_web_query20150708).persist(StorageLevel.DISK_ONLY)

    df_result_csv.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv")