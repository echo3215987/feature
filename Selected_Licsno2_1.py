import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from Constant import *
from SparkUDF import *
from Selected_Licsno2_2 import *

def selected_licsno_code2_1(spark, Log_File):

    df_CRCAMF = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_2.csv")

    write_Log(Log_File, "09. %s | Compare with scrapped data from web query and df_SSHSCHISTORY......" % str(
        datetime.datetime.now()))
    df_SSHSCHISTORY = spark.read.option('header', 'true').csv(Temp_Path + "df_SSHSCHISTORY_clean.csv")

    # print u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除'
    '''
    df_web_query = spark.read.option('header', 'true').csv(Import_Data_Path + "df_web_query_fix.csv")
    df_web_query = df_web_query.withColumn('CARDATE_fix', transDatetime_UDF(array('CARDATE', lit(DATETIME_FORMAT2))))
    df_web_query = df_web_query.where(df_web_query['CARDATE_fix'] < datetime.datetime(2015, 7, 8, 0, 0, 0, 0))
    '''
    df_web_query = getWeb_query_filter(spark)

    df_web_query = series_str_cleaner(df_web_query, 'EGNO')
    # 五碼以下不清
    remove_LICSNO_list = df_web_query.filter(expr("length(EGNO)>5"))
    df_SSHSCHISTORY = df_SSHSCHISTORY.join(remove_LICSNO_list, df_SSHSCHISTORY.ENGINENO == remove_LICSNO_list.EGNO, "leftanti")
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "10. %s | Clean df_SSHSCHISTORY ......." % str(datetime.datetime.now()))

    # 清整比對報廢車輛 並 標示於CRCAMF, 取倒數9位
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'ENGINENO', 9)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'BODYNO', 9)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'EGNOM', 9)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'BDNOM', 9)

    # 報廢資料 限定8碼以上 六碼者 後面另行比對
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'ENGINENO', 8)
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'BODYNO', 8)
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'EGNOM', 8)
    df_SSHSCHISTORY = check_length_over_replacement(df_SSHSCHISTORY, 'BDNOM', 8)

    # 5碼以下的不使用
    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn('ENGINENO_6', lengthReplacementOver_UDF(array('ENGINENO', lit(5))))
    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn('BODYNO_6', lengthReplacementOver_UDF(array('BODYNO', lit(5))))
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'ENGINENO_6', 6)
    df_SSHSCHISTORY = substr_last_char(df_SSHSCHISTORY, 'BODYNO_6', 6)
    write_Log(Log_File,"ok\n")

    # data cleaning
    write_Log(Log_File, "11. %s | Clearn df_CRCAMF......" % str(datetime.datetime.now()))
    df_CRCAMF = series_str_cleaner(df_CRCAMF, 'BDNO')
    df_CRCAMF = series_str_cleaner(df_CRCAMF, 'EGNO')
    df_CRCAMF = series_str_cleaner(df_CRCAMF, 'VIN')

    # BDNO, EGNO, VIN取倒數十位
    df_CRCAMF = substr_last_char(df_CRCAMF, 'BDNO', 9)
    df_CRCAMF = substr_last_char(df_CRCAMF, 'EGNO', 9)
    df_CRCAMF = substr_last_char(df_CRCAMF, 'VIN', 9)

    list_HIST = ['ENGINENO', 'BODYNO', 'EGNOM', 'BDNOM']
    list_CRCAMF = ['BDNO', 'EGNO', 'VIN']

    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARNM_M", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT")
    # 交叉比對 也比對車名
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            df_CRCAMF = df_CRCAMF.repartition(indexCRCAMF, "CARNM_M").join(df_SSHSCHISTORY.where(df_SSHSCHISTORY[indexHIST] != '')
                      .select(indexHIST, 'GRPNM', 'is_scrapped').repartition(indexHIST, "GRPNM").dropDuplicates([indexHIST]),
                      (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                                  df_CRCAMF[indexCRCAMF] == df_SSHSCHISTORY[indexHIST]), "leftanti").persist(StorageLevel.DISK_ONLY)
    # 這邊需要確認 去留數量???

    # 九碼以上的移除 因為已在先前比對過
    df_CRCAMF = df_CRCAMF.withColumn('BDNO_6', lengthReplacementUnder_UDF(array('BDNO', lit(9))))
    df_CRCAMF = df_CRCAMF.withColumn('EGNO_6', lengthReplacementUnder_UDF(array('EGNO', lit(9))))
    #df_CRCAMF = df_CRCAMF.withColumn('VIN_6', lengthReplacementUnder_UDF(array('VIN', lit(9))))  #skip VIN

    df_CRCAMF = substr_last_char(df_CRCAMF, 'BDNO_6', 6)
    df_CRCAMF = substr_last_char(df_CRCAMF, 'EGNO_6', 6)
    #df_CRCAMF = substr_last_char(df_CRCAMF, 'VIN_6', 6) #skip VIN

    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARNM_M", "CARMDL", "BDNO", "EGNO", "VIN", "BDNO_6", "EGNO_6", "UCDELIVIDT")
    # 只選六碼時 限定 BD VS BD  , EG VS EG 避免 因為碼數較小誤判
    df_CRCAMF = df_CRCAMF.repartition("EGNO_6", "CARNM_M").join(df_SSHSCHISTORY.filter(df_SSHSCHISTORY['ENGINENO_6'] != '')
        .select("ENGINENO_6", "GRPNM", "is_scrapped").dropDuplicates(["ENGINENO_6", "GRPNM"]).repartition("ENGINENO_6", "GRPNM"),
        (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (df_CRCAMF['EGNO_6'] == df_SSHSCHISTORY['ENGINENO_6']), "leftanti")\
        .persist(StorageLevel.DISK_ONLY)

    df_CRCAMF = df_CRCAMF.repartition("BDNO_6", "CARNM_M").join(df_SSHSCHISTORY.filter(df_SSHSCHISTORY['BODYNO_6'] != '').select("BODYNO_6", "GRPNM","is_scrapped")
        .dropDuplicates(["BODYNO_6", "GRPNM"]).repartition("BODYNO_6", "GRPNM"),
              (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                          df_CRCAMF['BDNO_6'] == df_SSHSCHISTORY['BODYNO_6']), "leftanti").persist(StorageLevel.DISK_ONLY)

    ##重複者一併刪除 數量約在3000以下
    # 任一條件皆刪除
    '''
    df_CRCAMF = df_CRCAMF.filter((df_CRCAMF['BDNO_ENGINENO']=='0')&(df_CRCAMF['EGNO_ENGINENO']=='0')&
        (df_CRCAMF['VIN_ENGINENO']=='0')&(df_CRCAMF['BDNO_BODYNO']=='0')&
        (df_CRCAMF['EGNO_BODYNO']=='0')&(df_CRCAMF['VIN_BODYNO']=='0')&
        (df_CRCAMF['BDNO_EGNOM']=='0')&(df_CRCAMF['EGNO_EGNOM']=='0')&
        (df_CRCAMF['VIN_EGNOM']=='0')&(df_CRCAMF['BDNO_BDNOM']=='0')&
        (df_CRCAMF['EGNO_BDNOM']=='0')&(df_CRCAMF['VIN_BDNOM']=='0')&
        (df_CRCAMF['EGNO_6_ENGINENO_6']=='0')&(df_CRCAMF['BDNO_6_BODYNO_6']=='0'))\
        .select("LICSNO", "CARNM", "CARNM_M", "CARMDL", "BDNO", "EGNO", "VIN", "BDNO_6", "EGNO_6", "UCDELIVIDT")
    '''
    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARNM_M", "CARMDL", "BDNO", "EGNO", "VIN", "BDNO_6", "EGNO_6", "UCDELIVIDT")
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF_used = df_CRCAMF.filter(df_CRCAMF['UCDELIVIDT'].isNotNull())
    df_CRCAMF_used = df_CRCAMF_used.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT")
    df_CRCAMF_NOTused = df_CRCAMF.filter(df_CRCAMF['UCDELIVIDT'].isNull())

    # 如果中古車的 VIN、EGNO、BDNO 在非中古車中出現，就將該非中古車移除（以 xxx_used 為主）
    remove_VIN_list = df_CRCAMF_used.filter(expr("length(VIN)>5"))
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(remove_VIN_list, df_CRCAMF_NOTused.VIN == remove_VIN_list.VIN,
                               "leftanti")
    remove_EGNO_list = df_CRCAMF_used.filter(expr("length(EGNO)>5"))
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(remove_EGNO_list, df_CRCAMF_NOTused.EGNO == remove_VIN_list.EGNO,
                                               "leftanti")
    remove_BDNO_list = df_CRCAMF_used.filter(expr("length(BDNO)>5"))
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(remove_BDNO_list, df_CRCAMF_NOTused.BDNO == remove_VIN_list.BDNO,
                                               "leftanti")
    write_Log(Log_File, "ok\n")

    # 特別注意，SSHUCHISTORY是過戶歷史檔（SSHSCHISTORY是報廢歷史檔）
    # 過戶 車身號碼 引擎號碼 清理
    df_SSHSCHISTORY = None
    df_SSHUCHISTORY = SQL_df_SSHUCHISTORY(spark)  # df_SSHUCHISTORY 只執行 1 次，所以不用Original_ 跟 .copy()
    # print "有過戶的就移除 因為不確定新的車主資料"

    write_Log(Log_File, "12. %s | Clearn df_SSHUCHISTORY......" % str(datetime.datetime.now()))
    # print ('留下15年以上的車輛')
    df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn('ISSUE_fix', transDatetime_UDF(array('ISSUE', lit(DATETIME_FORMAT1))))
    df_SSHUCHISTORY = df_SSHUCHISTORY.where(
        (df_SSHUCHISTORY.ISSUE_fix <= datetime.datetime(today.year - Candidate_Car_age, today.month, 1, 0, 0, 0, 0)) &
        (df_SSHUCHISTORY.ISSUE_fix >= datetime.datetime(1988, 1, 1, 0, 0, 0, 0)))
    df_SSHUCHISTORY = series_str_cleaner(df_SSHUCHISTORY, 'ENGINENO')
    df_SSHUCHISTORY = series_str_cleaner(df_SSHUCHISTORY, 'EGNOM')
    df_SSHUCHISTORY = series_str_cleaner(df_SSHUCHISTORY, 'BODYNO')
    df_SSHUCHISTORY = series_str_cleaner(df_SSHUCHISTORY, 'BDNOM')

    df_SSHUCHISTORY = exist_value_replacement(df_SSHUCHISTORY, 'EGNOM', 'ENGINENO')
    df_SSHUCHISTORY = exist_value_replacement(df_SSHUCHISTORY, 'BDNOM', 'BODYNO')
    df_SSHUCHISTORY = strip_string(df_SSHUCHISTORY, 'GRPNM')
    df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn("is_scrapped", lit(1))

    # 清整比對過戶車輛 並 標示於CRCAMF
    # 因為 CRCAMF BDNO -9重複較多
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'ENGINENO', 9)
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'BODYNO', 9)
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'EGNOM', 9)
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'BDNOM', 9)

    # 過戶資料 限定8碼以上
    df_SSHUCHISTORY = check_length_over_replacement(df_SSHUCHISTORY, 'ENGINENO', 8)
    df_SSHUCHISTORY = check_length_over_replacement(df_SSHUCHISTORY, 'BODYNO', 8)
    df_SSHUCHISTORY = check_length_over_replacement(df_SSHUCHISTORY, 'EGNOM', 8)
    df_SSHUCHISTORY = check_length_over_replacement(df_SSHUCHISTORY, 'BDNOM', 8)
    #df_CRCAMF_NOTused = df_CRCAMF_NOTused.reset_index(drop=True)

    list_HIST = ['ENGINENO', 'BODYNO', 'EGNOM', 'BDNOM']
    list_CRCAMF = ['BDNO', 'EGNO', 'VIN']
    '''
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            # print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF_NOTused.select('BDNO', 'EGNO', 'VIN', 'CARNM_M') \
                .join(df_SSHUCHISTORY.where(df_SSHUCHISTORY[indexHIST] != '')
                      .select(indexHIST, 'GRPNM', 'is_scrapped').dropDuplicates([indexHIST]),
                      (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                            df_CRCAMF_NOTused[indexCRCAMF] == df_SSHUCHISTORY[indexHIST]), "left")
            merged = merged.withColumnRenamed('is_scrapped', 'is_scrapped_temp')
            merged = merged.withColumn(indexCRCAMF + '_' + indexHIST, fillna_INT_UDF(merged.is_scrapped_temp))
            df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(merged.select('BDNO', indexCRCAMF + '_' + indexHIST), 'BDNO', "left")
    '''
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.select('BDNO', 'EGNO', 'VIN', 'CARNM_M', 'EGNO_6', 'BDNO_6')
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            df_CRCAMF_NOTused = df_CRCAMF_NOTused.repartition(indexCRCAMF, "CARNM_M").join(df_SSHUCHISTORY.where(df_SSHUCHISTORY[indexHIST] != '')
                      .select(indexHIST, 'GRPNM', 'is_scrapped').repartition(indexHIST, "GRPNM").dropDuplicates([indexHIST]),
                      (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                              df_CRCAMF_NOTused[indexCRCAMF] == df_SSHUCHISTORY[indexHIST]), "leftanti").persist(StorageLevel.DISK_ONLY)

    # 五碼以下不使用
    df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn('ENGINENO_6', lengthReplacementOver_UDF(array('ENGINENO', lit(5))))
    df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn('BODYNO_6', lengthReplacementOver_UDF(array('BODYNO', lit(5))))
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'ENGINENO_6', 6)
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'BODYNO_6', 6)
    write_Log(Log_File, "ok\n")


    # 只選六碼時 限定 BD VS BD  , EG VS EG 避免 因為碼數較小誤判
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.repartition(EGNO_6, "CARNM_M")\
        .join(df_SSHUCHISTORY.filter(df_SSHUCHISTORY['ENGINENO_6'] != '').select("ENGINENO_6", "GRPNM","is_scrapped")
        .dropDuplicates(["ENGINENO_6", "GRPNM"]).repartition("ENGINENO_6", "GRPNM"),
              (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                      df_CRCAMF_NOTused['EGNO_6'] == df_SSHUCHISTORY['ENGINENO_6']), "leftanti").persist(StorageLevel.DISK_ONLY)

    df_CRCAMF_NOTused = df_CRCAMF_NOTused.repartition(BDNO_6, "CARNM_M")\
        .join(df_SSHUCHISTORY.filter(df_SSHUCHISTORY['BODYNO_6'] != '').select("BODYNO_6", "GRPNM","is_scrapped")
        .dropDuplicates(["BODYNO_6", "GRPNM"]).repartition("BODYNO_6", "GRPNM"),
            (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                df_CRCAMF_NOTused['BDNO_6'] == df_SSHUCHISTORY['BODYNO_6']), "leftanti").persist(StorageLevel.DISK_ONLY)
    del df_SSHUCHISTORY
    '''
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.filter((df_CRCAMF_NOTused['BDNO_ENGINENO'] == '0') & (df_CRCAMF_NOTused['EGNO_ENGINENO'] == '0') & (
                    df_CRCAMF_NOTused['VIN_ENGINENO'] == '0') & (df_CRCAMF_NOTused['BDNO_BODYNO'] == '0') & (
                    df_CRCAMF_NOTused['EGNO_BODYNO'] == '0') & (df_CRCAMF_NOTused['VIN_BODYNO'] == '0') & (
                    df_CRCAMF_NOTused['BDNO_EGNOM'] == '0') & (df_CRCAMF_NOTused['EGNO_EGNOM'] == '0') & (
                    df_CRCAMF_NOTused['VIN_EGNOM'] == '0') & (df_CRCAMF_NOTused['BDNO_BDNOM'] == '0') & (
                    df_CRCAMF_NOTused['EGNO_BDNOM'] == '0') & (df_CRCAMF_NOTused['VIN_BDNOM'] == '0') & (
                    df_CRCAMF_NOTused['EGNO_6_ENGINENO_6'] == '0') & (df_CRCAMF_NOTused['BDNO_6_BODYNO_6'] == '0'))\
                    .select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT")
    '''
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT")
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF = df_CRCAMF_NOTused.union(df_CRCAMF_used)
    df_CRCAMF.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_union.csv")

    #selected_licsno_code_2(spark)