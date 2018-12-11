import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from Constant import *
from SparkUDF import *
from Selected_Licsno2_1 import *

def selected_licsno_code2(spark, Log_File):

    # Code 2: 05CRCAMF_car_selection(6-9)
    ## CRCAMF 篩選出所有和泰車
    write_Log(Log_File, "06. %s | HT's cars selection......" % str(datetime.datetime.now()))

    df_CRCAMF = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_remove_LICSNO.csv")

    # 去除CARNM出現次數過少者'
    # 將欄位CARNM去空白
    df_CRCAMF = strip_string(df_CRCAMF, 'CARNM')
    brand_list = df_CRCAMF.groupBy('CARNM').count()
    #TODO 1要改成123, 寫1是為了測試使用
    brand_list = brand_list.filter(brand_list['count'] > 1)
    df_CRCAMF = df_CRCAMF.join(brand_list, df_CRCAMF.CARNM == brand_list.CARNM,
                                           "leftanti")
    # 去除車輛狀態不是為 更改為 不得為 2過戶3失竊5報廢'
    df_CRCAMF = df_CRCAMF.filter((df_CRCAMF.STSCD != '2') & (df_CRCAMF.STSCD != '3') & (df_CRCAMF.STSCD != '5'))

    # 為符合報廢車輛名稱 進行名稱轉換
    # 將EXSIOR, PREMIO, ALTIS取代成CORONA
    # dict_replacce_CARNM = {'EXSIOR': 'CORONA', 'PREMIO': 'CORONA', 'ALTIS': 'COROLLA'}
    df_CRCAMF = df_CRCAMF.withColumn('CARNM_M', replaceValue_UDF(col("CARNM")))
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "07. %s | Remove scrapped car from web query......" % str(datetime.datetime.now()))
    # print u'6. 扣除車牌(指定時間之前的才清除 避免網站查詢不同步)已在網站上查詢到, 後續會加回最近報廢的'
    '''
    df_web_query = spark.read.option('header', 'true').csv(Import_Data_Path + "df_web_query_fix.csv")
    df_web_query = strip_string(df_web_query, 'LICSNO')
    df_web_query = upper_string(df_web_query, 'LICSNO')
    df_web_query = df_web_query.withColumn('CARDATE_fix', transDatetime_UDF(array('CARDATE', lit(DATETIME_FORMAT2))))
    '''
    df_web_query = getWeb_query_filter(spark)
    # 指定時間之前的才清除 避免網站查詢不同步
    df_web_query = df_web_query.where(
        df_web_query['CARDATE_fix'] < datetime.datetime.strptime(END_DATE, DATETIME_FORMAT3))
    df_CRCAMF = df_CRCAMF.repartition("LICSNO_upper").join(df_web_query, df_CRCAMF.LICSNO_upper == df_web_query.LICSNO,
                               "leftanti")
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "08. %s | Remove stolen cars......" % str(datetime.datetime.now()))
    # print u'7. 扣除車牌"所有"已在網站上查詢到, 已經車牌失竊或車輛失竊'
    df_stolen = spark.read.option('header', 'true').csv(Import_Data_Path + "Stolen.csv")
    df_stolen = df_stolen.withColumnRenamed("車型", "type").withColumnRenamed("車牌", "LICSNO").withColumnRenamed("失車查詢結果", "result")\
        .withColumnRenamed("查詢時間", "date")
    # 將欄位Licsno去空白且轉大寫
    df_stolen = strip_string(df_stolen, 'LICSNO')
    df_stolen = upper_string(df_stolen, 'LICSNO')
    df_stolen = df_stolen.filter(df_stolen['result'] != u'查無資料')
    df_CRCAMF = df_CRCAMF.join(df_stolen, df_CRCAMF.LICSNO_upper == df_stolen.LICSNO,
                               "leftanti").persist(StorageLevel.DISK_ONLY)
    write_Log(Log_File, "ok\n")

    df_CRCAMF.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_2.csv")

    #selected_licsno_code2_1(spark)

    '''
    write_Log(Log_File, "09. %s | Compare with scrapped data from web query and df_SSHSCHISTORY......" % str(
        datetime.datetime.now()))
    df_SSHSCHISTORY = spark.read.option('header', 'true').csv(Temp_Path + "df_SSHSCHISTORY_clean.csv")

    # print u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除'
    
    #df_web_query = spark.read.option('header', 'true').csv(Import_Data_Path + "df_web_query_fix.csv")
    #df_web_query = df_web_query.withColumn('CARDATE_fix', transDatetime_UDF(array('CARDATE', lit(DATETIME_FORMAT2))))
    #df_web_query = df_web_query.where(df_web_query['CARDATE_fix'] < datetime.datetime(2015, 7, 8, 0, 0, 0, 0))
    
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
    
    #df_CRCAMF = df_CRCAMF.filter((df_CRCAMF['BDNO_ENGINENO']=='0')&(df_CRCAMF['EGNO_ENGINENO']=='0')&
    #    (df_CRCAMF['VIN_ENGINENO']=='0')&(df_CRCAMF['BDNO_BODYNO']=='0')&
    #    (df_CRCAMF['EGNO_BODYNO']=='0')&(df_CRCAMF['VIN_BODYNO']=='0')&
    #    (df_CRCAMF['BDNO_EGNOM']=='0')&(df_CRCAMF['EGNO_EGNOM']=='0')&
    #    (df_CRCAMF['VIN_EGNOM']=='0')&(df_CRCAMF['BDNO_BDNOM']=='0')&
    #    (df_CRCAMF['EGNO_BDNOM']=='0')&(df_CRCAMF['VIN_BDNOM']=='0')&
    #    (df_CRCAMF['EGNO_6_ENGINENO_6']=='0')&(df_CRCAMF['BDNO_6_BODYNO_6']=='0'))\
    #    .select("LICSNO", "CARNM", "CARNM_M", "CARMDL", "BDNO", "EGNO", "VIN", "BDNO_6", "EGNO_6", "UCDELIVIDT")
    
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
    
    #for indexHIST in list_HIST:
    #    for indexCRCAMF in list_CRCAMF:
    #        # print (indexHIST, indexCRCAMF)
    #        merged = None
    #        merged = df_CRCAMF_NOTused.select('BDNO', 'EGNO', 'VIN', 'CARNM_M') \
    #            .join(df_SSHUCHISTORY.where(df_SSHUCHISTORY[indexHIST] != '')
    #                  .select(indexHIST, 'GRPNM', 'is_scrapped').dropDuplicates([indexHIST]),
    #                  (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
    #                        df_CRCAMF_NOTused[indexCRCAMF] == df_SSHUCHISTORY[indexHIST]), "left")
    #        merged = merged.withColumnRenamed('is_scrapped', 'is_scrapped_temp')
    #        merged = merged.withColumn(indexCRCAMF + '_' + indexHIST, fillna_INT_UDF(merged.is_scrapped_temp))
    #        df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(merged.select('BDNO', indexCRCAMF + '_' + indexHIST), 'BDNO', "left")
    
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
    
    #df_CRCAMF_NOTused = df_CRCAMF_NOTused.filter((df_CRCAMF_NOTused['BDNO_ENGINENO'] == '0') & (df_CRCAMF_NOTused['EGNO_ENGINENO'] == '0') & (
    #                df_CRCAMF_NOTused['VIN_ENGINENO'] == '0') & (df_CRCAMF_NOTused['BDNO_BODYNO'] == '0') & (
    #                df_CRCAMF_NOTused['EGNO_BODYNO'] == '0') & (df_CRCAMF_NOTused['VIN_BODYNO'] == '0') & (
    #                df_CRCAMF_NOTused['BDNO_EGNOM'] == '0') & (df_CRCAMF_NOTused['EGNO_EGNOM'] == '0') & (
    #                df_CRCAMF_NOTused['VIN_EGNOM'] == '0') & (df_CRCAMF_NOTused['BDNO_BDNOM'] == '0') & (
    #                df_CRCAMF_NOTused['EGNO_BDNOM'] == '0') & (df_CRCAMF_NOTused['VIN_BDNOM'] == '0') & (
    #                df_CRCAMF_NOTused['EGNO_6_ENGINENO_6'] == '0') & (df_CRCAMF_NOTused['BDNO_6_BODYNO_6'] == '0'))\
    #                .select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT")
    
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT")
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF = df_CRCAMF_NOTused.union(df_CRCAMF_used)
    df_CRCAMF.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_union.csv")

    #selected_licsno_code3(spark)

    
     # 找出 最近三年內有回廠的15年以上車
    write_Log(Log_File, "13. %s | Look out the repaired cars over 15 years old in the past 3 years......" % str(
        datetime.datetime.now()))
    df_CRCAMF_3year = getCRCAMF_3year(spark)
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "14. %s | Remove unused data......" % str(datetime.datetime.now()))

    remove_LICSNO_list = None

    # 去除車輛狀態不是為 更改為 不得為 2過戶3失竊5報廢'
    df_CRCAMF_3year = df_CRCAMF_3year.filter(
        (df_CRCAMF_3year.STSCD != '2') & (df_CRCAMF_3year.STSCD != '3') & (df_CRCAMF_3year.STSCD != '5'))

    # 扣除車牌"所有"已在網站上查詢到, 後續會加回最近報廢的'
    
    #df_web_query = spark.read.option('header', 'true').csv(Import_Data_Path + "df_web_query_fix.csv")
    #df_web_query = strip_string(df_web_query, 'LICSNO')
    #df_web_query = upper_string(df_web_query, 'LICSNO')
    #df_web_query = df_web_query.withColumn('CARDATE_fix', transDatetime_UDF(array('CARDATE', lit(DATETIME_FORMAT2))))
    
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
    df_stolen = df_stolen.filter(df_stolen['result'] != u'查無資料')
    df_CRCAMF_3year = df_CRCAMF_3year.join(df_stolen, df_CRCAMF_3year.LICSNO == df_stolen.LICSNO,
                                               "leftanti")
    write_Log(Log_File, "ok\n")

    df_CRCAMF_3year = df_CRCAMF_3year.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN")
    # print '清除報廢後 再加上在20150708 之後報廢的車輛 記得要改路徑'

    df_scrapped_20150708 = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv")
    df_CRCAMF = df_CRCAMF.union(df_scrapped_20150708)
    # print ' 再加上三年內有回廠 車齡15年以上的車輛 '
    df_CRCAMF = df_CRCAMF.union(df_CRCAMF_3year)
    df_CRCAMF = strip_string(df_CRCAMF, 'LICSNO')
    df_CRCAMF = df_CRCAMF.dropDuplicates('LICSNO')
    df_CRCAMF.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF.csv")

    df_CRAURF = getCRAURF(spark)

    # 資料清整
    df_CRCAMF = df_CRCAMF.withColumn("qualified", '1') # df.loc[rows, col] = 'value'

    # 清除前後空白
    df_CRCAMF = strip_string(df_CRCAMF, 'LICSNO')
    df_CRCAMF = df_CRCAMF.dropDuplicates('LICSNO')
    df_CRAURF = strip_string(df_CRAURF, 'LICSNO')
    # df_CRAURF 五種人 所以本來就會重複

    df_CRAURF = df_CRAURF.filter(df_CRAURF['LICSNO']!='')
    # matching for LICSNO with qualified car for all people
    # 找出符合條件的車輛下的所有人
    # df_CRCAMF 這邊LICSNO 尚未清整 因此 MERGE後 會變多
    # 已經DROP DUPLICATES
    df_CRAURF = df_CRAURF.join(df_CRCAMF.select("LICSNO", "qualified"), "LICSNO", "left").persist(StorageLevel.DISK_ONLY)
    # print '留下qualified')
    df_CRAURF = df_CRAURF.filter(df_CRAURF['qualified']=='1').select("CUSTID", "qualified")

    # print '清掉前後空白'
    df_CRAURF = df_CRAURF.filter(df_CRAURF['CUSTID'].isNotNull())
    df_CRAURF = strip_string(df_CRAURF, 'CUSTID')

    # print '身分證空的清掉'
    df_CRAURF = df_CRAURF.filter(df_CRAURF['CUSTID'] != '')

    # print '身分證一樣的清掉'
    df_CRAURF = df_CRAURF.dropDuplicates("CUSTID")

    # RE-DOWNLOAD df_CRAURF FOR MATCHING CUSTID
    # 再由找到的所有人 去找出所有車輛
    df_CRAURF_all = getCRAURF(spark)

    # 清掉前後空白
    df_CRAURF_all = df_CRAURF_all.filter(df_CRAURF_all['CUSTID'].isNotNull())
    df_CRAURF = df_CRAURF[df_CRAURF['CUSTID'].isNotNull()]
    df_CRAURF_all = strip_string(df_CRAURF_all, 'CUSTID')
    df_CRAURF = strip_string(df_CRAURF, 'CUSTID')
    df_CRAURF_all = df_CRAURF_all.filter(df_CRAURF_all['CUSTID']!='')
    df_CRAURF = df_CRAURF.filter(df_CRAURF['CUSTID'] != '')
    df_CRAURF_all = df_CRAURF_all.join(df_CRAURF, "CUSTID", "left").persist(StorageLevel.DISK_ONLY)
    df_CRAURF_all = strip_string(df_CRAURF_all, 'LICSNO') #車牌清掉空白後 會有一筆重複車牌
    df_CRAURF_all.filter(df_CRAURF_all.qualified == '1').dropDuplicates("LICSNO").write.option('header', 'true').csv(Temp_Path + "df_CRAUR.csv")
    del df_CRAURF_all
    write_Log(Log_File, "ok\n")
    '''