import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from Constant import *
from SparkUDF import *
from Selected_Licsno2_1 import *


def selected_licsno_code2(spark):

    # Code 2: 05CRCAMF_car_selection(6-9)
    ## CRCAMF 篩選出所有和泰車
    write_Log(Log_File, "06. %s | HT's cars selection......" % str(datetime.datetime.now()))

    df_CRCAMF = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_remove_LICSNO.csv")

    # 去除車輛狀態不是為 更改為 不得為 2過戶3失竊5報廢'
    df_CRCAMF = df_CRCAMF.filter(((df_CRCAMF.STSCD != '2') & (df_CRCAMF.STSCD != '3') & (df_CRCAMF.STSCD != '5')) | df_CRCAMF.STSCD.isNull())
    print(df_CRCAMF.count())
    # 去除CARNM出現次數過少者'
    # 將欄位CARNM去空白
    df_CRCAMF = strip_string(df_CRCAMF, 'CARNM')
    brand_list = df_CRCAMF.groupBy('CARNM').count()

    #TODO 0要改成123, 寫0是為了測試使用
    brand_list = brand_list.filter(brand_list['count'] > 123)
    df_CRCAMF = df_CRCAMF.join(brand_list, "CARNM", "inner")
    print(df_CRCAMF.count())
    # 為符合報廢車輛名稱 進行名稱轉換
    # 將EXSIOR, PREMIO, ALTIS取代成CORONA
    # dict_replacce_CARNM = {'EXSIOR': 'CORONA', 'PREMIO': 'CORONA', 'ALTIS': 'COROLLA'}
    df_CRCAMF = df_CRCAMF.withColumn('CARNM_M', replaceValue_UDF(col("CARNM")))
    write_Log(Log_File, "ok\n")

    write_Log(Log_File, "07. %s | Remove scrapped car from web query......" % str(datetime.datetime.now()))
    # print u'6. 扣除車牌(指定時間之前的才清除 避免網站查詢不同步)已在網站上查詢到, 後續會加回最近報廢的'
    df_web_query = getWeb_query_filter(spark)
    # 指定時間之前的才清除 避免網站查詢不同步
    df_web_query = df_web_query.where(
        df_web_query['CARDATE_fix'] < datetime.datetime.strptime(END_DATE, DATETIME_FORMAT3))
    print(df_web_query.count())
    df_CRCAMF = df_CRCAMF.repartition("LICSNO_upper").join(df_web_query, df_CRCAMF.LICSNO_upper == df_web_query.LICSNO,
                               "leftanti")
    write_Log(Log_File, "ok\n")
    print(df_CRCAMF.count())
    write_Log(Log_File, "08. %s | Remove stolen cars......" % str(datetime.datetime.now()))
    # print u'7. 扣除車牌"所有"已在網站上查詢到, 已經車牌失竊或車輛失竊'
    df_stolen = spark.read.option('header', 'true').csv(Import_Data_Path + "Stolen.csv")
    df_stolen = df_stolen.withColumnRenamed("車型", "type").withColumnRenamed("車牌", "LICSNO").withColumnRenamed("失車查詢結果", "result")\
        .withColumnRenamed("查詢時間", "date")

    # 將欄位Licsno去空白且轉大寫
    df_stolen = strip_string(df_stolen, 'LICSNO')
    df_stolen = upper_string(df_stolen, 'LICSNO')
    df_stolen = df_stolen.filter((df_stolen['result'] != u'查無資料') | (df_stolen.result.isNull()))
    print(df_stolen.count())
    df_CRCAMF = df_CRCAMF.join(df_stolen, df_CRCAMF.LICSNO_upper == df_stolen.LICSNO,
                               "leftanti").persist(StorageLevel.DISK_ONLY)
    write_Log(Log_File, "ok\n")
    print(df_CRCAMF.count())
    write_Log(Log_File, "09. %s | Compare with scrapped data from web query and df_SSHSCHISTORY......" % str(
        datetime.datetime.now()))

    df_SSHSCHISTORY = SQL_df_SSHSCHISTORY(spark)
    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn('ISSUE_fix', transDatetime_UDF(array('ISSUE', lit(DATETIME_FORMAT1))))

    # ISSUE_fix 大於 1988/1/1號 且 ISSUE_fix 小於等於 15年前當月的1號
    df_SSHSCHISTORY = df_SSHSCHISTORY.where(
        (df_SSHSCHISTORY.ISSUE_fix <= datetime.datetime(today.year - Candidate_Car_age, today.month,
                                                        1, 0, 0, 0, 0)) & (
                    df_SSHSCHISTORY.ISSUE_fix >= datetime.datetime(1988, 1, 1, 0, 0, 0, 0)))
    print(df_SSHSCHISTORY.count())
    # print "df_SSHSCHISTORY 2015-07-08 之後報廢的需要移除 因為後續會加回  只需清除之前報廢的"
    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn('MODDT_fix', transDatetime_UDF(array('MODDT', lit(DATETIME_FORMAT1))))
    df_SSHSCHISTORY = df_SSHSCHISTORY.where(df_SSHSCHISTORY.MODDT_fix < datetime.datetime(2015, 7, 8, 0, 0, 0, 0))

    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'ENGINENO')
    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'EGNOM')
    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'BODYNO')
    df_SSHSCHISTORY = series_str_cleaner(df_SSHSCHISTORY, 'BDNOM')
    df_SSHSCHISTORY = strip_string(df_SSHSCHISTORY, 'GRPNM')

    df_SSHSCHISTORY = exist_value_replacement(df_SSHSCHISTORY, 'EGNOM', 'ENGINENO')
    df_SSHSCHISTORY = exist_value_replacement(df_SSHSCHISTORY, 'BDNOM', 'BODYNO')

    df_SSHSCHISTORY = df_SSHSCHISTORY.withColumn("is_scrapped", lit(1))
    print(df_SSHSCHISTORY.count())

    # print u'6. 扣除車牌 7/8之前 已在網站上查詢到   已由車牌進行排除'
    df_web_query = getWeb_query_filter(spark)
    df_web_query = df_web_query.filter(df_web_query['CARDATE_fix'] < datetime.datetime(2015, 7, 8, 0, 0, 0, 0))
    df_web_query = series_str_cleaner(df_web_query, 'EGNO')
    # 五碼以下不清
    print(df_web_query.count())
    remove_LICSNO_list = df_web_query.filter(expr("length(EGNO)>5"))
    print(remove_LICSNO_list.count())
    df_SSHSCHISTORY = df_SSHSCHISTORY.join(remove_LICSNO_list, df_SSHSCHISTORY.ENGINENO == remove_LICSNO_list.EGNO,
                                           "leftanti")
    write_Log(Log_File, "ok\n")
    print(df_SSHSCHISTORY.count())
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
    write_Log(Log_File, "ok\n")

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
            df_CRCAMF = df_CRCAMF.repartition(indexCRCAMF, "CARNM_M").join(
                df_SSHSCHISTORY.where((df_SSHSCHISTORY[indexHIST] != '') | (df_SSHSCHISTORY[indexHIST].isNull()))
                .select(indexHIST, 'GRPNM', 'is_scrapped').repartition(indexHIST, "GRPNM").dropDuplicates([indexHIST]),
                (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                        df_CRCAMF[indexCRCAMF] == df_SSHSCHISTORY[indexHIST]), "leftanti").persist(
                StorageLevel.DISK_ONLY)
    # 這邊需要確認 去留數量???
    print(df_CRCAMF.count())
    # 九碼以上的移除 因為已在先前比對過
    df_CRCAMF = df_CRCAMF.withColumn('BDNO_6', lengthReplacementUnder_UDF(array('BDNO', lit(9))))
    df_CRCAMF = df_CRCAMF.withColumn('EGNO_6', lengthReplacementUnder_UDF(array('EGNO', lit(9))))
    # df_CRCAMF = df_CRCAMF.withColumn('VIN_6', lengthReplacementUnder_UDF(array('VIN', lit(9))))  #skip VIN

    df_CRCAMF = substr_last_char(df_CRCAMF, 'BDNO_6', 6)
    df_CRCAMF = substr_last_char(df_CRCAMF, 'EGNO_6', 6)
    # df_CRCAMF = substr_last_char(df_CRCAMF, 'VIN_6', 6) #skip VIN

    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARNM_M", "CARMDL", "BDNO", "EGNO", "VIN", "BDNO_6", "EGNO_6",
                                 "UCDELIVIDT")
    # 只選六碼時 限定 BD VS BD  , EG VS EG 避免 因為碼數較小誤判
    # 任一條件皆刪除
    df_CRCAMF = df_CRCAMF.repartition("EGNO_6", "CARNM_M").join(
        df_SSHSCHISTORY.filter((df_SSHSCHISTORY['ENGINENO_6'] != '') | (df_SSHSCHISTORY.ENGINENO_6.isNull()))
        .select("ENGINENO_6", "GRPNM", "is_scrapped").dropDuplicates(["ENGINENO_6", "GRPNM"])
            .repartition("ENGINENO_6", "GRPNM"),
        (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (df_CRCAMF['EGNO_6'] == df_SSHSCHISTORY['ENGINENO_6']),
        "leftanti") \
        .persist(StorageLevel.DISK_ONLY)
    print(df_CRCAMF.count())
    df_CRCAMF = df_CRCAMF.repartition("BDNO_6", "CARNM_M").join(
        df_SSHSCHISTORY.filter((df_SSHSCHISTORY['BODYNO_6'] != '') | (df_SSHSCHISTORY.BODYNO_6.isNull())).select("BODYNO_6", "GRPNM", "is_scrapped")
        .dropDuplicates(["BODYNO_6", "GRPNM"]).repartition("BODYNO_6", "GRPNM"),
        (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                df_CRCAMF['BDNO_6'] == df_SSHSCHISTORY['BODYNO_6']), "leftanti").persist(StorageLevel.DISK_ONLY)
    print(df_CRCAMF.count())
    ##重複者一併刪除 數量約在3000以下
    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT", "CARNM_M", "EGNO_6", "BDNO_6")
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF_used = df_CRCAMF.filter(df_CRCAMF.UCDELIVIDT.isNotNull())
    df_CRCAMF_NOTused = df_CRCAMF.filter(df_CRCAMF.UCDELIVIDT.isNull())
    print(df_CRCAMF_used.count())
    print(df_CRCAMF_NOTused.count())
    # 如果中古車的 VIN、EGNO、BDNO 在非中古車中出現，就將該非中古車移除（以 xxx_used 為主）
    remove_VIN_list = df_CRCAMF_used.filter(expr("length(VIN)>5"))
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(remove_VIN_list, "VIN",
                                               "leftanti")
    print(df_CRCAMF_NOTused.count())
    remove_EGNO_list = df_CRCAMF_used.filter(expr("length(EGNO)>5"))
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(remove_EGNO_list, "EGNO",
                                               "leftanti")
    print(df_CRCAMF_NOTused.count())
    remove_BDNO_list = df_CRCAMF_used.filter(expr("length(BDNO)>5"))
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.join(remove_BDNO_list, "BDNO",
                                               "leftanti")
    print(df_CRCAMF_NOTused.count())

    df_CRCAMF_NOTused.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_NOTused.csv")
    df_CRCAMF_used.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_used.csv")
    write_Log(Log_File, "ok\n")

    df_SSHSCHISTORY = None

    #selected_licsno_code2_1(spark)
