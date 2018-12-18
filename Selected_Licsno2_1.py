import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from Constant import *
from SparkUDF import *
from Selected_Licsno2_2 import *

def selected_licsno_code2_1(spark):
    df_CRCAMF_NOTused = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_NOTused.csv")
    df_CRCAMF_used = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_used.csv")
    # 特別注意，SSHUCHISTORY是過戶歷史檔（SSHSCHISTORY是報廢歷史檔）
    # 過戶 車身號碼 引擎號碼 清理
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
    #df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn("is_scrapped", lit(1))

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

    df_CRCAMF_NOTused = df_CRCAMF_NOTused.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "UCDELIVIDT", "CARNM_M", "EGNO_6", "BDNO_6")
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            df_CRCAMF_NOTused = df_CRCAMF_NOTused.repartition(indexCRCAMF, "CARNM_M")\
                .join(df_SSHUCHISTORY.where((df_SSHUCHISTORY[indexHIST] != '') | (df_SSHUCHISTORY[indexHIST].isNull()))
                      .select(indexHIST, 'GRPNM').repartition(indexHIST, "GRPNM").dropDuplicates([indexHIST]),
                      (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                              df_CRCAMF_NOTused[indexCRCAMF] == df_SSHUCHISTORY[indexHIST]), "leftanti").persist(StorageLevel.DISK_ONLY)

    # 五碼以下不使用
    df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn('ENGINENO_6', lengthReplacementOver_UDF(array('ENGINENO', lit(5))))
    df_SSHUCHISTORY = df_SSHUCHISTORY.withColumn('BODYNO_6', lengthReplacementOver_UDF(array('BODYNO', lit(5))))
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'ENGINENO_6', 6)
    df_SSHUCHISTORY = substr_last_char(df_SSHUCHISTORY, 'BODYNO_6', 6)
    write_Log(Log_File, "ok\n")


    # 只選六碼時 限定 BD VS BD  , EG VS EG 避免 因為碼數較小誤判
    df_CRCAMF_NOTused = df_CRCAMF_NOTused.repartition("EGNO_6", "CARNM_M")\
        .join(df_SSHUCHISTORY.filter((df_SSHUCHISTORY['ENGINENO_6'] != '') | (df_SSHUCHISTORY['ENGINENO_6'].isNull())).select("ENGINENO_6", "GRPNM")
        .dropDuplicates(["ENGINENO_6", "GRPNM"]).repartition("ENGINENO_6", "GRPNM"),
              (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                      df_CRCAMF_NOTused['EGNO_6'] == df_SSHUCHISTORY['ENGINENO_6']), "leftanti").persist(StorageLevel.DISK_ONLY)

    df_CRCAMF_NOTused = df_CRCAMF_NOTused.repartition("BDNO_6", "CARNM_M")\
        .join(df_SSHUCHISTORY.filter((df_SSHUCHISTORY['BODYNO_6'] != '') | (df_SSHUCHISTORY['BODYNO_6'].isNull())).select("BODYNO_6", "GRPNM")
        .dropDuplicates(["BODYNO_6", "GRPNM"]).repartition("BODYNO_6", "GRPNM"),
            (df_CRCAMF_NOTused['CARNM_M'] == df_SSHUCHISTORY['GRPNM']) & (
                df_CRCAMF_NOTused['BDNO_6'] == df_SSHUCHISTORY['BODYNO_6']), "leftanti").persist(StorageLevel.DISK_ONLY)
    del df_SSHUCHISTORY

    df_CRCAMF_NOTused = df_CRCAMF_NOTused.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN")
    # 移除相關BDNO_ENGINENO 避免後續過戶資料重複

    df_CRCAMF = df_CRCAMF_NOTused.repartition("LICSNO").union(df_CRCAMF_used.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN").repartition("LICSNO")).persist(StorageLevel.DISK_ONLY)
    df_CRCAMF.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_union.csv")

    selected_licsno_code2_2(spark)