import datetime
from Utils_Function import *
from pyspark.sql.functions import *
from pyspark.storagelevel import *
from Constant import *
from SparkUDF import *

def selected_licsno_code1_1(spark):

    df_SSHSCHISTORY = spark.read.option('header', 'true').csv(Temp_Path + "df_SSHSCHISTORY.csv")
    df_CRCAMF = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF.csv")
    df_CRCAMF_web_query20150708 = spark.read.option('header', 'true').csv(Temp_Path + "df_CRCAMF_web_query20150708.csv")


    list_HIST = ['BODYNO', 'BDNOM']
    list_CRCAMF = ['BDNO']
    '''
    # 交叉比對 也比對車名
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            # print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF.select('BDNO', 'EGNO', 'VIN', 'CARNM_M').repartition(indexCRCAMF, "CARNM_M") \
                .join(df_SSHSCHISTORY.where(df_SSHSCHISTORY[indexHIST] != '')
                      .select(indexHIST, 'GRPNM', 'is_scrapped').dropDuplicates([indexHIST]).repartition(indexHIST,
                                                                                                         "GRPNM"),
                      (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                                  df_CRCAMF[indexCRCAMF] == df_SSHSCHISTORY[indexHIST]), "left").persist(
                StorageLevel.DISK_ONLY)
            merged = merged.withColumnRenamed('is_scrapped', 'is_scrapped_temp')
            merged = merged.withColumn(indexCRCAMF + '_' + indexHIST, fillna_INT_UDF(merged.is_scrapped_temp))
            # 透過BDNO進行join
            df_CRCAMF = df_CRCAMF.repartition(indexCRCAMF)\
                .join(merged.select(indexCRCAMF, indexCRCAMF + '_' + indexHIST).repartition(indexCRCAMF),
                                       indexCRCAMF, "left").persist(StorageLevel.DISK_ONLY)
                                       
    # 加回已知在20150708報廢的車輛 網站上查詢到 在crcamf比對到的 from  0001_web_query_minipulation.ipynb
    # 留下 BDNO_BODYNO 與 BDNO_BDNOM 是1的資料
    df_result_csv = df_CRCAMF.filter((df_CRCAMF.BDNO_BODYNO == 1) | (df_CRCAMF.BDNO_BDNOM == 1)) \
        .select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN")
    df_result_csv = df_result_csv.union(df_CRCAMF_web_query20150708).persist(StorageLevel.DISK_ONLY)

    df_result_csv.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv")
            
    '''
    df_CRCAMF = df_CRCAMF.select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN", "CARNM_M")
    #將BDNO_BODYNO與BDNO_BDNOM比對結果append
    df_result_csv = None
    # 交叉比對 也比對車名
    for indexHIST in list_HIST:
        for indexCRCAMF in list_CRCAMF:
            # print (indexHIST, indexCRCAMF)
            merged = None
            merged = df_CRCAMF.repartition(indexCRCAMF, "CARNM_M") \
                .join(df_SSHSCHISTORY.where(df_SSHSCHISTORY[indexHIST] != '')
                      .select(indexHIST, 'GRPNM', 'is_scrapped').dropDuplicates([indexHIST]).repartition(indexHIST,
                                                                                                         "GRPNM"),
                      (df_CRCAMF['CARNM_M'] == df_SSHSCHISTORY['GRPNM']) & (
                              df_CRCAMF[indexCRCAMF] == df_SSHSCHISTORY[indexHIST]), "left").persist(
                StorageLevel.DISK_ONLY)
            merged = merged.withColumn('is_scrapped_temp', fillna_INT_UDF(merged.is_scrapped))
            merged.printSchema()
            merged.show()
            if df_result_csv is None:
                df_result_csv = merged
            else:
                df_result_csv = df_result_csv.union(merged)
    df_CRCAMF_web_query20150708.show()
    # 加回已知在20150708報廢的車輛 網站上查詢到 在crcamf比對到的 from  0001_web_query_minipulation.ipynb
    # 留下 BDNO_BODYNO 與 BDNO_BDNOM 是1的資料
    df_result_csv = df_result_csv.filter(df_result_csv.is_scrapped_temp == 1)\
        .select("LICSNO", "CARNM", "CARMDL", "BDNO", "EGNO", "VIN")\
        .union(df_CRCAMF_web_query20150708).persist(StorageLevel.DISK_ONLY)

    df_result_csv.write.option('header', 'true').csv(Temp_Path + "df_CRCAMF_scrapped_after20150708.csv")
