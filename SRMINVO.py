import datetime
from Utils import write_Log
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType, StringType, FloatType
from pyspark.sql.window import Window


#SRMINVO 發票金額分析   BY VIN   因為歷史資料或15年以上車輛 在發票上沒有登記 車號
# def getSRMINVO_query_data(query_colNM):
'''
def getSRMINVO_query_data(spark):
    # SRMIVSLP發票工單對照檔  SRMSLPH工單主檔 SRMINVO發票主檔
    df_temp1 = spark.read.format("org.apache.spark.sql.cassandra")\
        .options(table="srmivslp", keyspace="cdp").load()\
        .select("DLRCD", "BRNHCD", "WORKNO", "INVONO")

    df_temp2 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srmslph", keyspace="cdp").load() \
        .filter((col('VIN') != '') & (col('VIN').isNotNull()) & (col('CMPTDT').isNotNull())) \
        .select("VIN", "DLRCD", "BRNHCD", "WORKNO")

    df_temp3 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srminvo", keyspace="cdp").load()\
        .select("INVSTS", "DETRMK", "INVONO", "INVODT", "DLRCD", "BRNHCD", "INVTXCD", "TOTAMT", "INSURCD", "IRNAMT", "WSHAMT")

    # 過濾 INVSTS = B and INVSTS = C and INVSTS = D and INVSTS = E and INVSTS = *
    df_temp3 = df_temp3.filter((df_temp3.INVSTS != 'B') & (df_temp3.INVSTS != 'C') & (df_temp3.INVSTS != 'D') & (df_temp3.INVSTS != 'E') & (df_temp3.INVSTS != '*'))

    df_SRMINVO = df_temp1.join(df_temp2, ['DLRCD', 'BRNHCD', 'WORKNO'], "left")
    df_SRMINVO = df_SRMINVO.join(df_temp3, ['DLRCD', 'BRNHCD', 'INVONO'], "left")
    df_SRMINVO = df_SRMINVO.select('VIN','INVODT', 'DLRCD', 'BRNHCD', 'INVONO', 'INVTXCD', 'TOTAMT', 'INSURCD', 'IRNAMT', 'WSHAMT')

    df_temp1 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srhivslp", keyspace="cdp").load() \
        .select("DLRCD", "BRNHCD", "WORKNO", "INVONO")

    df_temp2 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srhslph", keyspace="cdp").load() \
        .filter((col('VIN') != '') & (col('VIN').isNotNull()) & (col('CMPTDT').isNotNull())) \
        .select("VIN", "DLRCD", "BRNHCD", "WORKNO")

    df_temp3 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srhinvo", keyspace="cdp").load() \
        .select("INVSTS", "DETRMK", "INVONO", "INVODT", "DLRCD", "BRNHCD", "INVTXCD", "TOTAMT", "INSURCD", "IRNAMT",
                "WSHAMT")

    # 過濾 INVSTS = B and INVSTS = C and INVSTS = D and INVSTS = E and INVSTS = *
    df_temp3 = df_temp3.filter(
        (df_temp3.INVSTS != 'B') & (df_temp3.INVSTS != 'C') & (df_temp3.INVSTS != 'D') & (df_temp3.INVSTS != 'E') & (
                    df_temp3.INVSTS != '*'))

    df_SRHINVO = df_temp1.join(df_temp2, ['DLRCD', 'BRNHCD', 'WORKNO'], "left")
    df_SRHINVO = df_SRHINVO.join(df_temp3, ['DLRCD', 'BRNHCD', 'INVONO'], "left")
    df_SRHINVO = df_SRHINVO.select('VIN', 'INVODT', 'DLRCD', 'BRNHCD', 'INVONO', 'INVTXCD', 'TOTAMT', 'INSURCD',
                                   'IRNAMT', 'WSHAMT')

    df_temp1 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srhivslp15", keyspace="cdp").load() \
        .select("DLRCD", "BRNHCD", "WORKNO", "INVONO")

    df_temp2 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srhslph15", keyspace="cdp").load() \
        .filter((col('VIN') != '') & (col('VIN').isNotNull()) & (col('CMPTDT').isNotNull())) \
        .select("VIN", "DLRCD", "BRNHCD", "WORKNO")

    df_temp3 = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="srhinvo15", keyspace="cdp").load() \
        .select("INVSTS", "DETRMK", "INVONO", "INVODT", "DLRCD", "BRNHCD", "INVTXCD", "TOTAMT", "INSURCD", "IRNAMT",
                "WSHAMT")

    # 過濾 INVSTS = B and INVSTS = C and INVSTS = D and INVSTS = E and INVSTS = *
    df_temp3 = df_temp3.filter(
        (df_temp3.INVSTS != 'B') & (df_temp3.INVSTS != 'C') & (df_temp3.INVSTS != 'D') & (df_temp3.INVSTS != 'E') & (
                    df_temp3.INVSTS != '*'))
    df_SRHINVO15 = df_temp1.join(df_temp2, ['DLRCD', 'BRNHCD', 'WORKNO'], "left")
    df_SRHINVO15 = df_SRHINVO15.join(df_temp3, ['DLRCD', 'BRNHCD', 'INVONO'], "left")
    df_SRHINVO15 = df_SRHINVO15.select('VIN', 'INVODT', 'DLRCD', 'BRNHCD', 'INVONO', 'INVTXCD', 'TOTAMT', 'INSURCD', 'IRNAMT', 'WSHAMT')

    df_allSRMINVO = df_SRMINVO.union(df_SRHINVO).union(df_SRHINVO15)

    return df_allSRMINVO
'''

def Feature_SRMINVO(Log_File, END_DATE, DATE_3y):
    write_Log(Log_File, "21. %s | Features construction from cdp.SRMINVO......" % str(datetime.datetime.now()))

    spark = SparkSession.builder.appName("21.Features-construction-from-cdp.SRMINVO") \
        .master("local") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.3.2") \
        .getOrCreate()

    # start
    FIX_DATE = datetime.datetime.strptime('1988-01-01', "%Y-%m-%d")
    END_DATE_TIME = datetime.datetime.strptime(END_DATE, "%Y-%m-%d")
    DATE_3y_TIME = datetime.datetime.strptime(DATE_3y, "%Y-%m-%d")
    # TODO
    df_allSRMINVO = spark.read.parquet("C:/Users/foxconn/Desktop/test.parquet")

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
    # TODO
    ##save csv
    result_SRMINVO.coalesce(1).write.option('header', 'true').csv("C:/Users/foxconn/Desktop/Feature_SRMINVO.csv")

    df_allSRMINVO = None
    write_Log(Log_File, "ok\n")

    spark.stop()