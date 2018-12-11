import os
from pyspark.sql import SparkSession
from Utils_Function import *
from SRMINVO import *
from Selected_Licsno1 import selected_licsno_code1
from Selected_Licsno2 import selected_licsno_code2
from Constant import *

def main():
    '''
    if not os.path.isdir(Result_Path):
        os.makedirs(Result_Path)
    else:
        command = "rm -rf %s"
        command = command % Result_Path
        os.system(command)  # shutil.rmtree(Result_Path) by HD
        os.makedirs(Result_Path)

    if not os.path.isdir(Temp_Path):
        os.makedirs(Temp_Path)
    else:
        command = "rm -rf %s"
        command = command % Temp_Path
        os.system(command)  # 全部檔案連同目錄都砍掉	#shutil.rmtree(Temp_Path) by HD
        os.makedirs(Temp_Path)  # 重新建目錄給他
    '''

    if not os.path.isdir(Log_Path):
        os.makedirs(Log_Path)

    # spark init
    spark = SparkSession.builder.appName("HT_Final") \
        .master("local") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.3.2") \
        .config("spark.executor.memory", "6g") \
        .config("spark.debug.maxToStringFields", "100") \
        .getOrCreate()

    # Main Code
    # Code 1: 02CRCAMF_vs_SSHSCHISTORY_after20150708(2-5)
    selected_licsno_code1(spark, Log_File)

    # Code 2: 05CRCAMF_car_selection(6-14)
    #selected_licsno_code2(spark, Log_File)

    #21. %s | Features construction from cdp.SRMINVO......
    #Feature_SRMINVO(spark)

    spark.stop()

if __name__== "__main__":
    main()