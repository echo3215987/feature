import os
from pyspark.sql import SparkSession
from Utils_Function import *
from SRMINVO import *
from Selected_Licsno1 import selected_licsno_code1
from Selected_Licsno2 import selected_licsno_code2
from Constant import *
from Selected_Licsno2_1 import *
from Selected_Licsno2_2 import *

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
        .config("spark.sql.broadcastTimeout", "36000") \
        .getOrCreate()

    write_Log(Log_File, "\n01. Initial folder process......OK\n")
    write_Log(Log_File, "    Start time is %s\n" % str(datetime.datetime.now()))
    write_Log(Log_File, "    END_DATE of data is %s\n" % END_DATE)

    # Main Code
    # Code 1: 02CRCAMF_vs_SSHSCHISTORY_after20150708(2-5)
    #selected_licsno_code1(spark)

    # Code 2: 05CRCAMF_car_selection(6-14)
    selected_licsno_code2(spark)
    #selected_licsno_code2_1(spark)
    #selected_licsno_code2_2(spark)

    #21. %s | Features construction from cdp.SRMINVO......
    #Feature_SRMINVO(spark)

    spark.stop()

if __name__== "__main__":
    main()