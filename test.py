from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from Utils_Function import *
import os

def main():
    spark = SparkSession.builder.appName("HT_Final") \
        .master("local") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.3.2") \
        .getOrCreate()

    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)

    cwd = os.getcwd()
    print(cwd)

    df = spark.read.option('header', 'true').csv("C:/Users/foxconn/Desktop/tt.csv")
    #df.selectExpr("substring(INSURCD, -10, 10)").show()
    df.withColumn('INSURCD_STRING', col('INSURCD').substr(-10, 10)).show()

    '''
    df = df.withColumn('INSURCD_TRIM', trim(df.INSURCD))
    df.where((df.INSURCD_TRIM != '') | (col("INSURCD_TRIM").isNull())).show()
    
    df = series_str_cleaner(df, 'INSURCD')
    df.show()
    '''

if __name__== "__main__":
    main()