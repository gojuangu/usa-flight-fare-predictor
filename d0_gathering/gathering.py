import zipfile
import os
import glob
import zipfile
from os import listdir
from os.path import isfile, join
from pyspark.sql import SparkSession


# This function allows you to unzip all the downloaded files functions

def unzipp(path, downloads_path):
    print('descompressing')

    for file in os.listdir(downloads_path):
        if file.endswith(".zip"):
            file_name = os.path.abspath(file)
            zip_ref = zipfile.ZipFile(file_name)
            zip_ref.extractall(downloads_path)
            zip_ref.close()
    print('descompresed')

# Function to create the of pySpark
def sparkbuilder():
    print('Build the Spark Session')
    spark = SparkSession.builder. \
        appName('<nombre_app>'). \
        master('local[2]'). \
        config('spark.sql.session.timeZone', 'UTC'). \
        config('spark.sql.repl.eagerEval.enabled', True). \
        config('spark.driver.memory', '4G'). \
        getOrCreate()

    return spark


# Function to read the CSV and convert them to parquet
def spark_parquet_ticket(spark, path_t):
    print('gathering the data of the Tickets')

    df_t = spark.read.format("csv") \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .load(f'{path_t}/Origin_and_Destination_Survey_DB1BTicket*.csv')

    return df_t

def spark_parquet_coupon(spark, path_c):
    print('gathering the data of the Coupons')

    df_c = spark.read.format("csv") \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .load(f'{path_c}/Origin_and_Destination_Survey_DB1BCoupon*.csv')

    return df_c
