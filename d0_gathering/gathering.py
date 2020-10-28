import os
import zipfile
from pyspark.sql import SparkSession


# This function allows you to unzip all the downloaded files functions

def unzipp(downloads_path, path):
    print('decompressing')

    for file in os.listdir(downloads_path):
        if file.endswith('.zip'):
            file_name = (downloads_path.strip() + file.strip())
            print(file_name)
            with zipfile.ZipFile(file_name, 'r') as zip_ref:
                for name in zip_ref.namelist():
                    if name.endswith('.csv'):
                        zip_ref.extractall(path)
                        zip_ref.close()
    print('decompressed')

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
def spark_parquet_ticket(spark, path):
    print('gathering the data of the Tickets')

    df_t = spark.read.format("csv") \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .load(f'{path}/Origin_and_Destination_Survey_DB1BTicket*.csv')

    return df_t

def spark_parquet_coupon(spark, path):
    print('gathering the data of the Coupons')

    df_c = spark.read.format("csv") \
        .option("header", "true") \
        .option("mode", "DROPMALFORMED") \
        .load(f'{path}/Origin_and_Destination_Survey_DB1BCoupon*.csv')

    return df_c
