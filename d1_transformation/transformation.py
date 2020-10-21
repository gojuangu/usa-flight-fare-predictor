import pandas as pd
import cpi
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

#Function to transform the parquet file of Tickets
def tickets_transformation(df_t):
    df_t = df_t_t

    # let's filter just for those tickets with 2 coupons
    coupons = [2]
    df_t_t = df_t_t.filter(sf.col('Coupons').isin(coupons))

    # now, we are going to calculate the fare
    df_t_t = df_t_t.withColumn('Fare', (sf.col('FarePerMile') * sf.col('Distance')))

    # drop all not necessary columns
    cols_to_delete = ['Coupons', 'Year', 'Quarter', 'Origin', 'OriginAirportID', 'OriginAirportSeqID',
                       'OriginCityMarketID', 'OriginCountry', 'OriginStateFips', 'OriginState', 'OriginStateName',
                       'OriginWac', 'RoundTrip', 'OnLine', 'DollarCred', 'FarePerMile', 'RPCarrier', 'Passengers',
                       'ItinFare', 'BulkFare', 'Distance', 'DistanceGroup', 'MilesFlown', 'ItinGeoType', '_c25']
    for col in cols_to_delete:
        df_t_t = df_t_t.drop(col)
    print('tickets transformed')
    return df_t_t


def coupons_transformation(df_c):
    df_c = df_c_t

    # let's filter just for those tickets with 2 coupons
    coupons = [2]
    df_c_t = df_c_t.filter(sf.col('Coupons').isin(coupons))

    # only itineraries where selling, operating and reporting are made by the same carrier
    df_c_t = df_c_t.withColumn('carrier_itin_0', (sf.col('TkCarrier') == sf.col('OpCarrier')))
    df_c_t = df_c_t.withColumn('carrier_itin_1', (sf.col('OpCarrier') == sf.col('RPCarrier')))

    df_c_t = df_c_t.where('carrier_itin_0!=false')
    df_c_t = df_c_t.where('carrier_itin_1!=false')

    # itinerary
    df_c_t = df_c_t.withColumn('Itinerary', sf.concat(sf.col('Origin'), sf.lit('_'), sf.col('Dest')))

    # drop all not necessary columns
    cols_to_delete = ['MktID', 'SeqNum', 'OriginAirportSeqID', 'OriginCityMarketID', 'OriginCountry', 'OriginStateFips',
                      'OriginStateName', 'OriginWac', 'DestAirportSeqID', 'DestCityMarketID', 'DestStateFips',
                      'DestWac', 'Break', 'CouponType', 'TkCarrier', 'OpCarrier', 'Gateway', 'ItinGeoType', '_c36',
                      'carrier_itin_0', 'carrier_itin_1']
    for col in cols_to_delete:
        df_c_t = df_c_t.drop(col)
    print('Coupons transformed!')
    return df_c_t


def join_coupons_tickets(df_c_t, df_t_t):
    spark.conf.set("spark.sql.crossJoin.enabled", True)
    df = df_c_t.join(df_t_t, on=['ItinID'], how='left_outer')

    df = df.dropDuplicates(['ItinID', 'Distance'])
    df = df.dropDuplicates(['ItinID'])
    print('Joined!')

    df = df.toPandas()
    df.to_csv('/home/juan/Downloads/definitive_1.csv')
    print('Saved in a CSV!')