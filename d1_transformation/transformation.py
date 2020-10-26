from pyspark.sql import functions as sf

#Function to transform the parquet file of Tickets
def tickets_transformation(df_t):
    print('Transforming the data...')
    # let's filter just for those tickets with 2 coupons
    coupons = [2]
    df_t = df_t.filter(sf.col('Coupons').isin(coupons))

    # now, we are going to calculate the fare
    df_t = df_t.withColumn('Fare', (sf.col('FarePerMile') * sf.col('Distance')))

    # drop all not necessary columns
    cols_to_delete = ['Coupons', 'Year', 'Quarter', 'Origin', 'OriginAirportID', 'OriginAirportSeqID',
                       'OriginCityMarketID', 'OriginCountry', 'OriginStateFips', 'OriginState', 'OriginStateName',
                       'OriginWac', 'RoundTrip', 'OnLine', 'DollarCred', 'FarePerMile', 'RPCarrier', 'Passengers',
                       'ItinFare', 'BulkFare', 'Distance', 'DistanceGroup', 'MilesFlown', 'ItinGeoType', '_c25']
    for col in cols_to_delete:
        df_t = df_t.drop(col)
    print('Ticket data transformed!')
    return df_t


def coupons_transformation(df_c):
    print('Now, transforming the Coupon data...')
    # let's filter just for those tickets with 2 coupons
    coupons = [2]
    df_c = df_c.filter(sf.col('Coupons').isin(coupons))

    # only itineraries where selling, operating and reporting are made by the same carrier
    df_c = df_c.withColumn('carrier_itin_0', (sf.col('TkCarrier') == sf.col('OpCarrier')))
    df_c = df_c.withColumn('carrier_itin_1', (sf.col('OpCarrier') == sf.col('RPCarrier')))

    df_c = df_c.where('carrier_itin_0!=false')
    df_c = df_c.where('carrier_itin_1!=false')

    # itinerary
    df_c = df_c.withColumn('Itinerary', sf.concat(sf.col('Origin'), sf.lit('_'), sf.col('Dest')))

    # drop all not necessary columns
    cols_to_delete = ['MktID', 'SeqNum', 'OriginAirportSeqID', 'OriginCityMarketID', 'OriginCountry', 'OriginStateFips',
                      'OriginStateName', 'OriginWac', 'DestAirportSeqID', 'DestCityMarketID', 'DestStateFips',
                      'DestWac', 'Break', 'CouponType', 'TkCarrier', 'OpCarrier', 'Gateway', 'ItinGeoType', '_c36',
                      'carrier_itin_0', 'carrier_itin_1']
    for col in cols_to_delete:
        df_c = df_c.drop(col)
    print('Coupon data transformed!')
    return df_c


def join_coupons_tickets(spark, df_c, df_t, path):
    print('Preparing the transformed data for the model...')
    spark.conf.set("spark.sql.crossJoin.enabled", True)
    df = df_c.join(df_t, on=['ItinID'], how='left_outer')

    df = df.dropDuplicates(['ItinID', 'Distance'])
    df = df.dropDuplicates(['ItinID'])
    df = df.drop('ItinID', 'Coupons', 'Origin', 'DestAirportID', 'OriginAirportID', 'OriginState', 'Dest',
                 'DestCountry', 'DestState', 'DestStateName', 'Year')
    df = df.withColumn("Passengers", df["Passengers"].cast("int"))
    df = df.groupby(
        ['itinerary', 'Quarter', 'RPCarrier', 'FareClass', 'Distance', 'DistanceGroup', 'CouponGeoType']).agg(
        sf.sum('Passengers'), sf.round(sf.mean('Fare'),2))
    df = df.withColumnRenamed('round(avg(Fare), 2)', 'avg_fare')
    df = df.withColumnRenamed('sum(Passengers)', 'passengers')
    df = df.where('avg_fare>50.00')
    df = df.where('avg_fare<2500.00')

    print('... we are ready to model!')


    df = df.toPandas()
    df.to_csv(f'{path}/definitive_2.csv')
    print('Just in case, saved in a CSV!')