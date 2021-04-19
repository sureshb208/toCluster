# yarn logs -applicationId application_1609183734776_2282
# File Location 1: 
#    /data/share/bdm/core-places-nyc.csv
# File Location 2: 
#    /data/share/bdm/weekly-patterns-nyc-2019-2020

if __name__=='__main__':
    # ================================ #
    #       Initiate Packages
    # ================================ #
    from pyspark import SparkContext
    import sys
    import os
    import pyspark
    import pandas as pd
    import datetime, json, csv
    import numpy as np
    from itertools import compress
    from pyspark.sql.session import SparkSession
    #from toolz import pipe

    sc = SparkContext()
    LOCAL = True
    #LOCAL = False
    # ================================ #
    #           load data
    # ================================ #        
    if LOCAL: 
        root = os.getcwd() + "/dev/gradschool/bigData/HW4/"
        data = os.path.join(root, "data")
        placeFile = os.path.join(data, "core_poi_ny.csv")
        patternFile = os.path.join(data, "weekly-patterns-nyc-2019-2020/*")
    else:
        placeFile = "hdfs:///data/share/bdm/core-places-nyc.csv"
        patternFile = "hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*"
    
    #place = sc.textFile(placeFile, use_unicode=True)
    #pattern = sc.textFile(patternFile, use_unicode=True).cache()

    # ======================================================= #
    #   Define pipe from toolz package because not on server
    # ======================================================= #
    def pipe(data, *funcs):
        """ Pipe a value through a sequence of functions

        I.e. ``pipe(data, f, g, h)`` is equivalent to ``h(g(f(data)))``

        We think of the value as progressing through a pipe of several
        transformations, much like pipes in UNIX

        ``$ cat data | f | g | h``

        >>> double = lambda i: 2 * i
        >>> pipe(3, double, str)
        '6'

        See Also:
            compose
            compose_left
            thread_first
            thread_last
        """
        for func in funcs:
            data = func(data)
        return data

    #------- define groups --------
    name0 = "Big Box Grocers.csv"
    name1 = "Convenience Stores.csv"
    name2 = "Drinking Places.csv"
    name3 = "Full-Service Restaurants.csv"
    name4 = "Limited-Service Restaurants.csv"
    name5 = "Pharmacies and Drug Stores.csv"
    name6 = "Snack and Bakeries.csv"
    name7 = "Specialty Food Stores.csv"
    name8 = "Supermarkets (except Convenience Stores).csv"

    code0 = ('452210', '452311')
    code1 = ('445120')
    code2 = ('722410')
    code3 = ('722511')
    code4 = ('722513')
    code5 = ('446110', '446191')
    code6 = ('311811', '722515')
    code7 = ('445210', '445220', '445230', '445291', '445292', '445299')
    code8 = ('445110')

    spark = SparkSession(sc)

    place = spark.read.load(
        placeFile, 
        format='com.databricks.spark.csv', 
        header='true', 
        inferSchema='true'
    ).select(['safegraph_place_id', 'naics_code'])


    from pyspark.sql.functions import col
    from pyspark.sql.functions import when

    place = place.withColumn("id", 
        (when(col("naics_code").isin(['452210', '452311']), 0)) \
        .when(col("naics_code") == '445120', 1) \
        .when(col("naics_code") == '722410', 2) \
        .when(col("naics_code") == '722511', 3) \
        .when(col("naics_code") == '722513', 4) \
        .when(col("naics_code").isin(['446110', '446191']), 5) \
        .when(col("naics_code").isin(['311811', '722515']), 6) \
        .when(col("naics_code").isin(['445210', '445220', '445230', '445291', '445292', '445299']), 7) \
        .when(col("naics_code") == '445110', 8)
    ).filter("id is not null") \
    .select(['safegraph_place_id', 'id'])

    pattern = spark.read.load(
        patternFile, 
        format='com.databricks.spark.csv', 
        header='true', 
        inferSchema='true'
    ).select(['safegraph_place_id', 'date_range_start', 'date_range_end', 'visits_by_day'])
    
    df = pattern.join(place, ['safegraph_place_id'], 'inner')
    df = df.filter(
        (df['date_range_start'] >= datetime.datetime(2019,1,1)) & 
        (df['date_range_end'] < datetime.datetime(2021,1,1))
    )
    
    dateData = pipe(pd.date_range("2020-01-01", "2020-12-31"), sc.parallelize) \
    .map(lambda x: (pipe(x, str)[:10], 0))

    def trnsfm(x):        
        dr = pd.date_range(x.date_range_start, x.date_range_end)
        dates = [str(i)[:10] for i in dr]
        vals = pipe(x.visits_by_day, json.loads, enumerate)
        out = [((dates[i], x.id), val) for i, val in vals]
        return(out) 

    # .item converts numpy to python type
    df = df.rdd.map(lambda x: trnsfm(x)).flatMap(lambda x: x) \
    .groupByKey().map(lambda x:  (x[0], [i for i in x[1]])) \
    .map(lambda x:  (x[0], np.median([i for i in x[1]]).item(), np.std([i for i in x[1]]).item()))
    df = df.map(lambda x: (x[0][0], x[0][1], x[1], x[2]))
    df = df.toDF(['date', 'id', 'median', 'sd'])

    #from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType, DoubleType
    # schema = StructType([ \
    #     StructField("date", StringType(),True), \
    #     StructField("id", IntegerType(),True), \
    #     StructField("median",FloatType(),True), \
    #     StructField("sd", FloatType(), True)        
    # ])
    # spark.createDataFrame(df, schema = schema)

    df.filter(df.id == 0).save(name0, 'com.databricks.spark.csv')
    # df.filter(df.id == 2).show()
    # df.filter(df.id == 3).show()
    # df.filter(df.id == 4).show()
    # df.filter(df.id == 5).show()
    # df.filter(df.id == 6).show()
    # df.filter(df.id == 7).show()
    # df.filter(df.id == 8).show()

    #Spark 1.3
    #df.save('mycsv.csv', 'com.databricks.spark.csv')
    #Spark 1.4+
    #df.write.format('com.databricks.spark.csv').save('mycsv.csv')
    #In Spark 2.0+ you can use csv data source directly:
    #df.write.csv('mycsv.csv')
