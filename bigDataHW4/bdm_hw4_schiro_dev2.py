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

    # ================================ #
    #           load data
    # ================================ #    

    placeFile = "hdfs:///data/share/bdm/core-places-nyc.csv"
    patternFile = "hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*"
    
    place = sc.textFile(placeFile, use_unicode=True)
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
    name1 = "Big Box Grocers"
    name2 = "Convenience Stores"
    name3 = "Drinking Places"
    name4 = "Full-Service Restaurants"
    name5 = "Limited-Service Restaurants"
    name6 = "Pharmacies and Drug Stores"
    name7 = "Snack and Bakeries"
    name8 = "Specialty Food Stores"
    name9 = "Supermarkets (except Convenience Stores)"

    code1 = ('452210', '452311')
    code2 = ('445120')
    code3 = ('722410')
    code4 = ('722511')
    code5 = ('722513')
    code6 = ('446110', '446191')
    code7 = ('311811', '722515')
    code8 = ('445210', '445220', '445230', '445291', '445292', '445299')
    code9 = ('445110')

    # repeat for all sets in production code
    set4 = set(place \
        .map(lambda x: x.split(',')) \
        .map(lambda x: (x[1], x[9], x[13])) \
        .filter(lambda x: (x[1] in code4)) \
        .map(lambda x: x[0]) \
        .collect()
    )

    dateData = pipe(pd.date_range("2020-01-01", "2020-12-31"), sc.parallelize) \
    .map(lambda x: (pipe(x, str)[:10], 0))

    
    spark = SparkSession(sc)
    
    def trnsfm(x):        
        dr = pd.date_range(x.date_range_start, x.date_range_end)
        dates = [str(i)[:10] for i in dr]
        vals = pipe(x.visits_by_day, json.loads, enumerate)
        out = [(dates[i], val) for i, val in vals]
        return(out) 

    pattern = spark.read.load(
        patternFile, 
        format='com.databricks.spark.csv', 
        header='true', 
        inferSchema='true'
    ).cache()
    pattern = pattern.filter(pattern['safegraph_place_id'].isin(set4)) \
    .filter(
        (pattern['date_range_start'] >= datetime.datetime(2019,1,1)) & 
        (pattern['date_range_end'] < datetime.datetime(2021,1,1))
    ) \
    .select(['safegraph_place_id', 'date_range_start', 'date_range_end', 'visits_by_day']) \
    .rdd.map(lambda x: trnsfm(x)).flatMap(lambda x: x).cache()
    pattern = sc.union([pattern, dateData]).cache()
    pattern = pattern.groupByKey().map(lambda x:  (x[0], [i for i in x[1]]))
    checkVar = pattern.getNumPartitions()
    pipe(np.repeat(checkVar, 50), sc.parallelize).saveAsTextFile("checkPartitions")
    # .map(lambda x:  (x[0], np.median([i for i in x[1]]), np.std([i for i in x[1]]))) \
    # .saveAsTextFile("TEST2")

    # .union(dateData) \
    # .groupByKey() \
    # .map(lambda x:  (x[0], np.median([i for i in x[1]]), np.std([i for i in x[1]]))) \
    # .saveAsTextFile("TEST2")

# WHY DOES MERGE MAKE IT ERROR OUT?? 
# Probably because in this scenario have to appen lists
# maybe median (list1, list2) is why fail

# rdd = spark.sparkContext.parallelize(pd.date_range("2020-01-01", "2020-12-31"))
# rdd.toDF()
 