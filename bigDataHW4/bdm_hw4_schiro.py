
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
    import re
    import pandas as pd
    import datetime, json, csv
    import numpy as np
    from itertools import compress
    from toolz import pipe

    sc = SparkContext()

    # ================================ #
    #           load data
    # ================================ #
    root = os.getcwd() # + "/dev/gradschool/bigData/HW4/"
    data = "/data/share/bdm/"

    
    placeFile = os.path.join(data, "core-places-nyc.csv")
    patternFile = os.path.join(data, "weekly-patterns-nyc-2019-2020")
    place = sc.textFile(placeFile, use_unicode=True).cache()
    pattern = sc.textFile(patternFile, use_unicode=True).cache()

    #------- define groups --------
    NYC_CITIES = set(['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island'])

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
        .filter(lambda x: (x[1] in code4) and (x[2] in NYC_CITIES)) \
        .map(lambda x: x[0]) \
        .collect()
    )

    # list(enumerate(pattern.first().split(',')))

    # get the 2020 leap-year dates
    dateData = pipe(pd.date_range("2020-01-01", "2020-12-31"), sc.parallelize) \
    .map(lambda x: (pipe(x, str)[:10], 0))


    output = pattern \
    .map(lambda x: next(csv.reader([x]))) \
    .filter(lambda x: x[1] in set4) \
    .filter(lambda x: 
        datetime.datetime.strptime(x[13][:10], "%Y-%m-%d") >= datetime.datetime(2019,1,1) and
        datetime.datetime.strptime(x[13][:10], "%Y-%m-%d") < datetime.datetime(2021,1,1)
    ) \
    .map(lambda x: 
        (x[1], (x[12][:10], x[13][:10], x[16]))) \
    .flatMap(lambda x: [(
        #x[0], if I need ID for some reason
        pipe(pd.date_range(x[1][0], x[1][1])[count], str)[:10], 
        value
    ) for 
        count, value in pipe(x[1][2], json.loads, enumerate)
    ]).union(dateData) \
    .groupByKey().map(lambda x:  (x[0], np.median([i for i in x[1]]), np.std([i for i in x[1]]))) \
    .map(lambda x: (
        x[0], x[1],
        pipe([0, x[1] - x[2]], np.array, np.max),
        pipe([0, x[1] + x[2]], np.array, np.max)
    )) \
    .map(lambda x: (
        pipe(x[0][:4], int), # year
        '2020' + x[0][4:], # 2020 date for leap year
        x[1], x[2], x[3]
    ))
    out = output.sortBy(lambda x: x[1])
    
    out.saveAsTextFile(os.path.join(root, "results/TEST"))