if __name__=='__main__':
    # ================================ #
    #       Initiate Packages
    # ================================ #
    from pyspark import SparkContext
    import sys
    import os
    import pyspark
    #import re
    import pandas as pd
    import datetime, json
    import numpy as np
    from itertools import compress
    #from toolz import pipe # server currently doesnt have this package ugh

    sc = SparkContext()
    #root = os.getcwd() # + "/dev/gradschool/bigData/HW4/"
    #data = "/data/share/bdm/"

    
    #placeFile = os.path.join(data, "core-places-nyc.csv")
    placeFile = "hdfs:///data/share/bdm/core-places-nyc.csv"
    patternFile = "hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*"
    
    place = sc.textFile(placeFile, use_unicode=True).cache()
    pattern = sc.textFile(patternFile, use_unicode=True).cache()

    place.saveAsTextFile('debug_test2')
    # # place = spark.read.format('csv') \
    # # .option('header',True) \
    # # .option('multiLine', True) \
    # # .load(placeFile).cache()
    
    # place.saveAsTextFile(os.path.join(root, "results/TEST"))

    #sc = SparkContext()
    # sc.textFile('book.txt') \
    #     .flatMap(lambda x: x.split()) \
    #     .map(lambda x: (x,1)) \
    #     .reduceByKey(lambda x,y: x+y) \
    #     .saveAsTextFile('debug_test1')