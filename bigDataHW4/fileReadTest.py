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
    root = os.getcwd() # + "/dev/gradschool/bigData/HW4/"
    data = "/data/share/bdm/"

    
    placeFile = os.path.join(data, "core-places-nyc.csv")
    place = spark.read.format('csv') \
    .option('header',True) \
    .option('multiLine', True) \
    .load(placeFile).cache()
    
    place.saveAsTextFile(os.path.join(root, "results/TEST"))