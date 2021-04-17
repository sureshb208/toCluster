from pyspark import SparkContext
import sys
#hadoop fs -getmerge output word_counts.txt
#head -n 3 word_counts.txt
if __name__=='__main__':
    sc = SparkContext()
    sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'book.txt') \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda x,y: x+y) \
        .saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')
