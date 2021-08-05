from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
import numpy as np
import os
import gc
path=r'D:\big_num_trade'
dirs=os.listdir(path)
for i in dirs:
    PATH=os.path.join(path,i)
    print(PATH)
    sc = SparkContext("local[2]", "First Spark App")
    spark = SparkSession(sc)
    # data=spark.read.csv(r'E:\extract\1\0-68732.csv')
    data = spark.read.format("csv").load(PATH)
    coun = data.count()
    with open(r'D:\count.txt', 'a+') as obj:
        obj.write(r'位于区间%s的交易个数:' % (i.split('c')[0].split('e')[1]) + str(coun - 1) + '\n')

    del data, spark
    gc.collect()
    import time
    time.sleep(3)
    sc.stop()
    print("Done!")
# uniqueUsers = data.rdd.map(lambda record: record[0]).distinct().count()
# def floatt(x):
#     return float(x.replace('[','').replace(']','').replace('\'','').replace(',',''))
# h=data.rdd.map(lambda line: line[3]).map(lambda x: floatt(x))
# print(uniqueUsers)
# print(exitt.take(3))
# df = spark.read.csv(r'e:\Oneday_numasum_block.csv')
# data.write.csv(r'e:\fortemp.csv',header=True)
# print(data.take(3))