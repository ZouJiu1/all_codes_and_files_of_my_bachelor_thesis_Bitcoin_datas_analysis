from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from operator import add
sc = SparkContext("local[2]", "First Spark App")
spark = SparkSession(sc)
data = spark.read.format("csv").load(r"e:\extract\temp.csv")
data.groupBy('_c0').count().show()
# print(x.take(10))
# print(data.take(10))
# data = sc.textFile(r"E:\extract\All_IO_Address.csv")#.map(lambda line:line.split(","))#.map(lambda record: (record[0], record[1], record[2])
# # uniqueUsers = data.rdd.map(lambda record: record[0]).distinct().count()
# exitt=data.filter(lambda line:  '3A1KUd5H4hBEHk4bZB4C3hGgvuXuVX7p7t' or '1JoktQJhCzuCQkt3GnQ8Xddcq4mUgNyXEa' in line)
# # df = spark.read.csv(r'e:\Input\0-68732.csv')
# temp=data.map(lambda x: x.split(","))#.toDF()
# print(exitt.take(3))
# df = spark.createDataFrame(temp)#.collect()
# x.write.csv(r'e:\extract\tempcount.csv')
# print('Done!')
# data = sc.textFile(r"E:\extract\for.csv")
# temp=data.map(lambda x: (x,1))
# temp1=temp.reduceByKey(add)
# a=temp1.filter(lambda x: x[1]>=3)
# c=a.map(lambda line: str(line).replace('(','').replace(')','').replace('\'','').split(','))
# df = spark.createDataFrame(c)#.collect()
# df.write.csv(r'e:\FORTEMP.csv')
# print('Done!')
# print(a.take(6))
# temp=data.filter(lambda x: 'Unable' in x)
# print(temp.take(10))
