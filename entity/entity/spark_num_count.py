from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext("local[2]", "First Spark App")
spark = SparkSession(sc)
# data = sc.textFile(r"E:\All_Num_Address.csv")
# number=data.distinct().count()
# print('number:',number)
# print('真实数量：',number-1)
x=spark.read.format('csv').load(r"E:\All_Num_Address.csv")
h=x.distinct().count()
print(h)
print(x.take(10))