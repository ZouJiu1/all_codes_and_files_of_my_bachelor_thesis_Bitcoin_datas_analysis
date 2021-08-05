from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType
from pyspark import SparkContext, SparkConf
sc = SparkContext("local[2]", "First Spark App")
spark = SparkSession(sc)
# data = spark.read.format("csv").load(r"E:\Input\origin\0-68732.csv")
data = sc.textFile(r"E:\Input\entity.csv")#.map(lambda line:line.split(",")).map(lambda record: (record[0], record[1], record[2])
Data=data.map(lambda x: x + str(','))
def change(numb,j,line):
    # return line.split(',')[0] + ',' + str(j) + ','
    strr=','+str(numb)+','
    if strr in line:
        return line.split(',')[0]+','+str(j)+','
    else:
        return line
couj=0
print(Data.count())
for i in range(291185393):
    strr=','+str(i)+','
    exitt=Data.filter(lambda line: strr in line)
    if exitt.count()==0:
        continue
    else:
        exitt_t = exitt.map(lambda x: x.split(',')[0]).collect()
        for addr in exitt_t:
            addrt_t=Data.filter(lambda line: addr in line).\
                                map(lambda x: int(x.split(',')[1])).collect()
            MIN=min(addrt_t)
            addrt_t.remove(MIN)
            if len(set(addrt_t))<1:
                continue
            else:
                for numb in addrt_t:
                    Data=Data.map(lambda line: change(numb,MIN, line))
                    # h=Data.filter(lambda x: ','+str(numb)+',' in x).map(lambda line: change(MIN,line))
                    # print(Data.take(6))
                    # Data=Data.union(h).filter(lambda x: ','+str(numb)+',' not in x)
                    Data.persist(StorageLevel.DISK_ONLY)
    couj=couj+1
    if couj%300000==0:
        print(couj,round((couj/291185392)*100,3),'%',end=' ')
    if couj%6000000==0:
        Data = Data.distinct()
print('Waiting...')
Data=Data.distinct()
temp=Data.map(lambda line: line[0:-1]).map(lambda x: x.split(","))#.toDF()
df = spark.createDataFrame(temp)#.collect()
df.write.csv(r'e:\FORTEMP.csv')
print('Done!')