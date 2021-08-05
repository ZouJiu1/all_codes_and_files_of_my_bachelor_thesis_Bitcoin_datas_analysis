from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from operator import add
# import time
# time.sleep(3000)
sc = SparkContext("local[2]", "First Spark App")
spark = SparkSession(sc)
data = sc.textFile(r"E:\extract\All_IO_Address.csv")
temp=data.map(lambda x: (x,1))
temp1=temp.reduceByKey(add)
a1=temp1.filter(lambda x: x[1]==1).count()
a3=temp1.filter(lambda x: (x[1]==2) or (x[1]==3)).count()
a9=temp1.filter(lambda x: (x[1]<10) and (x[1]>=4)).count()
a99=temp1.filter(lambda x: (x[1]<100) and (x[1]>=10)).count()
a999=temp1.filter(lambda x: (x[1]<1000) and (x[1]>=100)).count()
a4999=temp1.filter(lambda x: (x[1]<5000) and (x[1]>=1000)).count()
a9999=temp1.filter(lambda x: (x[1]<10000) and (x[1]>=5000)).count()
a99999=temp1.filter(lambda x: (x[1]<100000) and (x[1]>=10000)).count()
a499999=temp1.filter(lambda x: (x[1]<500000) and (x[1]>=100000)).count()
a1000000=temp1.filter(lambda x: (x[1]<1000000) and (x[1]>=500000)).count()
af=temp1.filter(lambda x: x[1]>=1000000).count()

for_save=temp1.filter(lambda x: x[1]>=1000)
c=for_save.map(lambda line: str(line).replace('(','').replace(')','').replace('\'','').split(','))
df = spark.createDataFrame(c)#.collect()
df.write.csv(r'e:\ACTIVE_COUNT_EACHADDR.csv')
print('[1-2]:',a1,'\n[2-4]:',a3,'\n[4-10]:',a9,'\n[10-100]:',a99,'\n[100-1000]:',a999,'\n[1000-5000]:',a4999,\
      '\n[5000-10000]:',a9999,'\n[10000-100000]:',a99999,'\n[100000-500000]:',\
      a499999,'\n[500000-1000000]:',a1000000,'\n[1000000~]:',af)
with open(r'E:\extract\active_addr.txt', 'a+') as obj:
    obj.write('位于区间的交易个数: \n[1-2]:'+str(a1)+'\n[2-4]:'+str(a3)+'\n[4-10]:'+str(a9)+'\n[10-100]:'\
                +str(a99)+'\n[100-1000]:'+str(a999)+'\n[1000-5000]:'+str(a4999)+\
                '\n[5000-10000]:'+str(a9999)+'\n[10000-100000]:'+str(a99999)+\
                '\n[100000-500000]:'+str(a499999)+'\n[500000-1000000]:'+str(a1000000)+'\n[1000000~]:'+str(af))
print('Done!')
print(for_save.take(6))
