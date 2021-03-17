import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate()

txtFile = sc.textFile("sample.txt")
wordCount = txtFile.flatMap(lambda x: x.split()).map(lambda x:(x,1))
count = wordCount.reduceByKey(lambda x,y: x+y)
print(count.keys().collect())
print(count.values().collect())
cnt = count.collectAsMap()
print(cnt)