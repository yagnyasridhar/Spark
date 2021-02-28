import pyspark
from pyspark import SparkContext
sc =SparkContext()
print(sc)

sc.textFile("airlines.csv")
print(sc)

frst = sc.textFile("airlines.csv").first()
print(frst)

clct = sc.textFile("airlines.csv").collect()
for item in clct:    
    print(item)