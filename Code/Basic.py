import pyspark
from pyspark import SparkContext
sc =SparkContext()
print(sc)

airline = sc.textFile("airlines.csv")
print(sc)

airline = airline.filter(lambda a: 'Description' not in a)
print(airline.first())

a = airline.map(lambda d: d.split(","))
print(a.take(10))
