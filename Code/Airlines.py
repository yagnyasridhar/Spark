import pyspark
from pyspark import SparkContext
from datetime import datetime
from collections import namedtuple

fields   = ['date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance']
Flight = namedtuple('Flight', fields)

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"
def parse(row):
    row[0]  = datetime.strptime(row[0], DATE_FMT).date(),
    row[5]  = datetime.strptime(row[5], TIME_FMT).time(),
    row[6]  = float(row[6]),
    row[7]  = datetime.strptime(row[7], TIME_FMT).time(),
    row[8]  = float(row[8]),
    row[9]  = float(row[9]),
    row[10] = float(row[10]),
    return Flight(*row[:11])

sc =SparkContext()
flights = sc.textFile("flights.csv")
airlines = sc.textFile("airlines.csv")
airports = sc.textFile("airports.csv")

flightsParsed = flights.map(lambda x: x.split(',')).map(parse)
flightsParsed.persist()
#print(flightsParsed.take(1))
totalDistance = flightsParsed.map(lambda x: x.distance[0]).reduce(lambda x,y: x+y)
#print(totalDistance)

avgDistance = totalDistance/float(flightsParsed.count())
#print(avgDistance)
percntDelay = flightsParsed.filter(lambda x: x.dep_delay[0]).count()/float(flightsParsed.count())
#print(percntDelay)

sumCount = flightsParsed.map(lambda x: x.dep_delay[0]).aggregate((0, 0),(lambda acc, value: (acc[0] + value, acc[1] + 1)), (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])))
avgDelay = sumCount[0]/float(sumCount[1])
#print(avgDelay)

frequencyDst = flightsParsed.map(lambda x:int(x.dep_delay[0]/60)).countByValue()
'''
print(frequencyDst)
for x, y in frequencyDst.items():
    print("Key {0} value {1}",x,y)
'''

flightsParsed.unpersist()