import pyspark
from pyspark import SparkContext
from datetime import datetime
from collections import namedtuple
import csv
from io import StringIO

fields   = ['date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance']
Flight = namedtuple('Flight', fields)

DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"
def parse(row):
    row[0]  = datetime.strptime(row[0], DATE_FMT).date(),
    row[1]  = int(row[1]),
    row[2]  = int(row[2]),
    row[3]  = str(row[3]),
    row[4]  = str(row[4]),
    row[5]  = datetime.strptime(row[5], TIME_FMT).time(),
    row[6]  = float(row[6]),
    row[7]  = datetime.strptime(row[7], TIME_FMT).time(),
    row[8]  = float(row[8]),
    row[9]  = float(row[9]),
    row[10] = float(row[10]),
    return Flight(*row[:11])

sc =SparkContext.getOrCreate()
flights = sc.textFile("flights.csv")

flightsParsed = flights.map(lambda x: x.split(',')).map(parse)
airportDelays = flightsParsed.map(lambda x: (x.origin, x.dep_delay[0]))
#print(airportDelays.keys().take(1))
#print(airportDelays.values().take(10))
# Reduce by
'''
totalDelays = airportDelays.reduceByKey(lambda x,y: x+y)
#print(totalDelays.values().take(5))
count = airportDelays.mapValues(lambda x:1).reduceByKey(lambda x,y: x+y)
aiportSumDelay = totalDelays.join(count)
AvgDelay = aiportSumDelay.mapValues(lambda x: x[0]/float(x[1]))
print(AvgDelay.values().collect())
'''

# Combine by
airportSumCount = airportDelays.combineByKey((lambda value:(value,1)), (lambda acc, value: (acc[0]+value,acc[1]+1)),(lambda acc1, acc2: (acc1[0]+acc2[0],acc1[1]+acc2[1])))
#print(airportSumCount.take(1))
AvgDelay1 = airportSumCount.mapValues(lambda x : x[0]/float(x[1]))
#print(AvgDelay1.values().collect())
AvgDelay1.sortBy(lambda x: -x[1])
#print(AvgDelay1.values().take(10))

def spliter(lines):
    reader = csv.reader(StringIO(lines))
    return next(reader)

airports = sc.textFile("airports.csv").filter(lambda x: "Description" not in x).map(spliter)
#print(airports.take(1))
airportlookup = airports.collectAsMap()
airportloopkupBC = sc.broadcast(airportlookup)
AvgDelay1.map(lambda x: (airportloopkupBC.value[x[0]], x[1]))
print(AvgDelay1.take(1))