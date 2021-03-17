import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
logs = sc.textFile("hbase.log")
accumulator_var = sc.accumulator(0)
def ProcessFile(line):
    global accumulator_var
    date_Field = line[:24]
    logField = line[24:]

    if "ERROR" in line:
        accumulator_var += 1
    return (date_Field, logField)

logs.map(ProcessFile).saveAsTextFile("Processed.log")
print(accumulator_var)