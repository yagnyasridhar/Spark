import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext(appName="Streaming")
    scc = StreamingContext(sc,1)
    ssc.checkpoint("D://spark//")
    lines = ssc.socketTestStream(sys.argv[1], int(sys.argv[2])
    count = lines.flatMap(lambda line: line.split("")).filter(lambda word: "Error" in word).map(lambda word: (word,1)).reduceByKey(lambda x, y: x+y)
    # just change reduce by key to
    # reduceByKeyAndWindow(lambda x,y: x+y, lambda x,y: x-y, 30, 10)

    count.pprint()
    ssc.start()
    ssc.awaitTermination()


