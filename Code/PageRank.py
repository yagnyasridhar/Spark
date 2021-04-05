from pyspark import SparkContext

sc = SparkContext.getOrCreate()
googleWebLinks = sc.textFile("web-Google.txt").filter(lambda x: "#" not in x).map(lambda x: x.split("\t"))
#print(googleWebLinks.take(5))

def computeContribs(urls, rank):
    Contrib = []
    num_urls = len(urls)
    for url in urls:
        Contrib.append((url, rank / num_urls))
    return Contrib

link = googleWebLinks.groupByKey().partitionBy(100).cache()
rank = link.map(lambda url_neighbors: url_neighbors[0], 1.0) 

for i in range(10):
    contribs = link.join(rank).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    #print(contribs.take(1))
    rank = contribs.reduceByKey(lambda x,y:x+y).mapValues(lambda rank: rank * 0.85 + 0.15)
    #print(rank.take(1))
