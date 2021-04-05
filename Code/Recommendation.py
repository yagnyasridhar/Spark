from pyspark import SparkContext
from pyspark.mllib.recommendation import *

sc = SparkContext.getOrCreate()
rawUserArtistData = sc.textFile("user_artist_data1.txt")
#print(rawUserArtistData.take(10))
#rawUserArtistData.map(lambda x: float(x.split(" ")[2])).stats()
uadata = rawUserArtistData.map(lambda x: x.split(" ")).filter(lambda x: float(x[2])>=20).map(lambda x:Rating(x[0],x[1],x[2]))
uadata.persist()
#print(uadata.take(10))

model = ALS.trainImplicit(uadata,10,5,0.01)
recommendation = model.recommendProducts(1000002,5)
print(recommendation)