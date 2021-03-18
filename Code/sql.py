'''
import datapackage
import pandas as pd

data_url = 'https://datahub.io/jgoodman8/twitter-2012-presidential-election/datapackage.json'

# to load Data Package into storage
package = datapackage.Package(data_url)

# to load only tabular data
resources = package.resources
for resource in resources:
    if resource.tabular:
        data = pd.read_csv(resource.descriptor['path'])
        print (data)
'''
import pyspark
from pyspark import SparkContext,SQLContext
import json

sc = SparkContext.getOrCreate()
sqlsc = SQLContext(sc)
twitter = sqlsc.read.json("data.JSON")
twitter.registerTempTable("twitterTable")
print(sqlsc.sql("Select * from twitterTable").collect())
