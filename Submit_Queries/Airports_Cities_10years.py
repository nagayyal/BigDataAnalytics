from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col, avg, max
from pyspark.sql.functions import udf
import sys
import time


# SQL context. can also be replaced with SparkContext but we get RDD instaed of dataframes
sc = SparkContext()
sqlContext = SQLContext(sc)

start_time = time.time()
#DF creation for all the airtime csv file
df_air_time = sqlContext.read.format(source="com.databricks.spark.csv").options(header='true', inferschema='true').load(sys.argv[1])

#DF creation for airport
df_airports = sqlContext.read.format(source="com.databricks.spark.csv").options(header='true', inferschema='true').load(sys.argv[2])

#DF creation for carrier
df_carrier = sqlContext.read.format(source="com.databricks.spark.csv").options(header='true', inferschema='true').load(sys.argv[3])
# print count




print "9"
finalset = set()
 
def pairToSet(origin, dest):
    finalset.add(origin)
    finalset.add(dest)
    return finalset
 
udfpairToSet = udf(pairToSet)
print "New airports built in cities since last ten years"
airports1998DFOrg = df_air_time.filter( (df_air_time.YEAR == 1987) ).select('ORIGIN').distinct().rdd
airports1998DFDest = df_air_time.filter( (df_air_time.YEAR == 1987) ).select('DEST').distinct().rdd
airports1998union = airports1998DFOrg.union(airports1998DFDest).distinct()
airports1998unionlist = airports1998union.map(lambda x:tuple([y for y in x ]))
 
airports2008DFOrg = df_air_time.filter( (df_air_time.YEAR == 2012) ).select('ORIGIN').distinct().rdd
airports2008DFDest = df_air_time.filter( (df_air_time.YEAR == 2012) ).select('DEST').distinct().rdd
airports2008union = airports2008DFOrg.union(airports2008DFDest).distinct()
airports2008unionlist = airports2008union.map(lambda x:tuple([y for y in x ]))
 
latestairports = airports2008unionlist.subtract(airports1998unionlist)
 
print "iata codes to cites latest:"
latestairportsfilter = latestairports.toDF(['iatcode'])
 
latestairportscities = latestairportsfilter.join(df_airports, latestairportsfilter.iatcode == df_airports.iata)
print latestairportscities.select('city').show()











print("--- %s seconds ---" % (time.time() - start_time))