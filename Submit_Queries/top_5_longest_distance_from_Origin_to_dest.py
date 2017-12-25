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




print "10) top 5 longest distance from Origin to dest-"
print df_air_time.head(10)
distmaxorigdest = df_air_time.filter( (df_air_time.ORIGIN.isNotNull()) & ( df_air_time.DISTANCE != "NA" ) &(df_air_time.DEST.isNotNull())).select(  df_air_time.ORIGIN, df_air_time.DEST, df_air_time.UNIQUE_CARRIER, df_air_time.DISTANCE.cast("float"))
print "after cast:"
print distmaxorigdest.head(1)
distmaxorigdestgroupby = distmaxorigdest.groupby('ORIGIN', 'DEST', 'UNIQUE_CARRIER').agg(F.max('DISTANCE').alias('Distance')).orderBy( 'Distance', ascending=False).limit(5)
distmaxorigdestgroupbyseljoined = distmaxorigdestgroupby.join(df_carrier, distmaxorigdestgroupby.UNIQUE_CARRIER == df_carrier.Code)
print "carrier description:"

print distmaxorigdestgroupbyseljoined.select('ORIGIN','DEST',  'DISTANCE','UNIQUE_CARRIER','Description').show()




print("--- %s seconds ---" % (time.time() - start_time))