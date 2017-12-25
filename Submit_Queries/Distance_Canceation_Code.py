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


print "11) top 100 distance groupby where cancellationcode is 1 --effect of flight distance on cancellation "
top100distcance = df_air_time.filter( ( df_air_time.CANCELLED == 1 ) & (df_air_time.DISTANCE > 0))
top100distcancegrpby = top100distcance.groupby('DISTANCE').count().orderBy('count', ascending=False).limit(100)
print top100distcancegrpby.collect()



print("--- %s seconds ---" % (time.time() - start_time))