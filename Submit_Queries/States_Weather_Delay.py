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




print "2) States having Weather delay"
 
WeatherDelayflightdelay = df_air_time.filter((df_air_time.WEATHER_DELAY > 100))
 
print "States having Weather delay"
statesweatherdelay = WeatherDelayflightdelay.groupby('ORIGIN_STATE_ABR').count().orderBy('count', ascending=False).limit(5)
print statesweatherdelay.select('ORIGIN_STATE_ABR','count').collect()