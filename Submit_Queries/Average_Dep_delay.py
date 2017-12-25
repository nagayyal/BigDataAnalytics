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



print "6) Top 5 cities with the most avg departure delays in minutes:"
departureDelayFlights = df_air_time.filter( (df_air_time.DEP_DELAY.isNotNull()) & (df_air_time.ORIGIN.isNotNull()))
departureDelayFlightsgrpOrigin = departureDelayFlights.groupby('ORIGIN').agg(avg(col("DEP_DELAY")).alias('avg')).orderBy('avg', ascending=False).limit(5)
departureDelayFlightsgrpOriginjoined = departureDelayFlightsgrpOrigin.join(df_airports, departureDelayFlightsgrpOrigin.ORIGIN == df_airports.iata)
print departureDelayFlightsgrpOriginjoined.select('city', 'avg').show()







print("--- %s seconds ---" % (time.time() - start_time))