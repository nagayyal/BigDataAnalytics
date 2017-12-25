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





# #1) Top 5 most visited destinations
df_air_time_grp = df_air_time.groupby('DEST').count().orderBy('count', ascending=False).limit(5)
print "Top 5 destinations to visit:"
print df_air_time_grp.collect()
print "City names corresponding codes:"
#df_csv_airport_join_df = df_airports.join (df_air_time_grp, col("df_air_time_grp.DEST") == col("df_airports.iata"))
df_csv_airport_join_df = df_air_time_grp.join(df_airports, df_air_time_grp.DEST == df_airports.iata)
print df_csv_airport_join_df.select('city', 'count').collect()
#print df_airports.filter( df_airports['iata'] == df_air_time_grp['DEST']).show()
print "---end of airports----"
print "-------Quer1 end-------"
 
 
 print("--- %s seconds ---" % (time.time() - start_time))