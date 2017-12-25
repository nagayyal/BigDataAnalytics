AirLine Analysis using Pyspark

Description: 
Performing data analytics on the airline dataset using pyspark in aws .

Steps to execute the program in Spark

1) Download the data from https://packages.revolutionanalytics.com/datasets/AirOnTime87to12/AirOnTimeCSV.zip (unzipped version)
2) Download airports.csv , carriers.csv from http://stat-computing.org/dataexpo/2009/supplemental-data.html
3) In the amazon aws account put all the fies in a s3 bucket (eg: bucket-name)
4) Setup the Emr cluster with the following configuration: ec2 instance type: m4.2x large
 number of nodes: 6(1 master and 5 worker nodes)
 Select the option as spark Framework
5) Place all the queries in the cluster
6) extract all these .py queries and to execute each of them use the following command:
spark-submit Airports_Cities_10years.py s3://bucket-name/*.csv s3://bucket-name/airports.csv s3://bucket-name/carriers.csv

the executed command would give the time and also the results
Used pyspark to execute in spark framework

Few Sample queries are
1) Top 10 states having high airports
2) Best Airlines till now(1987-2012)
3) Best times in the week to travel
4) Best times in the year to travel
5) Cities wherein new airports are built since last 15 years
6) City Pairs with Maximum diversions
7) Top 5 Visited Destinations

