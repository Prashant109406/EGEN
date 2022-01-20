
#!/usr/bin/env python
# coding: utf-8

# In[20]:


from pandas.io import gbq
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

def main():
    query=""" SELECT * FROM `my-project-egen.egen_bigquery.egen-flight` """
    data= gbq.read_gbq(query,project_id='my-project-egen')


    spark = SparkSession.builder.appName('Simple Pub/Sub Lite Read')    .getOrCreate()
    #Create PySpark DataFrame from Pandas


    sparkDF=spark.createDataFrame(data) 
    sparkDF.printSchema()
   


# cast string to int using window function

    sparkDF= sparkDF.withColumn("YEAR",col("YEAR").cast(IntegerType()))
    sparkDF= sparkDF.withColumn("MONTH",col("MONTH").cast(IntegerType()))
    sparkDF= sparkDF.withColumn("DAY",col("DAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("DAY_OF_WEEK",col("DAY_OF_WEEK").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("FLIGHT_NUMBER",col("FLIGHT_NUMBER").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("SCHEDULED_DEPARTURE",col("SCHEDULED_DEPARTURE").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("DEPARTURE_TIME",col("DEPARTURE_TIME").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("DEPARTURE_DELAY",col("DEPARTURE_DELAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("TAXI_OUT",col("TAXI_OUT").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("WHEELS_OFF",col("WHEELS_OFF").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("SCHEDULED_TIME",col("SCHEDULED_TIME").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("ELAPSED_TIME",col("ELAPSED_TIME").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("AIR_TIME",col("AIR_TIME").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("DISTANCE",col("DISTANCE").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("WHEELS_ON",col("WHEELS_ON").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("TAXI_IN",col("TAXI_IN").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("SCHEDULED_ARRIVAL",col("SCHEDULED_ARRIVAL").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("ARRIVAL_TIME",col("ARRIVAL_TIME").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("ARRIVAL_DELAY",col("ARRIVAL_DELAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("DIVERTED",col("DIVERTED").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("CANCELLED",col("CANCELLED").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("CANCELLATION_REASON",col("CANCELLATION_REASON").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("AIR_SYSTEM_DELAY",col("AIR_SYSTEM_DELAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("SECURITY_DELAY",col("SECURITY_DELAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("AIRLINE_DELAY",col("AIRLINE_DELAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("LATE_AIRCRAFT_DELAY",col("LATE_AIRCRAFT_DELAY").cast(IntegerType())) 
    sparkDF= sparkDF.withColumn("WEATHER_DELAY",col("WEATHER_DELAY").cast(IntegerType()))

    sparkDF.show()
    sparkDF.printSchema()
    
# drop null values
    sparkDF = sparkDF.drop(col('CANCELLATION_REASON'))
    sparkDF = sparkDF.drop(col('AIR_SYSTEM_DELAY'))
    sparkDF = sparkDF.drop(col('SECURITY_DELAY'))
    sparkDF = sparkDF.drop(col('AIRLINE_DELAY'))
    sparkDF = sparkDF.drop(col('LATE_AIRCRAFT_DELAY'))
    sparkDF = sparkDF.drop(col('WEATHER_DELAY'))
    
    cols=["YEAR","MONTH","DAY"]
    sparkDF= sparkDF.withColumn("date",concat_ws("-",*cols))

    
    pandasDF = sparkDF.toPandas()
    pandasDF.to_gbq(destination_table="egen_bigquery.flight",project_id="my-project-egen",if_exists="replace")


if __name__ == "__main__":
     main()

# In[ ]:





