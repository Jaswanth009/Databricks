# Uber has a database table called trips that stores information about each trip taken by customers. You've been asked to find the top 2 drivers with the highest driver revenue. Assume that Uber takes a 20% cut on each trip. Round your results to 3 decimals.

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

data = [
    (1, 101, 201, '2024-09-01 08:00:00', '2024-09-01 08:30:00', 10.5, 25.00),
    (2, 102, 202, '2024-09-01 09:00:00', '2024-09-01 09:20:00', 8.0, 20.00),
    (3, 103, 201, '2024-09-01 10:00:00', '2024-09-01 10:15:00', 5.0, 15.00),
    (4, 104, 203, '2024-09-01 11:00:00', '2024-09-01 11:30:00', 12.0, 30.00),
    (5, 105, 202, '2024-09-01 12:00:00', '2024-09-01 12:25:00', 7.5, 18.00),
    (6, 106, 204, '2024-09-01 13:00:00', '2024-09-01 13:45:00', 15.0, 40.00),
    (7, 107, 201, '2024-09-01 14:00:00', '2024-09-01 14:30:00', 10.0, 25.00),
    (8, 108, 203, '2024-09-01 15:00:00', '2024-09-01 15:20:00', 6.0, 16.00),
    (9, 109, 202, '2024-09-01 16:00:00', '2024-09-01 16:45:00', 14.0, 35.00),
    (10, 110, 204, '2024-09-01 17:00:00', '2024-09-01 17:30:00', 9.0, 22.00),
    (11, 111, 201, '2024-09-01 18:00:00', '2024-09-01 18:15:00', 3.0, 10.00),
    (12, 112, 203, '2024-09-01 19:00:00', '2024-09-01 19:45:00', 20.0, 50.00),
    (13, 113, 202, '2024-09-01 20:00:00', '2024-09-01 20:30:00', 5.0, 15.00),
    (14, 114, 201, '2024-09-01 21:00:00', '2024-09-01 21:45:00', 18.0, 45.00),
    (15, 115, 204, '2024-09-01 22:00:00', '2024-09-01 22:30:00', 4.5, 12.00)
]

# Step 3: Define the schema
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("distance", DoubleType(), True),
    StructField("cost", DoubleType(), True)
])

# Step 4: Create the DataFrame
df = spark.createDataFrame(data, schema)

df = df.groupBy("driver_id").agg(sum("cost").alias("Total_revenue")).orderBy(col("Total_revenue").desc())
df = df.withColumn("Total_revenue",round((col("Total_revenue")*0.8),2)).orderBy(col("Total_revenue").desc()).limit(2)
 
df.show()