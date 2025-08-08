#As part of your task for Amazon, find the shipment with the 3rd heaviest weight. Your query should return the shipment_id and the weight of this shipment.

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime, date
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

shipment_data = [
    (1, 120, date(2024,9,15)),
    (2, 85,  date(2024,9,16)),
    (3, 150, date(2024,9,17)),
    (4, 120, date(2024,9,18)),
    (5, 100, date(2024,9,19)),
    (6, 135, date(2024,9,20)),
    (7, 95,  date(2024,9,21)),
    (8, 140, date(2024,9,22)),
    (9, 110, date(2024,9,23)),
    (10, 80, date(2024,9,24)),
    (11, 135,date(2024,2,14)),
]

# Step 3: Define the schema
shipment_schema = StructType([
    StructField("shipment_id", IntegerType(), False),
    StructField("weight", IntegerType(), True),
    StructField("shipment_date", DateType(), True),
])

# Step 4: Create DataFrame
df_shipments = spark.createDataFrame(shipment_data, schema=shipment_schema)

window_specification = Window.orderBy(col("weight").desc())
df_shipments = df_shipments.withColumn("dense_rank",dense_rank().over(window_specification)).filter(col("dense_rank") == 3)

# Step 5: Show the DataFrame
df_shipments.show()
