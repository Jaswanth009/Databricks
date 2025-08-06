##1.You are given with the dataset below, get the 5-digit pincodes out in new columns from Address_Information column.

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

data = [
    (1, "The ZIP code is 12345 and the pin code is 67890 and 78902"),
    (2, "The ZIP code is 54321 and the pin code is 09876"),
    (3, "The ZIP code is 11223 and the pin code is 44556 and 64321"),
    (4, "The ZIP code is 22334 and the pin code is 66778"),
    (5, "The ZIP code is 33445 and the pin code is 88990")
]

# Define schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Address_Information", StringType(), True)
])

df = spark.createDataFrame(data,schema)
df = df.withColumn("Address", split(regexp_replace("Address_information","[^0-9]+"," ")," "))
df = df.withColumn("Address",expr("filter(Address, x -> x != '')"))
df.show(truncate=False)