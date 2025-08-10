## Your goal is to return the names of the student that starts with the letter 'A' (it can also start with lower caps) along with their count and letter column
containing values like ('A/a', 'P/p').

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import *
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

schema = StructType([
    StructField("company", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("revenue", DecimalType(18, 2), True)
])

data = [
    ('hlan',), ('adam',), ('Betty',), ('Charlie',), ('dlice',), ('David',),
    ('aaron',), ('Eve',), ('Abigail',), ('gustin',), ('sngela',), ('piden',),
    ('unthony',), ('tshley',), ('fmber',), ('Avery',), ('ulicia',), ('rlfred',),
    ('April',), ('unnie',), ('sria',), ('radam',), ('Abel',), ('ylina',),
    ('oshlyn',), ('tmelia',), ('pdrian',), ('brianna',), ('clec',), ('vlyssa',),
    ('dubrey',)
]

# Schema
schema = StructType([
    StructField("first_name", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
#df.show(truncate=False)

df = df.withColumn("first_name", lower(col("first_name")))\
.withColumn("first_letter", substring(col("first_name"),1,1))

df = df.groupBy("first_letter").agg(collect_list("first_name").alias("first_name")).orderBy(col("first_name").desc())
df.show(truncate= False)