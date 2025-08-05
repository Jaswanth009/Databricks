##  You are given with source and target table, based on both tables get the desired output 

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

# Source table data
source_data = [
    (1, 'A'),
    (2, 'B'),
    (3, 'C'),
    (4, 'D')
]

# Target table data
target_data = [
    (1, 'A'),
    (2, 'B'),
    (4, 'X'),
    (5, 'F')
]

# Define column names
columns = ['id', 'name']

# Create DataFrames
source_df = spark.createDataFrame(source_data, columns)
target_df = spark.createDataFrame(target_data, columns)

# Show DataFrames
source_df.show()
target_df.show()

df = source_df.join(target_df, source_df.id == target_df.id, "outer").select(source_df["id"].alias("source_id"), source_df["name"].alias("source_name"), target_df["id"].alias("target_id"), target_df["name"].alias("target_name"))

df = df.withColumn("Record_Type", \
    when((col("source_id") == col("target_id")) & (col("source_name") == col("target_name")), lit("Match")).
    when((col("source_id") == col("target_id")) & (col("source_name") != col("target_name")), lit("MisMatch")).
    when((col("source_id").isNull()) & (col("target_id").isNotNull()) & (col("source_name").isNull()) & (col("target_name").isNotNull()) , lit("New Records in Target")).
    when((col("source_id").isNotNull()) & (col("target_id").isNull()) & (col("source_name").isNotNull()) & (col("target_name").isNull()) , lit("New Records in Source"))
    .otherwise(lit(None))
    )

df.show()