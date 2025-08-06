# 1.You are given with the below dataset do the Null data analysis by getting Null count and Not Null count with columns names side by side as shown in the resultant dataset.
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()
data = [
    (1, "Van Dijk", 23),
    (2, None, 32),
    (3, "Fabinho", None),
    (4, None, None),
    (5, "Kaka", None),
    (6, "Ronaldo", 35),
    (7, None, 28),
    (8, "Messi", None),
    (None, "Neymar", 29),
    (10, "Mbappe", 22),
]

# Define schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("NAME", StringType(), True),
    StructField("AGE", IntegerType(), True),
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

cols = [df.columns]

df.createOrReplaceTempView("df")

df1 = spark.sql("""
                select sum(case when ID is null then 1 else 0 end) as id_null_count,
                       sum(case when ID is not null then 1 else 0 end) as id_not_null_count,
                       sum(case when NAME is null then 1 else 0 end) as name_null_count,
                       sum(case when NAME is not null then 1 else 0 end) as name_not_null_count,
                       sum(case when AGE is null then 1 else 0 end) as age_null_count,
                       sum(case when AGE is not null then 1 else 0 end) as age_not_null_count 
                from df 
		""")

df1.createOrReplaceTempView("df1")
df1 = spark.sql("""
                 select "ID" as Column_name, id_null_count as Null_count, id_not_null_count as Not_null_count from df1
                 union all
                 select "NAME" as Column_name, name_null_count, name_not_null_count from df1
                 union all
                 select "AGE" as Column_name, age_null_count, age_not_null_count from df1
        """)
df1.show()