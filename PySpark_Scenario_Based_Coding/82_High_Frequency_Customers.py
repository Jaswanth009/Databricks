## 1. Write a SQL query to calculate the % of high-frequency customers for January 2023.
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import *
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

schema = StructType([
    StructField("restaurant_id", IntegerType(), True),
    StructField("delivery_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_timestamp", TimestampType(), True)
])

# Create data
data = [
    (1, 1001, 1, datetime(2023, 1, 1, 12)),
    (1, 1002, 1, datetime(2023, 1, 2, 12)),
    (1, 1003, 1, datetime(2023, 1, 3, 12)),
    (1, 1004, 1, datetime(2023, 1, 4, 12)),
    (1, 1005, 1, datetime(2023, 1, 5, 12)),
    (1, 1006, 1, datetime(2023, 1, 6, 12)),
    (1, 1007, 1, datetime(2023, 1, 7, 12)),

    (1, 1008, 2, datetime(2023, 1, 1, 12)),
    (1, 1009, 2, datetime(2023, 1, 3, 12)),
    (1, 1010, 2, datetime(2023, 1, 5, 12)),
    (1, 1011, 2, datetime(2023, 1, 7, 12)),
    (1, 1012, 2, datetime(2023, 1, 10, 12)),
    (1, 1013, 2, datetime(2023, 1, 12, 12)),
    (1, 1014, 2, datetime(2023, 1, 15, 12)),

    (1, 1015, 3, datetime(2023, 1, 2, 12)),
    (1, 1016, 3, datetime(2023, 1, 10, 12)),
    (1, 1017, 3, datetime(2023, 1, 12, 12)),
    (1, 1018, 3, datetime(2023, 1, 18, 12)),
    (1, 1019, 3, datetime(2023, 1, 25, 12)),

    (1, 1020, 4, datetime(2023, 1, 5, 12)),
    (1, 1021, 4, datetime(2023, 1, 12, 12)),
    (1, 1022, 4, datetime(2023, 1, 20, 12)),

    (1, 1023, 5, datetime(2023, 1, 3, 12)),
    (1, 1024, 5, datetime(2023, 1, 7, 12)),

    (1, 1025, 6, datetime(2023, 1, 1, 12)),
    (1, 1026, 6, datetime(2023, 1, 2, 12)),
    (1, 1027, 6, datetime(2023, 1, 3, 12)),
    (1, 1028, 6, datetime(2023, 1, 4, 12)),
    (1, 1029, 6, datetime(2023, 1, 5, 12)),
    (1, 1030, 6, datetime(2023, 1, 6, 12)),
    (1, 1031, 6, datetime(2023, 1, 7, 12)),

    (1, 1032, 7, datetime(2023, 1, 1, 12)),
    (1, 1033, 7, datetime(2023, 1, 10, 12)),

    (1, 1034, 8, datetime(2023, 1, 2, 12)),
    (1, 1035, 8, datetime(2023, 1, 7, 12)),
    (1, 1036, 8, datetime(2023, 1, 15, 12)),
    (1, 1037, 8, datetime(2023, 1, 22, 12))
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

df = df.groupBy("customer_id").agg(count(col("delivery_id")).alias("Delivery_Count"))

df_count = df.count()
df_eligible_count = df.filter(col("Delivery_Count") > 5).count()
df_eligible_percentage = Decimal(df_eligible_count) / Decimal(df_count) * Decimal(100)

# Show data
print(df_eligible_percentage)