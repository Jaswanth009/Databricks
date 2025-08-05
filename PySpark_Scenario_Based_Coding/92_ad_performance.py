#Using the sales data from the marketing_campaign table, can you determine the overall performance of the ad campaign? Specifically, categorize the products into performance categories based on the total quantity sold:
#Outstanding: 30 or more units sold
#Satisfactory: 20 to 29 units sold
#Unsatisfactory: 10 to 19 units sold
#Poor: 1 to 9 units sold

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DecimalType(10, 2), True)
])

# Define data
data = [
    (1,  datetime(2024, 9, 1, 10, 0, 0), 101, 35, Decimal('19.99')),
    (2,  datetime(2024, 9, 1, 11, 0, 0), 101, 25, Decimal('19.99')),
    (3,  datetime(2024, 9, 1, 12, 0, 0), 102, 12, Decimal('29.99')),
    (4,  datetime(2024, 9, 1, 13, 0, 0), 103, 5, Decimal('15.99')),
    (5,  datetime(2024, 9, 1, 14, 0, 0), 104, 22, Decimal('25.00')),
    (6,  datetime(2024, 9, 1, 15, 0, 0), 104, 40, Decimal('25.00')),
    (7,  datetime(2024, 9, 1, 16, 0, 0), 105, 3, Decimal('9.99')),
    (8,  datetime(2024, 9, 1, 17, 0, 0), 106, 18, Decimal('20.00')),
    (9,  datetime(2024, 9, 1, 18, 0, 0), 101, 30, Decimal('19.99')),
    (10, datetime(2024, 9, 1, 19, 0, 0), 102, 20, Decimal('29.99')),
    (11, datetime(2024, 9, 1, 20, 0, 0), 107, 15, Decimal('14.99')),
    (12, datetime(2024, 9, 1, 21, 0, 0), 108, 8, Decimal('12.99')),
    (13, datetime(2024, 9, 1, 22, 0, 0), 109, 1, Decimal('5.99')),
    (14, datetime(2024, 9, 1, 23, 0, 0), 110, 28, Decimal('17.49')),
    (15, datetime(2024, 9, 2, 9,  0, 0), 111, 32, Decimal('24.99')),
    (16, datetime(2024, 9, 2, 10, 0, 0), 111, 29, Decimal('24.99')),
    (17, datetime(2024, 9, 2, 11, 0, 0), 112, 16, Decimal('35.00')),
    (18, datetime(2024, 9, 2, 12, 0, 0), 113, 7, Decimal('19.99')),
    (19, datetime(2024, 9, 2, 13, 0, 0), 114, 25, Decimal('30.00'))
]

# Create DataFrame
campaign_df = spark.createDataFrame(data, schema)
df = campaign_df.groupBy("product_id").agg(sum(col("quantity")).alias("total_quantity"))
ad_performance_df = df.withColumn("ad_performance", \
    when(col("total_quantity") > 30, lit("Outstanding"))
    .when(col("total_quantity").between(20,29), lit("Satisfactory"))
    .when(col("total_quantity").between(10,19), lit("Unsatisfactory"))
    .when(col("total_quantity").between(1,9), lit("Poor"))
    .otherwise(lit(None)))
final_df = ad_performance_df.groupBy("ad_performance").agg(count(col("product_id")).alias("count"),sum(col("total_quantity")).alias("Total_quantities_sold")).orderBy(col("Total_quantities_sold").desc())
final_df.show()
