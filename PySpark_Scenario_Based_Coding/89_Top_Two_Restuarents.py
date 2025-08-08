# 1.As the holiday season nears, your team is gearing up for heightened sales during Christmas. To effectively prepare, you've been assigned the task of identifying the 2 top-performing restaurants based on total sales by their Names.

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import datetime, date
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

data = [
    (1, "2023-12-01 11:00:00", "2023-12-01 12:00:00", "2023-12-01 12:00:00", 5, 101, "Pasta Place", 201),
    (2, "2023-12-01 12:30:00", "2023-12-01 13:30:00", "2023-12-01 13:30:00", 4, 102, "Burger Bistro", 202),
    (3, "2023-12-02 18:00:00", "2023-12-02 19:00:00", "2023-12-02 19:15:00", 5, 103, "Sushi Spot", 203),
    (4, "2023-12-03 14:00:00", "2023-12-03 15:00:00", None, 3, 104, "Pizza Palace", 204),
    (5, "2023-12-03 16:00:00", "2023-12-03 17:00:00", "2023-12-03 17:00:00", 5, 105, "Taco Town", 205),
    (6, "2023-12-04 17:30:00", "2023-12-04 18:30:00", None, 4, 106, "Salad Station", 206),
    (7, "2023-12-05 10:00:00", "2023-12-05 11:00:00", "2023-12-05 11:00:00", 5, 107, "Sandwich Stop", 207),
    (8, "2023-12-05 15:30:00", "2023-12-05 16:30:00", "2023-12-05 16:45:00", 3, 108, "BBQ Barn", 208),
    (9, "2023-12-06 13:00:00", "2023-12-06 14:00:00", "2023-12-06 14:15:00", 5, 109, "Deli Delight", 209),
    (10, "2023-12-07 17:00:00", "2023-12-07 18:00:00", "2023-12-07 18:00:00", 5, 110, "Breakfast Café", 210),
    (11, "2023-12-08 11:00:00", "2023-12-08 12:00:00", None, 4, 111, "Chinese Chomp", 211),
    (12, "2023-12-09 19:00:00", "2023-12-09 20:00:00", "2023-12-09 20:00:00", 3, 112, "Indian Flavors", 212),
    (13, "2023-12-10 20:00:00", "2023-12-10 21:00:00", "2023-12-10 21:15:00", 5, 113, "Greek Taverna", 213),
    (14, "2023-12-11 15:30:00", "2023-12-11 16:30:00", "2023-12-11 16:45:00", 5, 114, "Vegan Vibes", 214),
    (15, "2023-12-12 18:00:00", "2023-12-12 19:00:00", "2023-12-12 19:10:00", 5, 115, "Dessert Den", 215)
]

# Define schema
schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("order_placed_time", StringType(), True),
    StructField("predicted_delivery_time", StringType(), True),
    StructField("actual_delivery_time", StringType(), True),
    StructField("delivery_rating", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("restaurant_name", StringType(), True),
    StructField("consumer_id", IntegerType(), True)
])

rate_data = [
    (1, 150.00),
    (2, 200.50),
    (3, 75.00),
    (4, 300.00),
    (5, 120.75),
    (6, 180.25),
    (7, 250.00),
    (8, 90.00),
    (9, 500.00),
    (10, 650.00),
    (11, 100.00),
    (12, 60.00),
    (13, 300.50),
    (14, 400.00),
    (15, 350.00),
    (16, 120.00),
    (17, 450.75),
    (18, 80.00)
]

# Step 3: Define schema
rate_schema = StructType([
    StructField("delivery_id", IntegerType(), True),
    StructField("sales_amount", DoubleType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
rate_df = spark.createDataFrame(rate_data, rate_schema)
#df.show()
#rate_df.show()

df_jn = df.join(rate_df, df.delivery_id == rate_df.delivery_id, "inner").select(df["*"],rate_df["sales_amount"])

df_jn = df_jn.orderBy(col("sales_amount").desc()).limit(2)
df_jn = df_jn.select("restaurant_name","sales_amount")
df_jn.show()