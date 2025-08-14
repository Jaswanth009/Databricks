## IBM is developing a new feature to analyze user purchasing behavior for all Fridays in the first quarter of the year 2024.
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import *
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("date", DateType(), True),
    StructField("amount_spent", DecimalType(10, 2), True)
])

# Create data
data = [
    # January (Q1)
    (1, date(2024, 1, 5), Decimal(100.00)),
    (2, date(2024, 1, 5), Decimal(150.00)),
    (1, date(2024, 1, 8), Decimal(200.00)),
    (3, date(2024, 1, 10), Decimal(250.00)),
    (4, date(2024, 1, 12), Decimal(300.00)),
    (2, date(2024, 1, 15), Decimal(50.00)),
    (5, date(2024, 1, 20), Decimal(400.00)),

    # February (Q1)
    (1, date(2024, 2, 2), Decimal(500.00)),
    (6, date(2024, 2, 3), Decimal(600.00)),
    (3, date(2024, 2, 7), Decimal(150.00)),
    (2, date(2024, 2, 9), Decimal(200.00)),
    (7, date(2024, 2, 14), Decimal(180.00)),
    (8, date(2024, 2, 23), Decimal(220.00)),

    # March (Q1)
    (1, date(2024, 3, 1), Decimal(300.00)),
    (9, date(2024, 3, 4), Decimal(400.00)),
    (10, date(2024, 3, 8), Decimal(350.00)),
    (11, date(2024, 3, 15), Decimal(450.00)),
    (12, date(2024, 3, 22), Decimal(500.00)),
    (13, date(2024, 3, 29), Decimal(550.00)),

    # April (Q2)
    (14, date(2024, 4, 5), Decimal(700.00)),
    (15, date(2024, 4, 10), Decimal(250.00)),
    (14, date(2024, 4, 12), Decimal(300.00)),
    (16, date(2024, 4, 19), Decimal(320.00)),
    (17, date(2024, 4, 21), Decimal(400.00)),

    # May (Q2)
    (18, date(2024, 5, 3), Decimal(450.00)),
    (19, date(2024, 5, 10), Decimal(500.00)),
    (20, date(2024, 5, 15), Decimal(550.00)),
    (21, date(2024, 5, 17), Decimal(600.00)),

    # June (Q2)
    (22, date(2024, 6, 7), Decimal(700.00)),
    (23, date(2024, 6, 14), Decimal(750.00)),
    (24, date(2024, 6, 21), Decimal(800.00)),
    (25, date(2024, 6, 28), Decimal(850.00)),

    # July (Q3)
    (26, date(2024, 7, 5), Decimal(900.00)),
    (27, date(2024, 7, 12), Decimal(950.00)),
    (28, date(2024, 7, 19), Decimal(1000.00)),

    # August (Q3)
    (29, date(2024, 8, 2), Decimal(1100.00)),
    (30, date(2024, 8, 9), Decimal(1150.00)),
    (31, date(2024, 8, 16), Decimal(1200.00)),
    (32, date(2024, 8, 23), Decimal(1250.00)),

    # September (Q3)
    (33, date(2024, 9, 6), Decimal(1300.00)),
    (34, date(2024, 9, 13), Decimal(1350.00)),
    (35, date(2024, 9, 20), Decimal(1400.00)),
    (36, date(2024, 9, 27), Decimal(1450.00)),
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

df = df.orderBy(col("user_id").asc())

df = df.withColumn("quarter", quarter(col("date"))) \
            .withColumn("Day", dayofweek(col("date")))\
            .withColumn("Year", year(col("date")))\
            .withColumn("Week", weekofyear(col("date")))\
            .withColumn("Day_name", date_format(col("date"),"EEEE"))
df = df.filter(col("year") == 2024)

df = df.groupBy("week","Day").agg(avg("amount_spent").alias("avg_amount_spent")).orderBy(col("week")).filter(col("day") == 6)

# df_count = df.count()
# print(df_count)
# df_friday_count = df.filter(col("Day") == 6).count()
# print(df_friday_count)
# 
# df_friday_avg = df_friday_count/df_count *100
# print(df_friday_avg)
# 
# Show DataFrame
display(df)