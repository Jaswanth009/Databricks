1. Identify companies whose revenue consistently increases every year without any dips.
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

# Define the data
data = [
    ("Tech Innovations", 2018, Decimal(5000000.00)),
    ("Tech Innovations", 2019, Decimal(6000000.00)),
    ("Tech Innovations", 2020, Decimal(6500000.00)),
    ("Tech Innovations", 2021, Decimal(7000000.00)),
    ("Tech Innovations", 2022, Decimal(7500000.00)),

    ("Green Solutions", 2018, Decimal(3000000.00)),
    ("Green Solutions", 2019, Decimal(3500000.00)),
    ("Green Solutions", 2020, Decimal(4000000.00)),
    ("Green Solutions", 2021, Decimal(4500000.00)),
    ("Green Solutions", 2022, Decimal(4400000.00)),  # Dip here

    ("Finance Corp", 2018, Decimal(8000000.00)),
    ("Finance Corp", 2019, Decimal(9000000.00)),
    ("Finance Corp", 2020, Decimal(9500000.00)),
    ("Finance Corp", 2021, Decimal(10000000.00)),
    ("Finance Corp", 2022, Decimal(10500000.00)),

    ("Health Services", 2018, Decimal(2000000.00)),
    ("Health Services", 2019, Decimal(2500000.00)),
    ("Health Services", 2020, Decimal(2600000.00)),
    ("Health Services", 2021, Decimal(2700000.00)),
    ("Health Services", 2022, Decimal(2800000.00)),

    ("EduTech", 2018, Decimal(1500000.00)),
    ("EduTech", 2019, Decimal(1600000.00)),
    ("EduTech", 2020, Decimal(1700000.00)),
    ("EduTech", 2021, Decimal(1800000.00)),
    ("EduTech", 2022, Decimal(1900000.00)),

    ("Retail Giants", 2018, Decimal(9000000.00)),
    ("Retail Giants", 2019, Decimal(8500000.00)),  # Dip here
    ("Retail Giants", 2020, Decimal(9000000.00)),
    ("Retail Giants", 2021, Decimal(9500000.00)),
    ("Retail Giants", 2022, Decimal(10000000.00))
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)
window_specification = Window.partitionBy(col("company")).orderBy(col("year").asc())
df = df.withColumn("revenue_lag", lag(col("revenue")).over(window_specification))
# Show the DataFrame
df = df.withColumn("revenue_status", (col("revenue") < col("revenue_lag")).cast("int"))
df = df.groupBy("company").agg(sum(col("revenue_status")).alias("sum"))
df = df.filter(col("sum") == 0).drop("sum").show()
