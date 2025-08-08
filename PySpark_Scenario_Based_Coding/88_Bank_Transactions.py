#1. Bank of Ireland has instructed to identify unauthorized transactions during December 2022.
#A transaction is considered invalid if it falls outside the bank's regular hours: Monday to Friday 09:00 - 16:00, closed on Saturdays, Sundays, and Irish Public Holidays (25th and #26th December).

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import *
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

transactions_data = [
    (1,  "2022-12-01 10:30:00"),
    (2,  "2022-12-01 17:30:00"),
    (3,  "2022-12-03 10:30:00"),
    (4,  "2022-12-05 15:30:00"),
    (5,  "2022-12-25 11:00:00"),
    (6,  "2022-12-26 09:00:00"),
    (7,  "2022-12-07 09:30:00"),
    (8,  "2022-12-09 15:00:00"),
    (9,  "2022-12-12 08:00:00"),
    (10, "2022-12-15 14:45:00"),
    (11, "2022-12-23 10:00:00"),
    (12, "2022-12-25 16:00:00"),
    (13, "2022-12-26 10:30:00"),
    (14, "2024-11-15 13:30:00"),
    (15, "2021-12-20 08:30:00"),
    (16, "2022-01-21 10:30:00") 
]

# Step 3: Define schema
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("time_stamp", StringType(), True)
])

df = spark.createDataFrame(transactions_data,schema)
df = df.withColumn("time_stamp", to_timestamp(col("time_stamp"),"yyyy-MM-dd HH:mm:ss")) \
       .withColumn("date", to_date(col("time_stamp"))) \
       .withColumn("day", dayofweek(col("date"))) \
       .withColumn("time", date_format(col("time_stamp"), "HH:mm:ss"))\
       .withColumn("year", year(col("date")))
df = df.withColumn("Transaction_Type_Desc", when((col("day") == 1) | (col("day") == 7), lit("Invalid, Weekend")) \
    .when(col("date") == ("2022-12-25"), lit("Invalid, Public Holiday"))
    .when(col("date") == ("2022-12-26"), lit("Invalid, Public Holiday"))
    .when(col("time") < lit("09:00:00"), lit("Invalid, Before Opening"))
    .when(col("time") > lit("17:00:00"), lit("Invalid, After Closing"))
    .when(col("year") > lit("2022"), lit("Invalid, Incorrect Year"))
    .otherwise(lit("Valid")))
#split_col = split(col("Transaction_Type_Desc"),",")
def get_status(desc):
    if desc:
        return desc.split(",")[0].strip()
    return None
def get_reason(desc):
    if desc:
        return desc.split(",")[1].strip()
    return None

get_status_udf = udf(get_status, StringType())
get_reason_rdf = udf(get_reason, StringType())


df = df.withColumn("Transaction_Status", get_status_udf(col("Transaction_Type_Desc")))\
       .withColumn("Failure_Reason", when(size(split_col) > 1, trim(split_col.getItem(1))).otherwise(lit(None)))

df.show()