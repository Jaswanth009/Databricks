## Cleaning the raw data and loading it into the silver layer as uber master data
import pyspark
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("Uber Analysis").getOrCreate()
source_df = spark.read.format("csv") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load("/Volumes/databricks_jaspar/uber_bronze/uber_raw_data/ncr_ride_bookings.csv")

def clean_column_name(name):
    # Replace invalid characters with underscore
    cleaned = re.sub(r'[ ,;{}()\n\t=]', '_', name)
    # Remove repeated underscores and lowercase it (optional)
    cleaned = re.sub(r'_+', '_', cleaned).strip('_').lower()
    return cleaned

cleaned_df = source_df.select([
    col(c).alias(clean_column_name(c)) for c in source_df.columns
])

cleaned_df = cleaned_df.withColumn("driver_ratings", when(col("driver_ratings") == "null", lit(None)).otherwise(col("driver_ratings"))) \
                    .withColumn("customer_rating", when(col("customer_rating") == "null", lit(None)).otherwise(col("customer_rating"))) \
                    .withColumn("reason_for_cancelling_by_customer", when(col("reason_for_cancelling_by_customer") == "null", lit(None)).otherwise(col("reason_for_cancelling_by_customer"))) \
                    .withColumn("driver_cancellation_reason", when(col("driver_cancellation_reason") == "null", lit(None)).otherwise(col("driver_cancellation_reason")))

cleaned_df.write.format("delta").mode("overwrite").saveAsTable("databricks_jaspar.uber_silver.uber_master_data")