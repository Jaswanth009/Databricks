## Percentage of Customer cancelled_rides by reason, Loading it into Gold Layer as driver_cancelled_reason Table
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("Uber Analysis").getOrCreate()

uber_master_df = spark.read.table("databricks_jaspar.uber_silver.uber_master_data")

uber_customer_cancelled_df =  uber_master_df.filter(col("reason_for_cancelling_by_customer") != "NULL")
uber_customer_cancelled_count = uber_customer_cancelled_df.count()
customer_cancelled_reason_df = uber_customer_cancelled_df.groupBy("reason_for_cancelling_by_customer").agg(count("*").alias("specific_count"))
customer_cancelled_reason_df = customer_cancelled_reason_df.withColumn("effect_percent", round((col("specific_count")/uber_customer_cancelled_count *100),2))
customer_cancelled_reason_df.write.format("delta").mode("overwrite").saveAsTable("databricks_jaspar.uber_gold.customer_cancelled_reason")