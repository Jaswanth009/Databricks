## Percentage of Driver cancelled_rides by reason, Loading it into Gold Layer as driver_cancelled_reason Table
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("Uber Analysis").getOrCreate()

uber_master_df = spark.read.table("databricks_jaspar.uber_silver.uber_master_data")

uber_driver_cancelled_df =  uber_master_df.filter(col("driver_cancellation_reason") != "NULL")
uber_driver_cancelled_count = uber_driver_cancelled_df.count()

driver_cancelled_reason_df = uber_driver_cancelled_df.groupBy("driver_cancellation_reason").agg(count("*").alias("specific_count"))
driver_cancelled_reason_df = driver_cancelled_reason_df.withColumn("effect_percent", round((col("specific_count")/uber_driver_cancelled_count *100),2))
driver_cancelled_reason_df.write.format("delta").mode("overwrite").saveAsTable("databricks_jaspar.uber_gold.driver_cancelled_reason")