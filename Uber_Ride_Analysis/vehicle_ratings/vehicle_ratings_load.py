## Average driver and customer ratings for each vehicle type, Loading it into Gold Layer as vehicle_ratings Table
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("Uber Analysis").getOrCreate()

uber_master_df = spark.read.table("databricks_jaspar.uber_silver.uber_master_data")
vehicle_ratings = uber_master_df.groupBy("vehicle_type").agg(avg("driver_ratings").alias("avg_driver_ratings"), \
                                avg("customer_rating").alias("avg_customer_ratings"))
vehicle_ratings = vehicle_ratings.withColumn("avg_driver_ratings", round(col("avg_driver_ratings"), 2)) \
                                .withColumn("avg_customer_ratings", round(col("avg_customer_ratings"), 2))
vehicle_ratings.write.format("delta").mode("overwrite").saveAsTable("databricks_jaspar.uber_gold.vehicle_ratings")