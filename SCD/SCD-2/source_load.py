from pyspark.sql.functions import *
from pyspark.sql.types import *  
from pyspark.sql.window import Window
from delta.tables import DeltaTable

df = spark.read.format('csv') \
          .option("inferSchema", "true") \
          .option("header", "true") \
          .load("/Volumes/databricks_jaspar/bronze/bronze_volume/customers/dim_customers.csv")
df = df.withColumn("cob_dt", col("signup_date"))
df.write.format("Delta").mode("overwrite").partitionBy("cob_dt").saveAsTable("databricks_jaspar.source.dim_customers")