from pyspark.sql.functions import *
from delta.tables import DeltaTable

source_df = spark.read.table("databricks_jaspar.source.dim_customers") \
    .withColumn("domain", split(col("email"), "@").getItem(1)) \
    .withColumn("end_dt", lit("9999-12-31"))

target_table = "databricks_jaspar.bronze.customer_data_enr"

if spark.catalog.tableExists(target_table):

    delta_target = DeltaTable.forName(spark, target_table)
    target_df = delta_target.toDF().filter("end_dt = '9999-12-31'")

    # Find records where any attributes have changed
    changed_records = source_df.alias("src") \
        .join(target_df.alias("tgt"), "customer_id", "inner") \
        .filter(
            (col("src.name") != col("tgt.name")) |
            (col("src.location") != col("tgt.location"))
        ) \
        .select("src.*")  # Keep only new records to insert later

    changed_records.show()
    # Update `end_dt` of existing rows that have changed
    records_to_update = changed_records.select("customer_id").distinct()

    records_to_update.show()

    delta_target.alias("tgt").merge(
        records_to_update.alias("updates"),
        "tgt.customer_id = updates.customer_id AND tgt.end_dt = '9999-12-31'"
    ).whenMatchedUpdate(set={"end_dt": "current_date()"}).execute()
    
    # Insert new records (changed ones + brand new)
    final_inserts = source_df.alias("src").join(
        target_df.alias("tgt"),
        on="customer_id",
        how="leftanti"
    ).unionByName(changed_records)

    final_inserts.write.format("delta").mode("append").saveAsTable(target_table)
else:
    # Initial load
    source_df.write.format("delta").partitionBy("cob_dt").saveAsTable(target_table)
