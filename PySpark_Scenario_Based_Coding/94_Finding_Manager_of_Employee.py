###### You are given with dataset, write a sql query to find the names of manager who manages more than 4 employees #####

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

employee_data = [
    (1, 'John', 'HR', None),
    (2, 'Bob', 'HR', 1),
    (3, 'Olivia', 'HR', 1),
    (4, 'Emma', 'Finance', None),
    (5, 'Sophia', 'HR', 1),
    (6, 'Mason', 'Finance', 4),
    (7, 'Ethan', 'HR', 1),
    (8, 'Ava', 'HR', 1),
    (9, 'Lucas', 'HR', 1),
    (10, 'Isabella', 'Finance', 4),
    (11, 'Harper', 'Finance', 4),
    (12, 'Hemla', 'HR', 3),
    (13, 'Aisha', 'HR', 2),
    (14, 'Himani', 'HR', 2),
    (15, 'Lily', 'HR', 2)
]

employee_columns = ['id', 'name', 'department', 'managerId']

employee_df = spark.createDataFrame(employee_data, employee_columns)

employee_df.show()

df = employee_df.groupBy("managerId").agg(count(col("id")).alias("count")).filter(col("count") > 4).orderBy(col("count").desc())
final_df = df.join(employee_df, df.managerId == employee_df.id, "inner").select(df["count"], employee_df["name"])
final_df.show()
