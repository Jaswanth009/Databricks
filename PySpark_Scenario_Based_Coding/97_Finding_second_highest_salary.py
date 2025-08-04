import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Employees DataFrame") \
    .getOrCreate()
    
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Define data
data = [
    (1, 'Amit', 'HR', 50000),
    (2, 'Raj', 'HR', 60000),
    (3, 'Sita', 'HR', 70000),
    (4, 'Karan', 'IT', 80000),
    (5, 'Priya', 'IT', 90000),
    (6, 'Vikram', 'IT', 85000),
    (7, 'Anjali', 'Finance', 75000),
    (8, 'Manoj', 'Finance', 72000),
    (9, 'Ravi', 'Finance', 71000)
]

df = spark.createDataFrame(data=data,schema=schema)
window_specification = Window.partitionBy(col("department")).orderBy(col("salary").desc())
df = df.withColumn("rn",row_number().over(window_specification)).filter(col("rn") == 2)
df.drop("rn")
df.show()