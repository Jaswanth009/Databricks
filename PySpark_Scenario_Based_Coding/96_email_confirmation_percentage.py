import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("confirmation_status", StringType(), True)
])

# Define data
data = [
    (1, 'user1@example.com', '2024-02-01', 'Confirmed'),
    (2, 'user2@example.com', '2024-02-02', 'Confirmed'),
    (3, 'user3@example.com', '2024-02-03', 'Unconfirmed'),
    (4, 'user4@example.com', '2024-02-04', 'Confirmed'),
    (5, 'user5@example.com', '2024-02-05', 'Unconfirmed'),
    (6, 'user6@example.com', '2024-02-06', 'Confirmed'),
    (7, 'user7@example.com', '2024-02-07', 'Unconfirmed'),
    (8, 'user8@example.com', '2024-02-08', 'Confirmed'),
    (9, 'user9@example.com', '2024-02-09', 'Confirmed'),
    (10, 'user10@example.com', '2024-02-10', 'Unconfirmed')
]

df = spark.createDataFrame(data=data,schema=schema)
df_total_count = df.count()
confirmed_count = df.filter(col("confirmation_status") == "Confirmed").count()
unconfirmed_count = df.filter(col("confirmation_status") == "Unconfirmed").count()
print("Confirmed_percentage", (confirmed_count/df_total_count)*100)
print("Unconfirmed_percentage",(unconfirmed_count/df_total_count)*100)