# How many issues are currently open in the avocado-toast repository, and were created on the month of October 2020?
import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from datetime import *
from decimal import Decimal

spark = SparkSession.builder.appName("SparkSessionExample").getOrCreate()

data = [
    (1, 'avocado-toast', 'open', date(2020,10,5), 'Fix the avocado color issue'),
    (2, 'avocado-toast', 'closed', date(2020,10,10), 'Improve toast texture'),
    (3, 'avocado-toast', 'open', date(2020,10,15), 'Add more toppings'),
    (4, 'avocado-toast', 'open', date(2020,9,25), 'Optimize recipe'),
    (5, 'avocado-toast', 'open', date(2020,10,20), 'Adjust serving size'),
    (6, 'other-repo', 'open', date(2020,10,1), 'Non-related issue'),
    (7, 'avocado-toast', 'open', date(2023,10,22), 'Consider gluten-free bread'),
    (8, 'avocado-toast', 'closed', date(2020,10,18), 'New presentation ideas'),
    (9, 'avocado-toast', 'open', date(2022,10,30), 'Recipe for spicy avocado toast'),
    (10, 'avocado-toast', 'open', date(2020,2,11), 'Investigate avocado storage'),
    (11, 'avocado-toast', 'closed', date(2020,10,2), 'Enhance flavor profile'),
    (12, 'avocado-toast', 'open', date(2020,10,25), 'Add nutrition info'),
    (13, 'other-repo', 'open', date(2023,10,15), 'Random issue unrelated'),
    (14, 'other-repo', 'closed', date(2020,10,20), 'Another non-related issue'),
    (15, 'avocado-toast', 'closed', date(2020,11,1), 'Finalized recipe changes'),
]

# Step 3: Schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("repository", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", DateType(), True),
    StructField("title", StringType(), True)
])

# Step 4: Create DataFrame
df = spark.createDataFrame(data, schema)
df = df.filter((year(col("created_at")) == "2020") & (month(col("created_at")) == "10"))
#df.show(truncate = False)

df = df.groupBy("repository","status").agg(count("*").alias("count")).orderBy(col("count").desc())
df.show(truncate=False)