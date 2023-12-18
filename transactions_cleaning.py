# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: PySpark (Local)
#     language: python
#     name: pyspark
# ---

# +
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
        .getOrCreate()

# Read data from a CSV file
file_path = "branches.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show()
# -

df.dtypes

# +
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
schema = StructType([
    StructField("BranchId", StringType(), True),
    StructField("Bank_Name", StringType(), True),
    StructField("Branch_Registration", StringType(), True),
    StructField("Bank_city", StringType(), True)
   
])
df = spark.read.csv(file_path, header=True, schema=schema)

# Show the DataFrame
df.show()
# -

df_filtered = df.na.drop(subset=["BranchId","Branch_Registration","Bank_Name","Bank_city"])

from pyspark.sql.functions import *
df_transformed = df.withColumn("Bank_Name", upper(trim(col("Bank_Name"))))
df_transformed = df_transformed.withColumn("Bank_city", upper(trim(col("Bank_city"))))

df_transformed.show()

# +
desired_file_name = "gs://capstondata/silver_layer/transaction.csv"
 
# Write the cleaned DataFrame to a CSV file with the desired file name
df_transformed.write.csv(desired_file_name, header=True, mode="append")
# -


