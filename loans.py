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

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# +
# Specify the path to your CSV file
csv_file_path = "gs://capstondata/loans_partition_1.csv"

# Read the CSV file into a DataFrame with inferred schema
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the contents of the DataFrame
df.show()
# -

#REMOVE DUPLICATES and remove nulls
df=df.dropDuplicates()
df= df.na.drop(subset='Loan_id')

df.printSchema()

print("\nNull values in the DataFrame:")
for column in df.columns:
    count_null = df.filter(col(column).isNull()).count()
    print(f"{column}: {count_null} null values")


df= df.na.fill(0, subset=["Current Loan Amount"])

df.select("Term").show()

# +
# Calculate the mode of the "Term" column
mode_value = df.groupBy("Term").count().sort(col("count").desc()).first()["Term"]

# Fill null values with the mode
df = df.na.fill(mode_value, subset=["Term"])
# -

df.select("Annual Income").show()

# +
# Calculate the mean 
mean_value = df.select(mean("Annual Income")).collect()[0][0]

# Fill null values with the mean
df = df.na.fill(mean_value, subset=["Annual Income"])
# -

df.select("Home Ownership", "Purpose","Monthly Debt").show()

# +
# Calculate the mode of the "Home Ownership" column
mode_value = df.groupBy("Home Ownership").count().sort(col("count").desc()).first()["Home Ownership"]

# Fill null values with the mode
df = df.na.fill(mode_value, subset=["Home Ownership"])
# -

df= df.na.fill("Other", subset=["Purpose"])

df= df.na.fill(0, subset=["Monthly Debt"])

# +
# Specify the desired file name and path
desired_file_name = "gs://capstondata/silver_layer/loans.csv"

# Write the cleaned DataFrame to a CSV file with the desired file name
df.write.csv(desired_file_name, header=True, mode="append")
# -


