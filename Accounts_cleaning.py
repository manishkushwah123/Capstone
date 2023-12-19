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

# incremental_data_load.py loads data to dfs dictionary
# fetching directly from that dfs
customers=dfs["customers"]

# Show the contents of the DataFrame
customers.show()
# -

df.printSchema()

#REMOVE DUPLICATES
df=df.dropDuplicates()

#dropping entire row if null
df= df.na.drop(subset='AccountId')

df= df.join(customers, col("CustomerID") == col("Customer_id"), how="left")\
       .select('AccountId', 'Customer_id','AccountType', 'Balance', 'last_kyc_updated', 'branch_id', 'account_created')

df.show()

# +
# Calculate the mode of the "AccountType" column
mode_value = df.groupBy("AccountType").count().sort(col("count").desc()).first()["AccountType"]

# Fill null values with the mode
df = df.na.fill(mode_value, subset=["AccountType"])
# -

df= df.na.fill(0, subset=["Balance"])

# +
# Specify the path to your CSV file
csv_file_path = "gs://capstondata/branches.csv"

# Read the CSV file into a DataFrame with inferred schema
branches = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the contents of the DataFrame
branches.show()
# -

df= df.join(branches, col("BranchId") == col("branch_id"), how="left")\
       .select('AccountId', 'Customer_id','AccountType', 'Balance', 'last_kyc_updated', 'branch_id', 'account_created')

df.show()

df = df.withColumn("Balance",round( df["Balance"].cast(DecimalType(10,2)),2))

df.show()


df = df.withColumn("last_kyc_updated",date_format(to_date(col("last_kyc_updated"),"M/d/yyyy"),"MM/dd/yyyy"))

df = df.withColumn("account_created",date_format(to_date(col("account_created"),"M/d/yyyy"),"MM/dd/yyyy"))

df.show()

df.printSchema()

# +
# Specify the desired file name and path
desired_file_name = "gs://silver-layer-capstone/accounts/"

# Write the cleaned DataFrame to a CSV file with the desired file name
df.write.csv(desired_file_name, header=True, mode="append")
