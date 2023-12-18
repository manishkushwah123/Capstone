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
# Read data from a CSV file
input_path = 'gs://capstone-g4/branches.csv'
branches_df = spark.read.csv(input_path, header=True, inferSchema=True)

# Show the first few rows of the DataFrame
branches_df.show()

# -

branches_df.printSchema()

branches_df = branches_df.na.drop(subset = 'BranchId')

# Handle missing values
cleaned_branches_data = branches_df.fillna({'Bank_Name': 'Unknown', 'Bank_city': 'Unknown'})

# Handle duplicates
cleaned_branches_data = branches_df.dropDuplicates(["BranchId"])
cleaned_branches_data.show()

cleaned_branches_data1 = cleaned_branches_data.withColumn('Bank_Name', upper('Bank_name'))
cleaned_branches_data1.show()

cleaned_branches_data2 = cleaned_branches_data1.withColumn('Bank_city', initcap('Bank_city'))
cleaned_branches_data2.show()

# Specify the desired file name and path desired
file_name = "gs://capstone-g4/cleaned_data/cleaned_branches_data.csv"
# Write the cleaned DataFrame to a CSV file with the desired file name
cleaned_branches_data2.write.csv(file_name, header=True, mode="append")


