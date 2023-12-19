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
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Read data from a CSV file
input_path = 'gs://capstone-g4/customers.csv'
customers_df = spark.read.csv(input_path, header=True, inferSchema=True)

# Show the first few rows of the DataFrame
customers_df.show()
# -

customers_df.printSchema()

cleaned_data = customers_df.withColumn('dob', to_date('dob', 'M/d/yyyy'))

cleaned_data.printSchema()

cleaned_data.show()

# Handle missing values
cleaned_customer_data = cleaned_data.fillna({'Full_Name': 'Unknown', 'Customer_Email': 'Unknown','Customer_Phone': 0})

# Handle duplicates
cleaned_customer_data = cleaned_data.dropDuplicates(["Customer_id"])

# Replace invalid phone numbers with null values
cleaned_customer_data = cleaned_data.withColumn('Customer_Phone', when(length(col('Customer_Phone')) == 10, col('Customer_Phone')).otherwise(None))
cleaned_customer_data.show()

cleaned_customer_data1 = cleaned_customer_data.withColumn('Full_Name', initcap('Full_Name'))
cleaned_customer_data1.show()

cleaned_customer_data2 = cleaned_customer_data1.withColumn("email_check", regexp_extract(col("Customer_Email"), r'^\S+@\S+\.\S+', 0))


cleaned_customer_data2 = cleaned_customer_data2.withColumn("Customer_Email", when(col("email_check") != "", col("Customer_Email")).otherwise(None))


cleaned_customer_data2.show()

# +
# Drop the temporary 'email_check' column
cleaned_customer_data2 = cleaned_customer_data1.drop("email_check")

# Display the DataFrame with null values for invalid email addresses
cleaned_customer_data2.show()

# +
# Specify the desired file name and path
desired_file_name = "gs://capstone-g4/cleaned_data/customers.csv"

# Write the cleaned DataFrame to a CSV file with the desired file name
cleaned_customer_data2.write.csv(desired_file_name, header=True, mode="append")
# -


