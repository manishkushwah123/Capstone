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
import pyspark
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from datetime import datetime

schema = StructType([
    StructField("Customer_id", StringType(), True),
    StructField("Full_Name", StringType(), True),
    StructField("Customer_Email", StringType(), True),
    StructField("dob", DateType(), True),  # Convert to DateType
    StructField("Customer_Phone", IntegerType(), True)  # Convert to IntegerType
])

def load_df_for_table_from_bucket(bucket_name,table_name,ext,schema):
        def fetch_last_run_and_update_last_run(bucket_name,table_name):
            last_run=spark.read.format("csv").option("recursiveFileLookup", "true") \
                                             .option("header", "true") \
                                             .option("inferSchema", "true") \
                                             .load(f"gs://{bucket_name}/{table_name}/last_run/*.csv")
            last_run = last_run.withColumn("Last_run_timestamp", current_timestamp())
            last_run.write.format("csv").mode("overwrite").option("header","true").save(f"gs://{bucket_name}/{table_name}/last_run/")

            return last_run.collect()[0][0]
            
        def list_blobs(bucket_name,last_run_time,table_name,files_list=None):
                if files_list is None:
                    files_list=[]
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blobs = bucket.list_blobs()
                for blob in blobs:
                    if blob.updated.replace(tzinfo=None) > last_run and blob.name.startswith(f"{table_name}/") and blob.name.endswith(f".{ext}"):
                        if blob.name.startswith(f"{table_name}/last_run/"):
                            continue  # Skip blobs within "last_run" folder
                        else:
                            files_list.append(f"gs://{bucket_name}/{blob.name}")
                return files_list
        def read_to_dataframe(filepath,ext):
            df = spark.read.format(ext) \
            .option("header", "true") \
            .schema(schema)\
            .load(file_path)
            return df
        def combine_dataframes_to_single_df(dataframes):
            combined_df = None
            for df in dataframes:
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.union(df)
            return combined_df
        dataframes=[]
        for file in list_blobs(bucket_name,fetch_last_run_and_update_last_run(bucket_name,table_name),table_name):
            dataframes.append(read_to_dataframe(file))
        return combine_dataframes_to_single_df(dataframes)




customers_df = load_df_for_table_from_bucket("bronze-layer-capstone","customers","csv",schema)

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
desired_file_name = "gs://silver-layer-capstone/customers/"

# Write the cleaned DataFrame to a CSV file with the desired file name
cleaned_customer_data2.write.csv(desired_file_name, header=True, mode="append")
# -


