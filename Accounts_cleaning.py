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
from pyspark.sql.window import *
from google.cloud import storage
from datetime import datetime

def load_df_for_table_from_bucket(bucket_name,table_name,ext):
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
            .option("inferSchema", "true") \
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

df=load_df_for_table_from_bucket("bronze-layer-capstone","accounts","csv")


#REMOVE DUPLICATES
df=df.dropDuplicates()

#dropping entire row if null
df= df.na.drop(subset='AccountId')

# +
# Calculate the mode of the "AccountType" column
mode_value = df.groupBy("AccountType").count().sort(col("count").desc()).first()["AccountType"]

# Fill null values with the mode
df = df.na.fill(mode_value, subset=["AccountType"])
# -

df= df.na.fill(0, subset=["Balance"])


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
