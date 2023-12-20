#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from datetime import datetime

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("AccountID", StringType(), True),
    StructField("TransactionOperation", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("Amount", DoubleType(), True)
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




df = load_df_for_table_from_bucket("bronze-layer-capstone","transactions","csv",schema)



# In[ ]:





# In[ ]:





# In[30]:


#Checking if there are any null values in the dataframe
from pyspark.sql.functions import *
null_counts = [df.where(col(c).isNull()).count() for c in df.columns]
null_counts





# In[32]:


# Dropping the columns where there are null values in the following columns
df_filtered = df.na.drop(subset=["transaction_id","Amount","AccountId"])


# In[33]:


# replacing  the null values with unknown
df_filled = df.fillna("unknown", subset=["TransactionOperation", "transaction_type"])


# In[45]:


#converting the string to upper and removing any spaces 

df_transformed = df.withColumn("transaction_type", upper(trim(col("transaction_type"))))
df_transformed = df_transformed.withColumn("TransactionOperation", upper(trim(col("TransactionOperation"))))






