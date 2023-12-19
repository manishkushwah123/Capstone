from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from datetime import datetime

def load_csv_for_table_from_bucket(bucket_name,table_name):
        def fetch_last_run_and_update_last_run(bucket_name,table_name):
            last_run=spark.read.format("csv").option("recursiveFileLookup", "true") \
                                             .option("header", "true") \
                                             .option("inferSchema", "true") \
                                             .load(f"gs://{bucket_name}/{table_name}/last_run/*.csv")
            last_run = last_run.withColumn("Last_run_timestamp", current_timestamp())
            last_run.write.format("csv").mode("overwrite").option("header","true").save(f"gs://{bucket_name}/{table_name}/last_run/")

            return last_run.collect()[0][0]
            
        def list_csv_blobs(bucket_name,last_run_time,table_name,csv_files_list=None):
                if csv_files_list is None:
                    csv_files_list=[]
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blobs = bucket.list_blobs()
                for blob in blobs:
                    if blob.updated.replace(tzinfo=None) > last_run and blob.name.startswith(f"{table_name}/") and blob.name.endswith(".csv"):
                        if blob.name.startswith(f"{table_name}/last_run/"):
                            continue  # Skip csv_blobs within "last_run" folder
                        else:
                            csv_files_list.append(f"gs://{bucket_name}/{blob.name}")
                return csv_blobs_list
        def read_csv_to_dataframe(filepath):
            df = spark.read.format("csv") \
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
        for file in list_csv_blobs(bucket_name,fetch_last_run_and_update_last_run(bucket_name,table_name),table_name):
            dataframes.append(read_csv_to_dataframe(file))
        return combine_dataframes_to_single_df(dataframes)
dfs={}
for table in ["customers","transactions","accounts","branches","loans","credits"]:
  dfs[table]=load_csv_for_table_from_bucket('bronze-layer-capstone",table)
  
