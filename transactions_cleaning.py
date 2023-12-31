from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from pyspark.sql.window import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from google.cloud import storage
from datetime import datetime
spark=SparkSession.builder.appName("capstone").getOrCreate()
def load_df_for_table_from_bucket(bucket_name,table_name,ext,schema):
        timestamp=datetime.utcnow()
        
        def delete_files_from_folder(bucket_name,folder_name):
            from google.cloud import storage
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=folder_name)
            for blob in blobs:
                if blob.name!= folder_name:
                        blob.delete()
                        print(f"Deleted blob: {blob.name}")
        def update_last_run(timestamp):
            delete_files_from_folder(bucket_name,f"{table_name}/last_run/")
            data = [(timestamp,)]
            columns = ["last_run"]
            df = spark.createDataFrame(data, columns)
            df.write.csv(f"gs://{bucket_name}/{table_name}/last_run/", header=True, mode="append")
        def read_last_run():
            last_run=spark.read.format("csv").option("recursiveFileLookup", "true") \
                                             .option("header", "true") \
                                             .option("inferSchema", "true") \
                                             .load(f"gs://{bucket_name}/{table_name}/last_run/*.csv")

            return last_run.collect()[0][0]
            
        
        def list_blobs(bucket_name,last_run_time,table_name,files_list=None):
                if files_list is None:
                    files_list=[]
                client = storage.Client()
                bucket = client.get_bucket(bucket_name)
                blobs = bucket.list_blobs()
                for blob in blobs:
                    if blob.updated.replace(tzinfo=None) > last_run_time and blob.name.startswith(f"{table_name}/") and blob.name.endswith(f".{ext}"):
                        if blob.name.startswith(f"{table_name}/last_run/"):
                            continue  # Skip blobs within "last_run" folder
                        else:
                            files_list.append(f"gs://{bucket_name}/{blob.name}")
                print(files_list)
                return files_list
        def read_to_dataframe(file_path):
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
        for file in list_blobs(bucket_name,read_last_run(),table_name):
            dataframes.append(read_to_dataframe(file))
        update_last_run(timestamp)
        return combine_dataframes_to_single_df(dataframes)

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("AccountID", StringType(), True),
    StructField("TransactionOperation", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("transaction_date", TimestampType(), True),
    StructField("Amount", DoubleType(), True)
])

df = load_df_for_table_from_bucket("bronze-layer-capstone","transactions","csv",schema)

if df is not None:        
        #Checking if there are any null values in the dataframe
        
        null_counts = [df.where(col(c).isNull()).count() for c in df.columns]
        null_counts
        
        # Dropping the columns where there are null values in the following columns
        df_filtered = df.na.drop(subset=["transaction_id","Amount","AccountId"])
        
        # replacing  the null values with unknown
        df_filled = df.fillna("unknown", subset=["TransactionOperation", "transaction_type"])
        
        #converting the string to upper and removing any spaces 
        
        df_transformed = df.withColumn("transaction_type", upper(trim(col("transaction_type"))))
        df_transformed = df_transformed.withColumn("TransactionOperation", upper(trim(col("TransactionOperation"))))
        
        desired_file_path="gs://silver-layer-capstone/transactions/"
        df_transformed.write.csv(desired_file_path,header=True,mode="append")
